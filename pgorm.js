'use strict';

var _ = require('lodash'),
  path = require('path'),
  log = require('tracer').colorConsole(),
  colors = require("colors"),
  queryLog = require('tracer').colorConsole({
    methods:["info"],
    filters:[colors.grey]
    }),
  pg = require('pg'),
  moment=require('moment'),
  pgTypes = require('pg').types,

  queryFormat = require('pg-format');

pgTypes.setTypeParser(20, function(value) {
    return parseInt(value)
})

let pool, gConfig

class JoinGraph {
    constructor(rootTableName) {
        // log.info("creating JoinGraph with table="+rootTableName)
        // console.dir(tableNameMap)
        this.rootAlias = rootTableName
        this.graph = this.createNode(this.rootAlias)
        // {a_a: {alias:"a_a", next:...}}
        this.tables = new Set([rootTableName])
        this.tableAliasCount = {}
    }

    createNode(alias) {
        return { alias: alias, next:{} }
    }

    hasJoins() {
        console.dir(this.tables)
        return this.tables.size > 1
    }

    addChain(nodes) {
        console.log("addChain "+nodes)
        console.log(JSON.stringify(this.graph, null, 2))
        let lastTableAlias
        let current = this.graph // root
        for (let n of nodes) {
            console.log("nc")
            console.dir(current)
            let tableName = tableNameMap[n]
            if (current.next[tableName]) {
                // same branch already exists
            } else {
                // new branch
                if (this.tables.has(tableName)) {
                    // table name already used elsewhere; need table alias
                    this.tableAliasCount[tableName]++
                    let alias = tableName + this.tableAliasCount[tableName]
                    current.next[tableName] = this.createNode(alias)
                    lastTableAlias = alias
                } else {
                    // no need for alias
                    this.tableAliasCount[tableName] = 0
                    current.next[tableName] = this.createNode(tableName)
                    // console.log("upd gr")
                    // console.log(JSON.stringify(this.graph, null, 2))
                    // console.log("aftr c")
                    // console.log(JSON.stringify(current, null, 2))
                    lastTableAlias = tableName
                }
            }
            current = current.next[tableName]
            console.dir(current)
            this.tables.add(tableName)
            log.info("lta=" + lastTableAlias)
        }

        return lastTableAlias
    }

    // Depth first traverse to build join table alias pairs
    getJoinPairs() {
        // console.log("getting pairs: graph: ")
        // console.dir(this.graph)
        let stack = [[null, this.graph]]
        let out = []
        while (stack.length > 0) {
            console.dir(stack)
            let [parentAlias, c] = stack.pop()
            if (parentAlias) {
                out.push([parentAlias, c.alias])
            }
            if (c.next) {
                for (let k in c.next) {
                    stack.push([c.alias, c.next[k]])
                }
            }
        }
        return out
    }

    getJoinClause() {
        let pairs=this.getJoinPairs()
        let joined = new Set()
        let clauses = ""
        for (let [t1, t2] of pairs) {
            let field = fkFieldMap[t1][t2]
            if (field) {
                clauses += `INNER JOIN "${t2}" ON ("${t1}"."${field}"="${t2}"."id") `
            } else {
                field = fkFieldMap[t2][t1]
                if (!field) {
                    throw new Error(`No relationship found between ${t1} and ${t2}`)
                }
                clauses += `INNER JOIN "${t2}" ON ("${t1}"."id"="${t2}"."${field}") `
            }
        }
        return `"${this.graph.alias}" ${clauses}`
    }
}

module.exports.JoinGraph = JoinGraph

let SPECIAL_TOKENS = new Set(["isnull", "in", "not in", "lt", "lte", "gt", "gte", "eq", "ne"])
class QueryClause {
    constructor(data) {
        this.data = data
    }

    getQueryAndValues(joinGraph, valueIndex) {
        var conditions = []
        var values = []
        for (var key in this.data) {
            var value = this.data[key]

            let tokens = key.split("__")
            let last = tokens.pop()
            // log.info("ddx last="+last)
            let special
            if (SPECIAL_TOKENS.has(last)) {
                // log.info("is special")
                special = last
                last = tokens.pop()
                key = last
            }

            let tableAlias = joinGraph.rootAlias
            if (tokens.length > 0) {
                // has remaining tokens; join required
                tableAlias = joinGraph.addChain(tokens)
                key = last
            }

            if (special == "isnull") {
                if (value) {
                    conditions.push(`"${tableAlias}"."${last}" IS NULL`)
                } else {
                    conditions.push(`"${tableAlias}"."${last}" IS NOT NULL`)
                }
            }
            else if (special == "in" || special == "not_in") {
                let op = (special == "in") ? "IN" : "NOT IN"
                let indices = []
                for (let v of value) {
                    indices.push(`$${valueIndex++}`)
                    values.push(v)
                }
                indices = indices.join(",")
                conditions.push(`"${key}" ${op} (${indices})`)
            }
            else {
                // log.info("table alias=" + tableAlias)
                // in and not_in for handling special case of one element
                let opMap = {gt: ">", lt:"<", gte:">=", lte:"<=", eq:"=", ne:"!="}
                let op = special ? opMap[special] : "="
                if (!op) {
                    throw new Error("Unknown operator")
                }

                conditions.push(`"${tableAlias}"."${key}"${op}$${valueIndex}`)

                if (value instanceof Model) {
                    value = value.id
                }

                values.push(value)
                valueIndex++
            }
        }

        let compound = conditions.join(" AND ")
        if (!compound) {
            return [null, values]
        }

        return [`(${compound})`, values];
    }
}

function asArray(t) {
    if (!t) {
        return []
    }

    if (!Array.isArray(t)) {
        t = [t]
    }
    return t
}

class Query {
    constructor(spec) {
        this.clauses = []
        this.modifiers={}
        if (spec) {
            this.clauses.push(new QueryClause(spec))
        }
    }

    or(spec) {
        if (spec) {
            this.clauses.push(new QueryClause(spec))    
        }
        
        return this
    }

    limit(limit) {
        this.modifiers["limit"] = limit
        return this
    }

    lock(stat) {
        this.modifiers["lock"] = stat
        return this
    }

    getUpdateQueryAndValues(modelClass, data) {
        log.info("getUpdateQuery table="+modelClass.tableName + " data=")
        console.dir(data)
        var values = []
        var dataClauses = []
        var specClauses = []
        var valueIndex = 1

        for (var key in data) {
            if (!modelClass.columnDefinitions[key]) {
                continue
            }

            if (modelClass.columnDefinitions[key].noUpdate) {
                continue
            }

            dataClauses.push(`"${key}"=$${valueIndex}`)
            let value = data[key]
            if (value instanceof Model) {
                value = value.id
            }

            values.push(value)
            valueIndex++
        }
        var dataQuery = dataClauses.join(",")

        let [specQuery, specValues] = this.getQueryAndValues(modelClass.tableName, valueIndex, true)
        values = values.concat(specValues)

        var query = `UPDATE ${modelClass.tableName} SET ${dataQuery} ${specQuery}`
        return [query, values]
    }

    getQueryAndValues(tableName, valueIndex, isUpdate) {
        // log.info("getQueryAndValues:"+tableName)
        if (undefined===valueIndex) {
            valueIndex = 1
        }
        var values=[]
        var clauseQueries=[]
        let joinGraph = new JoinGraph(tableName)

        for (let clause of this.clauses) {
            let [clauseQuery, clauseValues] = clause.getQueryAndValues(joinGraph, valueIndex)
            if (!clauseQuery) {
                continue
            }

            // rm.jslog.info("pushing q" + clauseQuery)
            clauseQueries.push(clauseQuery)
            values.push(...clauseValues)
            valueIndex+=clauseValues.length
        }

        let selectClause = this.modifiers.lock ? "SELECT" : "SELECT DISTINCT"
        let operation = (this.type == "delete") ? "DELETE" :`${selectClause} ${tableName}.*`
        if (isUpdate) {
            operation = `SELECT ${tableName}.id`
        }

        let textSearch = this.modifiers.textSearch
        if (textSearch) {
            let targetField = textSearch.field
            let targetValue = textSearch.value.split(/\s+/)
            targetValue = targetValue.join("|")
            let field = `ts_rank(to_tsvector('english', ${targetField}), '${targetValue}') AS tempScore`
            operation += `, ${field}`

            this.modifiers.orderBy = this.modifiers.orderBy || []
            this.modifiers.orderBy.unshift("-tempScore")
        }

        if (this.modifiers.sum || this.modifiers.count) {
            if (!this.modifiers.groupBy) {
                throw new Error("sum without group by")
            }

            let fields = asArray(this.modifiers.groupBy).slice()
            let sumFields = asArray(this.modifiers.sum)
            let countFields = asArray(this.modifiers.count)           
            console.log("count fields") 
            console.dir(countFields)

            for (let sumField of sumFields) {
                fields.push(`SUM("${sumField}") AS "${sumField}"`)
            }

            for (let countField of countFields) {
                fields.push(`COUNT("${countField}") AS "${countField}"`)
            }

            fields = fields.join(", ")
            operation = `SELECT ${fields}`
        }

        var query = `${operation} from ${joinGraph.getJoinClause()} `
        if (clauseQueries.length > 0) {
            var compound=clauseQueries.join (" OR ")
            query += `WHERE ${compound} `
        }

        if (this.modifiers.orderBy) {
            let fields = asArray(this.modifiers.orderBy)
            let cs = []
            for (let f of fields) {
                if (f[0]=="-") {
                    f = f.substr(1)
                    // BUGBUG: sucks
                    if (f == "tempScore") {
                        cs.push(`${f} DESC`)
                    } else {
                        cs.push(`"${f}" DESC`)
                    }
                } else {
                    cs.push(`"${f}" ASC`)
                }
            }
            cs = cs.join(", ")
            query += `ORDER BY ${cs}`
        }

        if (this.modifiers.groupBy) {
            let t = asArray(this.modifiers.groupBy).join(", ")
            log.info(`grouping by` +t)
            console.dir(this.modifiers)
            query += `GROUP BY ${t}`
        }

        if (this.modifiers.lock) {
            query+="FOR UPDATE "
        }

        if (this.modifiers.limit) {
            query+= ` LIMIT ${this.modifiers.limit} `
        }

        if (isUpdate) {
            if (joinGraph.hasJoins()) {
                log.info("has join; update")
                query = `WHERE "${tableName}"."id" IN (${query})`
            } else {
                log.info("no join; update")
                query = `WHERE ${compound} `
            }
        }

        if (textSearch) {
            query = `SELECT * from (${query}) subquery WHERE tempScore > 0`
        }

        return [query, values]
    }
}

async function querySimple(query,values,transactionClient){
    queryLog.info(`running query simple:\n${query}\n${values}`)
    const client = transactionClient || await pool.connect()
    try {
      var result= await client.query(query,values)
      // log.info(`query success`)
      return result
    } catch(e) {
      log.error(`ERROR executing simple query:\n${query}\n${e}`)
      console.trace()
      throw e
    } finally {
        if (!transactionClient) {
            client.release()      
        }      
    }
}

async function tx(callback) {
      const client = await pool.connect()
      try {
          await client.query('BEGIN')
          try {
              var result=await callback(client)
            
              await client.query('COMMIT')
              return result
          } catch(e) {
            log.error(e)
            console.trace()
            client.query('ROLLBACK')
            throw e
          }
      } finally {        
        client.release()
      }
}

let models=new Set()
module.exports.models=models
let modelNameMap = {}
let tableNameMap = {}
// 2D array source table -> dest table
let fkFieldMap = {}

class Model {
    constructor(data) {
        // log.info(`constructing model`)
        // console.dir(data)
        Object.assign(this, data)

        this.generateFields()
    }

    generateFields(isUpdate) {
        var modelClass = this.constructor
        // console.log("generating keys: "+modelClass.name)

        for (let timeKey of modelClass.timeKeys) {
            let columnDef = modelClass.columnDefinitions[timeKey]

            if (!this[timeKey] && (columnDef.autoNow || columnDef.autoNowAdd)) {
                this[timeKey] = new Date()
            }

            if (isUpdate && columnDef.autoNow) {
                this[timeKey] = new Date()
            }

            if (this[timeKey] && columnDef.generateFromNow) {
                let fnKey = timeKey + "FromNow"
                this[fnKey] = moment(this[timeKey]).fromNow()
            }
        }
    }

    async get(foreignKey, transactionClient) {
        // log.info("ddx get fk="+foreignKey)
        var modelClass=this.constructor
        var fkClassName = modelClass.columnDefinitions[foreignKey].target
        let fkClass = modelNameMap[fkClassName]
        // log.info("ddx "+fkClass)
        return fkClass.objects.get({"id": this[foreignKey]}, transactionClient)
    }

    async save(transactionClient) {
        var modelClass=this.constructor

        if (this.id) {
            this.generateFields(true)
            return modelClass.objects.update({"id":this.id}, this, transactionClient)
        } else {
            this.generateFields()
            // TODO; update current id
            return modelClass.objects.create(this, transactionClient)    
        }        
    }

    async update(data, transactionClient) {
        if (data.id && data.id!=this.id) {
            throw new Error("ID mismatch")
        }

        Object.assign(this, data)
        return this.save(transactionClient)
    }

    async delete(transactionClient) {
        var modelClass=this.constructor

        if (this.id) {
            return modelClass.objects.delete({"id":this.id}, transactionClient)
        }
    }

    flatten() {
        var modelClass = this.constructor
        for (let fk of modelClass.foreignKeys) {
            if (this[fk] && this[fk] instanceof Model) {
                this[fk] = this[fk].id
                if (!this[fk]) {
                    throw new Error("Unexpected missing id in fk")
                }
            }
        }
    }
}

module.exports.Model = Model

function getCreateColumnQuery(modelClass, name, def) {
    var tokens = []  
    tokens.push(`"${name}"`)
    let postConstraints=[]
    let indices=[]

    let TYPE_MAP = {
        "string": "character",
        "text": "text",
        "long": "bigint",
        "boolean": "boolean",
        "integer": "integer",
        "foreignKey": "integer",
        "datetime": "TIMESTAMPTZ",
        "date": "date",
        "decimal":"decimal"
    }

    if (!TYPE_MAP[def.type]) {
        throw "unknown type: " + def.type
    }

    tokens.push(TYPE_MAP[def.type])

    if ("string" == def.type) {
        tokens.push(`varying(${def.maxLength})`)
    }

    if (def.type=="foreignKey"||def.index) {
        let indexName = `${modelClass.tableName}_${name}`
        indices.push({ name: indexName, column: name, table: modelClass.tableName})
    }

    if (!def.optional) {
        tokens.push("NOT NULL")
    }

    if (undefined!=def.default) {
        tokens.push(`DEFAULT '${def.default}'`)
    } else if (def.autoNow || def.autoNowAdd) {
        tokens.push("default current_timestamp")
    }
    else {
        tokens.push(`DEFAULT NULL`)
    }

    if (def.unique) {
        tokens.push("UNIQUE")
    }

    if ("foreignKey" == def.type) {
        var targetModule=def.targetModule || modelClass.moduleName

        if (!modelNameMap[def.target]) {
            console.dir(def)
            log.error("Can't find model for foreign key:"+def.target)
        }

        let targetTableName = modelNameMap[def.target].tableName
        let deleteSpec= (def.deleteCascade) ? " ON DELETE CASCADE" : " ON DELETE SET NULL"
        let constraint=`alter table ${modelClass.tableName} ADD CONSTRAINT ${name}_fk FOREIGN KEY ("${name}") REFERENCES ${targetTableName} ${deleteSpec} DEFERRABLE INITIALLY DEFERRED`
        
        // Track foreign key info
        if (!fkFieldMap[modelClass.tableName]) {
            fkFieldMap[modelClass.tableName] = {}
        }
        fkFieldMap[modelClass.tableName][targetTableName] = name

        postConstraints.push(constraint)
        // tokens.push(`REFERENCES ${targetModule}_${def.target}`)

        // tokens.push("DEFERRABLE INITIALLY DEFERRED")
    }

    let createQuery= tokens.join(" ")
    return [createQuery, postConstraints,indices]
}

function getCreateTableQuery(modelClass) {
    let structure = modelClass.structure
    let queries = ["id serial PRIMARY KEY"]
    let allPostConstraints=[]
    let allIndices = []

   if (modelClass.constraints) {
        for (let c of modelClass.constraints) {
            allPostConstraints.push(`alter table ${modelClass.tableName} ${c}`)
        }
    }

    for (var i = 0; i < structure.length; i++) {
        var [name,def] = structure[i]  
        let [createQuery, postConstraints,indices] =getCreateColumnQuery(modelClass, name, def)
    
        allPostConstraints.push(...postConstraints)
        allIndices.push(...indices)

        queries.push(createQuery)
    }  

    queries = queries.join(",\n")

    let q = `CREATE TABLE IF NOT EXISTS ${modelClass.tableName} (
        ${queries}
    )`

    return [q, allPostConstraints, allIndices]
}

async function createTables() {
    log.info("ddx1 CREATING TABLES")
    let queries = []
    let allPostConstraints=[]
    let allIndices=[]

    let or = await tx(async client => {
        for (let model of models) {
            log.info("ddx creating table for model="+model.tableName)
            let [mq, postConstraints, indices] = getCreateTableQuery(model)
            allPostConstraints.push(...postConstraints)
            allIndices.push(...indices)

             log.info("ddx executing query:\n" + mq)
             let res = await client.query(mq);
             // log.info("read")
             log.info("ddx:" + res)
        };

        return null
    });

    // apply foreign key references last until all tables have been created
    for (let postConstraint of allPostConstraints) {    
        log.info(`creating constraint:\n${postConstraint}`)
        try {
            await querySimple(postConstraint)    
        } catch (error) {
            if (42710==error.code) {
                // ignore duplicate constraint                    
            } else {
                log.error(error.stack)
                throw error
            }                
        }        
    }

    for (let index of allIndices){
        let q =`CREATE INDEX IF NOT EXISTS "${index.name}" ON "${index.table}" ("${index.column}")`
        try {
            await querySimple(q)
        } catch (error) {
            log.error(error.stack)
            throw error
        }
    }

  log.info("ddx FINISHED CREATING TABLES")
  return null
}

// BUGBUG: move this outside
function loadModels() {
  // Globbing model files
  let config = require(path.resolve('./config/config'))
  config.files.server.models.forEach(function (modelPath) {
    log.info(`loading ${modelPath}`)
    require(path.resolve(modelPath));
  });
  log.info("finish loading models")
};

async function updateModelInstances(spec,data, transactionClient){
    if (spec.constructor === Array) {
        log.info("converting spec to ids")
        let v = []
        for (let s of spec) {
            v.push(s.id)
        }
        spec = { "id__in": v }
    }

    let q = new Query(spec)
    let [query, values] = q.getUpdateQueryAndValues(this, data)
    var result=await querySimple(query,values,transactionClient)
    return true
}    

async function truncate(transactionClient){
    var q = `TRUNCATE ${this.tableName} CASCADE`
    var result=await querySimple(q, transactionClient)
    return result
}

async function aggregateModelInstances(agg, queryOrSpec, transactionClient) {
    let t
    if (queryOrSpec instanceof Query) {
        t = queryOrSpec.modifiers
    } else {
        t = queryOrSpec
    }
    Object.assign(t, agg)

    return filterModelInstances.call(this, queryOrSpec, transactionClient)    
}

async function filterModelInstances(spec,transactionClient){
    var keywords=["limit","lock","groupBy","sum", "orderBy", "count", "textSearch"]
    var keyValues={}
    var query 

    if (spec instanceof Query) {
        query = spec
    } else {
        query = new Query()
        for (var key in spec) {
            if (keywords.includes(key)) {
                query.modifiers[key]=spec[key]
                delete spec[key]
            }
        }

        query.or(spec)
    }

    let [q, values] = query.getQueryAndValues(this.tableName)
    var result=await querySimple(q,values,transactionClient)
    // console.dir(query)

    if (query.modifiers.groupBy) {
        return result.rows
    }

    return convertModel(this,result.rows,(1==query.modifiers.limit))
}

async function deleteModelInstances(spec,transactionClient){    
    var query 

    if (spec instanceof Query) {
        query = spec        
    } else {
        query = new Query() 
        query.or(spec)
    }

    query.type="delete"

    let [q, values] = query.getQueryAndValues(this.tableName)
    var result=await querySimple(q,values,transactionClient)
    // console.dir(query)
    return null
}


async function getModelInstance(queryOrSpec,transactionClient){
    if (queryOrSpec instanceof Query) {
        queryOrSpec.modifiers.limit=1
    } else {
        queryOrSpec.limit=1
    }

    return filterModelInstances.call(this, queryOrSpec, transactionClient)
}

async function getRandomModelInstance(spec,transactionClient){    
    var choices=await filterModelInstances.call(this, spec,transactionClient)
    var index = Math.floor(Math.random() * choices.length);
    return choices[index]
}


function convertModel(modelClass,entries,isSingle) {    
    // log.info(`converting models`+isSingle)
    // console.dir(entries)
    for (var i = 0; i < entries.length; i++) {
        var row=entries[i]
        var out=new modelClass(row)
        // Object.assign(out,row)
        entries[i] = out
    }

    if (isSingle) {
       entries=entries[0]
    }

    // log.info(`returning entries`)
    // console.dir(entries)    
    return entries
}

// TODO: optimize if entries are already model type
async function createModelInstances(rawEntries, transactionClient){
    log.info(`dd3 creating model instances of type ${this.tableName}: tx=`+ transactionClient)
    var isSingle=false

    var keywords=["ignoreConflict","updateConflict"]
    var keyValues={}

    if (!Array.isArray(rawEntries)) {
        isSingle=true

        for (var key in rawEntries) {
            if (keywords.includes(key)) {
                keyValues[key]=rawEntries[key]
            }
        }

        rawEntries=[rawEntries]
    }

    let entries = []
    // for raw dicts, convert to objects to run Model constructor
    // console.dir(rawEntries)
    for (let raw of rawEntries) {
        delete raw["id"]
        if (! (raw instanceof Model)) {
            // log.info("dd3 convert model")
            entries.push(new this(raw))
        } else {
            // log.info("dd3 no convert model: " + (raw instanceof Model))
            // console.dir(raw)
            entries.push(raw)
        }
    }

    let queryColumnNames = null
    var values=[]
    for (var i = 0; i < entries.length; i++) {
      var entry=entries[i]
      var row=[]
      let rowColumnNames = []
      for (var j = 0; j < this.columns.length; j++) {
        var column=this.columns[j]
        var value = entry[column]
      
        if (value === undefined) {
            if (this.columnDefinitions[column].default) {
                value = this.columnDefinitions[column].default
            } else {
                continue
            }
        }

        rowColumnNames.push(column)

          if (value instanceof Model) {            
            value=value.id
        }
        
        row.push(value)  
      };

      values.push(row)

      // TODO: how to detect mismatch rows?
      if (!queryColumnNames) {
          queryColumnNames = rowColumnNames;
      }
    };
    // console.dir(entries)
    // console.dir(values)
    var queryColumns=[]
    for (var i = 0; i < queryColumnNames.length; i++) {
        queryColumns.push(`"${queryColumnNames[i]}"`)
    }
    queryColumns=queryColumns.join(",")

    let conflictQuery=""
    if (keyValues.ignoreConflict) {
        conflictQuery="ON CONFLICT DO NOTHING"
    } else if (keyValues.updateConflict) {
        conflictQuery="ON CONFLICT DO UPDATE"
    }
    var query=queryFormat(`INSERT INTO ${this.tableName} (${queryColumns}) VALUES %L ${conflictQuery} RETURNING id`, values)

    var result=await querySimple(query,null,transactionClient)
    // log.info(" result=" + result)
    // console.dir(result.rows)

    for (var i = 0; i < result.rows.length; i++) {
        let modelId=result.rows[i]
        entries[i].id=modelId.id
        entries[i].flatten()
    }

    if (isSingle) {
        entries=entries[0]
    }

    return entries
    // return convertModel(this, result.rows, isSingle)
}

module.exports.registerModel = function(modelClass, moduleName){
  // modelClass.prototype = Object.create(Model.prototype);
  // modelClass.prototype.constructor = modelClass;
  
  modelClass.moduleName=moduleName
  modelClass.tableName=moduleName+"_"+modelClass.name.toLowerCase()
  modelClass.columns=[]
  modelClass.foreignKeys = []
    modelClass.timeKeys = []
  modelClass.columnDefinitions={}

  console.dir(modelClass.structure)
  for (var i = 0; i < modelClass.structure.length; i++) {
    var column=modelClass.structure[i][0]
    var def = modelClass.structure[i][1]
    modelClass.columns.push(column)
    modelClass.columnDefinitions[column]=def
    if (def.type=="foreignKey") {
        modelClass.foreignKeys.push(column)
    } else if (def.type == "datetime" || def.type == "date") {
        modelClass.timeKeys.push(column)
    }

    // modelClass.columnTypes[column]=modelClass.structure[i][1]["type"]
  };

    modelClass.objects={}

    modelClass.objects.create=createModelInstances.bind(modelClass)
    modelClass.objects.get=getModelInstance.bind(modelClass)
    modelClass.objects.getRandom=getRandomModelInstance.bind(modelClass)
    modelClass.objects.filter=filterModelInstances.bind(modelClass)
    modelClass.objects.delete=deleteModelInstances.bind(modelClass)
    modelClass.objects.update=updateModelInstances.bind(modelClass)
    modelClass.objects.aggregate=aggregateModelInstances.bind(modelClass)
    modelClass.objects.truncate=truncate.bind(modelClass)

    log.info(`registering model ${modelClass.name}`)
    // log.info(`columns ${modelClass.columns}`)
    models.add(modelClass)
    tableNameMap[modelClass.name.charAt(0).toLowerCase() + modelClass.name.slice(1)] = modelClass.tableName
    modelNameMap[modelClass.name] = modelClass
}

module.exports.setConfig = async function (config) {
    gConfig = config
    log.info("-------------------------------")
    log.info("PG-ORM CONFIG")
    log.info("-------------------------------")
    
    console.dir(gConfig)
}

module.exports.init = async function (loadModels) {  
log.info(`init db`)  
    log.info("creating pg pool");
    console.dir(gConfig)

    pool = new pg.Pool(gConfig)

    if (loadModels) {
    loadModels()
    }

  return createTables()
};

module.exports.truncateAll = async function () {
    for (let model of models) {
        log.info(`truncating: ` + model.tableName)
        await model.objects.truncate()
    }
}

module.exports.dropdb = async function () {        
  log.info(`dropping database`)
  pool = new pg.Pool(gConfig)

  return  tx(async client => {
        await client.query('DROP SCHEMA public CASCADE; CREATE SCHEMA public');    
        log.info("dropped db")
    });    
};

module.exports.model = function (name) {    
  let m = modelNameMap[name];
  if (!m) {
    throw new Error("no model with name: " + name)
  }
  return m
};

module.exports.tx = tx

module.exports.Query = function (spec) {
    return new Query(spec)
}

//module.exports.disconnect = function (cb) {
//  mongoose.connection.db
//    .close(function (err) {
//      console.info(chalk.yellow('Disconnected from MongoDB.'));
//      return cb(err);
//    });
//};