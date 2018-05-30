'use strict';

var should = require('should'),
    chai = require('chai'),
    expect = chai.expect,
    path = require('path'),
    pgo = require("../pgorm.js"),
    Model = pgo.Model,
    JoinGraph = pgo.JoinGraph,
    log = require('tracer').colorConsole();

chai.use(require("chai-like"))
chai.use(require("chai-things"))

let moduleName = "testmapper"
class Gig extends Model {
    constructor(data) {
        super(data)
    }
}
Gig.structure = [
    ["name", { "type": "string", "maxLength": 120 }],
]

async function seedData() {
    let names = ["Design bootstrap 3", "design Bootstrap 4", "logo design", "create an amazing logo"]
    for (let name of names) {
        let g = new Gig({name: name})
        await g.save()
    }
}

describe('test text search', function () {
    before(async function () {
        var config = require("./config.json")
        pgo.setConfig(config)

        await pgo.dropdb()

        pgo.registerModel(Gig, moduleName)

        await pgo.init(true)

        return seedData()
    });

    beforeEach(async function () {
    });


    it('should prefer exact match', async function () {
        let r = await Gig.objects.filter({"textSearch": {"field":"name", "value": "bootstrap 3"}})
        expect(r.length).to.equal(2)
        expect(r[0].name).to.to.match(/bootstrap 3/i)
    });
});
