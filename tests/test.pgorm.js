'use strict';

var should = require('should'),
    chai = require('chai'),
    expect = chai.expect,
    path = require('path'),
    pgo = require("../pgorm.js"),
    Model=pgo.Model,
    JoinGraph = pgo.JoinGraph,
    log = require('tracer').colorConsole();

chai.use(require("chai-like"))
chai.use(require("chai-things"))

let moduleName = "testmapper"
class Tag extends Model {
    constructor(data) {
        super(data)
    }
}
Tag.structure = [
    ["name", { "type": "string", "maxLength": 120 }],
]

class Blog extends Model {
    constructor(data) {
        super(data)
    }
}
Blog.structure = [
    ["name", { "type": "string", "maxLength": 120 }],
    ["tag", { "type": "foreignKey", "target": "Tag", "optional": true }],
]

class Post extends Model {
    constructor(data) {
        super(data)
    }
}
Post.structure = [
    ["name", { "type": "string", "maxLength": 120 }],
    ["blog", { "type": "foreignKey", "target": "Blog" }],
    ["tag", { "type": "foreignKey", "target": "Tag", "optional": true }],
    ["time", { "type": "datetime", "autoNowAdd": true }]
]

var blogTag1,
    blogTag2,
    postTag1,
    postTag2,
    blog1,
    blog2,
    post11,
    post12,
    post21,
    post22

async function seedData() {
    blogTag1 = new Tag({ name: "blog_tag1" })
    await blogTag1.save()
    blogTag2 = new Tag({ name: "blog_tag2" })
    await blogTag2.save()

    postTag1 = new Tag({ name: "post_tag1" })
    await postTag1.save()
    postTag2 = new Tag({ name: "post_tag2" })
    await postTag2.save()

    blog1 = new Blog({ name: "blog1", tag:blogTag1 })
    await blog1.save()
    blog2 = new Blog({ name: "blog2", tag: blogTag2 })
    await blog2.save()

    post11 = new Post({ name: "post11", tag: postTag1, blog:blog1})
    await post11.save()
    post12 = new Post({ name: "post12", tag: postTag1, blog: blog1 })
    await post12.save()
    post21 = new Post({ name: "post21", tag: postTag2, blog: blog2})
    await post21.save()
    post22 = new Post({ name: "post22", tag: postTag2, blog: blog2})
    await post22.save()
}

describe('test mapper', function () {
    before(async function () {
        var config = require("./config.json")
        pgo.setConfig(config)
        
        await pgo.dropdb()

        pgo.registerModel(Tag, moduleName)
        pgo.registerModel(Blog, moduleName)
        pgo.registerModel(Post, moduleName)

        await pgo.init(true)

        return seedData()
    });

    beforeEach(async function () {
        Tag.objects.truncate()
        Blog.objects.truncate()
        Post.objects.truncate()
    });

    
    it('should track simple join chains', async function () {
        let jg = new JoinGraph("blog");
        jg.addChain(["post", "tag"])
        log.info(jg.getJoinPairs())
        let pairs = jg.getJoinPairs()
        expect(pairs).to.contain.something.like(['testmapper_blog', 'testmapper_post'])
        expect(pairs).to.contain.something.like(['testmapper_post', 'testmapper_tag'] )
    });

    it('should track conflicting join chains', async function () {
        let jg = new JoinGraph("blog");
        jg.addChain(["post", "tag"])
        jg.addChain(["tag"])
        log.info(jg.getJoinPairs())
        let pairs = jg.getJoinPairs()
        log.info(jg.getJoinClause())
        expect(pairs).to.contain.something.like(['testmapper_blog', 'testmapper_tag1'])
        expect(pairs).to.contain.something.like(['testmapper_blog', 'testmapper_post'])
        expect(pairs).to.contain.something.like(['testmapper_post', 'testmapper_tag'])
    });

    it('should merge subset chain into superset', async function () {
        let jg = new JoinGraph("blog");
        jg.addChain(["post", "tag"])
        jg.addChain(["post"])
        log.info(jg.getJoinPairs())
        let pairs = jg.getJoinPairs()
        expect(pairs).to.contain.something.like(['testmapper_blog', 'testmapper_post'])
        expect(pairs).to.contain.something.like(['testmapper_post', 'testmapper_tag'])
    });

    it('should track conflicting join chains', async function () {
        let jg = new JoinGraph("blog");
        jg.addChain(["post", "tag"])
        jg.addChain(["tag"])
        log.info(jg.getJoinPairs())
        let pairs = jg.getJoinPairs()
        log.info(jg.getJoinClause())
        expect(pairs).to.contain.something.like(['testmapper_blog', 'testmapper_tag1'])
        expect(pairs).to.contain.something.like(['testmapper_blog', 'testmapper_post'])
        expect(pairs).to.contain.something.like(['testmapper_post', 'testmapper_tag'])
    });

    it('should handle simple lookups', async function () {
        let o = await Blog.objects.filter({ "name": "blog1" })
        console.dir(o)
        expect(o.length).to.equal(1)
        expect(o[0].name).to.equal(blog1.name)
    });

    it('should handle simple joins', async function () {
        log.info("post1")
        console.dir(post11)
        let o = await Blog.objects.filter({"post__name":post11.name})
        console.dir(o)
        expect(o.length).to.equal(1)
        expect(o[0].name).to.equal(blog1.name)
    });
    
    it('should handle nested joins', async function () {
        let o = await Blog.objects.filter({ "post__tag__name": "post_tag1" })
        console.dir(o)
        expect(o.length).to.equal(1)
        expect(o[0].name).to.equal(blog1.name)
    });

    it('handle simple updates', async function () {
        let o = await Blog.objects.update({ "name": "blog1"}, {"name": "blog11" })
        
        let b = await Blog.objects.get({id: blog1.id})

        expect(b.name).to.equal("blog11")
    });

    it('handle simple updates that select with joins', async function () {
        let o = await Blog.objects.update({ "tag__name": "blog_tag1" }, { "name": "blog12" })

        let b = await Blog.objects.get({ id: blog1.id })

        expect(b.name).to.equal("blog12")
    });

    it.only('should compare dates properly', async function () {
        let today = new Date()
        let yesterday = new Date(today);
        yesterday.setDate(today.getDate() - 1)
        let lastWeek = new Date(today);
        lastWeek.setDate(today.getDate() - 7)

        let blog3 = new Blog({ name: "blog3" })
        await blog3.save()

        let postToday = new Post({ name: "posted today", time: today, blog: blog3})
        await postToday.save()

        let postLastWeek = new Post({ name: "posted last week", time: lastWeek, blog: blog3 })
        await postLastWeek.save()

        let r = await Post.objects.filter({ "time__lt": yesterday, blog: blog3})
        expect(r.length).to.equal(1)
        expect(r[0].name).to.equal("posted last week")

        r = await Post.objects.filter({ "time__gt": yesterday, blog: blog3 })
        expect(r.length).to.equal(1)
        expect(r[0].name).to.equal("posted today")
    });
});
