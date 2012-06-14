var it = require('it'),
        assert = require('assert'),
        patio = require("index"),
        comb = require("comb"),
        hitch = comb.hitch,
        helper = require("../data/timestampPlugin.helper.js");

var ret = module.exports = new comb.Promise();

var Employee = patio.addModel("employee", {
    plugins:[patio.plugins.TimeStampPlugin]
});
Employee.timestamp();

it.describe("Timestamp custom columns", function (it) {

    var emp;
    it.beforeAll(function () {
        return helper.createSchemaAndSync();
    });

    it.beforeEach(function (next) {
        Employee.remove().then(function () {
            Employee.save({
                firstname:"doug",
                lastname:"martin",
                midinitial:null,
                gender:"M",
                street:"1 nowhere st.",
                city:"NOWHERE"
            }).then(function (e) {
                    emp = e;
                    next();
                }, next);
        }, next);
    });

    it.should("set created column", function () {
        assert.isNull(emp.updated);
        assert.isNotNull(emp.created);
        assert.instanceOf(emp.created, patio.SQL.DateTime);
    });

    it.should("set updated column", function (next) {
        setTimeout(function () {
            emp.firstname = "dave";
            emp.save().then(function () {
                //force reload
                assert.isNotNull(emp.updated);
                assert.instanceOf(emp.updated, patio.SQL.DateTime);
                assert.notDeepEqual(emp.updated, emp.createdAt);
                next();
            });
        }, 1000);
    });

    it.afterAll(function () {
        return helper.dropModels();
    });

    it.run();

});