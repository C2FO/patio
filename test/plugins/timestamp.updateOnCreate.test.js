var it = require('it'),
    assert = require('assert'),
    patio = require("index"),
    comb = require("comb"),
    hitch = comb.hitch,
    helper = require("../data/timestampPlugin.helper.js");


it.describe("Timestamp updateOnCreate", function (it) {

    var emp, Employee;
    it.beforeAll(function () {
        Employee = patio.addModel("employee", {
            plugins: [patio.plugins.TimeStampPlugin]
        });
        Employee.timestamp({updateOnCreate: true});
        return helper.createSchemaAndSync();
    });

    it.beforeEach(function () {
        return Employee.remove().chain(function () {
            return Employee.save({
                firstname: "doug",
                lastname: "martin",
                midinitial: null,
                gender: "M",
                street: "1 nowhere st.",
                city: "NOWHERE"
            }).chain(function (e) {
                    emp = e;
                });
        });
    });

    it.should("set created column", function () {
        assert.instanceOf(emp.created, patio.SQL.DateTime);
        assert.instanceOf(emp.updated, patio.SQL.DateTime);
    });

    it.should("set created column on insertSql", function () {
        assert.isNotNull(emp.insertSql.match(/["|`]updated["|`], *["|`]created["|`]\)/));
    });

    it.should("set updated or updateSql", function () {
        assert.isNotNull(emp.updateSql.match(/["|`]updated["|`]\s*=/));
    });

    it.should("set updated column", function (next) {
        setTimeout(function () {
            emp.firstname = "dave";
            emp.save().chain(function () {
                //force reload
                assert.isNotNull(emp.updated);
                assert.instanceOf(emp.updated, patio.SQL.DateTime);
                assert.notDeepEqual(emp.updated, emp.created);
            }).classic(next);
        }, 1000);
    });

    it.afterAll(function () {
        return helper.dropModels();
    });
});