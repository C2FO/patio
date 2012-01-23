var vows = require('vows'),
        assert = require('assert'),
        patio = require("index"),
        comb = require("comb"),
        hitch = comb.hitch,
        helper = require("../data/timestampPlugin/timestamp.updateOnCreate.models");

var ret = module.exports = exports = new comb.Promise();

helper.loadModels().then(function() {
    Employee = patio.getModel("employee");
    var suite = vows.describe("TimeStampPlugin updateOnCreate");

    suite.addBatch({

        "when creating an employee" : {
            topic : function() {
                Employee.save({
                    firstname : "doug",
                    lastname : "martin",
                    midinitial : null,
                    gender : "M",
                    street : "1 nowhere st.",
                    city : "NOWHERE"
                }).then(hitch(this, function(e) {
                    //force reload
                    e.reload().then(hitch(this, "callback", null));
                }));
            },

            "the updated time stamp should be set" : function(topic) {
                assert.isNotNull(topic.updated);
                assert.isNotNull(topic.created);
                assert.deepEqual(topic.updated, topic.created);
                assert.instanceOf(topic.updated, patio.SQL.DateTime);
                assert.instanceOf(topic.created, patio.SQL.DateTime);
            }
        }
    });

    suite.run({reporter : require("vows").reporter.spec}, function() {
        helper.dropModels().then(comb.hitch(ret, "callback"), comb.hitch(ret, "errback"));
    });
});