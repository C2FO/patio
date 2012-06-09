var vows = require('vows'),
        assert = require('assert'),
        patio = require("index"),
        comb = require("comb"),
        hitch = comb.hitch,
        helper = require("../data/timestampPlugin.helper.js");

var ret = module.exports = new comb.Promise();

var Employee = patio.addModel("employee", {
    plugins:[patio.plugins.TimeStampPlugin]
});
Employee.timestamp({updateOnCreate : true});

helper.createSchemaAndSync().then(function() {
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