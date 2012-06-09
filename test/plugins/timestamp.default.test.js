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
Employee.timestamp();

helper.createSchemaAndSync().then(function() {
    var Employee = patio.getModel("employee");
    var suite = vows.describe("TimeStampPlugin default");
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
                    e.reload().then(hitch(this, "callback", null), hitch(this, "callback"));
                }), hitch(this, "callback"));
            },

            "the updatedAt time stamp should not be set" : function(topic) {
                assert.isNull(topic.updated);
            },

            "the createdAt time stamp should be set" : function(topic) {
                assert.isNotNull(topic.created);
                assert.instanceOf(topic.created, patio.SQL.DateTime);
            },

            "when updating an employee" : {
                topic : function(e) {
                    //setTimeout to ensure new timeout
                    setTimeout(hitch(this, function() {
                        e.firstname = "dave";
                        e.save().then(hitch(this, function(e) {
                            //force reload
                            e.reload().then(hitch(this, "callback", null), hitch(this, "callback"));
                        }), hitch(this, "callback"));
                    }), 1000);
                },

                "the updated time stamp should be set" : function(topic) {
                    assert.isNotNull(topic.updated);
                    assert.instanceOf(topic.updated, patio.SQL.DateTime);
                    assert.notDeepEqual(topic.updated, topic.created);
                }
            }
        }
    });

    suite.run({reporter : require("vows").reporter.spec}, function() {
        helper.dropModels().then(comb.hitch(ret, "callback"), comb.hitch(ret, "errback"));
    });
});
