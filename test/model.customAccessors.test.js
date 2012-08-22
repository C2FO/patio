var it = require('it'),
    assert = require('assert'),
    helper = require("./data/model.helper.js"),
    patio = require("index"),
    comb = require("comb-proxy");

it.describe("A model with custom accessors", function (it) {

    it.should("support custom getters", function(next) {
        var CustomGettersEmployee = patio.addModel("CustomGettersEmployee", {
            instance: {
                _getLastname : function(value) {
                    return value.toUpperCase();
                },
                _getFirstname : function(value) {
                    return value.toLowerCase();
                },
            },
            static: {
                init: function() {
                    this._super(arguments);
                    this.__tableName = 'employee';
                }
            }
        });
        comb.serial([
            helper.createSchemaAndSync,
            function() {
                return new CustomGettersEmployee({
                    firstname: "Leia",
                    lastname: "Skywalker"
                }).save();
            },
            function() {
                return CustomGettersEmployee.first().then(function (emp) {
                    // Check getters
                    assert.equal(emp.firstname, "leia");
                    assert.equal(emp.lastname, "SKYWALKER");
                    // And the actual (raw) value
                    assert.equal(emp.__values['firstname'], "Leia");
                    assert.equal(emp.__values['lastname'], "Skywalker");
                });
            },
            next
        ]);
    });
    
    it.afterAll(function () {
        return helper.dropModels();
    });
    
    it.should("support custom setters", function(next) {
        var CustomSettersEmployee = patio.addModel("CustomSettersEmployee", {
            instance: {
                _setLastname : function(value) {
                    var arrLastname = value.split("");
                    return arrLastname.join("#");
                },
                _setFirstname : function(value) {
                    var arrFirstname = value.split("");
                    return arrFirstname.join("_");
                }
            },
            static: {
                init: function() {
                    this._super(arguments);
                    this.__tableName = 'employee';
                }
            }
        });
        comb.serial([
            helper.createSchemaAndSync,
            function() {
                return new CustomSettersEmployee({
                    firstname: "Obi-Wan",
                    lastname: "Kenobi"
                }).save();
            },
            function() {
                return CustomSettersEmployee.first().then(function (emp) {
                    assert.equal(emp.firstname, "O_b_i_-_W_a_n");
                    assert.equal(emp.lastname, "K#e#n#o#b#i");
                    // Raw values must be the same, as transformation was made within setters
                    assert.equal(emp.__values['firstname'], "O_b_i_-_W_a_n");
                    assert.equal(emp.__values['lastname'], "K#e#n#o#b#i");
                });
            },
            next
        ]);
    });
});


