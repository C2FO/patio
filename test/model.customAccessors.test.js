var it = require('it'),
    assert = require('assert'),
    helper = require("./data/model.helper.js"),
    patio = require("index"),
    comb = require("comb-proxy");


it.describe("A model with custom accessors", function (it) {

    it.beforeAll(function () {
        return helper.createSchemaAndSync();
    });

    var CustomSettersEmployee, CustomGettersEmployee;

    it.beforeAll(function () {
        var db = patio.defaultDatabase;
        CustomSettersEmployee = patio.addModel(db.from("employee"), {
            instance:{
                _setLastname:function (value) {
                    var arrLastname = value.split("");
                    return arrLastname.join("#");
                },
                _setFirstname:function (value) {
                    var arrFirstname = value.split("");
                    return arrFirstname.join("_");
                }
            }
        });

        CustomGettersEmployee = patio.addModel(db.from("employee"), {
            instance:{
                _getLastname:function (value) {
                    return value.toUpperCase();
                },
                _getFirstname:function (value) {
                    return value.toLowerCase();
                }
            }
        });

        return patio.syncModels();
    });

    it.beforeEach(function () {
        return patio.defaultDatabase.from("employee").remove();
    });


    it.should("support custom getters", function (next) {

        comb.serial([
            function () {
                //console.log("HELLO")
                return new CustomGettersEmployee({
                    firstname:"Leia",
                    lastname:"Skywalker",
                    street : "Street",
                    city : "City"
                }).save();
            },
            function () {
                return CustomGettersEmployee.first().then(function (emp) {
                    // Check getters
                    //console.log(emp);
                    assert.equal(emp.firstname, "leia");
                    assert.equal(emp.lastname, "SKYWALKER");
                    // And the actual (raw) value
                    assert.equal(emp.__values['firstname'], "Leia");
                    assert.equal(emp.__values['lastname'], "Skywalker");
                });
            }
        ]).classic(next);
    });

    it.should("support custom setters", function (next) {

        comb.serial([
            function () {
                return new CustomSettersEmployee({
                    firstname:"Obi-Wan",
                    lastname:"Kenobi",
                    street : "Street",
                    city : "City"
                }).save();
            },
            function () {
                return CustomSettersEmployee.first().then(function (emp) {
                    assert.equal(emp.firstname, "O_b_i_-_W_a_n");
                    assert.equal(emp.lastname, "K#e#n#o#b#i");
                    // Raw values must be the same, as transformation was made within setters
                    assert.equal(emp.__values['firstname'], "O_b_i_-_W_a_n");
                    assert.equal(emp.__values['lastname'], "K#e#n#o#b#i");
                });
            }
        ]).classic(next);
    });

    it.afterAll(function () {
        return helper.dropModels();
    });

});


