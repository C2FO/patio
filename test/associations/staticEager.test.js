"use strict";

var it = require('it'),
    assert = require('assert'),
    helper = require("../data/oneToOne.helper.js"),
    patio = require("../../lib"),
    comb = require("comb");

var gender = ["M", "F"];

it.describe("patio.Model static eager method", function (it) {
    var Works, Employee, DB;
    it.beforeAll(function () {
        Works = patio.addModel("works");
        Works.manyToOne("employee", {fetchType: Works.fetchType.LAZY});
        Employee = patio.addModel("employee");
        Employee.oneToMany("works", {fetchType: Employee.fetchType.LAZY});
        DB = null;
        return helper.createSchemaAndSync(true).chain(function(db){
            DB = db;
        });
    });


    it.should("have associations", function () {
        assert.deepEqual(Employee.associations, ["works"]);
        assert.deepEqual(Works.associations, ["employee"]);
        var emp = new Employee();
        var work = new Works();
        assert.deepEqual(emp.associations, ["works"]);
        assert.deepEqual(work.associations, ["employee"]);
    });

    it.describe("load associations", function (it) {

        it.beforeEach(function () {
            return comb
                .when([
                    Employee.remove(),
                    Works.remove()
                ])
                .chain(function () {
                    return new Employee({
                        lastName: "last" + 1,
                        firstName: "first" + 1,
                        midInitial: "m",
                        gender: gender[1 % 2],
                        street: "Street " + 1,
                        city: "City " + 1,
                        works: [{
                            companyName: "Google",
                            salary: 100000
                        },{
                            companyName: "Alphabet",
                            salary: 100000
                        }]
                    }).save();
                }).chain(function () {
                    return new Employee({
                        lastName: "Skywalker",
                        firstName: "Luke",
                        midInitial: "m",
                        gender: gender[1 % 2],
                        street: "Street " + 1,
                        city: "City " + 1,
                        works: {
                            companyName: "C2FO",
                            salary: 200000
                        }
                    }).save();
                });

        });

        it.should("when querying", function () {
            return comb
                .when([Employee.eager('works').one(), Works.eager('employee').one()])
                .chain(function (res) {
                    var emp = res[0], work = res[1];
                    var empWorks = emp.works, worksEmp = work.employee;
                    assert(emp.works[0].id, work.id);
                    assert(work.employee.id, emp.id);
                });
        });

        it.should("when querying with filtering", function () {
            return Employee.eager('works').filter({lastName: "Skywalker"}).one()
                .chain(function (emp) {
                    assert(emp.id, emp.works[0].employeeId);
                });
        });

        it.should("and load other eager queries", function () {
            return Employee.eager('works').eager({
                who: function(emp) {
                    return Employee.findById(emp.id);
                }
            }).one().chain(function (emp) {
                assert(emp.id, emp.works[0].employeeId);
                assert(emp.id, emp.who.id);
            }).chain(function() {
                // run same queries back to back
                // make sure eager is not being cached across model instances
                return Employee.eager('works').eager({
                    you: function(emp) {
                        return Employee.findById(emp.id);
                    }
                }).one()
                    .chain(function (emp) {
                        assert(emp.id, emp.works[0].employeeId);
                        assert.isUndefined(emp.who);
                        assert(emp.id, emp.you.id);
                    });
            });
        });

    });

    it.describe("dataset loading", function (it) {

        it.beforeEach(function () {
            return comb
                .when([
                    Employee.remove(),
                    Works.remove()
                ])
                .chain(function () {
                    return new Employee({
                        lastName: "last" + 1,
                        firstName: "first" + 1,
                        midInitial: "m",
                        gender: gender[1 % 2],
                        street: "Street " + 1,
                        city: "City " + 1,
                        works: [{
                            companyName: "Google",
                            salary: 100000
                        },{
                            companyName: "Alphabet",
                            salary: 100000
                        }]
                    }).save();
                }).chain(function () {
                    return new Employee({
                        lastName: "Skywalker",
                        firstName: "Luke",
                        midInitial: "m",
                        gender: gender[1 % 2],
                        street: "Street " + 1,
                        city: "City " + 1,
                        works: {
                            companyName: "C2FO",
                            salary: 200000
                        }
                    }).save();
                });

        });

        it.should("and load other eager queries", function () {
            return DB.from('employee').filter({lastName: 'Skywalker'})
                .eager({
                    who: function(emp) {
                        return DB.from('works').filter({employeeId: emp.id}).one();
                    }
                }).one()
                    .chain(function (emp) {
                        assert(emp.lastName, 'Skywalker');
                        assert(emp.who.companyName, 'C2FO');
                        assert(emp.id, emp.who.employeeId);
                    });
        });

    });

    it.afterAll(function () {
        return helper.dropModels();
    });
});
