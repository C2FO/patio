"use strict";

var it = require('it'),
    assert = require('assert'),
    helper = require("../../data/oneToOne.helper.js"),
    patio = require("../../../lib"),
    comb = require("comb");

var gender = ["M", "F"];

it.describe("patio.Model oneToOne lazy", function (it) {

    var Works, Employee;
    it.beforeAll(function () {
        Works = patio.addModel("works").manyToOne("employee");
        Employee = patio.addModel("employee").oneToOne("works");
        return helper.createSchemaAndSync(true);
    });


    it.should("have associations", function () {
        assert.deepEqual(Employee.associations, ["works"]);
        assert.deepEqual(Works.associations, ["employee"]);
        var emp = new Employee();
        var work = new Works();
        assert.deepEqual(emp.associations, ["works"]);
        assert.deepEqual(work.associations, ["employee"]);
    });

    it.describe("create a new model with association", function (it) {

        it.beforeAll(function () {
            return comb.when([Employee.remove(), Works.remove()]);
        });


        it.should("save nested models when using new", function () {
            var employee = new Employee({
                lastName: "last" + 1,
                firstName: "first" + 1,
                midInitial: "m",
                gender: gender[1 % 2],
                street: "Street " + 1,
                city: "City " + 1,
                works: {
                    companyName: "Google",
                    salary: 100000
                }
            });
            return employee.save()
                .chain(function () {
                    return employee.works;
                })
                .chain(function (works) {
                    assert.equal(works.companyName, "Google");
                    assert.equal(works.salary, 100000);
                });
        });
    });

    it.context(function (it) {

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
                        works: {
                            companyName: "Google",
                            salary: 100000
                        }
                    }).save();
                });
        });

        it.should("not load associations when querying", function () {
            return comb.when([Employee.one(), Works.one()])
                .chain(function (res) {
                    var emp = res[0], work = res[1];
                    var workPromise = emp.works, empPromise = work.employee;
                    assert.isPromiseLike(workPromise);
                    assert.isPromiseLike(empPromise);
                    return comb.when([empPromise, workPromise])
                        .chain(function (res) {
                            var emp = res[0], work = res[1];
                            assert.instanceOf(emp, Employee);
                            assert.instanceOf(work, Works);
                        });
                });
        });

        it.should("allow the removing of associations", function () {
            return Employee.one()
                .chain(function (emp) {
                    emp.works = null;
                    return emp.save();
                })
                .chain(function (emp) {
                    return emp.works;
                })
                .chain(function (works) {
                    assert.isNull(works);
                    return Works.one();
                })
                .chain(function (work) {
                    assert.isNotNull(work);
                    return work.employee;
                })
                .chain(function (emp) {
                    assert.isNull(emp);
                });
        });

    });

    it.context(function () {
        it.beforeEach(function () {
            return comb.when([Works.remove(), Employee.remove()]);
        });

        it.should("allow the setting of associations", function () {
            var emp = new Employee({
                lastName: "last" + 1,
                firstName: "first" + 1,
                midInitial: "m",
                gender: gender[1 % 2],
                street: "Street " + 1,
                city: "City " + 1
            });
            return emp.save()
                .chain(function () {
                    return emp.works;
                })
                .chain(function (works) {
                    assert.isNull(works);
                    emp.works = {
                        companyName: "Google",
                        salary: 100000
                    };
                    return emp.save();
                })
                .chain(function () {
                    return emp.works;
                })
                .chain(function (works) {
                    assert.instanceOf(works, Works);
                });
        });

        it.should("not delete association when deleting the reciprocal side", function () {
            var e = new Employee({
                lastName: "last" + 1,
                firstName: "first" + 1,
                midInitial: "m",
                gender: gender[1 % 2],
                street: "Street " + 1,
                city: "City " + 1,
                works: {
                    companyName: "Google",
                    salary: 100000
                }
            });
            return e.save()
                .chain(function () {
                    return e.remove();
                })
                .chain(function () {
                    return comb.when([Employee.all(), Works.all()]);
                })
                .chain(function (res) {
                    assert.lengthOf(res[0], 0);
                    assert.lengthOf(res[1], 1);
                });
        });

    });

    it.afterAll(function () {
        return helper.dropModels();
    });
});