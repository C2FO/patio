"use strict";

var it = require('it'),
    assert = require('assert'),
    helper = require("../../data/manyToOne.helper.js"),
    patio = require("../../../lib"),
    comb = require("comb");

var gender = ["M", "F"];

it.describe("patio.Model manyToOne with a string for key", function (it) {

    var Company, Employee;
    it.beforeAll(function () {
        Company = patio.addModel("company").oneToMany("employees", {key: "companyId"});
        Employee = patio.addModel("employee").manyToOne("company", {key: "companyId"});
        return helper.createSchemaAndSync(true);
    });

    it.should("have associations", function () {
        assert.deepEqual(Employee.associations, ["company"]);
        assert.deepEqual(Company.associations, ["employees"]);
        var emp = new Employee();
        var company = new Company();
        assert.deepEqual(emp.associations, ["company"]);
        assert.deepEqual(company.associations, ["employees"]);
    });


    it.describe("saving a model with one to many", function (it) {

        it.beforeAll(function () {
            return comb.when([
                Company.remove(),
                Employee.remove()
            ]);
        });

        it.should("it should save the associations", function () {
            var c1 = new Company({
                companyName: "Google",
                employees: [
                    {
                        lastName: "last" + 1,
                        firstName: "first" + 1,
                        midInitial: "m",
                        gender: gender[1 % 2],
                        street: "Street " + 1,
                        city: "City " + 1
                    },
                    {
                        lastName: "last" + 2,
                        firstName: "first" + 2,
                        midInitial: "m",
                        gender: gender[2 % 2],
                        street: "Street " + 2,
                        city: "City " + 2
                    }
                ]
            });
            return c1.save().chain(function () {
                return c1.employees.chain(function (emps) {
                    assert.lengthOf(emps, 2);
                });
            });
        });

        it.should("have child associations when queried", function () {
            return Company.one().chain(function (company) {
                return company.employees.chain(function (emps) {
                    assert.lengthOf(emps, 2);
                    var ids = [1, 2];
                    emps.forEach(function (emp, i) {
                        assert.equal(ids[i], emp.id);
                    });
                });
            });
        });

        it.should("the child associations should also be associated to the parent ", function () {
            return Employee.all().chain(function (emps) {
                assert.lengthOf(emps, 2);
                return comb.when([
                    emps[0].company,
                    emps[1].company
                ]).chain(function (companies) {
                    assert.equal(companies[0].companyName, "Google");
                    assert.equal(companies[1].companyName, "Google");
                });
            });
        });

    });

    it.describe("saving a model with many to one", function (it) {

        it.beforeAll(function () {
            return comb.when([
                Company.remove(),
                Employee.remove()
            ]);
        });

        it.should("it should save the associations", function () {
            var emp = new Employee({
                lastName: "last" + 1,
                firstName: "first" + 1,
                midInitial: "m",
                gender: gender[1 % 2],
                street: "Street " + 1,
                city: "City " + 1,
                company: {
                    companyName: "Google"
                }
            });
            return emp.save().chain(function () {
                return emp.company.chain(function (company) {
                    assert.equal(company.companyName, "Google");
                });
            });
        });

        it.should("have child associations when queried", function () {
            return Company.one().chain(function (company) {
                return company.employees.chain(function (emps) {
                    assert.lengthOf(emps, 1);
                });
            });
        });

        it.should("the child associations should also be associated to the parent ", function () {
            return Employee.all().chain(function (emps) {
                assert.lengthOf(emps, 1);
                emps[0].company.chain(function (company) {
                    assert.equal(company.companyName, "Google");
                });
            });
        });

    });

    it.describe("add methods", function (it) {

        it.beforeEach(function () {
            return Company.remove().chain(function () {
                return new Company({companyName: "Google"}).save();
            });
        });

        it.should("have an add method", function () {
            return Company.one().chain(function (company) {
                var emp = new Employee({
                    lastName: "last",
                    firstName: "first",
                    midInitial: "m",
                    gender: gender[0],
                    street: "Street",
                    city: "City"
                });

                return company.addEmployee(emp)
                    .chain(function () {
                        return company.employees;
                    }).chain(function (emps) {
                        assert.lengthOf(emps, 1);
                    });
            });
        });
        it.should("have a add multiple method", function () {
            var employees = [];
            for (var i = 0; i < 3; i++) {
                employees.push({
                    lastName: "last" + i,
                    firstName: "first" + i,
                    midInitial: "m",
                    gender: gender[i % 2],
                    street: "Street " + i,
                    city: "City " + i
                });
            }
            return Company.one().chain(function (company) {
                return company.addEmployees(employees).chain(function () {
                    return company.employees.chain(function (emps) {
                        assert.lengthOf(emps, 3);
                        emps.forEach(function (emp) {
                            assert.instanceOf(emp, Employee);
                        });
                    });
                });
            });
        });

    });

    it.describe("remove methods", function (it) {
        var employees = [];
        for (var i = 0; i < 3; i++) {
            employees.push({
                lastName: "last" + i,
                firstName: "first" + i,
                midInitial: "m",
                gender: gender[i % 2],
                street: "Street " + i,
                city: "City " + i
            });
        }
        it.beforeEach(function () {
            return comb.when([
                Company.remove(),
                Employee.remove()
            ]).chain(function () {
                return new Company({companyName: "Google", employees: employees}).save();
            });
        });

        it.should("the removing of associations and deleting them", function () {
            return Company.one().chain(function (company) {
                return company.employees
                    .chain(function (emps) {
                        return company.removeEmployee(emps[0], true);
                    })
                    .chain(function () {
                        return comb.when([company.employees, Employee.count()]);
                    })
                    .chain(function (ret) {
                        assert.lengthOf(ret[0], 2);
                        assert.equal(ret[1], 2);
                    });
            });
        });

        it.should("allow the removing of associations without deleting", function () {
            return Company.one().chain(function (company) {
                return company.employees
                    .chain(function (emps) {
                        return company.removeEmployee(emps[0]);
                    })
                    .chain(function () {
                        return comb.when([company.employees, Employee.count()]);
                    })
                    .chain(function (ret) {
                        assert.lengthOf(ret[0], 2);
                        assert.equal(ret[1], 3);
                    });
            });
        });

        it.should("allow the removal of multiple associations and deleting them", function () {
            return Company.one().chain(function (company) {
                return company.employees
                    .chain(function (emps) {
                        return company.removeEmployees(emps, true);
                    })
                    .chain(function () {
                        return comb.when([company.employees, Employee.count()]);
                    })
                    .chain(function (ret) {
                        assert.lengthOf(ret[0], 0);
                        assert.equal(ret[1], 0);
                    });
            });
        });

        it.should("allow the removal of multiple associations and not deleting them", function () {
            return Company.one().chain(function (company) {
                return company.employees
                    .chain(function (emps) {
                        return company.removeEmployees(emps);
                    })
                    .chain(function () {
                        return comb.when([company.employees, Employee.count()]);
                    })
                    .chain(function (ret) {
                        assert.lengthOf(ret[0], 0);
                        assert.equal(ret[1], 3);
                    });
            });
        });
    });

    it.should("should not delete associations when deleting", function () {
        return Company.one().chain(function (company) {
            return company.remove()
                .chain(function () {
                    return Employee.count();
                })
                .chain(function (ret) {
                    assert.equal(ret, 3);
                });
        });
    });


    it.afterAll(function () {
        return helper.dropModels();
    });
});