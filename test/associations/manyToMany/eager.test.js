"use strict";
var it = require('it'),
    assert = require('assert'),
    helper = require("../../data/manyToMany.helper.js"),
    patio = require("../../../lib"),
    comb = require("comb"),
    hitch = comb.hitch;

var gender = ["M", "F"];


it.describe("patio.Model manyToMany eager camelize properties", function (it) {


    var Company, Employee;
    it.beforeAll(function () {
        Company = patio.addModel("company");
        Company.manyToMany("employees", {fetchType: Company.fetchType.EAGER});
        Employee = patio.addModel("employee");
        Employee.manyToMany("companies", {fetchType: Employee.fetchType.EAGER});
        return helper.createSchemaAndSync(true);
    });


    it.should("have associations", function () {
        assert.deepEqual(Employee.associations, ["companies"]);
        assert.deepEqual(Company.associations, ["employees"]);
        var emp = new Employee();
        var company = new Company();
        assert.deepEqual(emp.associations, ["companies"]);
        assert.deepEqual(company.associations, ["employees"]);
    });


    it.describe("creating a model with associations", function (it) {

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
                var emps = c1.employees;
                assert.lengthOf(emps, 2);
            });
        });

        it.should("have child associations when queried", function () {
            return Company.one().chain(function (company) {
                var emps = company.employees;
                assert.lengthOf(emps, 2);
                var ids = [1, 2];
                emps.forEach(function (emp, i) {
                    assert.equal(ids[i], emp.id);
                });
            });
        });

        it.should("the child associations should also be associated to the parent ", function () {
            return Employee.all().chain(function (emps) {
                assert.lengthOf(emps, 2);
                emps.forEach(function (emp) {
                    assert.lengthOf(emp.companies, 1);
                    emp.companies.forEach(function (company) {
                        assert.equal(company.companyName, "Google");
                    });
                });
            });
        });
    });

    it.describe("access children immediately after save operation", function (it) {
        it.beforeAll(function () {
            return comb.when([
                Company.remove(),
                Employee.remove()
            ]);
        });

        it.should("never return a promise for fetchType eager, parent null", function () {
            var c1 = new Company({
                companyName: "Bubu Inc."
            });

            return c1.save().chain(function () {
                assert.isFalse(c1.employees instanceof comb.Promise);
                assert.lengthOf(c1.employees, 0);
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
                return company.addEmployee(emp).chain(function () {
                    assert.lengthOf(company.employees, 1);
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
                    var emps = company.employees;
                    assert.lengthOf(emps, 3);
                    emps.forEach(function (emp) {
                        assert.instanceOf(emp, Employee);
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
            return comb.when([Company.remove(), Employee.remove()]).chain(function () {
                return new Company({companyName: "Google", employees: employees}).save();
            });
        });

        it.should("the removing of associations and deleting them", function () {
            return Company.one().chain(function (company) {
                return company.removeEmployee(company.employees[0], true)
                    .chain(function () {
                        return Employee.count();
                    })
                    .chain(function (ret) {
                        var emps = company.employees;
                        assert.lengthOf(emps, 2);
                        assert.equal(ret, 2);
                    });
            });

        });

        it.should("allow the removing of associations without deleting", function () {
            return Company.one().chain(function (company) {
                return company.removeEmployee(company.employees[0])
                    .chain(function () {
                        return Employee.count();
                    })
                    .chain(function (ret) {
                        var emps = company.employees;
                        assert.lengthOf(emps, 2);
                        assert.equal(ret, 3);
                    });
            });
        });

        it.should("allow the removal of multiple associations and deleting them", function () {
            return Company.one().chain(function (company) {
                return company.removeEmployees(company.employees, true)
                    .chain(function () {
                        return Employee.count();
                    })
                    .chain(function (ret) {
                        var emps = company.employees;
                        assert.lengthOf(emps, 0);
                        assert.equal(ret, 0);
                    });
            });
        });

        it.should("allow the removal of multiple associations and not deleting them", function () {
            return Company.one().chain(function (company) {
                return company.removeEmployees(company.employees)
                    .chain(function () {
                        return Employee.count();
                    })
                    .chain(function (ret) {
                        var emps = company.employees;
                        assert.lengthOf(emps, 0);
                        assert.equal(ret, 3);
                    });
            });
        });

        it.should("allow the removal of all associations and deleting them", function () {
            return Company.one().chain(function (company) {
                return company.removeAllEmployees(true)
                    .chain(function () {
                        return Employee.count();
                    })
                    .chain(function (ret) {
                        var emps = company.employees;
                        assert.lengthOf(emps, 0);
                        assert.equal(ret, 0);
                    });
            });
        });

        it.should("allow the removal of all associations and not deleting them", function () {
            return Company.one().chain(function (company) {
                return company.removeAllEmployees()
                    .chain(function () {
                        return Employee.count();
                    })
                    .chain(function (ret) {
                        var emps = company.employees;
                        assert.lengthOf(emps, 0);
                        assert.equal(ret, 3);
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
