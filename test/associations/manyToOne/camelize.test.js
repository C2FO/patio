var it = require('it'),
    assert = require('assert'),
    helper = require("../../data/manyToOne.helper.js"),
    patio = require("../../../lib"),
    comb = require("comb-proxy"),
    hitch = comb.hitch;


var gender = ["M", "F"];
it.describe("Many to one camelize properties", function (it) {

    var Company, Employee;
    it.beforeAll(function () {

        Company = patio.addModel("company", {
            "static": {

                identifierOutputMethod: "camelize",

                identifierInputMethod: "underscore",

                init: function () {
                    this._super(arguments);
                    this.oneToMany("employees");
                }
            }
        });
        Employee = patio.addModel("employee", {
            "static": {

                identifierOutputMethod: "camelize",

                identifierInputMethod: "underscore",

                init: function () {
                    this._super(arguments);
                    this.manyToOne("company");
                }
            }
        });
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
            return comb.when(
                Company.remove(),
                Employee.remove()
            );
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
            return comb.executeInOrder(assert, Employee,function (assert, Employee) {
                var emps = Employee.all();
                assert.lengthOf(emps, 2);
                return {company1: emps[0].company, company2: emps[1].company};
            }).chain(function (ret) {
                    assert.equal(ret.company1.companyName, "Google");
                    assert.equal(ret.company2.companyName, "Google");
                });
        });

    });

    it.describe("saving a model with many to one", function (it) {

        it.beforeAll(function () {
            return comb.when(
                Company.remove(),
                Employee.remove()
            );
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
            return comb.executeInOrder(assert, Employee,function (assert, Employee) {
                var emps = Employee.all();
                assert.lengthOf(emps, 1);
                return {company1: emps[0].company};
            }).chain(function (ret) {
                    assert.equal(ret.company1.companyName, "Google");
                });
        });

        it.should("save associations when set after save", function () {
            var company = new Company({
                companyName: "Google"
            });
            var emp = new Employee({
                lastName: "last" + 1,
                firstName: "first" + 1,
                midInitial: "m",
                gender: gender[1 % 2],
                street: "Street " + 1,
                city: "City " + 1
            });
            return emp.save().chain(function () {
                return emp.company.chain(function (comp) {
                    assert.isNull(comp);
                    emp.company = company;
                    return emp.save().chain(function () {
                        return emp.company.chain(function (company) {
                            assert.equal(company.companyName, "Google");
                        });
                    });
                });
            });
        });


    });

    it.describe("add methods", function (it) {

        it.beforeEach(function () {
            return comb.executeInOrder(Company, function (Company) {
                Company.remove();
                new Company({companyName: "Google"}).save();
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
                return comb.executeInOrder(company,function (company) {
                    company.addEmployee(emp);
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
            return comb.executeInOrder(Company,function (Company) {
                var company = Company.one();
                company.addEmployees(employees);
                return company.employees;
            }).chain(function (emps) {
                    assert.lengthOf(emps, 3);
                    emps.forEach(function (emp) {
                        assert.instanceOf(emp, Employee);
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
            return comb.executeInOrder(Company, Employee, function (Company, Employee) {
                Company.remove();
                Employee.remove();
                new Company({companyName: "Google", employees: employees}).save();
            });
        });

        it.should("the removing of associations and deleting them", function () {
            return comb.executeInOrder(Company, Employee,function (Company, Employee) {
                var company = Company.one();
                var emps = company.employees;
                company.removeEmployee(emps[0], true);
                return {employees: company.employees, empCount: Employee.count()};
            }).chain(function (ret) {
                    var emps = ret.employees;
                    assert.lengthOf(emps, 2);
                    assert.equal(ret.empCount, 2);
                });
        });

        it.should("allow the removing of associations without deleting", function () {
            return comb.executeInOrder(Company, Employee,function (Company, Employee) {
                var company = Company.one();
                var emps = company.employees;
                company.removeEmployee(emps[0]);
                return {employees: company.employees, empCount: Employee.count()};
            }).chain(function (ret) {
                    var emps = ret.employees;
                    assert.lengthOf(emps, 2);
                    assert.equal(ret.empCount, 3);
                });
        });

        it.should("allow the removal of multiple associations and deleting them", function () {
            return comb.executeInOrder(Company, Employee,function (Company, Employee) {
                var company = Company.one();
                var emps = company.employees;
                company.removeEmployees(emps, true);
                return {employees: company.employees, empCount: Employee.count()};
            }).chain(function (ret) {
                    assert.lengthOf(ret.employees, 0);
                    assert.equal(ret.empCount, 0);
                });
        });

        it.should("allow the removal of multiple associations and not deleting them", function () {
            return comb.executeInOrder(Company, Employee,function (Company, Employee) {
                var company = Company.one();
                var emps = company.employees;
                company.removeEmployees(emps);
                return {employees: company.employees, empCount: Employee.count()};
            }).chain(function (ret) {
                    assert.lengthOf(ret.employees, 0);
                    assert.equal(ret.empCount, 3);
                });
        });
    });

    it.should("should not delete associations when deleting", function () {
        return comb.executeInOrder(Company, Employee,function (Company, Employee) {
            var company = Company.one();
            company.remove();
            return Employee.count();
        }).chain(function (count) {
                assert.equal(count, 3);
            });
    });


    it.afterAll(function () {
        return helper.dropModels();
    });
});