var it = require('it'),
    assert = require('assert'),
    helper = require("../../data/manyToMany.helper.js"),
    patio = require("index"),
    comb = require("comb-proxy"),
    hitch = comb.hitch;

var gender = ["M", "F"];


it.describe("Many to Many camelize properties", function (it) {


    var Company, Employee;
    it.beforeAll(function () {

        Company = patio.addModel("company", {
            "static":{
                init:function () {
                    this._super(arguments);
                    this.manyToMany("employees", {fetchType:this.fetchType.EAGER});
                }
            }
        });
        Employee = patio.addModel("employee", {
            "static":{
                init:function () {
                    this._super(arguments);
                    this.manyToMany("companies", {fetchType:this.fetchType.EAGER});
                }
            }
        });
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

        it.should("it should save the associations", function (next) {
            var c1 = new Company({
                companyName:"Google",
                employees:[
                    {
                        lastName:"last" + 1,
                        firstName:"first" + 1,
                        midInitial:"m",
                        gender:gender[1 % 2],
                        street:"Street " + 1,
                        city:"City " + 1
                    },
                    {
                        lastName:"last" + 2,
                        firstName:"first" + 2,
                        midInitial:"m",
                        gender:gender[2 % 2],
                        street:"Street " + 2,
                        city:"City " + 2
                    }
                ]
            });
            c1.save().then(function () {
                var emps = c1.employees;
                assert.lengthOf(emps, 2);
                next();
            }, next);
        });

        it.should("have child associations when queried", function (next) {
            Company.one().then(function (company) {
                var emps = company.employees;
                assert.lengthOf(emps, 2);
                var ids = [1, 2];
                emps.forEach(function (emp, i) {
                    assert.equal(ids[i], emp.id);
                });
                next();

            }, next);
        });

        it.should("the child associations should also be associated to the parent ", function (next) {
            Employee.all().then(function (emps) {
                assert.lengthOf(emps, 2);
                emps.forEach(function (emp) {
                    assert.lengthOf(emp.companies, 1);
                    emp.companies.forEach(function (company) {
                        assert.equal(company.companyName, "Google");
                    });
                });
                next();
            });

        });
    });

    it.describe("add methods", function (it) {

        it.beforeEach(function () {
            return comb.executeInOrder(Company, function (Company) {
                Company.remove();
                new Company({companyName:"Google"}).save();
            });
        });

        it.should("have an add method", function (next) {
            Company.one().then(function (company) {
                var emp = new Employee({
                    lastName:"last",
                    firstName:"first",
                    midInitial:"m",
                    gender:gender[0],
                    street:"Street",
                    city:"City"
                });
                comb.executeInOrder(company,function (company) {
                    company.addEmployee(emp);
                    return company;
                }).then(function (company) {
                        assert.lengthOf(company.employees, 1);
                        next();
                    });
            }, next);
        });
        it.should("have a add multiple method", function (next) {
            var employees = [];
            for (var i = 0; i < 3; i++) {
                employees.push({
                    lastName:"last" + i,
                    firstName:"first" + i,
                    midInitial:"m",
                    gender:gender[i % 2],
                    street:"Street " + i,
                    city:"City " + i
                });
            }
            comb.executeInOrder(Company,
                function (Company) {
                    var company = Company.one();
                    company.addEmployees(employees);
                    return company;
                }).then(function (company) {
                    var emps = company.employees;
                    assert.lengthOf(emps, 3);
                    emps.forEach(function (emp) {
                        assert.instanceOf(emp, Employee);
                    });
                    next();
                }, next);
        });

    });

    it.describe("remove methods", function (it) {
        var employees = [];
        for (var i = 0; i < 3; i++) {
            employees.push({
                lastName:"last" + i,
                firstName:"first" + i,
                midInitial:"m",
                gender:gender[i % 2],
                street:"Street " + i,
                city:"City " + i
            });
        }
        it.beforeEach(function () {
            return comb.executeInOrder(Company, Employee, function (Company, Employee) {
                Company.remove();
                Employee.remove();
                new Company({companyName:"Google", employees:employees}).save();
            });
        });

        it.should("the removing of associations and deleting them", function (next) {
            comb.executeInOrder(Company, Employee,
                function (Company, Employee) {
                    var company = Company.one();
                    var emps = company.employees;
                    company.removeEmployee(emps[0], true);
                    return {company:company, empCount:Employee.count()};
                }).then(function (ret) {
                    var emps = ret.company.employees;
                    assert.lengthOf(emps, 2);
                    assert.equal(ret.empCount, 2);
                    next();
                }, next);
        });

        it.should("allow the removing of associations without deleting", function (next) {
            comb.executeInOrder(Company, Employee,
                function (Company, Employee) {
                    var company = Company.one();
                    var emps = company.employees;
                    company.removeEmployee(emps[0]);
                    return {company:company, empCount:Employee.count()};
                }).then(function (ret) {
                    var emps = ret.company.employees;
                    assert.lengthOf(emps, 2);
                    assert.equal(ret.empCount, 3);
                    next();
                }, next);
        });

        it.should("allow the removal of multiple associations and deleting them", function (next) {
            comb.executeInOrder(Company, Employee,function (Company, Employee) {
                var company = Company.one();
                var emps = company.employees;
                company.removeEmployees(emps, true);
                return {company:company, empCount:Employee.count()};
            }).then(function (ret) {
                    assert.lengthOf(ret.company.employees, 0);
                    assert.equal(ret.empCount, 0);
                    next();
                }, next);
        });

        it.should("allow the removal of multiple associations and not deleting them", function (next) {
            comb.executeInOrder(Company, Employee,function (Company, Employee) {
                var company = Company.one();
                var emps = company.employees;
                company.removeEmployees(emps);
                return {company:company, empCount:Employee.count()};
            }).then(function (ret) {
                    assert.lengthOf(ret.company.employees, 0);
                    assert.equal(ret.empCount, 3);
                    next();
                }, next);
        });

        it.should("allow the removal of all associations and deleting them", function (next) {
            comb.executeInOrder(Company, Employee,function (Company, Employee) {
                var company = Company.one().removeAllEmployees(true).reload();
                return {company:company, empCount:Employee.count()};
            }).then(function (ret) {
                    assert.lengthOf(ret.company.employees, 0);
                    assert.equal(ret.empCount, 0);
                    next();
                }, next);
        });

        it.should("allow the removal of all associations and not deleting them", function (next) {
            comb.executeInOrder(Company, Employee,function (Company, Employee) {
                var company = Company.one().removeAllEmployees().reload();
                return {company:company, empCount:Employee.count()};
            }).then(function (ret) {
                    assert.lengthOf(ret.company.employees, 0);
                    assert.equal(ret.empCount, 3);
                    next();
                }, next);
        });
    });

    it.should("should not delete associations when deleting", function (next) {
        comb.executeInOrder(Company, Employee,
            function (Company, Employee) {
                var company = Company.one();
                company.remove();
                return Employee.count();
            }).then(function (count) {
                assert.equal(count, 3);
                next();
            }, next);
    });


    it.afterAll(function () {
        return helper.dropModels();
    });

});
