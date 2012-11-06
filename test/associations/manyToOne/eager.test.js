var it = require('it'),
    assert = require('assert'),
    helper = require("../../data/manyToOne.helper.js"),
    patio = require("index"),
    comb = require("comb-proxy"),
    hitch = comb.hitch;


var gender = ["M", "F"];

it.describe("Many to one eager", function (it) {

    var Company, Employee;
    it.beforeAll(function () {
        Company = patio.addModel("company", {
            "static":{
                init:function () {
                    this._super(arguments);
                    this.oneToMany("employees", {fetchType:this.fetchType.EAGER});
                }
            }
        });
        Employee = patio.addModel("employee", {
            "static":{
                init:function () {
                    this._super(arguments);
                    this.manyToOne("company", {fetchType:this.fetchType.EAGER});
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
                var emps = company.employees
                assert.lengthOf(emps, 2);
                var ids = [1, 2];
                emps.forEach(function (emp, i) {
                    assert.equal(ids[i], emp.id);
                });
                next();

            }, next);
        });

        it.should("the child associations should also be associated to the parent ", function (next) {
            comb.executeInOrder(assert, Employee,
                function (assert, Employee) {
                    var emps = Employee.all();
                    assert.lengthOf(emps, 2);
                    return {company1:emps[0].company, company2:emps[1].company};
                }).then(function (ret) {
                    assert.equal(ret.company1.companyName, "Google");
                    assert.equal(ret.company2.companyName, "Google");
                    next();
                }, next);
        });

    });

    it.describe("saving a model with many to one - with parent side being null", function (it) {

        it.beforeAll(function () {
            return comb.when(
                Company.remove(),
                Employee.remove()
            );
        });
        
        it.should("never return a promise for fetchType eager", function (next) {
            var e1 = new Employee({
                lastName:"last" + 1,
                firstName:"first" + 1,
                midInitial:"m",
                gender:gender[1 % 2],
                street:"Street " + 1,
                city:"City " + 1
            });

            e1.save().then(function () {
                assert.isFalse(comb.isPromiseLike(e1.company));
                next();
            }, next);
        });
    });

    it.describe("saving a model with many to one", function (it) {

        it.beforeAll(function () {
            return comb.when(
                Company.remove(),
                Employee.remove()
            );
        });

        it.should("it should save the associations", function (next) {
            var emp = new Employee({
                lastName:"last" + 1,
                firstName:"first" + 1,
                midInitial:"m",
                gender:gender[1 % 2],
                street:"Street " + 1,
                city:"City " + 1,
                company:{
                    companyName:"Google"
                }
            });
            emp.save().then(function () {
                assert.equal(emp.company.companyName, "Google");
                next();
            }, next);
        });

        it.should("have child associations when queried", function (next) {
            Company.one().then(function (company) {
                var emps = company.employees;
                assert.lengthOf(emps, 1);
                next();
            }, next);
        });

        it.should("the child associations should also be associated to the parent ", function (next) {
            comb.executeInOrder(assert, Employee,
                function (assert, Employee) {
                    var emps = Employee.all();
                    assert.lengthOf(emps, 1);
                    return {company1:emps[0].company};
                }).then(function (ret) {
                    assert.equal(ret.company1.companyName, "Google");
                    next();
                }, next);
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
                    var company = ret.company, emps = company.employees;
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
                    var company = ret.company, emps = company.employees;
                    assert.lengthOf(emps, 2);
                    assert.equal(ret.empCount, 3);
                    next();
                }, next);
        });

        it.should("allow the removal of multiple associations and deleting them", function (next) {
            comb.executeInOrder(Company, Employee,
                function (Company, Employee) {
                    var company = Company.one();
                    var emps = company.employees;
                    company.removeEmployees(emps, true);
                    return {company:company, empCount:Employee.count()};
                }).then(function (ret) {
                    var company = ret.company, emps = company.employees;
                    assert.lengthOf(emps, 0);
                    assert.equal(ret.empCount, 0);
                    next();
                }, next);
        });

        it.should("allow the removal of multiple associations and not deleting them", function (next) {
            comb.executeInOrder(Company, Employee,
                function (Company, Employee) {
                    var company = Company.one();
                    var emps = company.employees;
                    company.removeEmployees(emps);
                    return {company:company, empCount:Employee.count()};
                }).then(function (ret) {
                    var company = ret.company, emps = company.employees;
                    assert.lengthOf(emps, 0);
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
}).as(module);