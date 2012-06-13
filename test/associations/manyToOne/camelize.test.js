var it = require('it'),
    assert = require('assert'),
    helper = require("../../data/manyToOne.helper.js"),
    patio = require("index"),
    comb = require("comb"),
    hitch = comb.hitch;


var gender = ["M", "F"];
var Company = patio.addModel("company", {
    "static":{

        identifierOutputMethod:"camelize",

        identifierInputMethod:"underscore",

        init:function () {
            this._super(arguments);
            this.oneToMany("employees");
        }
    }
});
var Employee = patio.addModel("employee", {
    "static":{

        identifierOutputMethod:"camelize",

        identifierInputMethod:"underscore",

        init:function () {
            this._super(arguments);
            this.manyToOne("company");
        }
    }
});
it.describe("Many to one camelize properties", function (it) {


    it.beforeAll(function () {
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
                c1.employees.then(function (emps) {
                    assert.lengthOf(emps, 2);
                    next();
                }, next);
            }, next);
        });

        it.should("have child associations when queried", function (next) {
            Company.one().then(function (company) {
                company.employees.then(function (emps) {
                    assert.lengthOf(emps, 2);
                    var ids = [1, 2];
                    emps.forEach(function (emp, i) {
                        assert.equal(ids[i], emp.id);
                    });
                    next();
                }, next);
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
                emp.company.then(function (company) {
                    assert.equal(company.companyName, "Google");
                    next();
                }, next);
            }, next);
        });

        it.should("have child associations when queried", function (next) {
            Company.one().then(function (company) {
                company.employees.then(function (emps) {
                    assert.lengthOf(emps, 1);
                    next();
                }, next);
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
                    return company.employees;
                }).then(function (emps) {
                        assert.lengthOf(emps, 1);
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
                    return company.employees;
                }).then(function (emps) {
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
                    return {employees:company.employees, empCount:Employee.count()};
                }).then(function (ret) {
                    var emps = ret.employees;
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
                    return {employees:company.employees, empCount:Employee.count()};
                }).then(function (ret) {
                    var emps = ret.employees;
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
                    return {employees:company.employees, empCount:Employee.count()};
                }).then(function (ret) {
                    assert.lengthOf(ret.employees, 0);
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
                    return {employees:company.employees, empCount:Employee.count()};
                }).then(function (ret) {
                    assert.lengthOf(ret.employees, 0);
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

    it.run();

});


