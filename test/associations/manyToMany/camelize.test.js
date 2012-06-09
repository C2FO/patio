var vows = require('vows'),
    assert = require('assert'),
    helper = require("../../data/manyToMany.helper.js"),
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
            this.manyToMany("employees");
        }
    }
});
var Employee = patio.addModel("employee", {
    "static":{

        identifierOutputMethod:"camelize",

        identifierInputMethod:"underscore",

        init:function () {
            this._super(arguments);
            this.manyToMany("companies");
        }
    }
});

var ret = module.exports  = new comb.Promise();
helper.createSchemaAndSync(true).then(function () {

    var suite = vows.describe("Many to many Lazy association ");

    suite.addBatch({
        "A model":{
            topic:function () {
                return Employee;
            },

            "should have associations":function () {
                assert.deepEqual(Employee.associations, ["companies"]);
                assert.deepEqual(Company.associations, ["employees"]);
                var emp = new Employee();
                var company = new Company();
                assert.deepEqual(emp.associations, ["companies"]);
                assert.deepEqual(company.associations, ["employees"]);
            }
        }
    });

    suite.addBatch({

        "When creating a company with employees":{
            topic:function () {
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
                c1.save().then(hitch(this, "callback", null), hitch(this, "callback"));
            },

            " the company should have employees ":{
                topic:function (company) {
                    company.employees.then(hitch(this, "callback", null), hitch(this, "callback"));
                },

                " when querying the employees ":{
                    topic:function (emps, company) {
                        assert.lengthOf(emps, 2);
                        comb.executeInOrder(assert, Employee,
                            function (assert, Employee) {
                                var emps = Employee.all();
                                assert.lengthOf(emps, 2);
                                return {companies1:emps[0].companies, companies2:emps[1].companies};
                            }).then(hitch(this, "callback", null), hitch(this, "callback"));

                    },

                    "the employees company should be loaded":function (ret) {
                        assert.isTrue(ret.companies1.every(function (c) {
                            return c.companyName === "Google";
                        }));
                        assert.isTrue(ret.companies2.every(function (c) {
                            return c.companyName === "Google";
                        }));
                    }
                }
            }
        }

    });

    suite.addBatch({

        "When finding a company":{
            topic:function () {
                Company.one().then(hitch(this, "callback", null), hitch(this, "callback"));
            },

            " the companys employees should not be loaded ":{
                topic:function (company) {
                    company.employees.then(hitch(this, "callback", null), hitch(this, "callback"));
                },

                " but after fetching them there should be two":function (emps) {
                    assert.lengthOf(emps, 2);
                    var ids = [1, 2];
                    emps.forEach(function (emp, i) {
                        assert.equal(ids[i], emp.id);
                    });
                },

                " and adding an employee":{
                    topic:function (i, company) {
                        var emp = new Employee({
                            lastName:"last" + 3,
                            firstName:"first" + 3,
                            midInitial:"m",
                            gender:gender[1 % 3],
                            street:"Street " + 3,
                            city:"City " + 3
                        });
                        comb.executeInOrder(company,
                            function (company) {
                                company.addEmployee(emp);
                                return company.employees;
                            }).then(hitch(this, "callback", null), hitch(this, "callback"));
                    },

                    "the company should have three employees ":function (emps) {
                        assert.lengthOf(emps, 3);
                    }
                }
            }



        }
    });

    suite.addBatch({

        "When finding a company and removing an employee and deleting the employee":{
            topic:function () {
                comb.executeInOrder(Company, Employee,
                    function (Company, Employee) {
                        var company = Company.one();
                        var emps = company.employees;
                        company.removeEmployee(emps[0], true);
                        return {employees:company.employees, empCount:Employee.count()};
                    }).then(hitch(this, "callback", null), hitch(this, "callback"));
            },

            "the company should have two employees ":function (ret) {
                var emps = ret.employees;
                assert.lengthOf(emps, 2);
                assert.equal(ret.empCount, 2);
            }
        }

    });

    suite.addBatch({

        "When finding a company and removing multiple employees and deloting the employees":{
            topic:function () {
                comb.executeInOrder(Company, Employee,
                    function (Company, Employee) {
                        var company = Company.one();
                        var emps = company.employees;
                        company.removeEmployees(emps, true);
                        return {employees:company.employees, empCount:Employee.count()};
                    }).then(hitch(this, "callback", null), hitch(this, "callback"));
            },

            "the company should have no employees ":function (ret) {
                assert.lengthOf(ret.employees, 0);
                assert.equal(ret.empCount, 0);
            }
        }

    });

    suite.addBatch({

        "When finding a company and adding employees":{
            topic:function () {
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
                    }).then(hitch(this, "callback", null), hitch(this, "callback"));
            },

            "the company should have three employees ":function (emps) {
                assert.lengthOf(emps, 3);
                emps.forEach(function (emp) {
                    assert.instanceOf(emp, Employee);
                });
            }
        }

    });

    suite.addBatch({

        "When finding a company and removing an employee and not deleting the employee":{
            topic:function () {
                comb.executeInOrder(Company, Employee,
                    function (Company, Employee) {
                        var company = Company.one();
                        var emps = company.employees;
                        company.removeEmployee(emps[0]);
                        return {employees:company.employees, empCount:Employee.count()};
                    }).then(hitch(this, "callback", null), hitch(this, "callback"));
            },


            "the company should have two employees ":function (ret) {
                var emps = ret.employees;
                assert.lengthOf(emps, 2);
                assert.equal(ret.empCount, 3);
            }
        }

    });

    suite.addBatch({

        "When finding a company and removing multiple employees and not deleting the employees":{
            topic:function () {
                comb.executeInOrder(Company, Employee,
                    function (Company, Employee) {
                        var company = Company.one();
                        var emps = company.employees;
                        company.removeEmployees(emps);
                        return {employees:company.employees, empCount:Employee.count()};
                    }).then(hitch(this, "callback", null), hitch(this, "callback"));
            },

            "the company should have no employees ":function (ret) {
                assert.lengthOf(ret.employees, 0);
                assert.equal(ret.empCount, 3);
            }
        }

    });


    suite.addBatch({
        "When deleting a company":{
            topic:function () {
                comb.executeInOrder(Company, Employee,
                    function (Company, Employee) {
                        var company = Company.one();
                        company.remove();
                        return Employee.count();
                    }).then(hitch(this, "callback", null), hitch(this, "callback"));
            },

            " the company should no employees ":function (count) {
                assert.equal(count, 3);
            }
        }
    });


    suite.run({reporter:require("vows").reporter.spec}, function () {
        helper.dropModels().then(comb.hitch(ret, "callback"), comb.hitch(ret, "errback"));
    });

}, function(err){
    console.log(err);
});

