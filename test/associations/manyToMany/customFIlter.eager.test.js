var vows = require('vows'),
    assert = require('assert'),
    helper = require("../../data/manyToMany/customFilter.eager.models"),
    patio = require("../../../lib"),
    comb = require("comb"),
    hitch = comb.hitch;

var ret = module.exports = exports = new comb.Promise();

var gender = ["M", "F"];
var cities = ["Omaha", "Lincoln", "Kearney"];
helper.loadModels().then(function () {
    var Company = patio.getModel("company"), Employee = patio.getModel("employee");

    var suite = vows.describe("Many to Many Eager association with a customFilter ");

    suite.addBatch({
        "A model":{
            topic:function () {
                return Employee;
            },

            "should have associations":function () {
                assert.deepEqual(Employee.associations, ["companies"]);
                assert.deepEqual(Company.associations, ["employees", "omahaEmployees", "lincolnEmployees"]);
                var emp = new Employee();
                var company = new Company();
                assert.deepEqual(emp.associations, ["companies"]);
                assert.deepEqual(company.associations, ["employees", "omahaEmployees", "lincolnEmployees"]);
            }
        }
    });

    suite.addBatch({

        "When creating a company with employees":{
            topic:function () {

                var employees = [];
                for (var i = 0; i < 3; i++) {
                    employees.push({
                        lastname:"last" + i,
                        firstname:"first" + i,
                        midinitial:"m",
                        gender:gender[i % 2],
                        street:"Street " + i,
                        city:cities[i % 3]
                    });
                }
                var c1 = new Company({
                    companyName:"Google",
                    employees:employees
                });
                c1.save().then(hitch(this, "callback", null), hitch(this, "callback"));
            },

            " the company should have employees ":function (company) {
                var employees = company.employees,
                    omahaEmployees = company.omahaEmployees,
                    lincolnEmployees = company.lincolnEmployees;
                assert.lengthOf(employees, 3);
                assert.lengthOf(lincolnEmployees, 1);
                assert.equal(lincolnEmployees[0].city, "Lincoln")
                assert.lengthOf(omahaEmployees, 1);
                assert.equal(omahaEmployees[0].city, "Omaha")
            }
        }


    });

    suite.addBatch({
        " when querying the employees there should be employees, omahaEmployees, and lincolnEmployees ":{
            topic:function () {
                Employee.all().then(hitch(this, "callback", null), hitch(this, "callback"));

            },

            "the employees company should be loaded":function (ret) {
                var emps = ret;
                assert.lengthOf(emps, 3);
                assert.isTrue(emps[0].companies.every(function (c) {
                    return c.companyName == "Google";
                }));
                assert.isTrue(emps[1].companies.every(function (c) {
                    return c.companyName == "Google";
                }));
                assert.isTrue(emps[2].companies.every(function (c) {
                    return c.companyName == "Google";
                }));
            }
        }
    });

    suite.addBatch({

        "When finding a company":{
            topic:function () {
                Company.one().then(hitch(this, "callback", null), hitch(this, "callback"));
            },

            " the company's employees should be loaded ":{
                topic:function (company) {
                    var emps = company.employees;
                    assert.lengthOf(emps, 3);
                    return company;
                },

                " and adding an employee":{
                    topic:function (company) {
                        var emp = new Employee({
                            lastname:"last" + 3,
                            firstname:"first" + 3,
                            midinitial:"m",
                            gender:gender[1 % 3],
                            street:"Street " + 3,
                            city:"omaha"
                        });
                        comb.executeInOrder(company,
                            function (company) {
                                company.addEmployee(emp);
                                return {
                                    employees:company.employees,
                                    omahaEmployees:company.omahaEmployees,
                                    lincolnEmployees:company.lincolnEmployees
                                };
                            }).then(hitch(this, "callback", null), hitch(this, "callback"));
                    },

                    "the company should have four employees two omahaEmployees, and 1 lincolnEmployee ":function (ret) {
                        var emps = ret.employees;
                        assert.lengthOf(ret.employees, 4);
                        assert.lengthOf(ret.omahaEmployees, 2);
                        assert.isTrue(ret.omahaEmployees.every(function (emp) {
                            return emp.city.match(/omaha/i) != null;
                        }));
                        assert.lengthOf(ret.lincolnEmployees, 1);
                        assert.isTrue(ret.lincolnEmployees.every(function (emp) {
                            return emp.city.match(/lincoln/i) != null;
                        }));
                    }
                }
            }



        }
    });

    suite.addBatch({

        "When finding a company and removing an omaha employee and deleting the employee":{
            topic:function () {
                comb.executeInOrder(Company, Employee,
                    function (Company, Employee) {
                        var company = Company.one();
                        var emps = company.omahaEmployees;
                        company.removeOmahaEmployee(emps[0], true);
                        return {company:company, empCount:Employee.count()};
                    }).then(hitch(this, "callback", null), hitch(this, "callback"));
            },

            "the company should have three employees ":function (ret) {
                var emps = ret.company.employees;
                var omahaEmps = ret.company.omahaEmployees;
                assert.lengthOf(emps, 3);
                assert.lengthOf(omahaEmps, 1);
                assert.equal(ret.empCount, 3);
            }
        }

    });

    suite.addBatch({

        "When finding a company and removing a lincoln employee and deleting the employee":{
            topic:function () {
                comb.executeInOrder(Company, Employee,
                    function (Company, Employee) {
                        var company = Company.one();
                        var emps = company.lincolnEmployees;
                        company.removeLincolnEmployee(emps[0], true);
                        return {company:company, empCount:Employee.count()};
                    }).then(hitch(this, "callback", null), hitch(this, "callback"));
            },

            "the company should have two employees ":function (ret) {
                var emps = ret.company.employees;
                var lincolnEmps = ret.company.lincolnEmployees;
                assert.lengthOf(emps, 2);
                assert.lengthOf(lincolnEmps, 0);
                assert.equal(ret.empCount, 2);
            }
        }

    });

    suite.addBatch({

        "When finding a company and removing multiple employees and deleting the employees":{
            topic:function () {
                comb.executeInOrder(Company, Employee,
                    function (Company, Employee) {
                        var company = Company.one();
                        var emps = company.employees;
                        company.removeEmployees(emps, true);
                        return {company:company, empCount:Employee.count()};
                    }).then(hitch(this, "callback", null), hitch(this, "callback"));
            },

            "the company should have no employees ":function (ret) {
                assert.lengthOf(ret.company.employees, 0);
                assert.lengthOf(ret.company.omahaEmployees, 0);
                assert.lengthOf(ret.company.lincolnEmployees, 0);
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
                        lastname:"last" + i,
                        firstname:"first" + i,
                        midinitial:"m",
                        gender:gender[i % 2],
                        street:"Street " + i,
                        city:cities[i % 3]
                    });
                }
                comb.executeInOrder(Company,
                    function (Company) {
                        var company = Company.one();
                        company.addEmployees(employees);
                        return company;
                    }).then(hitch(this, "callback", null), hitch(this, "callback"));
            },

            "the company should have three employees ":function (ret) {
                var emps = ret.employees;
                assert.lengthOf(ret.employees, 3);
                assert.lengthOf(ret.omahaEmployees, 1);
                assert.isTrue(ret.omahaEmployees.every(function (emp) {
                    return emp.city.match(/omaha/i) != null;
                }));
                assert.lengthOf(ret.lincolnEmployees, 1);
                assert.isTrue(ret.lincolnEmployees.every(function (emp) {
                    return emp.city.match(/lincoln/i) != null;
                }));
            }
        }

    });

    suite.addBatch({

        "When finding a company and removing an omaha employee and not deleting the employee":{
            topic:function () {
                comb.executeInOrder(Company, Employee,
                    function (Company, Employee) {
                        var company = Company.one();
                        var emps = company.omahaEmployees;
                        company.removeOmahaEmployee(emps[0]);
                        return {company:company, empCount:Employee.count()};
                    }).then(hitch(this, "callback", null), hitch(this, "callback"));
            },


            "the company should have two employees and no omaha employees":function (ret) {
                var emps = ret.company.employees;
                assert.lengthOf(emps, 2);
                assert.lengthOf(ret.company.omahaEmployees, 0);
                assert.lengthOf(ret.company.lincolnEmployees, 1);
                assert.equal(ret.empCount, 3);
            }
        }

    });

    suite.addBatch({

        "When finding a company and removing a lincoln employee and not deleting the employee":{
            topic:function () {
                comb.executeInOrder(Company, Employee,
                    function (Company, Employee) {
                        var company = Company.one();
                        var emps = company.lincolnEmployees;
                        company.removeLincolnEmployee(emps[0]);
                        return {company:company, empCount:Employee.count()};
                    }).then(hitch(this, "callback", null), hitch(this, "callback"));
            },


            "the company should have one employees and no omaha or lincoln employees":function (ret) {
                var emps = ret.company.employees;
                assert.lengthOf(emps, 1);
                assert.lengthOf(ret.company.omahaEmployees, 0);
                assert.lengthOf(ret.company.lincolnEmployees, 0);
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
                        return {company:company, empCount:Employee.count()};
                    }).then(hitch(this, "callback", null), hitch(this, "callback"));
            },

            "the company should have no employees ":function (ret) {
                assert.lengthOf(ret.company.employees, 0);
                assert.lengthOf(ret.company.omahaEmployees, 0);
                assert.lengthOf(ret.company.lincolnEmployees, 0);
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

            " the company should not have any employees ":function (count) {
                assert.equal(count, 3);
            }
        }
    });


    suite.run({reporter:require("vows").reporter.spec}, function () {
        helper.dropModels().then(comb.hitch(ret, "callback"), comb.hitch(ret, "errback"));
    });

});

