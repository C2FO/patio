var vows = require('vows'),
    assert = require('assert'),
    helper = require("../../data/manyToOne.helper.js"),
    patio = require("index"),
    sql = patio.sql,
    comb = require("comb"),
    hitch = comb.hitch;

var ret = module.exports = new comb.Promise();

var gender = ["M", "F"];
var cities = ["Omaha", "Lincoln", "Kearney"];

var Company = patio.addModel("company", {
    "static":{
        init:function () {
            this._super(arguments);
            this.oneToMany("employees");
            this.oneToMany("omahaEmployees", {model:"employee"}, function (ds) {
                return ds.filter(sql.identifier("city").ilike("omaha"));
            });
            this.oneToMany("lincolnEmployees", {model:"employee"}, function (ds) {
                return ds.filter(sql.identifier("city").ilike("lincoln"));
            });
        }
    }
});

var Employee = patio.addModel("employee", {
    "static":{
        init:function () {
            this._super(arguments);
            this.manyToOne("company");
        }
    }
});

helper.createSchemaAndSync().then(function () {

    var suite = vows.describe("Many to one Lazy association with a customFilter ");

    suite.addBatch({
        "A model":{
            topic:function () {
                return Employee;
            },

            "should have associations":function () {
                assert.deepEqual(Employee.associations, ["company"]);
                assert.deepEqual(Company.associations, ["employees", "omahaEmployees", "lincolnEmployees"]);
                var emp = new Employee();
                var company = new Company();
                assert.deepEqual(emp.associations, ["company"]);
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

            " the company should have employees ":{
                topic:function (company) {
                    comb.executeInOrder(company,
                        function (company) {
                            return {
                                omahaEmployees:company.omahaEmployees,
                                lincolnEmployees:company.lincolnEmployees,
                                employees:company.employees
                            };
                        }).then(hitch(this, "callback", null), hitch(this, "callback"));
                },

                " when querying the employees there should be employees, omahaEmployees, and lincolnEmployees ":{
                    topic:function (ret, company) {
                        var employees = ret.employees,
                            omahaEmployees = ret.omahaEmployees,
                            lincolnEmployees = ret.lincolnEmployees;
                        assert.lengthOf(employees, 3);
                        assert.lengthOf(lincolnEmployees, 1);
                        assert.equal(lincolnEmployees[0].city, "Lincoln");
                        assert.lengthOf(omahaEmployees, 1);
                        assert.equal(omahaEmployees[0].city, "Omaha");
                        employees.forEach(function (emp, i) {
                            assert.equal(emp.id, i + 1);
                        }, this);
                        comb.executeInOrder(assert, Employee,
                            function (assert, Employee) {
                                var emps = Employee.filter({companyId:company.id}).all();
                                assert.lengthOf(emps, 3);
                                return {company1:emps[0].company, company2:emps[1].company, company3:emps[2].company};
                            }).then(hitch(this, "callback", null), hitch(this, "callback"));

                    },

                    "the employees company should be loaded":function (ret) {
                        assert.equal(ret.company1.companyName, "Google");
                        assert.equal(ret.company2.companyName, "Google");
                        assert.equal(ret.company3.companyName, "Google");
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

            " the company's employees should not be loaded ":{
                topic:function (company) {
                    company.employees.then(hitch(this, "callback", null), hitch(this, "callback"));
                },

                " but after fetching them there should be three":function (emps) {
                    assert.lengthOf(emps, 3);
                    var ids = [1, 2, 3];
                    emps.forEach(function (emp, i) {
                        assert.equal(ids[i], emp.id);
                    });
                },

                " and adding an employee":{
                    topic:function (i, company) {
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

                    "the company should have four employees two omahaEmployees, and 1 lincolEmployee ":function (ret) {
                        var emps = ret.employees;
                        assert.lengthOf(ret.employees, 4);
                        assert.lengthOf(ret.omahaEmployees, 2);
                        assert.isTrue(ret.omahaEmployees.every(function (emp) {
                            return emp.city.match(/omaha/i) !== null;
                        }));
                        assert.lengthOf(ret.lincolnEmployees, 1);
                        assert.isTrue(ret.lincolnEmployees.every(function (emp) {
                            return emp.city.match(/lincoln/i) !== null;
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
                        return {employees:company.employees, empCount:Employee.count()};
                    }).then(hitch(this, "callback", null), hitch(this, "callback"));
            },

            "the company should have three employees ":function (ret) {
                var emps = ret.employees;
                assert.lengthOf(emps, 3);
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

        "When finding a company and removing multiple employees and deleting the employees":{
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
                        return {
                            employees:company.employees,
                            omahaEmployees:company.omahaEmployees,
                            lincolnEmployees:company.lincolnEmployees
                        };
                    }).then(hitch(this, "callback", null), hitch(this, "callback"));
            },

            "the company should have three employees ":function (ret) {
                var emps = ret.employees;
                assert.lengthOf(ret.employees, 3);
                assert.lengthOf(ret.omahaEmployees, 1);
                assert.isTrue(ret.omahaEmployees.every(function (emp) {
                    return emp.city.match(/omaha/i) !== null;
                }));
                assert.lengthOf(ret.lincolnEmployees, 1);
                assert.isTrue(ret.lincolnEmployees.every(function (emp) {
                    return emp.city.match(/lincoln/i) !== null;
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
                        return {employees:company.employees, omahaEmployees:company.omahaEmployees, lincolnEmployees:company.lincolnEmployees, empCount:Employee.count()};
                    }).then(hitch(this, "callback", null), hitch(this, "callback"));
            },


            "the company should have two employees and no omaha employees":function (ret) {
                var emps = ret.employees;
                assert.lengthOf(emps, 2);
                assert.lengthOf(ret.omahaEmployees, 0);
                assert.lengthOf(ret.lincolnEmployees, 1);
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
                        return {employees:company.employees, omahaEmployees:company.omahaEmployees, lincolnEmployees:company.lincolnEmployees, empCount:Employee.count()};
                    }).then(hitch(this, "callback", null), hitch(this, "callback"));
            },


            "the company should have one employees and no omaha or lincoln employees":function (ret) {
                var emps = ret.employees;
                assert.lengthOf(emps, 1);
                assert.lengthOf(ret.omahaEmployees, 0);
                assert.lengthOf(ret.lincolnEmployees, 0);
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

});

