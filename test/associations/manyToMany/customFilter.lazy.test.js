"use strict";
var it = require('it'),
    assert = require('assert'),
    helper = require("../../data/manyToMany.helper.js"),
    patio = require("../../../lib"),
    sql = patio.sql,
    comb = require("comb-proxy"),
    hitch = comb.hitch;


var gender = ["M", "F"];
var cities = ["Omaha", "Lincoln", "Kearney"];

it.describe("patio.Model manyToMany lazy with filter", function (it) {
    var Company, Employee;
    it.beforeAll(function () {
        Company = patio.addModel("company")
            .manyToMany("buyers", {model: "company", joinTable: "buyerVendor", key: {vendorId: "buyerId"}})
            .manyToMany("vendors", {model: "company", joinTable: "buyerVendor", key: {buyerId: "vendorId"}})
            .manyToMany("employees")
            .manyToMany("omahaEmployees", {model: "employee"}, function (ds) {
                return ds.filter(sql.identifier("city").ilike("omaha"));
            })
            .manyToMany("lincolnEmployees", {model: "employee"}, function (ds) {
                return ds.filter(sql.identifier("city").ilike("lincoln"));
            });

        Employee = patio.addModel("employee")
            .manyToMany("companies");

        return helper.createSchemaAndSync();
    });


    it.should("have associations", function () {
        assert.deepEqual(Employee.associations, ["companies"]);
        assert.deepEqual(Company.associations, ["buyers", "vendors", "employees", "omahaEmployees", "lincolnEmployees"]);
        var emp = new Employee();
        var company = new Company();
        assert.deepEqual(emp.associations, ["companies"]);
        assert.deepEqual(company.associations, ["buyers", "vendors", "employees", "omahaEmployees", "lincolnEmployees"]);
    });


    it.describe("creating a model with associations", function (it) {

        it.should("it should save the associations", function () {
            var employees = [];
            for (var i = 0; i < 3; i++) {
                employees.push({
                    lastname: "last" + i,
                    firstname: "first" + i,
                    midinitial: "m",
                    gender: gender[i % 2],
                    street: "Street " + i,
                    city: cities[i % 3]
                });
            }
            var c1 = new Company({
                companyName: "Google",
                employees: employees
            });
            return c1.save().chain(function (company) {
                return comb
                    .when([
                        company.employees,
                        company.omahaEmployees,
                        company.lincolnEmployees
                    ]).chain(function (ret) {
                        assert.lengthOf(ret[0], 3);
                        assert.lengthOf(ret[1], 1);
                        assert.isTrue(ret[1].every(function (emp) {
                            return emp.city.match(/omaha/i) !== null;
                        }));
                        assert.lengthOf(ret[2], 1);
                        assert.isTrue(ret[2].every(function (emp) {
                            return emp.city.match(/lincoln/i) !== null;
                        }));
                    });
            });
        });

        it.should("have child associations when queried", function () {
            return Company.one().chain(function (company) {
                return company.employees.chain(function (emps) {
                    assert.lengthOf(emps, 3);
                });
            });
        });

        it.should("the child associations should also be associated to the parent ", function () {
            return Employee.all()
                .chain(function (emps) {
                    assert.lengthOf(emps, 3);
                    return comb.when([
                        emps[0].companies,
                        emps[1].companies
                    ]);
                })
                .chain(function (ret) {
                    assert.isTrue(ret[0].every(function (c) {
                        return c.companyName === "Google";
                    }));
                    assert.isTrue(ret[1].every(function (c) {
                        return c.companyName === "Google";
                    }));
                });
        });

    });

    it.describe("add methods", function (it) {

        it.beforeEach(function () {
            return Company.remove().chain(function () {
                return new Company({companyName: "Google"}).save();
            });
        });

        it.should("have an add method for filtered datasets", function () {
            return Company.one().chain(function (company) {
                var lincolnEmp = new Employee({
                    lastname: "last",
                    firstname: "first",
                    midInitial: "m",
                    gender: gender[0],
                    street: "Street",
                    city: "Lincoln"
                });
                var omahaEmp = new Employee({
                    lastname: "last",
                    firstname: "first",
                    midInitial: "m",
                    gender: gender[0],
                    street: "Street",
                    city: "Omaha"
                });
                return comb
                    .when([
                        company.addOmahaEmployee(omahaEmp),
                        company.addLincolnEmployee(lincolnEmp),
                    ])
                    .chain(function () {
                        return comb.when([
                            company.omahaEmployees,
                            company.lincolnEmployees
                        ]);
                    })
                    .chain(function (ret) {
                        assert.lengthOf(ret[0], 1);
                        assert.lengthOf(ret[1], 1);
                    });
            });
        });

        it.should("have a add multiple method for filtered associations", function () {
            var omahaEmployees = [], lincolnEmployees = [];
            for (var i = 0; i < 3; i++) {
                omahaEmployees.push({
                    lastname: "last" + i,
                    firstname: "first" + i,
                    midInitial: "m",
                    gender: gender[i % 2],
                    street: "Street " + i,
                    city: "Omaha"
                });
            }
            for (i = 0; i < 3; i++) {
                lincolnEmployees.push({
                    lastname: "last" + i,
                    firstname: "first" + i,
                    midInitial: "m",
                    gender: gender[i % 2],
                    street: "Street " + i,
                    city: "Lincoln"
                });
            }
            return Company.one().chain(function (company) {
                return comb
                    .when([
                        company.addOmahaEmployees(omahaEmployees),
                        company.addLincolnEmployees(lincolnEmployees),
                    ])
                    .chain(function () {
                        return comb.when([
                            company.omahaEmployees,
                            company.lincolnEmployees
                        ]);
                    })
                    .chain(function (ret) {
                        assert.lengthOf(ret[0], 3);
                        assert.lengthOf(ret[1], 3);
                    });
            });
        });

        it.should("filter many to many relationship to itself", function () {
            var vendors = [], buyers = [];
            for (var i = 0; i < 3; i++) {
                vendors.push({
                    companyName: "company" + i
                });
            }
            for (i = 3; i < 6; i++) {
                buyers.push({
                    companyName: "company" + i
                });
            }
            return Company.one()
                .chain(function (company) {
                    return comb
                        .when([
                            company.addBuyers(buyers),
                            company.addVendors(vendors)
                        ])
                        .chain(function () {
                            return comb.when([
                                company.vendorsDataset.filter({companyName: "company0"}).all(),
                                company.buyers
                            ]);
                        });
                })
                .chain(function (ret) {
                    return Company.findById(ret[0][0].id).chain(function (vendor) {
                        assert.lengthOf(ret[0], 1);
                        assert.equal(ret[0][0].companyName, vendor.companyName);
                        assert.lengthOf(ret[1], 3);
                    });
                });
        });

    });

    it.describe("remove methods", function (it) {
        var employees = [];
        for (var i = 0; i < 3; i++) {
            employees.push({
                lastname: "last" + i,
                firstname: "first" + i,
                midInitial: "m",
                gender: gender[i % 2],
                street: "Street " + i,
                city: cities[i % 3]
            });
        }
        it.beforeEach(function () {
            return comb.when([Company.remove(), Employee.remove()]).chain(function () {
                return new Company({companyName: "Google", employees: employees}).save();
            });
        });

        it.should("the removing of filtered associations and deleting them", function () {
            return Company.one().chain(function (company) {
                return comb
                    .when([
                        company.omahaEmployees,
                        company.lincolnEmployees
                    ])
                    .chain(function (emps) {
                        return comb.when([
                            company.removeOmahaEmployee(emps[0][0], true),
                            company.removeLincolnEmployee(emps[1][0], true)
                        ]);
                    })
                    .chain(function () {
                        return comb.when([
                            company.omahaEmployees,
                            company.lincolnEmployees,
                            Employee.count()
                        ]);
                    })
                    .chain(function (ret) {
                        var omahaEmps = ret[0], lincolnEmps = ret[1];
                        assert.lengthOf(omahaEmps, 0);
                        assert.lengthOf(lincolnEmps, 0);
                        assert.equal(ret[2], 1);
                    });
            });
        });

        it.should("the removing of filtered associations without deleting them", function () {
            return Company.one().chain(function (company) {
                return comb
                    .when([
                        company.omahaEmployees,
                        company.lincolnEmployees
                    ])
                    .chain(function (emps) {
                        return comb.when([
                            company.removeOmahaEmployee(emps[0][0]),
                            company.removeLincolnEmployee(emps[1][0])
                        ]);
                    })
                    .chain(function () {
                        return comb.when([
                            company.omahaEmployees,
                            company.lincolnEmployees,
                            Employee.count()
                        ]);
                    })
                    .chain(function (ret) {
                        var omahaEmps = ret[0], lincolnEmps = ret[1];
                        assert.lengthOf(omahaEmps, 0);
                        assert.lengthOf(lincolnEmps, 0);
                        assert.equal(ret[2], 3);
                    });
            });
        });

        it.should("the removing of filtered associations and deleting them", function () {
            return Company.one().chain(function (company) {
                return comb
                    .when([
                        company.omahaEmployees,
                        company.lincolnEmployees
                    ])
                    .chain(function (emps) {
                        return comb.when([
                            company.removeOmahaEmployees(emps[0], true),
                            company.removeLincolnEmployees(emps[1], true)
                        ]);
                    })
                    .chain(function () {
                        return comb.when([
                            company.omahaEmployees,
                            company.lincolnEmployees,
                            Employee.count()
                        ]);
                    })
                    .chain(function (ret) {
                        var omahaEmps = ret[0], lincolnEmps = ret[1];
                        assert.lengthOf(omahaEmps, 0);
                        assert.lengthOf(lincolnEmps, 0);
                        assert.equal(ret[2], 1);
                    });
            });
        });

        it.should("the removing of filtered associations without deleting them", function () {
            return Company.one().chain(function (company) {
                return comb
                    .when([
                        company.omahaEmployees,
                        company.lincolnEmployees
                    ])
                    .chain(function (emps) {
                        return comb.when([
                            company.removeOmahaEmployees(emps[0]),
                            company.removeLincolnEmployees(emps[1])
                        ]);
                    })
                    .chain(function () {
                        return comb.when([
                            company.omahaEmployees,
                            company.lincolnEmployees,
                            Employee.count()
                        ]);
                    })
                    .chain(function (ret) {
                        var omahaEmps = ret[0], lincolnEmps = ret[1];
                        assert.lengthOf(omahaEmps, 0);
                        assert.lengthOf(lincolnEmps, 0);
                        assert.equal(ret[2], 3);
                    });
            });
        });
    });

    it.afterAll(function () {
        return helper.dropModels();
    });
});