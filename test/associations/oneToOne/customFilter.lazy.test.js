var it = require('it'),
    assert = require('assert'),
    helper = require("../../data/oneToOne.helper.js"),
    patio = require("index"),
    comb = require("comb"),
    Promise = comb.Promise,
    hitch = comb.hitch;


var gender = ["M", "F"];
it.describe("One To One lazy with custom filter", function (it) {
    var Works, Employee;
    it.beforeAll(function () {
        Works = patio.addModel("works", {
            "static":{
                init:function () {
                    this._super(arguments);
                    this.manyToOne("employee");
                }
            }
        });
        Employee = patio.addModel("employee", {
            "static":{
                init:function () {
                    this._super(arguments);
                    this.oneToOne("works", function (ds) {
                        return ds.filter(function () {
                            return this.salary.gte(100000.00);
                        });
                    });
                }
            }
        });
        return helper.createSchemaAndSync(true);
    });


    it.should("have associations", function () {
        assert.deepEqual(Employee.associations, ["works"]);
        assert.deepEqual(Works.associations, ["employee"]);
        var emp = new Employee();
        var work = new Works();
        assert.deepEqual(emp.associations, ["works"]);
        assert.deepEqual(work.associations, ["employee"]);
    });

    it.describe("create a new model with association", function (it) {

        it.beforeAll(function () {
            return comb.when(
                Employee.remove(),
                Works.remove()
            );
        });


        it.should("save nested models when using new", function (next) {
            var employee = new Employee({
                lastName:"last" + 1,
                firstName:"first" + 1,
                midInitial:"m",
                gender:gender[1 % 2],
                street:"Street " + 1,
                city:"City " + 1,
                works:{
                    companyName:"Google",
                    salary:100000
                }
            });
            employee.save().then(function () {
                employee.works.then(function (works) {
                    assert.equal(works.companyName, "Google");
                    assert.equal(works.salary, 100000);
                    next();
                }, next);
                next();
            }, next);
        });
    });

    it.context(function (it) {

        it.beforeEach(function () {
            return comb.serial([
                hitch(Employee, "remove"),
                hitch(Works, "remove"),
                function () {
                    return new Employee({
                        lastName:"last" + 1,
                        firstName:"first" + 1,
                        midInitial:"m",
                        gender:gender[1 % 2],
                        street:"Street " + 1,
                        city:"City " + 1,
                        works:{
                            companyName:"Google",
                            salary:100000
                        }
                    }).save();
                }
            ]);
        });

        it.should("load associations when querying", function (next) {
            comb.when(Employee.one(), Works.one()).then(function (res) {
                var emp = res[0], work = res[1];
                var empWorks = emp.works, worksEmp = work.employee;
                assert.isPromiseLike(empWorks);
                assert.isPromiseLike(worksEmp);
                comb.when(empWorks, worksEmp).then(function (res) {
                    assert.instanceOf(res[1], Employee);
                    assert.instanceOf(res[0], Works);
                    next();
                }, next);
            }, next);
        });

        it.should("allow the removing of associations", function (next) {
            Employee.one().then(function (emp) {
                emp.works = null;
                emp.save().then(function (emp) {
                    emp.works.then(function (works) {
                        assert.isNull(works);
                        Works.one().then(function (works) {
                            assert.isNotNull(works);
                            works.employee.then(function (emp) {
                                assert.isNotNull(works.employee);
                                next();
                            }, next);
                        }, next);
                    }, next);
                }, next);
            }, next);
        });

        it.should("apply the filter", function (next) {
            Employee.one().then(function (emp) {
                emp.works.then(function (works) {
                    works.save({salary:10}).then(function () {
                        emp.reload().then(function () {
                            emp.works.then(function (works) {
                                assert.isNull(works);
                                next();
                            }, next);
                        }, next);
                    }, next);
                }, next);
            }, next);
        });

    });

    it.context(function () {
        it.beforeEach(function () {
            return comb.when(
                Works.remove(),
                Employee.remove()
            );
        });

        it.should("allow the setting of associations", function (next) {
            var emp = new Employee({
                lastName:"last" + 1,
                firstName:"first" + 1,
                midInitial:"m",
                gender:gender[1 % 2],
                street:"Street " + 1,
                city:"City " + 1
            });
            emp.save().then(function () {
                emp.works.then(function (works) {
                    assert.isNull(works);
                    emp.works = {
                        companyName:"Google",
                        salary:100000
                    };
                    emp.save().then(function () {
                        emp.works.then(function (works) {
                            assert.instanceOf(works, Works);
                            next();
                        }, next);
                    }, next);
                }, next);
            }, next);
        });

        it.should("not delete association when deleting the reciprocal side", function (next) {
            var e = new Employee({
                lastName:"last" + 1,
                firstName:"first" + 1,
                midInitial:"m",
                gender:gender[1 % 2],
                street:"Street " + 1,
                city:"City " + 1,
                works:{
                    companyName:"Google",
                    salary:100000
                }
            });
            e.save().then(function () {
                e.remove().then(function () {
                    comb.when(Employee.all(), Works.all()).then(function (res) {
                        assert.lengthOf(res[0], 0);
                        assert.lengthOf(res[1], 1);
                        next();
                    }, next);
                }, next);
            }, next);
        });

    });

    it.afterAll(function () {
        return helper.dropModels();
    });
});