var vows = require('vows'),
    assert = require('assert'),
    helper = require("../../data/oneToOne/customFilter.eager.models"),
    patio = require("../../../lib"),
    comb = require("comb"),
    hitch = comb.hitch;

var ret = module.exports = exports = new comb.Promise();

var gender = ["M", "F"];
helper.loadModels().then(function () {
    var Works = patio.getModel("works"), Employee = patio.getModel("employee");
    var suite = vows.describe("One to One Eager association woth filter ");

    suite.addBatch({
        "A model":{
            topic:function () {
                return Employee
            },

            "should have associations":function () {
                assert.deepEqual(Employee.associations, ["works"]);
                assert.deepEqual(Works.associations, ["employee"]);
                var emp = new Employee();
                var work = new Works();
                assert.deepEqual(emp.associations, ["works"]);
                assert.deepEqual(work.associations, ["employee"]);
            }
        }
    });

    suite.addBatch({

        "When creating a employee ":{
            topic:function () {
                var e1 = new Employee({
                    lastname:"last" + 1,
                    firstname:"first" + 1,
                    midinitial:"m",
                    gender:gender[1 % 2],
                    street:"Street " + 1,
                    city:"City " + 1,
                    works:{
                        companyName:"Google",
                        salary:100000
                    }
                }).save().then(hitch(this, "callback", null), hitch(this, "callback"));
            },

            " the employee should work at google ":function (employee) {
                var works = employee.works;
                assert.equal(works.companyName, "Google");
                assert.equal(works.salary, 100000);
                return employee;
            }
        }

    });

    suite.addBatch({

        "When finding an employee":{
            topic:function () {
                Employee.one().then(hitch(this, "callback", null), hitch(this, "callback"));
            },


            " the employee should work at google ":function (employee) {
                var works = employee.works;
                assert.equal(works.companyName, "Google");
                assert.equal(works.salary, 100000);
                return employee;
            }
        }

    });

    suite.addBatch({

        "When finding workers":{
            topic:function () {
                Works.one().then(hitch(this, "callback", null), hitch(this, "callback"));
            },


            " the worker should work at google and have an associated employee":function (works) {
                var emp = works.employee;
                assert.equal(works.companyName, "Google");
                assert.equal(works.salary, 100000);
                assert.equal(emp.lastname, "last" + 1);
                assert.equal(emp.firstname, "first" + 1);
                assert.equal(emp.midinitial, "m");
                assert.equal(emp.gender, gender[1 % 2]);
                assert.equal(emp.street, "Street " + 1);
                assert.equal(emp.city, "City " + 1);
                return emp;
            }
        }

    });


    suite.addBatch({

        "When deleting an employee":{
            topic:function () {
                Employee.one().chain(
                    function (e) {
                        return e.remove();
                    }).chain(hitch(Works, "count"), hitch(this, "callback")).then(hitch(this, "callback", null), hitch(this, "callback"));
            },


            " the the works count should be 1 ":function (count) {
                assert.equal(count, 1);
            }
        }

    });

    suite.addBatch({

        "When creating a employee ":{
            topic:function () {
                var e1 = new Employee({
                    lastname:"last" + 1,
                    firstname:"first" + 1,
                    midinitial:"m",
                    gender:gender[1 % 2],
                    street:"Street " + 1,
                    city:"City " + 1,
                    works:{
                        companyName:"Google",
                        salary:100000
                    }
                }).save().then(hitch(this, "callback", null), hitch(this, "callback"));
            },

            " the employee should work at google ":{
                topic:function (employee) {
                    var works = employee.works;
                    assert.equal(works.companyName, "Google");
                    assert.equal(works.salary, 100000);
                    employee.works = null;
                    Employee.findById(employee.id).then(hitch(this, "callback", null), hitch(this, "callback"));
                },


                "and when setting works to null and not saving the employee should still work at google":{
                    topic:function (employee) {
                        var works = employee.works;
                        assert.instanceOf(employee, Employee);
                        assert.instanceOf(works, Works);
                        assert.equal(works.companyName, "Google");
                        assert.equal(works.salary, 100000);
                        employee.works = null;
                        comb.executeInOrder(employee, Employee,
                            function (employee, Employee) {
                                employee.save();
                                return Employee.findById(employee.id);
                            }).then(hitch(this, "callback", null), hitch(this, "callback"));
                    },
                    "but when setting works to null and saving works should be null":function (employee) {
                        assert.isNull(employee.works);
                    }
                }
            }
        }
    });

    suite.addBatch({

        "When creating a employee ":{
            topic:function () {
                var e1 = new Employee({
                    lastname:"last" + 1,
                    firstname:"first" + 1,
                    midinitial:"m",
                    gender:gender[1 % 2],
                    street:"Street " + 1,
                    city:"City " + 1,
                    works:{
                        companyName:"Google",
                        salary:100000
                    }
                }).save().then(hitch(this, "callback", null), hitch(this, "callback"));
            },

            " the employee should work at google ":{
                topic:function (employee) {
                    var works = employee.works
                    assert.equal(works.companyName, "Google");
                    assert.equal(works.salary, 100000);
                    works.employee = null;
                    Works.findById(works.id).then(hitch(this, "callback", null), hitch(this, "callback"));
                },


                " and when setting employee to null on works and not saving works should still have an employee":{
                    topic:function (works) {
                        var employee = works.employee;
                        assert.instanceOf(employee, Employee);
                        assert.instanceOf(works, Works);
                        assert.equal(works.companyName, "Google");
                        assert.equal(works.salary, 100000);
                        works.employee = null;
                        works.save().chain(hitch(Works, "findById", works.id), hitch(this, "callback"))
                            .then(hitch(this, "callback", null, employee), hitch(this, "callback"));
                    },
                    "but when setting employee to null and saving":{
                        topic:function (employee, works) {
                            var emp = works.employee;
                            comb.executeInOrder(employee, Employee,
                                function (employee, Employee) {
                                    var newEmp = Employee.findById(employee.id);
                                    var nullWorks = newEmp.works;
                                    return {emp:emp, employee:newEmp, nullWorks:nullWorks };
                                }).then(hitch(this, "callback", null), hitch(this, "callback"));
                        },

                        "employee should be null but employee should still exists but not work anywhere":function (res) {
                            assert.isNull(res.emp);
                            assert.instanceOf(res.employee, Employee);
                            assert.isNull(res.nullWorks);
                        }
                    }
                }
            }
        }

    });


    suite.run({reporter:require("vows").reporter.spec}, function () {
        helper.dropModels().then(comb.hitch(ret, "callback"), comb.hitch(ret, "errback"));
    });

});
