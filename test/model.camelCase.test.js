var vows = require('vows'),
    assert = require('assert'),
    helper = require("./data/model.helper.js"),
    patio = require("index"),
    sql = patio.SQL,
    comb = require("comb"),
    hitch = comb.hitch;

var ret = module.exports = new comb.Promise();
var suite = vows.describe("model object");

var gender = ["M", "F"];

var Employee = patio.addModel("employee", {
    "static" : {
        camelize : true,
        //class methods
        findByGender : function(gender, callback, errback) {
            return this.filter({gender : gender}).all();
        }
    }
});

helper.createSchemaAndSync(true).then(function() {
    suite.addBatch({
        "should save an employee" : {
            topic : function() {
                var emp = new Employee({
                    firstName : "doug",
                    lastName : "martin",
                    position : 1,
                    midInitial : null,
                    gender : "M",
                    street : "1 nowhere st.",
                    city : "NOWHERE"}).save().then(hitch(this, "callback", null), hitch(this, "callback"));
            },

            " and get a list of one employees" : function(emp) {
                assert.instanceOf(emp, Employee);
                assert.equal("doug", emp.firstName);
                assert.equal("martin", emp.lastName);
                assert.isNull(emp.midInitial);
                assert.equal("M", emp.gender);
                assert.equal("1 nowhere st.", emp.street);
                assert.equal("NOWHERE", emp.city);
            }


        }
    });

    suite.addBatch({
        "should save a batch of employees" : {
            topic : function() {
                var emps = [];
                for (var i = 0; i < 20; i++) {
                    emps.push({
                        lastName : "last" + i,
                        position : i,
                        firstName : "first" + i,
                        midInitial : "m",
                        gender : gender[i % 2],
                        street : "Street " + i,
                        city : "City " + i
                    });
                }
                comb.executeInOrder(Employee,
                    function(emp) {
                        emp.truncate();
                        var ret = {};
                        ret.employees = emp.save(emps);
                        ret.count = emp.count();
                        return ret;
                    }).then(hitch(this, "callback", null), hitch(this, "callback"));
            },

            " and get a get a count of 20 and 20 different emplyees" : function(ret) {
                assert.equal(ret.count, 20);
                assert.lengthOf(ret.employees, 20);
                ret.employees.forEach(function(emp, i) {
                    assert.equal(emp.lastName, "last" + i);
                    assert.equal(emp.firstName, "first" + i);
                    assert.equal(emp.midInitial, "m");
                    assert.equal(emp.gender, gender[i % 2]);
                    assert.equal(emp.street, "Street " + i);
                    assert.equal(emp.city, "City " + i);
                });
            }


        }
    });

    suite.addBatch({
        "A Model " : {

            topic : function() {
                var emps = [];
                for (var i = 0; i < 20; i++) {
                    emps.push({
                        lastName : "last" + i,
                        firstName : "first" + i,
                        position : i,
                        midInitial : "m",
                        gender : gender[i % 2],
                        street : "Street " + i,
                        city : "City " + i
                    });
                }
                comb.executeInOrder(Employee,
                    function(emp) {
                        emp.truncate();
                        emp.save(emps);
                    }).then(hitch(this, "callback", null), hitch(this, "callback"));
            },

            "Should reload"  : {
                topic : function() {
                    comb.executeInOrder(Employee, function(Employee){
                        var emp = Employee.findById(1);
                        emp.lastName = "martin";
                        return emp.reload();
                    }).then(hitch(this, "callback", null), hitch(this, "callback"));
                },

                "and return employees" : function(topic) {
                    assert.instanceOf(topic, Employee);
                    assert.equal(topic.id, 1);
                    assert.equal(topic.lastName, "last0");
                }
            },

            "Should findById"  : {
                topic : function() {
                    Employee.findById(1).then(hitch(this, "callback", null), hitch(this, "callback"));
                },

                "and return employees" : function(topic) {
                    assert.instanceOf(topic, Employee);
                    assert.equal(topic.id, 1);
                }
            },

            "Should filter"  : {
                topic : function() {
                    var id = sql.identifier("id");
                    comb.executeInOrder(Employee,
                        function(Employee) {
                            var ret = {};
                            ret.query1 = Employee.filter({id : [1,2,3,4,5,6]}).all();
                            ret.query2 = Employee.filter(id.gt(5), id.lt(11)).order("id").last();
                            ret.query3 = Employee.filter(
                                function() {
                                    return this.firstName.like(/first1[1|2]*$/);
                                }).order("firstName").all();
                            ret.query4 = Employee.filter({id : {between : [1,5]}}).order("id").all();
                            ret.query5 = [];
                            Employee.filter(
                                function() {
                                    return this.id.gt(15);
                                }).forEach(
                                function(emp) {
                                    ret.query5.push(emp);
                                });
                            return ret;

                        }).then(hitch(this, "callback", null), hitch(this, "callback"));
                },

                "and return employees" : function(topic) {
                    var i = 1;
                    var query1 = topic.query1, query2 = topic.query2, query3 = topic.query3, query4 = topic.query4, query5 = topic.query5, query6 = topic.query6;
                    assert.lengthOf(query1, 6);
                    query1.forEach(function(t) {
                        assert.instanceOf(t, Employee);
                        assert.equal(i++, t.id);
                    });
                    assert.equal(query2.id, 10);
                    assert.lengthOf(query3, 3);
                    assert.instanceOf(query3[0], Employee);
                    assert.equal(query3[0].firstName, "first1");
                    assert.instanceOf(query3[1], Employee);
                    assert.equal(query3[1].firstName, "first11");
                    assert.instanceOf(query3[2], Employee);
                    assert.equal(query3[2].firstName, "first12");
                    assert.deepEqual(query4.map(function(e) {
                        assert.instanceOf(e, Employee);
                        return e.id;
                    }), [1,2,3,4,5]);
                    assert.deepEqual(query5.map(function(e) {
                        assert.instanceOf(e, Employee);
                        return e.id;
                    }), [16,17,18,19,20]);


                }
            },

            "Should find by gender"  : {
                topic : function() {
                    var self = this;
                    Employee.findByGender("F").then(hitch(this, "callback", null), hitch(this, "callback"));
                },

                "and return female employees" : function(topic) {
                    topic.forEach(function(emp) {
                        assert.instanceOf(emp, Employee);
                        assert.equal("F", emp.gender);
                    });
                }
            },

            "Should count employees"  : {
                topic : function() {
                    Employee.count().then(hitch(this, "callback", null), hitch(this, "callback"));
                },

                "and return 20" : function(topic) {
                    assert.equal(20, topic);
                }
            },

            "Should find all employees"  : {
                topic : function() {
                    Employee.all().then(hitch(this, "callback", null), hitch(this, "callback"));
                },

                "and return 20 employees" : function(topic) {
                    assert.lengthOf(topic, 20);
                    topic.forEach(function(e) {
                        assert.instanceOf(e, Employee);
                    });
                }
            },

            "Should map all employees"  : {
                topic : function() {
                    comb.executeInOrder(Employee,
                        function(Employee) {
                            var ret = {};
                            ret.query1 = Employee.map("id");
                            ret.query2 = Employee.order("position").map(function(e) {
                                return e.firstName + " " + e.lastName;
                            });
                            return ret;
                        }).then(hitch(this, "callback", null), hitch(this, "callback"));

                },

                "and return 20 ids" : function(topic) {
                    assert.lengthOf(topic.query1, 20);
                    topic.query1.forEach(function(id, i) {
                        assert.equal(id, i + 1);
                    });
                    assert.lengthOf(topic.query2, 20);
                    topic.query2.forEach(function(name, i) {
                        assert.equal(name, "first" + i + " last" + i);
                    });

                }
            },


            "Should loop through all employees"  : {
                topic : function() {
                    var ret = [];
                    var d = Employee.forEach(
                        function(emp) {
                            ret.push(emp);
                        }).then(hitch(this, "callback", null, ret), hitch(this, "callback"));
                },

                "and return 20 employees" : function(topic) {
                    assert.lengthOf(topic, 20);
                    topic.forEach(function(e) {
                        assert.instanceOf(e, Employee);
                    });
                }
            },

            "Should find first employee"  : {
                topic : function() {
                    var d = Employee.one().then(hitch(this, "callback", null), hitch(this, "callback"));
                },

                "and return employee with id of 1" : function(topic) {
                    assert.instanceOf(topic, Employee);
                    assert.equal(1, topic.id);
                }
            },

            "Should find first employee with query"  : {
                topic : function() {
                    var self = this;
                    this.count = 1;
                    var id = sql.identifier("id");
                    var d = Employee.first(id.gt(5), id.lt(11)).then(hitch(this, "callback", null), hitch(this, "callback"));
                },

                "and return employee with id of 1" : function(topic) {
                    assert.instanceOf(topic, Employee);
                    assert.equal(topic.id, 6);
                }
            },

            "Should find last employee"  : {
                topic : function() {
                    var self = this;
                    var d = Employee.order("firstName").last().then(hitch(this, "callback", null), hitch(this, "callback"));
                },

                "and return employee a first name of first9" : function(topic) {
                    assert.throws(hitch(Employee, "last"));
                    assert.instanceOf(topic, Employee);
                    assert.equal(topic.firstName, "first9");
                }
            }
        }
    });
    suite.addBatch({
        "Should save an employee"  : {
            topic : function() {
                Employee.save({
                    firstName : "doug",
                    lastName : "martin",
                    position : 21,
                    midInitial : null,
                    gender : "M",
                    street : "1 nowhere st.",
                    city : "NOWHERE"
                }).then(hitch(this, "callback", null), hitch(this, "callback"));
            },

            "and the employee should have an id" : function(emp) {
                assert.instanceOf(emp, Employee);
                assert.isNumber(emp.id);
            },

            "Should be able to update the employee" : {
                topic : function(emp) {
                    emp.firstName = "douglas";
                    emp.update().then(hitch(this, "callback", null), hitch(this, "callback"));
                },

                "and when querying the employee it should be updated" : {

                    topic : function(e, emp) {
                        assert.instanceOf(e, Employee);
                        assert.equal(e.firstName, "douglas");
                        Employee.one({id : emp.id}).then(hitch(this, "callback", null), hitch(this, "callback"));
                    },

                    " with the new name" : function(emp) {
                        assert.instanceOf(emp, Employee);
                        assert.isNumber(emp.id);
                        assert.equal(emp.firstName, "douglas");
                    },

                    "Should be able to delete the employee" :  {
                        topic : function(e, emp) {
                            emp.remove().then(hitch(this, "callback", null, emp), hitch(this, "callback"));
                        },

                        "and when when querying the deleted employee" : {

                            topic : function(emp) {
                                var self = this;
                                Employee.filter({id : emp.id}).one().then(hitch(this, "callback", null), hitch(this, "callback"));
                            },

                            "it should be null" : function(topic) {
                                assert.isNull(topic);
                            }
                        }
                    }
                }

            }
        }
    });

    suite.addBatch({
        "Should do a batch update" : {
            topic : function() {
                comb.executeInOrder(Employee,
                    function(Employee) {
                        Employee.update({firstName : "doug"});
                        return Employee.all();
                    }).then(hitch(this, "callback", null), hitch(this, "callback"));
            },

            " all records should be updated" : function(records) {
                assert.lengthOf(records, 20);
                records.forEach(function(r) {
                    assert.equal(r.firstName, "doug");
                });
            }
        }
    });

    suite.addBatch({
        "Should do an update on a single record" : {
            topic : function() {
                comb.executeInOrder(Employee,
                    function(Employee) {
                        Employee.update({firstName : "dougie"}, {id: 2});
                        return Employee.filter({id:2}).one();
                    }).then(hitch(this, "callback", null), hitch(this, "callback"));
            },

            " all records should be updated" : function(emp) {
                assert.instanceOf(emp, Employee);
                assert.equal(emp.firstName, "dougie");
            }
        }
    });

    suite.run({reporter : require("vows").reporter.spec}, function() {
        helper.dropModels().then(comb.hitch(ret, "callback"), comb.hitch(ret, "errback"));
    });
}, function(err) {
    throw err;
});


