var it = require('it'),
    assert = require('assert'),
    helper = require("./data/model.helper.js"),
    patio = require("index"),
    sql = patio.SQL,
    comb = require("comb"),
    hitch = comb.hitch;

var gender = ["M", "F"];


it.describe("A model with properites", function (it) {

    var Employee;
    it.beforeAll(function () {
        Employee = patio.addModel("employee", {
            static:{
                //class methods
                findByGender:function (gender, callback, errback) {
                    return this.filter({gender:gender}).all();
                }
            }
        });
        return helper.createSchemaAndSync();
    });

    it.should("type cast properties", function () {
        var emp = new Employee({
            firstname:"doug",
            lastname:"martin",
            position:1,
            midinitial:null,
            gender:"M",
            street:"1 nowhere st.",
            city:"NOWHERE",
            bufferType:"buffer data",
            textType:"text data",
            blobType:"blob data"
        });
        assert.isString(emp.firstname);
        assert.isString(emp.lastname);
        assert.isNumber(emp.position);
        assert.isNull(emp.midinitial);
        assert.isString(emp.gender);
        assert.isString(emp.street);
        assert.isString(emp.city);
        assert.isTrue(Buffer.isBuffer(emp.bufferType));
        assert.isTrue(Buffer.isBuffer(emp.textType));
        assert.isTrue(Buffer.isBuffer(emp.blobType));
    });


    it.should("save properly", function (next) {
        var emp = new Employee({
            firstname:"doug",
            lastname:"martin",
            position:1,
            midinitial:null,
            gender:"M",
            street:"1 nowhere st.",
            city:"NOWHERE",
            bufferType:"buffer data",
            textType:"text data",
            blobType:"blob data"
        });
        emp.save().then(function () {
            assert.instanceOf(emp, Employee);
            assert.equal(emp.firstname, "doug");
            assert.equal(emp.lastname, "martin");
            assert.isNull(emp.midinitial);
            assert.equal(emp.gender, "M");
            assert.equal(emp.street, "1 nowhere st.");
            assert.equal(emp.city, "NOWHERE");
            assert.deepEqual(emp.bufferType, new Buffer("buffer data"));
            assert.deepEqual(emp.textType, new Buffer("text data"));
            assert.deepEqual(emp.blobType, new Buffer("blob data"));
            next();
        }, next);
    });

    it.should("save multiple models", function (next) {
        var emps = [];
        for (var i = 0; i < 20; i++) {
            emps.push({
                lastname:"last" + i,
                position:i,
                firstname:"first" + i,
                midinitial:"m",
                gender:gender[i % 2],
                street:"Street " + i,
                city:"City " + i
            });
        }
        comb.executeInOrder(Employee,
            function (emp) {
                emp.truncate();
                var ret = {};
                ret.employees = emp.save(emps);
                ret.count = emp.count();
                return ret;
            }).then(function (ret) {
                assert.equal(ret.count, 20);
                assert.lengthOf(ret.employees, 20);
                ret.employees.forEach(function (emp, i) {
                    assert.equal(emp.lastname, "last" + i);
                    assert.equal(emp.firstname, "first" + i);
                    assert.equal(emp.midinitial, "m");
                    assert.equal(emp.gender, gender[i % 2]);
                    assert.equal(emp.street, "Street " + i);
                    assert.equal(emp.city, "City " + i);
                });
                next();
            }, next);
    });

    it.context(function (it) {

        it.beforeEach(function () {
            var emps = [];
            for (var i = 0; i < 20; i++) {
                emps.push({
                    lastname:"last" + i,
                    firstname:"first" + i,
                    position:i,
                    midInitial:"m",
                    gender:gender[i % 2],
                    street:"Street s" + i,
                    city:"City " + i
                });
            }
            return comb.executeInOrder(Employee, function (emp) {
                emp.truncate();
                emp.save(emps);
            });
        });

        it.should("should reload models", function (next) {
            comb.executeInOrder(Employee,function (Employee) {
                var emp = Employee.findById(1);
                emp.lastname = "martin";
                return emp.reload();
            }).then(function (emp) {
                    assert.instanceOf(emp, Employee);
                    assert.equal(emp.id, 1);
                    assert.equal(emp.lastname, "last0");
                    next();
                }, next)
        });

        it.should("find models by id", function (next) {
            Employee.findById(1).then(function (emp) {
                assert.instanceOf(emp, Employee);
                assert.equal(emp.id, 1);
                next();
            }, next);
        });

        it.should("support the filtering of models", function (next) {
            var id = sql.identifier("id");
            comb.executeInOrder(Employee,
                function (Employee) {
                    var ret = {};
                    ret.query1 = Employee.filter({id:[1, 2, 3, 4, 5, 6]}).all();
                    ret.query2 = Employee.filter(id.gt(5), id.lt(11)).order("id").last();
                    ret.query3 = Employee.filter(
                        function () {
                            return this.firstname.like(/first1[1|2]*$/);
                        }).order("firstname").all();
                    ret.query4 = Employee.filter({id:{between:[1, 5]}}).order("id").all();
                    ret.query5 = [];
                    Employee.filter(
                        function () {
                            return this.id.gt(15);
                        }).forEach(
                        function (emp) {
                            ret.query5.push(emp);
                        });
                    return ret;

                }).then(function (ret) {
                    var i = 1;
                    var query1 = ret.query1, query2 = ret.query2, query3 = ret.query3, query4 = ret.query4, query5 = ret.query5, query6 = ret.query6;
                    assert.lengthOf(query1, 6);
                    query1.forEach(function (t) {
                        assert.instanceOf(t, Employee);
                        assert.equal(i++, t.id);
                    });
                    assert.equal(query2.id, 10);
                    assert.lengthOf(query3, 3);
                    assert.instanceOf(query3[0], Employee);
                    assert.equal(query3[0].firstname, "first1");
                    assert.instanceOf(query3[1], Employee);
                    assert.equal(query3[1].firstname, "first11");
                    assert.instanceOf(query3[2], Employee);
                    assert.equal(query3[2].firstname, "first12");
                    assert.deepEqual(query4.map(function (e) {
                        assert.instanceOf(e, Employee);
                        return e.id;
                    }), [1, 2, 3, 4, 5]);
                    assert.deepEqual(query5.map(function (e) {
                        assert.instanceOf(e, Employee);
                        return e.id;
                    }), [16, 17, 18, 19, 20]);
                    next();
                }, next);
        });

        it.should("support custom query methods", function (next) {
            Employee.findByGender("F").then(function (emps) {
                emps.forEach(function (emp) {
                    assert.instanceOf(emp, Employee);
                    assert.equal("F", emp.gender);
                });
                next();
            }, next);
        });


        it.describe("dataset methods", function () {

            it.should("support count", function (next) {
                Employee.count().then(function (count) {
                    assert.equal(count, 20);
                    next();
                }, next);
            });

            it.should("support all", function (next) {
                Employee.all().then(function (emps) {
                    assert.lengthOf(emps, 20);
                    emps.forEach(function (e) {
                        assert.instanceOf(e, Employee);
                    });
                    next();
                }, next);
            });


            it.should("support map", function (next) {
                comb.executeInOrder(Employee,
                    function (Employee) {
                        var ret = {};
                        ret.query1 = Employee.map("id");
                        ret.query2 = Employee.order("position").map(function (e) {
                            return e.firstname + " " + e.lastname;
                        });
                        return ret;
                    }).then(function (res) {
                        assert.lengthOf(res.query1, 20);
                        res.query1.forEach(function (id, i) {
                            assert.equal(id, i + 1);
                        });
                        assert.lengthOf(res.query2, 20);
                        res.query2.forEach(function (name, i) {
                            assert.equal(name, "first" + i + " last" + i);
                        });
                        next();
                    }, next);
            });

            it.should("support forEach", function (next) {
                var ret = [];
                Employee.forEach(function (emp) {
                    ret.push(emp);
                }).then(function (topic) {
                        assert.lengthOf(topic, 20);
                        ret.forEach(function (e) {
                            assert.instanceOf(e, Employee);
                        });
                        next();
                    }, next);
            });

            it.should("support one", function (next) {
                Employee.one().then(function (emp) {
                    assert.instanceOf(emp, Employee);
                    assert.equal(emp.id, 1);
                    next();
                }, next);
            });

            it.should("support first", function (next) {
                var id = sql.identifier("id");
                Employee.first(id.gt(5), id.lt(11)).then(function (emp) {
                    assert.instanceOf(emp, Employee);
                    assert.equal(emp.id, 6);
                    next();
                });
            });

            it.should("support last", function (next) {
                Employee.order("firstname").last().then(function (emp) {
                    assert.throws(hitch(Employee, "last"));
                    assert.instanceOf(emp, Employee);
                    assert.equal(emp.firstname, "first9");
                    next();
                }, next);
            });

        });
    });


    it.context(function (it) {
        var emp;
        it.beforeEach(function () {
            return comb.serial([
                hitch(Employee, "remove"),
                function () {

                    emp = new Employee({
                        firstname:"doug",
                        lastName:"martin",
                        position:21,
                        midInitial:null,
                        gender:"M",
                        street:"1 nowhere st.",
                        city:"NOWHERE"
                    });
                    return emp.save();
                }
            ]);
        });

        it.should("support updates", function (next) {
            emp.firstname = "douglas";
            emp.update().then(function (e) {
                assert.equal(e.firstname, "douglas");
                Employee.one({id:emp.id}).then(function () {
                    assert.isNumber(emp.id);
                    assert.equal(emp.firstname, "douglas");
                    next();
                }, next);
            }, next);
        });


        it.should("support remove", function (next) {
            var id = emp.id;
            emp.remove().then(function () {
                Employee.filter({id:id}).one().then(function (e) {
                    assert.isNull(e);
                    next();
                }, next);
            }, next);
        });

        it.should("support support batch updates", function (next) {
            Employee.all().then(function (records) {
                assert.lengthOf(records, 1);
                records.forEach(function (r) {
                    assert.equal(r.firstname, "doug");
                });
                next();
            }, next);
        });

        it.should("support filters on batch updates", function (next) {
            comb.executeInOrder(Employee,function (Employee) {
                Employee.update({firstname:"dougie"}, {id:24});
                return Employee.filter({id:24}).one();
            }).then(function (emp) {
                    assert.instanceOf(emp, Employee);
                    assert.equal(emp.firstname, "dougie");
                    next();
                }, next);
        });


    });

    it.afterAll(function () {
        return helper.dropModels();
    });

});


