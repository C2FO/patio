var it = require('it'),
    assert = require('assert'),
    helper = require("./data/model.helper.js"),
    patio = require("../lib"),
    sql = patio.SQL,
    comb = require("comb"),
    hitch = comb.hitch;

var gender = ["M", "F"];

it.describe("A model with camelized properites", function (it) {

    var Employee;
    it.beforeAll(function () {
        Employee = patio.addModel("employee", {
            "static": {
                camelize: true,
                //class methods
                findByGender: function (gender, callback, errback) {
                    return this.filter({gender: gender}).all();
                }
            }
        });
        return helper.createSchemaAndSync(true);
    });


    it.should("type cast properties", function () {
        var emp = new Employee({
            firstName: "doug",
            lastName: "martin",
            position: 1,
            midInitial: null,
            gender: "M",
            street: "1 nowhere st.",
            city: "NOWHERE",
            bufferType: "buffer data",
            textType: "text data",
            blobType: "blob data"
        });
        assert.isString(emp.firstName);
        assert.isString(emp.lastName);
        assert.isNumber(emp.position);
        assert.isNull(emp.midInitial);
        assert.isString(emp.gender);
        assert.isString(emp.street);
        assert.isString(emp.city);
        assert.isTrue(Buffer.isBuffer(emp.bufferType));
        assert.isString(emp.textType);
        assert.isTrue(Buffer.isBuffer(emp.blobType));
    });


    it.should("save properly", function () {
        var emp = new Employee({
            firstName: "doug",
            lastName: "martin",
            position: 1,
            midInitial: null,
            gender: "M",
            street: "1 nowhere st.",
            city: "NOWHERE",
            bufferType: "buffer data",
            textType: "text data",
            blobType: "blob data"
        });
        return emp.save().chain(function () {
            assert.instanceOf(emp, Employee);
            assert.equal(emp.firstName, "doug");
            assert.equal(emp.lastName, "martin");
            assert.isNull(emp.midInitial);
            assert.equal(emp.gender, "M");
            assert.equal(emp.street, "1 nowhere st.");
            assert.equal(emp.city, "NOWHERE");
            assert.deepEqual(emp.bufferType, new Buffer("buffer data"));
            assert.deepEqual(emp.textType, "text data");
            assert.deepEqual(emp.blobType, new Buffer("blob data"));
        });
    });

    it.should("emit events", function () {
        var emp = new Employee({
            firstName: "doug",
            lastName: "martin",
            position: 1,
            midInitial: null,
            gender: "M",
            street: "1 nowhere st.",
            city: "NOWHERE",
            bufferType: "buffer data",
            textType: "text data",
            blobType: "blob data"
        });
        var emitCount = 0;
        var callback = function () {
            emitCount++;
        };
        var events = ["save", "update", "remove"];
        events.forEach(function (e) {
            emp.on(e, callback);
            Employee.on(e, callback);
        });
        return emp.save()
            .chain(function () {
                return emp.update({firstName: "ben"});
            })
            .chain(function () {
                return emp.remove();
            })
            .chain(function () {
                assert.equal(emitCount, 6);
                events.forEach(function (e) {
                    emp.removeListener(e, callback);
                    Employee.removeListener(e, callback);
                });
            });

    });

    it.should("save multiple models", function () {
        var emps = [];
        for (var i = 0; i < 20; i++) {
            emps.push({
                lastName: "last" + i,
                position: i,
                firstName: "first" + i,
                midInitial: "m",
                gender: gender[i % 2],
                street: "Street " + i,
                city: "City " + i
            });
        }
        return Employee.truncate().chain(function () {
            return Employee.save(emps);
        }).chain(function (employees) {
                assert.lengthOf(employees, 20);
                employees.forEach(function (emp, i) {
                    assert.equal(emp.lastName, "last" + i);
                    assert.equal(emp.firstName, "first" + i);
                    assert.equal(emp.midInitial, "m");
                    assert.equal(emp.gender, gender[i % 2]);
                    assert.equal(emp.street, "Street " + i);
                    assert.equal(emp.city, "City " + i);
                });
                return Employee.count();
            })
            .chain(function (count) {
                assert.equal(count, 20);
            });
    });

    it.context(function (it) {

        var emps;
        it.beforeEach(function () {
            emps = [];
            for (var i = 0; i < 20; i++) {
                emps.push(new Employee({
                    lastName: "last" + i,
                    firstName: "first" + i,
                    position: i,
                    midInitial: "m",
                    gender: gender[i % 2],
                    street: "Street s" + i,
                    city: "City " + i
                }));
            }
            return Employee.truncate().chain(function () {
                return Employee.save(emps);
            });
        });

        it.should("should reload models", function () {
            return Employee.first().chain(function (emp) {
                var orig = emp.lastName;
                emp.lastName = "martin";
                return emp.reload().chain(function () {
                    assert.instanceOf(emp, Employee);
                    assert.equal(emp.lastName, orig);
                });
            });
        });

        it.should("find models by id", function () {
            return Employee.findById(emps[0].id).chain(function (emp) {
                assert.instanceOf(emp, Employee);
                assert.equal(emp.id, emps[0].id);
            });
        });

        it.should("support the filtering of models", function () {
            var id = sql.identifier("id"), ids = emps.map(function (emp) {
                return emp.id;
            });
            return Employee.filter({id: ids.slice(0, 6)}).all()
                .chain(function (query1) {
                    var i = 0;
                    assert.lengthOf(query1, 6);
                    query1.forEach(function (t) {
                        assert.instanceOf(t, Employee);
                        assert.equal(ids[i++], t.id);
                    });
                    return Employee.filter(id.gt(ids[0]), id.lt(ids[5])).order("id").last();
                })
                .chain(function (query2) {
                    assert.equal(query2.id, ids[4]);
                    return Employee.filter(function () {
                        return this.firstName.like(/first1[1|2]*$/);
                    }).order("firstName").all();
                })
                .chain(function (query3) {
                    assert.lengthOf(query3, 3);
                    assert.instanceOf(query3[0], Employee);
                    assert.equal(query3[0].firstName, "first1");
                    assert.instanceOf(query3[1], Employee);
                    assert.equal(query3[1].firstName, "first11");
                    assert.instanceOf(query3[2], Employee);
                    assert.equal(query3[2].firstName, "first12");
                    return Employee.filter({id: {between: [ids[0], ids[5]]}}).order("id").all();
                })
                .chain(function (query4) {
                    assert.deepEqual(query4.map(function (e) {
                        assert.instanceOf(e, Employee);
                        return e.id;
                    }), ids.slice(0, 6));
                    return Employee.filter(function () {
                        return this.id.gt(ids[5]);
                    }).all();
                })
                .chain(function (query5) {
                    return assert.deepEqual(query5.map(function (e) {
                        assert.instanceOf(e, Employee);
                        return e.id;
                    }), ids.slice(6));
                });
        });

        it.should("support custom query methods", function () {
            return Employee.findByGender("F").chain(function (emps) {
                emps.forEach(function (emp) {
                    assert.instanceOf(emp, Employee);
                    assert.equal("F", emp.gender);
                });
            });
        });


        it.describe("dataset methods", function (it) {

            it.should("support count", function () {
                return Employee.count().chain(function (count) {
                    assert.equal(count, 20);
                });
            });

            it.should("support all", function () {
                return Employee.all().chain(function (emps) {
                    assert.lengthOf(emps, 20);
                    emps.forEach(function (e) {
                        assert.instanceOf(e, Employee);
                    });
                });
            });


            it.should("support map", function () {
                var ids = emps.map(function (emp) {
                    return emp.id;
                });
                return Employee.map("id")
                    .chain(function (res) {
                        assert.lengthOf(res, 20);
                        res.forEach(function (id, i) {
                            assert.equal(id, ids[i]);
                        });
                        return Employee.order("position").map(function (e) {
                            return e.firstName + " " + e.lastName;
                        });
                    }).chain(function (res) {
                        assert.lengthOf(res, 20);
                        res.forEach(function (name, i) {
                            assert.equal(name, "first" + i + " last" + i);
                        });
                    });
            });

            it.should("support forEach", function (next) {
                var ret = [];
                return Employee.forEach(function (emp) {
                    ret.push(emp);
                }).chain(function (topic) {
                        assert.lengthOf(topic, 20);
                        ret.forEach(function (e) {
                            assert.instanceOf(e, Employee);
                        });
                    });
            });

            it.should("support one", function (next) {
                return Employee.one().chain(function (emp) {
                    assert.instanceOf(emp, Employee);
                });
            });

            it.should("support first", function () {
                var id = sql.identifier("id"), ids = emps.map(function (emp) {
                    return emp.id;
                });
                return Employee.first(id.gt(ids[5]), id.lt(ids[11])).chain(function (emp) {
                    assert.instanceOf(emp, Employee);
                    assert.equal(emp.id, ids[6]);
                });
            });

            it.should("support last", function () {
                return Employee.order("firstName").last().chain(function (emp) {
                    assert.throws(hitch(Employee, "last"));
                    assert.instanceOf(emp, Employee);
                    assert.equal(emp.firstName, "first9");
                });
            });

        });
    });


    it.context(function (it) {
        var emp;
        it.beforeEach(function () {
            return Employee.remove().chain(function () {
                return (emp = Employee.create({
                    firstName: "doug",
                    lastName: "martin",
                    position: 21,
                    midInitial: null,
                    gender: "M",
                    street: "1 nowhere st.",
                    city: "NOWHERE"
                })).save();
            });
        });

        it.should("support updates", function () {
            emp.firstName = "douglas";
            return emp.update().chain(function (e) {
                assert.equal(e.firstName, "douglas");
                return Employee.one({id: emp.id}).chain(function () {
                    assert.isNumber(emp.id);
                    assert.equal(emp.firstName, "douglas");
                });
            });
        });


        it.should("support remove", function () {
            var id = emp.id;
            return emp.remove().chain(function () {
                return Employee.filter({id: id}).one().chain(function (e) {
                    assert.isNull(e);
                });
            });
        });

        it.should("support support batch updates", function () {
            return Employee.all().chain(function (records) {
                assert.lengthOf(records, 1);
                records.forEach(function (r) {
                    assert.equal(r.firstName, "doug");
                });
            });
        });

        it.should("support filters on batch updates", function () {
            return Employee.update({firstName: "dougie"}, {id: emp.id})
                .chain(function () {
                    return Employee.filter({id: emp.id}).one();
                })
                .chain(function (emp) {
                    assert.instanceOf(emp, Employee);
                    assert.equal(emp.firstName, "dougie");
                });
        });


    });

    it.afterAll(function () {
        return helper.dropModels();
    });

});
