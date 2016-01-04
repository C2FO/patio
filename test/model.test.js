"use strict";

var it = require('it'),
    assert = require('assert'),
    helper = require("./data/model.helper.js"),
    patio = require("../lib"),
    sql = patio.SQL,
    comb = require("comb"),
    hitch = comb.hitch,
    config = require("./test.config.js");

var gender = ["M", "F"];


it.describe("patio.Model", function (it) {

    var Employee;
    it.beforeAll(function () {
        Employee = patio.addModel("employee", {
            static: {
                //class methods
                findByGender: function (gender, callback, errback) {
                    return this.filter({gender: gender}).all();
                }
            }
        });
        return helper.createSchemaAndSync();
    });

    it.should("type cast properties", function () {
        var emp = new Employee({
            firstname: "doug",
            lastname: "martin",
            position: 1,
            midinitial: null,
            gender: "M",
            street: "1 nowhere st.",
            city: "NOWHERE",
            buffertype: "buffer data",
            texttype: "text data",
            blobtype: "blob data"
        });
        if (config.DB_TYPE === "pg") {
            emp.jsontype = {a: "b"};
            assert.instanceOf(emp.jsontype, sql.Json);
            emp.jsonarray = [{a: "b"}];
            assert.instanceOf(emp.jsonarray, sql.JsonArray);
        }
        assert.isString(emp.firstname);
        assert.isString(emp.lastname);
        assert.isNumber(emp.position);
        assert.isNull(emp.midinitial);
        assert.isString(emp.gender);
        assert.isString(emp.street);
        assert.isString(emp.city);
        assert.isTrue(Buffer.isBuffer(emp.buffertype));
        assert.isString(emp.texttype);
        assert.isTrue(Buffer.isBuffer(emp.blobtype));
    });


    it.should("save properly", function () {
        var emp = new Employee({
            firstname: "doug",
            lastname: "martin",
            position: 1,
            midinitial: null,
            gender: "M",
            street: "1 nowhere st.",
            city: "NOWHERE",
            buffertype: "buffer data",
            texttype: "text data",
            blobtype: "blob data"
        });
        if (config.DB_TYPE === "pg") {
            emp.jsontype = {a: "b"};
            emp.jsonarray = [{a: "b"}];
        }
        return emp.save().chain(function () {
            assert.instanceOf(emp, Employee);
            assert.equal(emp.firstname, "doug");
            assert.equal(emp.lastname, "martin");
            assert.isNull(emp.midinitial);
            assert.equal(emp.gender, "M");
            assert.equal(emp.street, "1 nowhere st.");
            assert.equal(emp.city, "NOWHERE");
            assert.deepEqual(emp.buffertype, new Buffer("buffer data"));
            assert.deepEqual(emp.texttype, "text data");
            assert.deepEqual(emp.blobtype, new Buffer("blob data"));
            if (config.DB_TYPE === "pg") {
                assert.instanceOf(emp.jsontype, sql.Json);
                assert.instanceOf(emp.jsonarray, sql.JsonArray);
            }
        });
    });

    it.should("emit events", function () {
        var emp = new Employee({
            firstname: "doug",
            lastname: "martin",
            position: 1,
            midinitial: null,
            gender: "M",
            street: "1 nowhere st.",
            city: "NOWHERE",
            buffertype: "buffer data",
            texttype: "text data",
            blobtype: "blob data"
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
                lastname: "last" + i,
                position: i,
                firstname: "first" + i,
                midinitial: "m",
                gender: gender[i % 2],
                street: "Street " + i,
                city: "City " + i
            });
        }
        return Employee.truncate()
            .chain(function () {
                return Employee.save(emps);
            })
            .chain(function (employees) {
                assert.lengthOf(employees, 20);
                employees.forEach(function (emp, i) {
                    assert.equal(emp.lastname, "last" + i);
                    assert.equal(emp.firstname, "first" + i);
                    assert.equal(emp.midinitial, "m");
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
            var emp;
            for (var i = 0; i < 20; i++) {
                emp = new Employee({
                    lastname: "last" + i,
                    firstname: "first" + i,
                    position: i,
                    midInitial: "m",
                    gender: gender[i % 2],
                    street: "Street s" + i,
                    city: "City " + i
                });
                if (config.DB_TYPE === "pg") {
                    emp.jsontype = {a: "b"};
                    emp.jsonarray = [{a: "b"}];
                }
                emps.push(emp);
            }
            return Employee.truncate().chain(function () {
                return Employee.save(emps);
            });
        });

        it.should("should reload models", function () {
            return Employee.first()
                .chain(function (emp) {
                    var orig = emp.lastname;
                    emp.lastname = "martin";
                    return emp.reload().chain(function () {
                        assert.instanceOf(emp, Employee);
                        assert.equal(emp.lastname, orig);
                    });
                });
        });

        it.should("find models by id", function () {
            return Employee.findById(emps[0].id).chain(function (emp) {
                assert.instanceOf(emp, Employee);
                assert.equal(emp.id, emps[0].id);
                if (config.DB_TYPE === "pg") {
                    assert.instanceOf(emp.jsontype, sql.Json);
                    assert.instanceOf(emp.jsonarray, sql.JsonArray);
                }
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
                        return this.firstname.like(/first1[1|2]*$/);
                    }).order("firstname").all();
                })
                .chain(function (query3) {
                    assert.lengthOf(query3, 3);
                    assert.instanceOf(query3[0], Employee);
                    assert.equal(query3[0].firstname, "first1");
                    assert.instanceOf(query3[1], Employee);
                    assert.equal(query3[1].firstname, "first11");
                    assert.instanceOf(query3[2], Employee);
                    assert.equal(query3[2].firstname, "first12");
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
                return comb.when([
                    Employee.map("id"),
                    Employee.order("position").map(function (e) {
                        return e.firstname + " " + e.lastname;
                    })
                ])
                    .chain(function (res) {
                        assert.lengthOf(res[0], 20);
                        res[0].forEach(function (id, i) {
                            assert.equal(id, ids[i]);
                        });
                        assert.lengthOf(res[1], 20);
                        res[1].forEach(function (name, i) {
                            assert.equal(name, "first" + i + " last" + i);
                        });
                    });
            });

            it.should("support forEach", function () {
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

            it.should("support one", function () {
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
                return Employee.order("firstname").last()
                    .chain(function (emp) {
                        assert.instanceOf(emp, Employee);
                        assert.equal(emp.firstname, "first9");
                    })
                    .chain(function () {
                        return Employee.last();
                    })
                    .chain(assert.fail, function (err) {
                        assert.equal(err.message, "QueryError : No order specified");
                    });
            });

            it.should("support stream", function (next) {
                var ret = [];
                Employee.stream()
                    .on("data", function (emp) {
                        ret.push(emp);
                    })
                    .on("error", next)
                    .on("end", function () {
                        assert.lengthOf(ret, 20);
                        ret.forEach(function (e) {
                            assert.instanceOf(e, Employee);
                        });
                        next();
                    });
            });

        });
    });


    it.context(function (it) {
        var emp;
        it.beforeEach(function () {
            return Employee.remove().chain(function () {
                var values = {
                    firstname: "doug",
                    lastname: "martin",
                    position: 21,
                    midInitial: null,
                    gender: "M",
                    street: "1 nowhere st.",
                    city: "NOWHERE"
                };
                if (config.DB_TYPE === "pg") {
                    values.jsontype = {a: "b"};
                    values.jsonarray = [{a: "b"}];
                }
                return (emp = Employee.create(values)).save();
            });
        });

        it.should("support updates", function () {
            emp.firstname = "douglas";
            return emp.update().chain(function (e) {
                assert.equal(e.firstname, "douglas");
                return Employee.one({id: emp.id}).chain(function () {
                    assert.isNumber(emp.id);
                    assert.equal(emp.firstname, "douglas");
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
                    assert.equal(r.firstname, "doug");
                });
            });
        });

        it.should("support filters on batch updates", function () {
            return Employee.update({firstname: "dougie"}, {id: emp.id})
                .chain(function () {
                    return Employee.filter({id: emp.id}).one();
                })
                .chain(function (emp) {
                    assert.instanceOf(emp, Employee);
                    assert.equal(emp.firstname, "dougie");
                });
        });

        it.should("have insertSql property", function () {
            if (config.DB_TYPE === "pg") {
                assert.isTrue(emp.insertSql.match(/INSERT INTO [`|"]employee[`|"] \([`|"]id[`|"], [`"]jsontype[`"], [`"]jsonarray[`"], [`|"]firstname[`|"], [`|"]lastname[`|"], [`|"]midinitial[`|"], [`|"]position[`|"], [`|"]gender[`|"], [`|"]street[`|"], [`|"]city[`|"], [`|"]buffertype[`|"], [`|"]texttype[`|"], [`|"]blobtype[`|"]\) VALUES \(\d+, '\{"a":"b"\}', '\[\{"a":"b"\}\]', 'doug', 'martin', NULL, 21, 'M', '1 nowhere st.', 'NOWHERE', NULL, NULL, NULL\)/) !== null);
            } else {
                assert.isTrue(emp.insertSql.match(/INSERT INTO [`|"]employee[`|"] \([`|"]id[`|"], [`|"]firstname[`|"], [`|"]lastname[`|"], [`|"]midinitial[`|"], [`|"]position[`|"], [`|"]gender[`|"], [`|"]street[`|"], [`|"]city[`|"], [`|"]buffertype[`|"], [`|"]texttype[`|"], [`|"]blobtype[`|"]\) VALUES \(\d+, 'doug', 'martin', NULL, 21, 'M', '1 nowhere st.', 'NOWHERE', NULL, NULL, NULL\)/) !== null);
            }
        });

        it.should("have the updateSql property", function () {
            assert.isTrue(emp.updateSql.match(/UPDATE +[`|"]employee[`|"] +SET  +WHERE +\([`|"]id[`|"] += +\d+\)/) !== null);
        });

        it.should("have the removeSql property", function () {
            assert.isTrue(emp.removeSql.match(/DELETE +FROM +[`|"]employee[`|"] +WHERE +\([`|"]id[`|"] += +\d+\)/) !== null);
        });

        it.should("have the deleteSql property", function () {
            assert.isTrue(emp.deleteSql.match(/DELETE +FROM +[`|"]employee[`|"] +WHERE +\([`|"]id[`|"] += +\d+\)/) !== null);
        });
    });


    it.should("allow values when initalizing that are not in the schema", function () {
        var m = new Employee({otherVal: "otherVal", firstname: "dougie"}, true);
        assert.equal(m.otherVal, "otherVal");
        assert.equal(m.firstname, "dougie");
    });

    it.should("throw an error when setting a non nullable column to null", function () {
        var m = new Employee({otherVal: "otherVal", firstname: "dougie"}, true);
        try {
            m.street = null;
        } catch (e) {
            assert.equal(e.message, "Model error : null is not allowed for the street column on model employee");
        }
    });

    it.afterAll(function () {
        return helper.dropModels();
    });
});
