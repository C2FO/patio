var it = require('it'),
    assert = require('assert'),
    patio = require("index"),
    Dataset = patio.Dataset,
    Database = patio.Database,
    sql = patio.SQL,
    Identifier = sql.Identifier,
    QualifiedIdentifier = sql.QualifiedIdentifier,
    SQLFunction = sql.SQLFunction,
    LiteralString = sql.LiteralString,
    helper = require("../helpers/helper"),
    MockDatabase = helper.MockDatabase,
    SchemaDatabase = helper.SchemaDatabase,
    MockDataset = helper.MockDataset,
    comb = require("comb"),
    when = comb.when,
    serial = comb.serial,
    hitch = comb.hitch;


//var ret = (module.exports = new comb.Promise());
it.describe("Dataset actions", function (it) {

    patio.identifierInputMethod = null;
    patio.identifierOutputMethod = null;
    var c = comb.define(Dataset, {
        instance:{

            fetchRows:function (sql, cb) {
                this._static.sql = sql;
                this.__columns = [sql.match(/SELECT COUNT/i) ? "count" : "a"];
                var val = {};
                val[this.__columns[0]] = 1;
                return new comb.Promise().callback(cb ? cb(val) : val);
            }
        },

        "static":{
            sql:null
        }
    });

    var DummyDataset = comb.define(Dataset, {
        instance:{
            VALUES:[
                {a:1, b:2},
                {a:3, b:4},
                {a:5, b:6}
            ],
            fetchRows:function (sql, block) {
                var ret = new comb.Promise();
                ret.callback(this.VALUES.forEach(block));
                return ret;
            }
        }
    });

    it.describe("#rowCb", function (it) {
        var ds = new DummyDataset().from("items");
        var dataset = new (comb.define(Dataset, {
            instance:{

                fetchRows:function (sql, cb) {
                    // yield a hash with kind as the 1 bit of a number
                    for (var i = 0; i < 10; i++) {
                        cb({kind:i});
                    }
                    return new comb.Promise().callback();
                }
            }
        }))().from("items");

        it.should("allow the setting of a rowCb ", function () {
            [function () {
            }, null].forEach(function (rowCb) {
                    assert.doesNotThrow(function () {
                        ds.rowCb = rowCb;
                    }, rowCb + " should not have thrown an exception");
                });
            ["HI", new Date(), true, false, undefined].forEach(function (rowCb) {
                assert.throws(function () {
                    ds.rowCb = rowCb;
                }, rowCb + " should have thrown an exception");
            });
        });


        it.should("cause dataset to pass all rows through the filter", function (next) {
            dataset.rowCb = function (h) {
                h.der = h.kind + 2;
                return h;
            };

            return dataset.all().then(function (rows) {
                assert.lengthOf(rows, 10);
                rows.forEach(function (r) {
                    assert.deepEqual(r.der, r.kind + 2);
                });
                dataset.rowCb = null;
            });
        });

        it.should("be copied over when dataset is cloned", function () {
            dataset.rowCb = function (h) {
                h.der = h.kind + 2;
                return h;
            };
            return dataset.filter({a:1}).first().then(function (r) {
                assert.deepEqual(r, {kind:0, der:2});
            });
        });

    });


    it.describe("#map", function (it) {
        var dataset = new DummyDataset().from("items");

        it.should("provide the usual functionality if no argument is given", function (next) {
            dataset.map(function (n) {
                return n.a + n.b;
            }).then(function (r) {
                    assert.deepEqual(r, [3, 7, 11]);
                    //with callback
                    dataset.map(
                        function (n) {
                            return n.a + n.b;
                        },
                        function (err, r) {
                            assert.isNull(err);
                            assert.deepEqual(r, [3, 7, 11]);
                            next();
                        }
                    );
                });
        });

        it.should("map using #[column name] if column name is given", function (next) {
            dataset.map(function (n) {
                return n.a;
            }).then(function (r) {
                    assert.deepEqual(r, [1, 3, 5]);
                    //with callback
                    dataset.map(
                        function (n) {
                            return n.a;
                        },
                        function (err, r) {
                            assert.isNull(err);
                            assert.deepEqual(r, [1, 3, 5]);
                            next();
                        }
                    );
                });
        });

        it.should("return the complete dataset values if nothing is given", function (next) {
            dataset.map().then(function (r) {
                assert.deepEqual(r, dataset.VALUES);
                dataset.map(null, function (err, r) {
                    assert.isNull(err);
                    assert.deepEqual(r, dataset.VALUES);
                    next();
                });
            });
        });
    });

    it.describe("#toHash", function (it) {
        var dataset = new DummyDataset().from("test");

        it.should("provide a hash with the first column as key and the second as value", function (next) {
            var a = {1:2, 3:4, 5:6}, b = {2:1, 4:3, 6:5};
            return when(
                dataset.toHash("a", "b").then(function (ret) {
                    assert.deepEqual(a, ret);
                }),
                dataset.toHash("b", "a").then(function (ret) {
                    assert.deepEqual(ret, b);
                }),

                dataset.toHash("a", "b", function (err, ret) {
                    assert.isNull(err);
                    assert.deepEqual(a, ret);
                }),
                dataset.toHash("b", "a", function (err, ret) {
                    assert.isNull(err);
                    assert.deepEqual(ret, b);
                })
            );

        });

        it.should("provide a hash with the first column as key and the entire hash as value if the value column is blank or null", function (next) {
            var a = {1:{a:1, b:2}, 3:{a:3, b:4}, 5:{a:5, b:6}};
            var b = {2:{a:1, b:2}, 4:{a:3, b:4}, 6:{a:5, b:6}};
            return when(
                dataset.toHash("a").then(function (ret) {
                    assert.deepEqual(ret, a);
                }),
                dataset.toHash("b").then(function (ret) {
                    assert.deepEqual(ret, b);
                }),
                dataset.toHash("a", function (err, ret) {
                    assert.isNull(err);
                    assert.deepEqual(ret, a);
                }),
                dataset.toHash("b", function (err, ret) {
                    assert.isNull(err);
                    assert.deepEqual(ret, b);
                })
            );
        });

    });

    it.describe("#count", function (it) {
        var dataset = new c().from("test");

        it.should("format SQL properly", function () {
            return when(
                dataset.count().then(function (res) {
                    assert.equal(res, 1);
                    assert.equal(c.sql, 'SELECT COUNT(*) AS count FROM test LIMIT 1');
                }),

                dataset.count(function (err, res) {
                    assert.isNull(err);
                    assert.equal(res, 1);
                    assert.equal(c.sql, 'SELECT COUNT(*) AS count FROM test LIMIT 1');
                })
            );
        });

        it.should("include the where clause if it's there", function () {
            return when(
                dataset.filter(sql.abc.sqlNumber.lt(30)).count().then(function (count) {
                    assert.equal(count, 1);
                    assert.equal(c.sql, 'SELECT COUNT(*) AS count FROM test WHERE (abc < 30) LIMIT 1');
                }),
                dataset.filter(sql.abc.sqlNumber.lt(30)).count(function (err, count) {
                    assert.isNull(err);
                    assert.equal(count, 1);
                    assert.equal(c.sql, 'SELECT COUNT(*) AS count FROM test WHERE (abc < 30) LIMIT 1');
                })
            );
        });

        it.should("count properly for datasets with fixed sql", function () {
            var dataset = new c().from("test");
            dataset.__opts.sql = "select abc from xyz";
            return when(
                dataset.count().then(function (count) {
                    assert.equal(count, 1);
                    assert.equal(c.sql, "SELECT COUNT(*) AS count FROM (select abc from xyz) AS t1 LIMIT 1");
                }),
                dataset.count(function (err, count) {
                    assert.isNull(err);
                    assert.equal(count, 1);
                    assert.equal(c.sql, "SELECT COUNT(*) AS count FROM (select abc from xyz) AS t1 LIMIT 1");
                })
            );
        });

        it.should("count properly when using UNION, INTERSECT, or EXCEPT", function () {
            return serial([
                function () {
                    return dataset.union(dataset).count().then(function (count) {
                        assert.equal(count, 1);
                        assert.equal(c.sql, "SELECT COUNT(*) AS count FROM (SELECT * FROM test UNION SELECT * FROM test) AS t1 LIMIT 1");
                    });
                },
                function () {
                    return dataset.intersect(dataset).count().then(function (count) {
                        assert.equal(count, 1);
                        assert.equal(c.sql, "SELECT COUNT(*) AS count FROM (SELECT * FROM test INTERSECT SELECT * FROM test) AS t1 LIMIT 1");
                    });
                },
                function () {
                    return dataset.except(dataset).count().then(function (count) {
                        assert.equal(count, 1);
                        assert.equal(c.sql, "SELECT COUNT(*) AS count FROM (SELECT * FROM test EXCEPT SELECT * FROM test) AS t1 LIMIT 1");
                    });
                },
                function () {
                    //with callback
                    return dataset.union(dataset).count(function (err, count) {
                        assert.isNull(err);
                        assert.equal(count, 1);
                        assert.equal(c.sql, "SELECT COUNT(*) AS count FROM (SELECT * FROM test UNION SELECT * FROM test) AS t1 LIMIT 1");
                    });
                },
                function () {
                    return dataset.intersect(dataset).count(function (err, count) {
                        assert.isNull(err);
                        assert.equal(count, 1);
                        assert.equal(c.sql, "SELECT COUNT(*) AS count FROM (SELECT * FROM test INTERSECT SELECT * FROM test) AS t1 LIMIT 1");
                    });
                },
                function () {
                    return dataset.except(dataset).count(function (err, count) {
                        assert.isNull(err);
                        assert.equal(count, 1);
                        assert.equal(c.sql, "SELECT COUNT(*) AS count FROM (SELECT * FROM test EXCEPT SELECT * FROM test) AS t1 LIMIT 1");
                    });
                }
            ]);

        });

        it.should("return limit if count is greater than it", function () {
            return when(
                dataset.limit(5).count().then(function (count) {
                    assert.equal(count, 1);
                    assert.equal(c.sql, "SELECT COUNT(*) AS count FROM (SELECT * FROM test LIMIT 5) AS t1 LIMIT 1");
                }),

                dataset.limit(5).count(function (err, count) {
                    assert.isNull(err);
                    assert.equal(count, 1);
                    assert.equal(c.sql, "SELECT COUNT(*) AS count FROM (SELECT * FROM test LIMIT 5) AS t1 LIMIT 1");
                })
            );

        });

        it.should("work on a graphed_dataset", function (next) {
            dataset.graph(dataset, ["a"], {tableAlias:"test2"}).then(function (ds) {
                when(
                    ds.count().then(function (count) {
                        assert.equal(count, 1);
                        assert.equal(c.sql, 'SELECT COUNT(*) AS count FROM test LEFT OUTER JOIN test AS test2 USING (a) LIMIT 1');
                    }),
                    ds.count(function (err, count) {
                        assert.isNull(err);
                        assert.equal(count, 1);
                        assert.equal(c.sql, 'SELECT COUNT(*) AS count FROM test LEFT OUTER JOIN test AS test2 USING (a) LIMIT 1');
                    })
                ).classic(next);
            });
        });

        it.should("not cache the columns value", function () {
            var ds = dataset.from("blah");
            ds.columns.then(function (cols) {
                assert.deepEqual(cols, ["a"]);
            });
            return when(
                ds.count().then(function (count) {
                    assert.equal(count, 1);
                    assert.equal(c.sql, 'SELECT COUNT(*) AS count FROM blah LIMIT 1');
                }),

                ds.columns.then(function (cols) {
                    assert.deepEqual(cols, ["a"]);
                }),

                //with callback
                ds.count(function (err, count) {
                    assert.isNull(err);
                    assert.equal(count, 1);
                    assert.equal(c.sql, 'SELECT COUNT(*) AS count FROM blah LIMIT 1');
                })
            );
        });

    });

    it.describe("#isEmpty", function (it) {
        var C = comb.define(c, {
            instance:{
                fetchRows:function (sql, cb) {
                    this._static.sql = sql;
                    return new comb.Promise().callback(cb(sql.match(/WHERE \'f\'/) ? null : {1:1}));
                }
            }
        });

        it.should("return true if records exist in the dataset", function () {
            var ds = new C().from("test");
            return serial([
                function () {
                    return ds.isEmpty().then(function (res) {
                        assert.isFalse(res);
                        assert.equal(C.sql, 'SELECT 1 FROM test LIMIT 1');
                    });
                },
                function () {
                    return ds.filter(false).isEmpty().then(function (res) {
                        assert.isTrue(res);
                        assert.equal(C.sql, "SELECT 1 FROM test WHERE 'f' LIMIT 1");
                    });
                },
                function () {
                    //with callback
                    return ds.isEmpty(function (err, res) {
                        assert.isNull(err);
                        assert.isFalse(res);
                        assert.equal(C.sql, 'SELECT 1 FROM test LIMIT 1');
                    });
                },
                function () {
                    return ds.filter(false).isEmpty(function (err, res) {
                        assert.isNull(err);
                        assert.isTrue(res);
                        assert.equal(C.sql, "SELECT 1 FROM test WHERE 'f' LIMIT 1");
                    });
                }
            ]);
        });
    });

    it.describe("#insertMultiple", function (it) {
        var d = new (comb.define(Dataset, {
            instance:{
                constructor:function () {
                    this.inserts = [];
                },
                insert:function (arg) {
                    this.inserts.push(arg);
                    return new comb.Promise().callback();
                }
            }
        }))();

        it.should("insert all items in the supplied array", function () {
            return when(
                d.insertMultiple(["aa", 5, 3, {1:2}]).then(function () {
                    assert.deepEqual(d.inserts, ["aa", 5, 3, {1:2}]);
                    d.inserts.length = 0;
                }),

                d.insertMultiple(["aa", 5, 3, {1:2}], null, function (err) {
                    assert.isNull(err);
                    assert.deepEqual(d.inserts, ["aa", 5, 3, {1:2}]);
                    d.inserts.length = 0;
                })

            );
        });

        it.should("pass array items through the supplied block if given", function () {
            var a = ["inevitable", "hello", "the ticking clock"];
            return when(
                d.insertMultiple(a,function (i) {
                    return i.replace(/l/g, "r");
                }).then(function () {
                        assert.deepEqual(d.inserts, ["inevitabre", "herro", "the ticking crock"]);
                        d.inserts.length = 0;
                    }),

                d.insertMultiple(
                    a,
                    function (i) {
                        return i.replace(/l/g, "r");
                    },
                    function (err) {
                        assert.isNull(err);
                        assert.deepEqual(d.inserts, ["inevitabre", "herro", "the ticking crock"]);
                        d.inserts.length = 0;
                    }
                )
            );

        });

    });

    it.describe("#max,#min,#sum,#avg", function (it) {
        var d = new (
            comb.define(Dataset, {
                instance:{
                    fetchRows:function (sql, cb) {
                        return new comb.Promise().callback(cb({1:sql}));
                    }
                }
            }))().from("test");

        it.should("include min", function () {
            return when(
                d.min("a").then(function (sql) {
                    assert.equal(sql, 'SELECT min(a) FROM test LIMIT 1');
                }),

                d.min("a", function (err, sql) {
                    assert.isNull(err);
                    assert.equal(sql, 'SELECT min(a) FROM test LIMIT 1');
                })
            );
        });

        it.should("include max", function () {
            return when(
                d.max("a").then(function (sql) {
                    assert.equal(sql, 'SELECT max(a) FROM test LIMIT 1');
                }),

                d.max("a", function (err, sql) {
                    assert.isNull(err);
                    assert.equal(sql, 'SELECT max(a) FROM test LIMIT 1');
                })
            );
        });

        it.should("include sum", function () {
            return when(
                d.sum("a").then(function (sql) {
                    assert.equal(sql, 'SELECT sum(a) FROM test LIMIT 1');
                }),
                d.sum("a", function (err, sql) {
                    assert.isNull(err);
                    assert.equal(sql, 'SELECT sum(a) FROM test LIMIT 1');
                })
            );
        });

        it.should("include avg", function () {
            return when(
                d.avg("a").then(function (sql) {
                    assert.equal(sql, 'SELECT avg(a) FROM test LIMIT 1');
                }),

                d.avg("a", function (err, sql) {
                    assert.isNull(err);
                    assert.equal(sql, 'SELECT avg(a) FROM test LIMIT 1');
                })
            );
        });

        it.should("accept qualified columns", function () {
            return when(
                d.avg("test__a").then(function (sql) {
                    assert.equal(sql, 'SELECT avg(test.a) FROM test LIMIT 1');
                }),

                d.avg("test__a", function (err, sql) {
                    assert.isNull(err);
                    assert.equal(sql, 'SELECT avg(test.a) FROM test LIMIT 1');
                })
            );
        });

        it.should("use a subselect for the same conditions as count", function () {
            d = d.order("a").limit(5);
            return when(
                d.avg("a").then(function (sql) {
                    assert.equal(sql, 'SELECT avg(a) FROM (SELECT * FROM test ORDER BY a LIMIT 5) AS t1 LIMIT 1');
                }),
                d.sum("a").then(function (sql) {
                    assert.equal(sql, 'SELECT sum(a) FROM (SELECT * FROM test ORDER BY a LIMIT 5) AS t1 LIMIT 1');
                }),
                d.min("a").then(function (sql) {
                    assert.equal(sql, 'SELECT min(a) FROM (SELECT * FROM test ORDER BY a LIMIT 5) AS t1 LIMIT 1');
                }),
                d.max("a").then(function (sql) {
                    assert.equal(sql, 'SELECT max(a) FROM (SELECT * FROM test ORDER BY a LIMIT 5) AS t1 LIMIT 1');
                }),


                //with callback
                d.avg("a", function (err, sql) {
                    assert.isNull(err);
                    assert.equal(sql, 'SELECT avg(a) FROM (SELECT * FROM test ORDER BY a LIMIT 5) AS t1 LIMIT 1');
                }),
                d.sum("a", function (err, sql) {
                    assert.isNull(err);
                    assert.equal(sql, 'SELECT sum(a) FROM (SELECT * FROM test ORDER BY a LIMIT 5) AS t1 LIMIT 1');
                }),
                d.min("a", function (err, sql) {
                    assert.isNull(err);
                    assert.equal(sql, 'SELECT min(a) FROM (SELECT * FROM test ORDER BY a LIMIT 5) AS t1 LIMIT 1');
                }),
                d.max("a", function (err, sql) {
                    assert.isNull(err);
                    assert.equal(sql, 'SELECT max(a) FROM (SELECT * FROM test ORDER BY a LIMIT 5) AS t1 LIMIT 1');
                })
            );
        });
    });

    it.describe("#range", function (it) {
        var d = new (comb.define(Dataset, {
            instance:{

                getters:{
                    lastSql:function () {
                        return this._static.sql;
                    }
                },

                fetchRows:function (sql, cb) {
                    this._static.sql = sql;
                    return new comb.Promise().callback(cb({v1:1, v2:10}));
                }
            },

            "static":{
                sql:null
            }
        }))().from("test");

        it.should("generate a correct SQL statement", function () {
            return when(
                d.range("stamp").then(function () {
                    assert.equal(d.lastSql, "SELECT min(stamp) AS v1, max(stamp) AS v2 FROM test LIMIT 1");
                }),
                d.filter(sql.price.sqlNumber.gt(100)).range("stamp").then(function () {
                    assert.equal(d.lastSql, "SELECT min(stamp) AS v1, max(stamp) AS v2 FROM test WHERE (price > 100) LIMIT 1");
                }),

                d.range("stamp", function (err) {
                    assert.isNull(err);
                    assert.equal(d.lastSql, "SELECT min(stamp) AS v1, max(stamp) AS v2 FROM test LIMIT 1");
                }),
                d.filter(sql.price.sqlNumber.gt(100)).range("stamp", function (err) {
                    assert.isNull(err);
                    assert.equal(d.lastSql, "SELECT min(stamp) AS v1, max(stamp) AS v2 FROM test WHERE (price > 100) LIMIT 1");
                })
            );
        });

        it.should("return a two values", function () {
            return when(
                d.range("tryme").then(function (one, two) {
                    assert.equal(one, 1);
                    assert.equal(two, 10);
                }),

                d.range("tryme", function (err, one, two) {
                    assert.isNull(err);
                    assert.equal(one, 1);
                    assert.equal(two, 10);
                })
            );
        });

        it.should("use a subselect for the same conditions as count", function () {
            return when(
                d.order("stamp").limit(5).range("stamp").then(function (one, two) {
                    assert.equal(one, 1);
                    assert.equal(two, 10);
                    assert.equal(d.lastSql, 'SELECT min(stamp) AS v1, max(stamp) AS v2 FROM (SELECT * FROM test ORDER BY stamp LIMIT 5) AS t1 LIMIT 1');
                }),

                d.order("stamp").limit(5).range("stamp", function (err, one, two) {
                    assert.isNull(err);
                    assert.equal(one, 1);
                    assert.equal(two, 10);
                    assert.equal(d.lastSql, 'SELECT min(stamp) AS v1, max(stamp) AS v2 FROM (SELECT * FROM test ORDER BY stamp LIMIT 5) AS t1 LIMIT 1');
                })
            );
        });
    });

    it.describe("#interval", function (it) {

        var d = new (
            comb.define(Dataset, {
                instance:{

                    getters:{
                        lastSql:function () {
                            return this._static.sql;
                        }
                    },

                    reset:function () {
                        this._static.sql = null;
                    },

                    fetchRows:function (sql, cb) {
                        this._static.sql = sql;
                        return new comb.Promise().callback(cb({v:1234}));
                    }
                },

                "static":{
                    sql:null
                }
            }))().from("test");

        it.should("generate a correct SQL statement", function () {
            return serial([
                function () {
                    return d.interval("stamp").then(function () {
                        assert.equal(d.lastSql, "SELECT (max(stamp) - min(stamp)) FROM test LIMIT 1");
                    });
                },
                function () {
                    return d.filter(sql.price.sqlNumber.gt(100)).interval("stamp").then(function () {
                        assert.equal(d.lastSql, "SELECT (max(stamp) - min(stamp)) FROM test WHERE (price > 100) LIMIT 1");
                    });
                },

                //with callback
                function () {
                    d.reset();
                    return  d.interval("stamp", function (err) {
                        assert.isNull(err);
                        assert.equal(d.lastSql, "SELECT (max(stamp) - min(stamp)) FROM test LIMIT 1");
                    });
                },
                function () {
                    return d.filter(sql.price.sqlNumber.gt(100)).interval("stamp", function (err) {
                        assert.isNull(err);
                        assert.equal(d.lastSql, "SELECT (max(stamp) - min(stamp)) FROM test WHERE (price > 100) LIMIT 1");
                    });
                }
            ]);

        });
        it.should("return an integer", function () {
            return when(
                d.interval("tryme").then(function (r) {
                    assert.equal(r, 1234);
                }),

                //with callback
                d.interval("tryme", function (err, r) {
                    assert.isNull(err);
                    assert.equal(r, 1234);
                })
            );
        });

        it.should("use a subselect for the same conditions as count", function () {
            return when(
                d.order("stamp").limit(5).interval("stamp").then(function (r) {
                    assert.equal(r, 1234);
                    assert.equal(d.lastSql, 'SELECT (max(stamp) - min(stamp)) FROM (SELECT * FROM test ORDER BY stamp LIMIT 5) AS t1 LIMIT 1');
                }),

                //with callback

                d.order("stamp").limit(5).interval("stamp", function (err, r) {
                    assert.isNull(err);
                    assert.equal(r, 1234);
                    assert.equal(d.lastSql, 'SELECT (max(stamp) - min(stamp)) FROM (SELECT * FROM test ORDER BY stamp LIMIT 5) AS t1 LIMIT 1');
                })
            );

        });


    });

    it.describe("#first, #last", function (it) {
        var d = new (comb.define(Dataset, {
            instance:{
                forEach:function (cb) {
                    var s = this.selectSql;
                    var x = ["a", 1, "b", 2, s];
                    var i = parseInt(s.match(/LIMIT (\d+)/)[1], 10);
                    var ret = new comb.Promise().callback();
                    for (var j = 0; j < i; j++) {
                        cb(x);
                    }
                    return ret;
                }
            }
        }))().from("test");

        it.should("return a single record if no argument is given", function () {
            return when(
                d.order("a").first().then(function (r) {
                    assert.deepEqual(r, ["a", 1, "b", 2, 'SELECT * FROM test ORDER BY a LIMIT 1']);
                }),
                d.order("a").last().then(function (r) {
                    assert.deepEqual(r, ["a", 1, "b", 2, 'SELECT * FROM test ORDER BY a DESC LIMIT 1']);
                }),

                //with callback
                d.order("a").first(function (err, r) {
                    assert.isNull(err);
                    assert.deepEqual(r, ["a", 1, "b", 2, 'SELECT * FROM test ORDER BY a LIMIT 1']);
                }),
                d.order("a").last(function (err, r) {
                    assert.isNull(err);
                    assert.deepEqual(r, ["a", 1, "b", 2, 'SELECT * FROM test ORDER BY a DESC LIMIT 1']);
                })
            );
        });

        it.should("return the first/last matching record if argument is not an Integer", function () {
            return when(
                d.order("a").first({z:26}).then(function (r) {
                    assert.deepEqual(r, ["a", 1, "b", 2, 'SELECT * FROM test WHERE (z = 26) ORDER BY a LIMIT 1']);
                }),
                d.order("a").first("z = ?", 15).then(function (r) {
                    assert.deepEqual(r, ["a", 1, "b", 2, 'SELECT * FROM test WHERE (z = 15) ORDER BY a LIMIT 1']);
                }),
                d.order("a").last({z:26}).then(function (r) {
                    assert.deepEqual(r, ["a", 1, "b", 2, 'SELECT * FROM test WHERE (z = 26) ORDER BY a DESC LIMIT 1']);
                }),
                d.order("a").last("z = ?", 15).then(function (r) {
                    assert.deepEqual(r, ["a", 1, "b", 2, 'SELECT * FROM test WHERE (z = 15) ORDER BY a DESC LIMIT 1']);
                }),

                //with callback

                d.order("a").first({z:26}, function (err, r) {
                    assert.isNull(err);
                    assert.deepEqual(r, ["a", 1, "b", 2, 'SELECT * FROM test WHERE (z = 26) ORDER BY a LIMIT 1']);
                }),
                d.order("a").first("z = ?", 15, function (err, r) {
                    assert.isNull(err);
                    assert.deepEqual(r, ["a", 1, "b", 2, 'SELECT * FROM test WHERE (z = 15) ORDER BY a LIMIT 1']);
                }),
                d.order("a").last({z:26}, function (err, r) {
                    assert.isNull(err);
                    assert.deepEqual(r, ["a", 1, "b", 2, 'SELECT * FROM test WHERE (z = 26) ORDER BY a DESC LIMIT 1']);
                }),
                d.order("a").last("z = ?", 15, function (err, r) {
                    assert.isNull(err);
                    assert.deepEqual(r, ["a", 1, "b", 2, 'SELECT * FROM test WHERE (z = 15) ORDER BY a DESC LIMIT 1']);
                })
            );

        });

        it.should("set the limit and return an array of records if the given number is > 1", function () {
            var i = Math.floor(Math.random() * 10) + 10;
            return serial([
                function () {
                    return d.order("a").first(i).then(function (r) {
                        assert.lengthOf(r, i);
                        assert.deepEqual(r[0], ["a", 1, "b", 2, comb.string.format("SELECT * FROM test ORDER BY a LIMIT %d", i)]);
                    });
                },
                function () {
                    return d.order("a").last((i = Math.floor(Math.random() * 10) + 10)).then(function (r) {
                        assert.lengthOf(r, i);
                        assert.deepEqual(r[0], ["a", 1, "b", 2, comb.string.format("SELECT * FROM test ORDER BY a DESC LIMIT %d", i)]);
                    });
                },
                function () {
                    //with callback
                    return d.order("a").first((i = Math.floor(Math.random() * 10) + 10), function (err, r) {
                        assert.isNull(err);
                        assert.lengthOf(r, i);
                        assert.deepEqual(r[0], ["a", 1, "b", 2, comb.string.format("SELECT * FROM test ORDER BY a LIMIT %d", i)]);
                    });
                },
                function () {
                    return d.order("a").last((i = Math.floor(Math.random() * 10) + 10), function (err, r) {
                        assert.isNull(err);
                        assert.lengthOf(r, i);
                        assert.deepEqual(r[0], ["a", 1, "b", 2, comb.string.format("SELECT * FROM test ORDER BY a DESC LIMIT %d", i)]);
                    });
                }
            ]);
        });

        it.should("return the first matching record if a block is given without an argument", function () {
            return serial([
                function () {
                    return d.first(function () {
                        return this.z.sqlNumber.gt(26);
                    }).then(function (r) {
                            assert.deepEqual(r, ["a", 1, "b", 2, 'SELECT * FROM test WHERE (z > 26) LIMIT 1']);
                        });
                },
                function () {
                    return d.order("name").last(function () {
                        return this.z.sqlNumber.gt(26);
                    }).then(function (r) {
                            assert.deepEqual(r, ["a", 1, "b", 2, 'SELECT * FROM test WHERE (z > 26) ORDER BY name DESC LIMIT 1']);
                        });
                },
                function () {

                    //with callback
                    return d.first(function () {
                            return this.z.sqlNumber.gt(26);
                        },
                        function (err, r) {
                            assert.isNull(err);
                            assert.deepEqual(r, ["a", 1, "b", 2, 'SELECT * FROM test WHERE (z > 26) LIMIT 1']);
                        });
                },
                function () {
                    return d.order("name").last(
                        function () {
                            return this.z.sqlNumber.gt(26);
                        },
                        function (err, r) {
                            assert.isNull(err);
                            assert.deepEqual(r, ["a", 1, "b", 2, 'SELECT * FROM test WHERE (z > 26) ORDER BY name DESC LIMIT 1']);
                        });
                }
            ]);
        });

        it.should("combine block and standard argument filters if argument is not an Integer", function () {
            return when(
                d.first({y:25},
                    function () {
                        return this.z.sqlNumber.gt(26);
                    }).then(function (r) {
                        assert.deepEqual(r, ["a", 1, "b", 2, 'SELECT * FROM test WHERE ((z > 26) AND (y = 25)) LIMIT 1']);
                    }),
                d.order("name").last("y = ?", 16,
                    function () {
                        return this.z.sqlNumber.gt(26);
                    }).then(function (r) {
                        assert.deepEqual(r, ["a", 1, "b", 2, 'SELECT * FROM test WHERE ((z > 26) AND (y = 16)) ORDER BY name DESC LIMIT 1']);
                    }),
                //with callback
                d.first({y:25},
                    function () {
                        return this.z.sqlNumber.gt(26);
                    },
                    function (err, r) {
                        assert.isNull(err);
                        assert.deepEqual(r, ["a", 1, "b", 2, 'SELECT * FROM test WHERE ((z > 26) AND (y = 25)) LIMIT 1']);
                    }),
                d.order("name").last("y = ?", 16,
                    function () {
                        return this.z.sqlNumber.gt(26);
                    },
                    function (err, r) {
                        assert.isNull(err);
                        assert.deepEqual(r, ["a", 1, "b", 2, 'SELECT * FROM test WHERE ((z > 26) AND (y = 16)) ORDER BY name DESC LIMIT 1']);
                    })
            );
        });

        it.should("return the first matching record if a block is given without an argument", function () {
            var i = Math.floor(Math.random() * 10) + 10;
            return serial([
                function () {
                    return d.order("a").first(i,function () {
                        return this.z.sqlNumber.gt(26);
                    }).then(function (r) {
                            assert.lengthOf(r, i);
                            assert.deepEqual(r[0], ["a", 1, "b", 2, comb.string.format("SELECT * FROM test WHERE (z > 26) ORDER BY a LIMIT %d", i)]);
                        });
                },
                function () {
                    return d.order("name").last((i = Math.floor(Math.random() * 10) + 10),
                        function () {
                            return this.z.sqlNumber.gt(26);
                        }).then(function (r) {
                            assert.lengthOf(r, i);
                            assert.deepEqual(r[0], ["a", 1, "b", 2, comb.string.format("SELECT * FROM test WHERE (z > 26) ORDER BY name DESC LIMIT %d", i)]);
                        });
                },
                function () {
                    //with callback
                    return d.order("a").first(i,
                        function () {
                            return this.z.sqlNumber.gt(26);
                        },
                        function (err, r) {
                            assert.isNull(err);
                            assert.lengthOf(r, i);
                            assert.deepEqual(r[0], ["a", 1, "b", 2, comb.string.format("SELECT * FROM test WHERE (z > 26) ORDER BY a LIMIT %d", i)]);
                        });
                },
                function () {
                    return d.order("name").last((i = Math.floor(Math.random() * 10) + 10),
                        function () {
                            return this.z.sqlNumber.gt(26);
                        },
                        function (err, r) {
                            assert.isNull(err);
                            assert.lengthOf(r, i);
                            assert.deepEqual(r[0], ["a", 1, "b", 2, comb.string.format("SELECT * FROM test WHERE (z > 26) ORDER BY name DESC LIMIT %d", i)]);
                        });
                }
            ]);
        });

        it.should("last should raise an error if no order is given", function () {
            assert.throws(hitch(d, "last"));
            assert.throws(hitch(d, "last", 2));
            assert.doesNotThrow(hitch(d.order("a"), "last"));
            assert.doesNotThrow(hitch(d.order("a"), "last", 2));
        });

        it.should("last should invert the order", function () {
            return when(
                d.order("a").last().then(function (r) {
                    assert.equal(r.pop(), 'SELECT * FROM test ORDER BY a DESC LIMIT 1');
                }),
                d.order(sql.b.desc()).last().then(function (r) {
                    assert.equal(r.pop(), 'SELECT * FROM test ORDER BY b ASC LIMIT 1');
                }),
                d.order("c", "d").last().then(function (r) {
                    assert.equal(r.pop(), 'SELECT * FROM test ORDER BY c DESC, d DESC LIMIT 1');
                }),

                d.order(sql.e.desc(), "f").last().then(function (r) {
                    assert.equal(r.pop(), 'SELECT * FROM test ORDER BY e ASC, f DESC LIMIT 1');
                }),

                //with callback

                d.order("a").last(function (err, r) {
                    assert.isNull(err);
                    assert.equal(r.pop(), 'SELECT * FROM test ORDER BY a DESC LIMIT 1');
                }),
                d.order(sql.b.desc()).last(function (err, r) {
                    assert.isNull(err);
                    assert.equal(r.pop(), 'SELECT * FROM test ORDER BY b ASC LIMIT 1');
                }),
                d.order("c", "d").last(function (err, r) {
                    assert.isNull(err);
                    assert.equal(r.pop(), 'SELECT * FROM test ORDER BY c DESC, d DESC LIMIT 1');
                }),

                d.order(sql.e.desc(), "f").last(function (err, r) {
                    assert.isNull(err);
                    assert.equal(r.pop(), 'SELECT * FROM test ORDER BY e ASC, f DESC LIMIT 1');
                })
            );
        });


    });

    it.describe("#singleRecord", function (it) {
        var d = new (comb.define(Dataset, {
            instance:{
                fetchRows:function (sql, cb) {
                    return new comb.Promise().callback(cb(sql));
                }
            }
        }))().from("test");

        var e = new (comb.define(Dataset, {
            instance:{
                fetchRows:function (sql, cb) {
                    return new comb.Promise().callback();
                }
            }
        }))().from("test");

        it.should("call each with a limit of 1 and return the record", function () {
            return when(
                d.singleRecord().then(function (r) {
                    assert.equal(r, 'SELECT * FROM test LIMIT 1');
                }),
                d.singleRecord(function (err, r) {
                    assert.isNull(err);
                    assert.equal(r, 'SELECT * FROM test LIMIT 1');
                })
            );
        });

        it.should("return null if no record is present", function () {
            return when(
                e.singleRecord().then(function (r) {
                    assert.isNull(r);
                }),
                e.singleRecord(function (err, r) {
                    assert.isNull(err);
                    assert.isNull(r);
                })
            );
        });
    });

    it.describe("#singleValue", function (it) {
        var d = new (comb.define(Dataset, {
            instance:{
                fetchRows:function (sql, cb) {
                    this.__columns = ["a"];
                    return new comb.Promise().callback(cb ? cb({1:sql}) : {1:sql});
                }
            }
        }))().from("test");

        var e = new (comb.define(Dataset, {
            instance:{
                fetchRows:function (sql, cb) {
                    return new comb.Promise().callback();
                }
            }
        }))().from("test");

        it.should("call each and return the first value of the first record", function () {
            return when(
                d.singleValue().then(function (r) {
                    assert.equal(r, 'SELECT * FROM test LIMIT 1');
                }),
                //with callback
                d.singleValue(function (err, r) {
                    assert.isNull(err);
                    assert.equal(r, 'SELECT * FROM test LIMIT 1');
                })
            );
        });

        it.should("return null if no record is present", function () {
            return when(
                e.singleValue().then(function (r) {
                    assert.isNull(r);
                }),
                //with callback
                e.singleValue(function (err, r) {
                    assert.isNull(err);
                    assert.isNull(r);
                })
            );
        });

        it.should("should work on a graphed ataset", function (next) {
            d.graph(d, ["a"], {tableAlias:"test2"}).then(function (d) {
                when(
                    d.singleValue().then(function (r) {
                        assert.equal(r, 'SELECT test.a, test2.a AS test2_a FROM test LEFT OUTER JOIN test AS test2 USING (a) LIMIT 1');
                    }),
                    //with callback
                    d.singleValue(function (err, r) {
                        assert.isNull(err);
                        assert.equal(r, 'SELECT test.a, test2.a AS test2_a FROM test LEFT OUTER JOIN test AS test2 USING (a) LIMIT 1');
                    })
                ).classic(next);
            });
        });
    });

    it.describe("#get", function (it) {
        var d = new (comb.define(Dataset, {
            instance:{
                lastSql:null,

                fetchRows:function (sql, cb) {
                    this.lastSql = sql;
                    return new comb.Promise().callback(cb({name:sql}));
                }
            }
        }))().from("test");

        it.should("select the specified column and fetch its value", function () {
            return when(
                d.get("name").then(function (r) {
                    assert.equal(r, "SELECT name FROM test LIMIT 1");
                }),
                d.get("abc").then(function (r) {
                    assert.equal(r, "SELECT abc FROM test LIMIT 1");
                }),

                //with callback
                d.get("name", function (err, r) {
                    assert.isNull(err);
                    assert.equal(r, "SELECT name FROM test LIMIT 1");
                }),
                d.get("abc", function (err, r) {
                    assert.isNull(err);
                    assert.equal(r, "SELECT abc FROM test LIMIT 1");
                })
            );
        });

        it.should("work with filters", function () {
            return when(
                d.filter({id:1}).get("name").then(function (r) {
                    assert.equal(r, "SELECT name FROM test WHERE (id = 1) LIMIT 1");
                }),
                d.filter({id:1}).get("name", function (err, r) {
                    assert.isNull(err);
                    assert.equal(r, "SELECT name FROM test WHERE (id = 1) LIMIT 1");
                })
            );
        });

        it.should("work with aliased fields", function () {
            return when(
                d.get(sql.x__b.as("name")).then(function (r) {
                    assert.equal(r, "SELECT x.b AS name FROM test LIMIT 1");
                }),
                //with callback
                d.get(sql.x__b.as("name"), function (err, r) {
                    assert.isNull(err);
                    assert.equal(r, "SELECT x.b AS name FROM test LIMIT 1");
                })
            );
        });

        it.should("accept a filter block", function () {
            return when(
                d.get(
                    function (o) {
                        return o.x__b.as("name");
                    }).then(function (r) {
                        assert.equal(r, "SELECT x.b AS name FROM test LIMIT 1");
                    }),
                d.get(
                    function () {
                        return this.x(1).as("name");
                    }).then(function (r) {
                        assert.equal(r, "SELECT x(1) AS name FROM test LIMIT 1");
                    }),
                //with callback
                d.get(
                    function (o) {
                        return o.x__b.as("name");
                    },
                    function (err, r) {
                        assert.isNull(err);
                        assert.equal(r, "SELECT x.b AS name FROM test LIMIT 1");
                    }),
                d.get(
                    function () {
                        return this.x(1).as("name");
                    },
                    function (err, r) {
                        assert.isNull(err);
                        assert.equal(r, "SELECT x(1) AS name FROM test LIMIT 1");
                    })
            );
        });
    });

    it.describe("#columns", function (it) {
        var i = 0;
        var arr = ["a", "b", "c"];
        var dataset = new (comb.define(DummyDataset, { instance:{
            forEach:function () {
                var ret = new comb.Promise().callback();
                this.__columns = this.selectSql + arr[i++];
                return ret;
            }
        } }))().from("items");


        it.should("return the value of columns if columns is not null", function (next) {
            dataset.__columns = ["a", "b", "c"];
            dataset.columns.then(function (arr) {
                assert.deepEqual(arr, ["a", "b", "c"]);
                next();
            }, next);
        });

        it.should("attempt to get a single record and return columns if columns is null", function (next) {
            dataset.__columns = null;
            dataset.columns.then(function (arr) {
                assert.deepEqual(arr, 'SELECT * FROM items LIMIT 1a');
                dataset.__opts.from = ["nana"];
                dataset.columns.then(function (arr) {
                    assert.deepEqual(arr, 'SELECT * FROM items LIMIT 1a');
                    next();
                }, next);
            }, next);
        });

        it.should("ignore any filters, orders, or DISTINCT clauses", function (next) {
            dataset = dataset.from("items").filter({b:100}).order("b").distinct();
            dataset.__columns = null;
            dataset.columns.then(function (arr) {
                assert.deepEqual(arr, 'SELECT * FROM items LIMIT 1b');
                next();
            }, next);
        });
    });


    it.describe("#import", function (it) {
        var db = new (comb.define(Database, {
            instance:{
                sqls:null,

                execute:function (sql, opts) {
                    var ret = new comb.Promise().callback();
                    this.sqls = this.sqls || [];
                    this.sqls.push(sql);
                    return ret;
                },

                transaction:function (opts, cb) {
                    if (comb.isFunction(opts)) {
                        cb = opts;
                        opts = {};
                    } else {
                        opts = opts || {};
                    }
                    var ret = new comb.Promise();
                    this.sqls = this.sqls || [];
                    this.sqls.push("BEGIN");
                    if (cb) {
                        cb();
                    }
                    this.sqls.push("COMMIT");
                    ret.callback();
                    return ret;
                },

                reset:function () {
                    this.sqls = [];
                }
            }
        }))();
        var ds = new Dataset(db).from("items");

        it.should("accept string keys as column names", function () {
            return when(
                ds["import"](['x', 'y'], [
                    [1, 2],
                    [3, 4]
                ]).then(function () {
                        assert.deepEqual(db.sqls, [
                            'BEGIN',
                            "INSERT INTO items (x, y) VALUES (1, 2)",
                            "INSERT INTO items (x, y) VALUES (3, 4)",
                            'COMMIT'
                        ]);
                        db.reset();
                    }),
                ds["import"](
                    ['x', 'y'],
                    [
                        [1, 2],
                        [3, 4]
                    ],
                    function (err) {
                        assert.isNull(err);
                        assert.deepEqual(db.sqls, [
                            'BEGIN',
                            "INSERT INTO items (x, y) VALUES (1, 2)",
                            "INSERT INTO items (x, y) VALUES (3, 4)",
                            'COMMIT'
                        ]);
                        db.reset();
                    })
            );
        });

        it.should("accept a columns array and a values array", function () {
            return when(
                ds["import"](
                    ["x", "y"],
                    [
                        [1, 2],
                        [3, 4]
                    ]).then(function () {
                        assert.deepEqual(db.sqls, [
                            'BEGIN',
                            "INSERT INTO items (x, y) VALUES (1, 2)",
                            "INSERT INTO items (x, y) VALUES (3, 4)",
                            'COMMIT'
                        ]);
                        db.reset();
                    }),
                ds["import"](
                    ["x", "y"],
                    [
                        [1, 2],
                        [3, 4]
                    ],
                    function (err) {
                        assert.isNull(err);
                        assert.deepEqual(db.sqls, [
                            'BEGIN',
                            "INSERT INTO items (x, y) VALUES (1, 2)",
                            "INSERT INTO items (x, y) VALUES (3, 4)",
                            'COMMIT'
                        ]);
                        db.reset();
                    })
            );
        });

        it.should("accept a columns array and a dataset", function () {
            var ds2 = new Dataset(db).from("cats").filter({purr:true}).select("a", "b");

            return when(
                ds["import"](["x", "y"], ds2).then(function () {
                    assert.deepEqual(db.sqls, [
                        'BEGIN',
                        "INSERT INTO items (x, y) SELECT a, b FROM cats WHERE (purr IS TRUE)",
                        'COMMIT'
                    ]);
                    db.reset();
                }),
                ds["import"](["x", "y"], ds2, function (err) {
                    assert.isNull(err);
                    assert.deepEqual(db.sqls, [
                        'BEGIN',
                        "INSERT INTO items (x, y) SELECT a, b FROM cats WHERE (purr IS TRUE)",
                        'COMMIT'
                    ]);
                    db.reset();
                })
            );
        });

        it.should("accept a columns array and a values array with {commitEvery) option", function () {
            return when(
                ds["import"](
                    ["x", "y"],
                    [
                        [1, 2],
                        [3, 4],
                        [5, 6]
                    ],
                    {commitEvery:3}
                ).then(function () {
                        assert.deepEqual(db.sqls, [
                            'BEGIN',
                            "INSERT INTO items (x, y) VALUES (1, 2)",
                            "INSERT INTO items (x, y) VALUES (3, 4)",
                            "INSERT INTO items (x, y) VALUES (5, 6)",
                            'COMMIT'
                        ]);
                        db.reset();
                    }),
                //with callback
                ds["import"](
                    ["x", "y"],
                    [
                        [1, 2],
                        [3, 4],
                        [5, 6]
                    ],
                    {commitEvery:3},
                    function (err) {
                        assert.isNull(err);
                        assert.deepEqual(db.sqls, [
                            'BEGIN',
                            "INSERT INTO items (x, y) VALUES (1, 2)",
                            "INSERT INTO items (x, y) VALUES (3, 4)",
                            "INSERT INTO items (x, y) VALUES (5, 6)",
                            'COMMIT'
                        ]);
                        db.reset();
                    })
            );
        });

        it.should("accept a columns array and a values array with slice option", function () {
            return when(
                ds["import"](
                    ["x", "y"],
                    [
                        [1, 2],
                        [3, 4],
                        [5, 6]
                    ],
                    {slice:2}
                ).then(function () {
                        assert.deepEqual(db.sqls, [
                            'BEGIN',
                            "INSERT INTO items (x, y) VALUES (1, 2)",
                            "INSERT INTO items (x, y) VALUES (3, 4)",
                            'COMMIT',
                            'BEGIN',
                            "INSERT INTO items (x, y) VALUES (5, 6)",
                            'COMMIT'
                        ]);
                        db.reset();
                    }),
                ds["import"](
                    ["x", "y"],
                    [
                        [1, 2],
                        [3, 4],
                        [5, 6]
                    ],
                    {slice:2},
                    function (err) {
                        assert.isNull(err);
                        assert.deepEqual(db.sqls, [
                            'BEGIN',
                            "INSERT INTO items (x, y) VALUES (1, 2)",
                            "INSERT INTO items (x, y) VALUES (3, 4)",
                            'COMMIT',
                            'BEGIN',
                            "INSERT INTO items (x, y) VALUES (5, 6)",
                            'COMMIT'
                        ]);
                        db.reset();
                    })
            );
        });


    });


    it.describe("dui methods", function (it) {
        var db = new Database(),
            ds = db.from("items");

        it.describe("#fetchRows", function (it) {
            it.should("raise an Error", function () {
                assert.throws(ds, "fetchRows", '', function () {
                });
            });
        });
        it.describe("#remove", function (it) {
            it.should("execute delete SQL", function () {
                var orig = db.execute;
                var s, o;
                db.execute = function (sql, opts) {
                    s = sql;
                    o = opts || {};
                    return new comb.Promise().callback(sql);
                };
                ds.remove();
                db.execute = orig;
                assert.equal(s, 'DELETE FROM items');
                assert.deepEqual(o, {server:"default"});
            });

            it.should("executeDui delete SQL", function () {
                var s, o;
                var orig = db.executeDui;
                db.executeDui = function (sql, opts) {
                    s = sql;
                    o = opts || {};
                    return new comb.Promise().callback(sql);
                };
                ds.remove();
                assert.equal(s, 'DELETE FROM items');
                assert.deepEqual(o, {server:"default"});
                db.executeDui = orig;
            });

        });

        it.describe("#insert", function (it) {
            it.should("execute insert SQL", function () {
                var orig = db.execute;
                var s, o;
                db.execute = function (sql, opts) {
                    s = sql;
                    o = opts || {};
                    return new comb.Promise().callback(sql);
                };
                ds.insert([]);
                db.execute = orig;
                assert.equal(s, 'INSERT INTO items DEFAULT VALUES');
                assert.deepEqual(o, {server:"default"});
            });

            it.should("executeDui insert SQL", function () {
                var s, o;
                var orig = db.executeDui;
                db.executeDui = function (sql, opts) {
                    s = sql;
                    o = opts || {};
                    return new comb.Promise().callback(sql);
                };
                ds.insert([]);
                assert.equal(s, 'INSERT INTO items DEFAULT VALUES');
                assert.deepEqual(o, {server:"default"});
                db.executeDui = orig;
            });
        });

        it.describe("#update", function (it) {
            it.should("execute update SQL", function () {
                var orig = db.execute;
                var s, o;
                db.execute = function (sql, opts) {
                    s = sql;
                    o = opts || {};
                    return new comb.Promise().callback(sql);
                };
                ds.update({number:1});
                db.execute = orig;
                assert.equal(s, 'UPDATE items SET number = 1');
                assert.deepEqual(o, {server:"default"});
            });

            it.should("executeDui update SQL", function () {
                var s, o;
                var orig = db.executeDui;
                db.executeDui = function (sql, opts) {
                    s = sql;
                    o = opts || {};
                    return new comb.Promise().callback(sql);
                };
                ds.update({number:1});
                assert.equal(s, 'UPDATE items SET number = 1');
                assert.deepEqual(o, {server:"default"});
                db.executeDui = orig;
            });
        });

        it.describe("#truncate", function (it) {
            it.should("execute truncate SQL", function () {
                var orig = db.execute;
                var s, o;
                db.execute = function (sql, opts) {
                    s = sql;
                    o = opts || {};
                    return new comb.Promise().callback(sql);
                };
                ds.truncate();
                db.execute = orig;
                assert.equal(s, 'TRUNCATE TABLE items');
                assert.deepEqual(o, {server:"default"});
            });

            it.should("executeDui truncate SQL", function () {
                var s, o;
                var orig = db.executeDui;
                db.executeDui = function (sql, opts) {
                    s = sql;
                    o = opts || {};
                    return new comb.Promise().callback(sql);
                };
                ds.truncate();
                assert.equal(s, 'TRUNCATE TABLE items');
                assert.deepEqual(o, {server:"default"});
                db.executeDui = orig;
            });

            it.should("raise an InvalidOperation exception if the dataset is filtered", function () {
                assert.throws(ds, "filter", {a:1});
            });
        });

        it.describe("#execute", function (it) {
            it.should("execute the SQL on the database", function () {
                var orig = db.execute;
                var s, o;
                db.execute = function (sql, opts) {
                    s = sql;
                    o = opts || {};
                };
                ds.execute("SELECT 1");
                db.execute = orig;
                assert.equal(s, 'SELECT 1');
                assert.deepEqual(o, {server:"readOnly"});

            });
        });

    });

    it.describe("#multiInsert", function (it) {
        var db = new (comb.define(null, {
            instance:{
                sqls:[],

                reset:function () {
                    this.sqls.length = 0;
                },

                execute:function (sql, opts) {
                    this.sqls.push(sql);
                    return new comb.Promise().callback();
                },

                executeDui:function () {
                    return this.execute.apply(this, arguments);
                },

                transaction:function (opts, cb) {
                    if (comb.isFunction(opts)) {
                        cb = opts;
                        opts = {};
                    } else {
                        opts = opts || {};
                    }
                    var ret = new comb.Promise();
                    this.sqls.push("BEGIN");
                    if (cb) {
                        cb();
                    }
                    this.sqls.push("COMMIT");
                    ret.callback();
                    return ret;
                }

            }
        }))();
        var ds = new Dataset(db).from("items");
        var list = [
            {name:'abc'},
            {name:'def'},
            {name:'ghi'}
        ];

        it.should("issue multiple insert statements inside a transaction", function () {
            return when(
                ds.multiInsert(list).then(function () {
                    assert.deepEqual(db.sqls, [
                        'BEGIN',
                        "INSERT INTO items (name) VALUES ('abc')",
                        "INSERT INTO items (name) VALUES ('def')",
                        "INSERT INTO items (name) VALUES ('ghi')",
                        'COMMIT'
                    ]);
                    db.reset();
                }),
                ds.multiInsert(list, function (err) {
                    assert.isNull(err);
                    assert.deepEqual(db.sqls, [
                        'BEGIN',
                        "INSERT INTO items (name) VALUES ('abc')",
                        "INSERT INTO items (name) VALUES ('def')",
                        "INSERT INTO items (name) VALUES ('ghi')",
                        'COMMIT'
                    ]);
                    db.reset();
                })
            );
        });

        it.should("handle different formats for tables", function () {

            return when(
                ds.from("sch__tab").multiInsert(list).then(function () {
                    assert.deepEqual(db.sqls, [
                        'BEGIN',
                        "INSERT INTO sch.tab (name) VALUES ('abc')",
                        "INSERT INTO sch.tab (name) VALUES ('def')",
                        "INSERT INTO sch.tab (name) VALUES ('ghi')",
                        'COMMIT'
                    ]);
                    db.reset();
                }),

                ds.from(sql.tab.qualify("sch")).multiInsert(list).then(function () {
                    assert.deepEqual(db.sqls, [
                        'BEGIN',
                        "INSERT INTO sch.tab (name) VALUES ('abc')",
                        "INSERT INTO sch.tab (name) VALUES ('def')",
                        "INSERT INTO sch.tab (name) VALUES ('ghi')",
                        'COMMIT'
                    ]);
                    db.reset();
                }),
                ds.from(new Identifier("sch__tab")).multiInsert(list).then(function () {
                    assert.deepEqual(db.sqls, [
                        'BEGIN',
                        "INSERT INTO sch__tab (name) VALUES ('abc')",
                        "INSERT INTO sch__tab (name) VALUES ('def')",
                        "INSERT INTO sch__tab (name) VALUES ('ghi')",
                        'COMMIT'
                    ]);
                    db.reset();
                }),


                //with callback
                ds.from("sch__tab").multiInsert(list, function (err) {
                    assert.isNull(err);
                    assert.deepEqual(db.sqls, [
                        'BEGIN',
                        "INSERT INTO sch.tab (name) VALUES ('abc')",
                        "INSERT INTO sch.tab (name) VALUES ('def')",
                        "INSERT INTO sch.tab (name) VALUES ('ghi')",
                        'COMMIT'
                    ]);
                    db.reset();
                }),

                ds.from(sql.tab.qualify("sch")).multiInsert(list, function (err) {
                    assert.isNull(err);
                    assert.deepEqual(db.sqls, [
                        'BEGIN',
                        "INSERT INTO sch.tab (name) VALUES ('abc')",
                        "INSERT INTO sch.tab (name) VALUES ('def')",
                        "INSERT INTO sch.tab (name) VALUES ('ghi')",
                        'COMMIT'
                    ]);
                    db.reset();
                }),
                ds.from(new Identifier("sch__tab")).multiInsert(list, function (err) {
                    assert.isNull(err);
                    assert.deepEqual(db.sqls, [
                        'BEGIN',
                        "INSERT INTO sch__tab (name) VALUES ('abc')",
                        "INSERT INTO sch__tab (name) VALUES ('def')",
                        "INSERT INTO sch__tab (name) VALUES ('ghi')",
                        'COMMIT'
                    ]);
                    db.reset();
                })
            );
        });


        it.should("accept the commitEvery option for committing every x records", function () {
            return when(
                ds.multiInsert(list, {commitEvery:1}).then(function () {
                    assert.deepEqual(db.sqls, [
                        'BEGIN',
                        "INSERT INTO items (name) VALUES ('abc')",
                        'COMMIT',
                        'BEGIN',
                        "INSERT INTO items (name) VALUES ('def')",
                        'COMMIT',
                        'BEGIN',
                        "INSERT INTO items (name) VALUES ('ghi')",
                        'COMMIT'
                    ]);
                    db.reset();
                }),

                //with callback
                ds.multiInsert(list, {commitEvery:1}, function (err) {
                    assert.isNull(err);
                    assert.deepEqual(db.sqls, [
                        'BEGIN',
                        "INSERT INTO items (name) VALUES ('abc')",
                        'COMMIT',
                        'BEGIN',
                        "INSERT INTO items (name) VALUES ('def')",
                        'COMMIT',
                        'BEGIN',
                        "INSERT INTO items (name) VALUES ('ghi')",
                        'COMMIT'
                    ]);
                    db.reset();
                })

            );
        });


        it.should("accept the slice option for committing every x records", function () {
            return when(
                ds.multiInsert(list, {slice:2}).then(function () {
                    assert.deepEqual(db.sqls, [
                        'BEGIN',
                        "INSERT INTO items (name) VALUES ('abc')",
                        "INSERT INTO items (name) VALUES ('def')",
                        'COMMIT',
                        'BEGIN',
                        "INSERT INTO items (name) VALUES ('ghi')",
                        'COMMIT'
                    ]);
                    db.reset();
                }),
                ds.multiInsert(list, {slice:2}, function (err) {
                    assert.isNull(err);
                    assert.deepEqual(db.sqls, [
                        'BEGIN',
                        "INSERT INTO items (name) VALUES ('abc')",
                        "INSERT INTO items (name) VALUES ('def')",
                        'COMMIT',
                        'BEGIN',
                        "INSERT INTO items (name) VALUES ('ghi')",
                        'COMMIT'
                    ]);
                    db.reset();
                })
            );
        });


        it.should("not do anything if no hashes are provided", function () {
            return when(
                ds.multiInsert([]).then(function () {
                    assert.deepEqual(db.sqls, []);
                }),
                ds.multiInsert([], function (err) {
                    assert.isNull(err);
                    assert.deepEqual(db.sqls, []);
                })
            );
        });
    });

    it.describe("#toCsv", function (it) {
        var ds = new (comb.define(Dataset, {

            instance:{
                data:[
                    {a:1, b:2, c:3},
                    {a:4, b:5, c:6},
                    { a:7, b:8, c:9}
                ],

                __columns:["a", "b", "c"],

                fetchRows:function (sql, cb) {
                    this.data.forEach(cb, this);
                    return new comb.Promise().callback();
                },

                naked:function () {
                    return this;
                }
            }
        }))().from("items");

        it.should("format a CSV representation of the records", function () {
            return when(
                ds.toCsv().then(function (csv) {
                    assert.equal(csv, "a, b, c\r\n1, 2, 3\r\n4, 5, 6\r\n7, 8, 9\r\n");
                }),
                ds.toCsv(function (err, csv) {
                    assert.isNull(err);
                    assert.equal(csv, "a, b, c\r\n1, 2, 3\r\n4, 5, 6\r\n7, 8, 9\r\n");
                })
            );
        });

        it.should("exclude column titles if so specified", function () {
            return when(
                ds.toCsv(false).then(function (csv) {
                    assert.equal(csv, "1, 2, 3\r\n4, 5, 6\r\n7, 8, 9\r\n");
                }),
                ds.toCsv(false, function (err, csv) {
                    assert.isNull(err);
                    assert.equal(csv, "1, 2, 3\r\n4, 5, 6\r\n7, 8, 9\r\n");
                })
            );
        });
    });


    it.describe("#all", function (it) {
        var ds = new (comb.define(Dataset, {
            instance:{
                fetchRows:function (sql, block) {
                    block({x:1, y:2});
                    block({x:3, y:4});
                    block(sql);
                    return new comb.Promise().callback();
                }
            }
        }))().from("items");

        it.should("return an array with all records", function () {
            return when(
                ds.all().then(function (ret) {
                    assert.deepEqual(ret, [
                        {x:1, y:2},
                        {x:3, y:4},
                        "SELECT * FROM items"
                    ]);
                }),
                //with callback
                ds.all(null, function (err, ret) {
                    assert.isNull(err);
                    assert.deepEqual(ret, [
                        {x:1, y:2},
                        {x:3, y:4},
                        "SELECT * FROM items"
                    ]);
                })
            );
        });

        it.should("iterate over the array if a block is given", function () {
            return comb.serial([
                function () {
                    var a = [];
                    return ds.all(
                        function (r) {
                            a.push(comb.isHash(r) ? r.x : r);
                        }).then(function () {
                            assert.deepEqual(a, [1, 3, "SELECT * FROM items"]);
                        });
                },
                function () {
                    //with callback
                    var a = [];
                    return ds.all(
                        function (r) {
                            a.push(comb.isHash(r) ? r.x : r);
                        },
                        function (err) {
                            assert.isNull(err);
                            assert.deepEqual(a, [1, 3, "SELECT * FROM items"]);
                        });
                }
            ]);
        });
    });

    it.describe("#selectMap", function (it) {
        var DS = comb.define(MockDataset, {
            instance:{
                fetchRows:function (sql, cb) {
                    var ret = this.db.run(sql);
                    cb({c:1});
                    cb({c:2});
                    return ret;
                }
            }
        });
        var MDB = comb.define(MockDatabase, {
            instance:{
                getters:{
                    dataset:function () {
                        return new DS(this);
                    }
                }
            }
        });
        var ds = new MDB().from("t");
        ds.db.reset();

        it.should("do select and map in one step", function () {

            return when(
                ds.selectMap("a").then(function (res) {
                    assert.deepEqual(res, [1, 2]);
                    assert.deepEqual(ds.db.sqls, ['SELECT a FROM t']);
                    ds.db.reset();
                }),

                ds.selectMap("a", function (err, res) {
                    assert.isNull(err);
                    assert.deepEqual(res, [1, 2]);
                    assert.deepEqual(ds.db.sqls, ['SELECT a FROM t']);
                    ds.db.reset();
                })
            );

        });

        it.should("handle implicit qualifiers in arguments", function () {
            return when(
                ds.selectMap("a__b").then(function (res) {
                    assert.deepEqual(res, [1, 2]);
                    assert.deepEqual(ds.db.sqls, ['SELECT a.b FROM t']);
                    ds.db.reset();
                }),
                ds.selectMap("a__b", function (err, res) {
                    assert.isNull(err);
                    assert.deepEqual(res, [1, 2]);
                    assert.deepEqual(ds.db.sqls, ['SELECT a.b FROM t']);
                    ds.db.reset();
                })
            );
        });

        it.should("handle implicit aliases in arguments", function () {
            return when(
                ds.selectMap("a___b").then(function (res) {
                    assert.deepEqual(res, [1, 2]);
                    assert.deepEqual(ds.db.sqls, ['SELECT a AS b FROM t']);
                    ds.db.reset();
                }),
                ds.selectMap("a___b", function (err, res) {
                    assert.isNull(err);
                    assert.deepEqual(res, [1, 2]);
                    assert.deepEqual(ds.db.sqls, ['SELECT a AS b FROM t']);
                    ds.db.reset();
                })
            );
        });

        it.should("handle other objects", function () {
            return when(
                ds.selectMap(sql.literal("a").as("b")).then(function (res) {
                    assert.deepEqual(res, [1, 2]);
                    assert.deepEqual(ds.db.sqls, ['SELECT a AS b FROM t']);
                    ds.db.reset();
                }),
                ds.selectMap(sql.literal("a").as("b"), function (err, res) {
                    assert.isNull(err);
                    assert.deepEqual(res, [1, 2]);
                    assert.deepEqual(ds.db.sqls, ['SELECT a AS b FROM t']);
                    ds.db.reset();
                })
            );
        });

        it.should("accept a block", function () {
            return when(
                ds.selectMap(
                    function (t) {
                        return t.a(t.t__c);
                    }).then(function (res) {
                        assert.deepEqual(res, [1, 2]);
                        assert.deepEqual(ds.db.sqls, ['SELECT a(t.c) FROM t']);
                        ds.db.reset();
                    }),
                ds.selectMap(
                    function (t) {
                        return t.a(t.t__c);
                    }, function (err, res) {
                        assert.isNull(err);
                        assert.deepEqual(res, [1, 2]);
                        assert.deepEqual(ds.db.sqls, ['SELECT a(t.c) FROM t']);
                        ds.db.reset();
                    })
            );
        });

    });

    it.describe("#selectOrderMap", function (it) {
        var DS = comb.define(MockDataset, {
            instance:{
                fetchRows:function (sql, cb) {
                    var ret = this.db.run(sql);
                    cb({c:1});
                    cb({c:2});
                    return ret;
                }
            }
        });
        var MDB = comb.define(MockDatabase, {
            instance:{
                getters:{
                    dataset:function () {
                        return new DS(this);
                    }
                }
            }
        });
        var ds = new MDB().from("t");
        ds.db.reset();


        it.should("do select and map in one step", function () {
            return when(
                ds.selectOrderMap("a").then(function (res) {
                    assert.deepEqual(res, [1, 2]);
                    assert.deepEqual(ds.db.sqls, ['SELECT a FROM t ORDER BY a']);
                    ds.db.reset();
                }),
                //with callback
                ds.selectOrderMap("a", function (err, res) {
                    assert.isNull(err);
                    assert.deepEqual(res, [1, 2]);
                    assert.deepEqual(ds.db.sqls, ['SELECT a FROM t ORDER BY a']);
                    ds.db.reset();
                })
            );
        });

        it.should("handle implicit qualifiers in arguments", function () {
            return when(
                ds.selectOrderMap("a__b").then(function (res) {
                    assert.deepEqual(res, [1, 2]);
                    assert.deepEqual(ds.db.sqls, ['SELECT a.b FROM t ORDER BY a.b']);
                    ds.db.reset();
                }),
                //with callback
                ds.selectOrderMap("a__b", function (err, res) {
                    assert.isNull(err);
                    assert.deepEqual(res, [1, 2]);
                    assert.deepEqual(ds.db.sqls, ['SELECT a.b FROM t ORDER BY a.b']);
                    ds.db.reset();
                })
            );
        });

        it.should("handle implicit aliases in arguments", function () {
            return when(
                ds.selectOrderMap("a___b").then(function (res) {
                    assert.deepEqual(res, [1, 2]);
                    assert.deepEqual(ds.db.sqls, ['SELECT a AS b FROM t ORDER BY a']);
                    ds.db.reset();
                }),
                //with callback
                ds.selectOrderMap("a___b", function (err, res) {
                    assert.isNull(err);
                    assert.deepEqual(res, [1, 2]);
                    assert.deepEqual(ds.db.sqls, ['SELECT a AS b FROM t ORDER BY a']);
                    ds.db.reset();
                })
            );
        });

        it.should("handle implicit qualifiers and aliases in arguments", function () {
            return when(
                ds.selectOrderMap("t__a___b").then(function (res) {
                    assert.deepEqual(res, [1, 2]);
                    assert.deepEqual(ds.db.sqls, ['SELECT t.a AS b FROM t ORDER BY t.a']);
                    ds.db.reset();
                }),
                //with callback
                ds.selectOrderMap("t__a___b", function (err, res) {
                    assert.isNull(err);
                    assert.deepEqual(res, [1, 2]);
                    assert.deepEqual(ds.db.sqls, ['SELECT t.a AS b FROM t ORDER BY t.a']);
                    ds.db.reset();
                })
            );
        });

        it.should("handle AliasedExpressions", function () {
            return when(
                ds.selectOrderMap(sql.literal("a").as("b")).then(function (res) {
                    assert.deepEqual(res, [1, 2]);
                    assert.deepEqual(ds.db.sqls, ['SELECT a AS b FROM t ORDER BY a']);
                    ds.db.reset();
                }),
                ds.selectOrderMap(sql.literal("a").as("b"), function (err, res) {
                    assert.isNull(err);
                    assert.deepEqual(res, [1, 2]);
                    assert.deepEqual(ds.db.sqls, ['SELECT a AS b FROM t ORDER BY a']);
                    ds.db.reset();
                })
            );
        });

        it.should("accept a block", function () {
            return when(
                ds.selectOrderMap(
                    function (t) {
                        return t.a(t.t__c);
                    }).then(function (res) {
                        assert.deepEqual(res, [1, 2]);
                        assert.deepEqual(ds.db.sqls, ['SELECT a(t.c) FROM t ORDER BY a(t.c)']);
                        ds.db.reset();
                    }),
                //with callback
                ds.selectOrderMap(
                    function (t) {
                        return t.a(t.t__c);
                    },
                    function (err, res) {
                        assert.isNull(err);
                        assert.deepEqual(res, [1, 2]);
                        assert.deepEqual(ds.db.sqls, ['SELECT a(t.c) FROM t ORDER BY a(t.c)']);
                        ds.db.reset();
                    })
            );
        });

    });

    it.describe("#selectHash", function (it) {
        var DS = comb.define(MockDataset, {
            instance:{

                fetchRows:function (sql, cb) {
                    var ret = this.db.run(sql);
                    var hs = [
                        {a:1, b:2},
                        {a:3, b:4}
                    ];
                    hs.forEach(cb, this);
                    return ret;
                }
            }
        });
        var MDB = comb.define(MockDatabase, {
            instance:{
                getters:{
                    dataset:function () {
                        return new DS(this);
                    }
                }
            }
        });
        var ds = new MDB().from("t");
        ds.db.reset();


        it.should("do select and map in one step", function () {
            return when(
                ds.selectHash("a", "b").then(function (ret) {
                    assert.deepEqual(ret, {1:2, 3:4});
                    assert.deepEqual(ds.db.sqls, ['SELECT a, b FROM t']);
                    ds.db.reset();
                }),
                //with callback
                ds.selectHash("a", "b", function (err, ret) {
                    assert.isNull(err);
                    assert.deepEqual(ret, {1:2, 3:4});
                    assert.deepEqual(ds.db.sqls, ['SELECT a, b FROM t']);
                    ds.db.reset();
                })
            );
        });

        it.should("should handle implicit qualifiers in arguments", function () {
            return when(
                ds.selectHash("t__a", "t__b").then(function (ret) {
                    assert.deepEqual(ret, {1:2, 3:4});
                    assert.deepEqual(ds.db.sqls, ['SELECT t.a, t.b FROM t']);
                    ds.db.reset();
                }),
                //with callback
                ds.selectHash("t__a", "t__b", function (err, ret) {
                    assert.isNull(err);
                    assert.deepEqual(ret, {1:2, 3:4});
                    assert.deepEqual(ds.db.sqls, ['SELECT t.a, t.b FROM t']);
                    ds.db.reset();
                })
            );
        });

        it.should("should handle implicit aliases in arguments", function () {
            return when(
                ds.selectHash("c___a", "d___b").then(function (ret) {
                    assert.deepEqual(ret, {1:2, 3:4});
                    assert.deepEqual(ds.db.sqls, ['SELECT c AS a, d AS b FROM t']);
                    ds.db.reset();
                }),
                //with callback
                ds.selectHash("c___a", "d___b", function (err, ret) {
                    assert.isNull(err);
                    assert.deepEqual(ret, {1:2, 3:4});
                    assert.deepEqual(ds.db.sqls, ['SELECT c AS a, d AS b FROM t']);
                    ds.db.reset();
                })
            );
        });

        it.should("should handle implicit qualifiers and aliases in arguments", function () {
            return when(
                ds.selectHash("t__c___a", "t__d___b").then(function (ret) {
                    assert.deepEqual(ret, {1:2, 3:4});
                    assert.deepEqual(ds.db.sqls, ['SELECT t.c AS a, t.d AS b FROM t']);
                    ds.db.reset();
                }),
                ds.selectHash("t__c___a", "t__d___b", function (err, ret) {
                    assert.isNull(err);
                    assert.deepEqual(ret, {1:2, 3:4});
                    assert.deepEqual(ds.db.sqls, ['SELECT t.c AS a, t.d AS b FROM t']);
                    ds.db.reset();
                })
            );
        });

    });

    it.describe("modifying joined datasets", function (it) {
        var ds = new MockDatabase().from("b", "c").join("d", ["id"]).where({id:2});
        ds.supportsModifyingJoins = true;
        ds.db.reset();

        it.should("allow deleting from joined datasets", function () {
            ds.remove();
            assert.deepEqual(ds.db.sqls, ['DELETE FROM b, c WHERE (id = 2)']);
            ds.db.reset();
        });

        it.should("allow updating joined datasets", function () {
            ds.update({a:1});
            assert.deepEqual(ds.db.sqls, ['UPDATE b, c INNER JOIN d USING (id) SET a = 1 WHERE (id = 2)']);
            ds.db.reset();
        });
    });


    it.afterAll(function () {
        return patio.disconnect();
    });
}).as(module);
