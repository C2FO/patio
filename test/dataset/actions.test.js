var vows = require('vows'),
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
    hitch = comb.hitch;


var ret = (module.exports = exports = new comb.Promise());
var suite = vows.describe("Dataset actions");

patio.identifierInputMethod = null;
patio.identifierOutputMethod = null;
var c = comb.define(Dataset, {
    instance : {

        fetchRows : function(sql, cb) {
            this._static.sql = sql;
            this.__columns = [sql.match(/SELECT COUNT/i) ? "count" : "a"];
            var val = {};
            val[this.__columns[0]] = 1;
            return new comb.Promise().callback(cb ? cb(val) : val);
        }
    },

    static : {
        sql : null
    }
});

var DummyDataset = comb.define(Dataset, {
    instance : {
        VALUES : [
            {a : 1, b : 2},
            {a : 3, b : 4},
            {a : 5, b : 6}
        ],
        fetchRows : function(sql, block) {
            var ret = new comb.Promise();
            ret.callback(this.VALUES.forEach(block));
            return ret;
        }
    }
});

suite.addBatch({
    "Dataset.rowCb" : {
        topic : new DummyDataset().from("items"),

        "should allow the setting of a rowCb " : function(ds){
            [function(){}, null].forEach(function(rowCb){
                assert.doesNotThrow(function(){
                    ds.rowCb = rowCb;
                }, rowCb + " should not have thrown an exception");
            });
            ["HI", new Date(), true, false, undefined].forEach(function(rowCb){
                assert.throws(function(){
                    ds.rowCb = rowCb;
                }, rowCb + " should have thrown an exception");
            });
        }
    }
});


suite.addBatch({
    "Dataset.map" : {
        topic : new DummyDataset().from("items"),

        "should provide the usual functionality if no argument is given" : function(dataset) {
            var arr;
            var ret = dataset.map(function(n) {
                return n.a + n.b
            });
            ret.then(function(r) {
                arr = r
            });
            assert.deepEqual(arr, [3,7,11]);
        },

        "should map using #[column name] if column name is given" : function(dataset) {
            var arr;
            var ret = dataset.map(function(n) {
                return n.a
            });
            ret.then(function(r) {
                arr = r
            });
            assert.deepEqual(arr, [1,3,5]);
        },

        "should return the complete dataset values if nothing is given" : function(dataset) {
            var arr;
            var ret = dataset.map();
            ret.then(function(r) {
                arr = r
            });
            assert.deepEqual(arr, dataset.VALUES);
        }
    },

    "Dataset.toHash" : {
        topic : new DummyDataset().from("test"),

        "should provide a hash with the first column as key and the second as value" : function(dataset) {
            var a = {1 : 2, 3 : 4, 5 : 6}, b = {2 : 1, 4 : 3, 6 : 5};
            dataset.toHash("a", "b").then(function(ret) {
                assert.deepEqual(a, ret);
            });
            dataset.toHash("b", "a").then(function(ret) {
                assert.deepEqual(ret, b);
            });
        },

        "should provide a hash with the first column as key and the entire hash as value if the value column is blank or nil" : function(dataset) {
            var a = {1 : {a : 1, b : 2}, 3 : {a : 3, b : 4}, 5 : {a : 5, b : 6}};
            var b = {2 : {a : 1, b : 2}, 4 : {a : 3, b : 4}, 6 : {a : 5, b : 6}};
            dataset.toHash("a").then(function(ret) {
                assert.deepEqual(ret, a);
            });
            dataset.toHash("b").then(function(ret) {
                assert.deepEqual(ret, b);
            });
        }
    },


    "Dataset.count" : {
        topic : function() {
            return new c().from("test");
        },

        "should format SQL properly" : function(dataset) {
            dataset.count().then(function(res) {
                assert.equal(res, 1);
                assert.equal(c.sql, 'SELECT COUNT(*) AS count FROM test LIMIT 1');
            });
        },

        "should include the where clause if it's there" : function(dataset) {
            dataset.filter(sql.abc.sqlNumber.lt(30)).count().then(function(count) {
                assert.equal(count, 1)
                assert.equal(c.sql, 'SELECT COUNT(*) AS count FROM test WHERE (abc < 30) LIMIT 1');
            });
        },

        "should count properly for datasets with fixed sql" : function(dataset) {
            var dataset = new c().from("test")
            dataset.__opts.sql = "select abc from xyz";
            dataset.count().then(function(count) {
                assert.equal(count, 1);
                assert.equal(c.sql, "SELECT COUNT(*) AS count FROM (select abc from xyz) AS t1 LIMIT 1");
            });
        },

        "should count properly when using UNION, INTERSECT, or EXCEPT" : function(dataset) {
            dataset.union(dataset).count().then(function(count) {
                assert.equal(count, 1);
            });
            assert.equal(c.sql, "SELECT COUNT(*) AS count FROM (SELECT * FROM test UNION SELECT * FROM test) AS t1 LIMIT 1");
            dataset.intersect(dataset).count().then(function(count) {
                assert.equal(count, 1);
            });
            assert.equal(c.sql, "SELECT COUNT(*) AS count FROM (SELECT * FROM test INTERSECT SELECT * FROM test) AS t1 LIMIT 1");
            dataset.except(dataset).count().then(function(count) {
                assert.equal(count, 1);
            });
            assert.equal(c.sql, "SELECT COUNT(*) AS count FROM (SELECT * FROM test EXCEPT SELECT * FROM test) AS t1 LIMIT 1");
        },

        "should return limit if count is greater than it" : function(dataset) {
            dataset.limit(5).count().then(function(count) {
                assert.equal(count, 1);
            });
            assert.equal(c.sql, "SELECT COUNT(*) AS count FROM (SELECT * FROM test LIMIT 5) AS t1 LIMIT 1");
        },

        "should work on a graphed_dataset" : function(dataset) {
            dataset.graph(dataset, ["a"], {tableAlias : "test2"}).then(function(ds) {
                ds.count().then(function(count) {
                    assert.equal(count, 1)
                })
            });
            assert.equal(c.sql, 'SELECT COUNT(*) AS count FROM test LEFT OUTER JOIN test AS test2 USING (a) LIMIT 1');
        },

        "should not cache the columns value" : function(dataset) {
            var ds = dataset.from("blah");
            ds.columns.then(function(cols) {
                assert.deepEqual(cols, ["a"]);
            });
            ds.count().then(function(c) {
                assert.equal(c, 1)
            });
            assert.equal(c.sql, 'SELECT COUNT(*) AS count FROM blah LIMIT 1');
            ds.columns.then(function(cols) {
                assert.deepEqual(cols, ["a"]);
            });
        }
    },

    "Dataset.empty" : {
        topic : function() {
            var C = comb.define(c, {
                instance : {
                    fetchRows : function(sql, cb) {
                        this._static.sql = sql;
                        return new comb.Promise().callback(cb(sql.match(/WHERE \'f\'/) ? null : {1 : 1}));
                    }
                }
            })
            return C;
        },
        "should return true if records exist in the dataset" : function(C) {
            var ds = new C().from("test");
            ds.isEmpty().then(function(res) {
                assert.isFalse(res);
                assert.equal(C.sql, 'SELECT 1 FROM test LIMIT 1');
            });
            ds.filter(false).isEmpty().then(function(res) {
                assert.isTrue(res);
                assert.equal(C.sql, "SELECT 1 FROM test WHERE 'f' LIMIT 1");
            });
        }
    },

    "Dataset.insertMultiple" : {
        topic : function() {
            var c = comb.define(Dataset, {
                instance : {
                    constructor : function() {
                        this.inserts = [];
                    },
                    insert : function(arg) {
                        this.inserts.push(arg);
                        return new comb.Promise().callback();
                    }
                }
            });

            return new c();
        },

        "should insert all items in the supplied array" : function(d) {
            d.insertMultiple(["aa", 5, 3, {1 : 2}]);
            assert.deepEqual(d.inserts, ["aa", 5, 3, {1 : 2}]);
            d.inserts.length = 0;
        },

        "should pass array items through the supplied block if given" : function(d) {
            var a = ["inevitable", "hello", "the ticking clock"];
            d.insertMultiple(a, function(i) {
                return i.replace(/l/g, "r")
            });
            assert.deepEqual(d.inserts, ["inevitabre", "herro", "the ticking crock"]);
        }
    },

    "Dataset aggregate methods" : {
        topic : function() {
            var c = comb.define(Dataset, {
                instance : {
                    fetchRows : function(sql, cb) {
                        return new comb.Promise().callback(cb({1 : sql}));
                    }
                }
            });
            return new c().from("test");
        },

        "should include min" : function(d) {
            d.min("a").then(function(sql) {
                assert.equal(sql, 'SELECT min(a) FROM test LIMIT 1')
            });
        },

        "should include max" : function(d) {
            d.max("a").then(function(sql) {
                assert.equal(sql, 'SELECT max(a) FROM test LIMIT 1')
            });
        },

        "should include sum" : function(d) {
            d.sum("a").then(function(sql) {
                assert.equal(sql, 'SELECT sum(a) FROM test LIMIT 1')
            });
        },

        "should include avg" : function(d) {
            d.avg("a").then(function(sql) {
                assert.equal(sql, 'SELECT avg(a) FROM test LIMIT 1')
            });
        },

        "should accept qualified columns" : function(d) {
            d.avg("test__a").then(function(sql) {
                assert.equal(sql, 'SELECT avg(test.a) FROM test LIMIT 1')
            });
        },

        "should use a subselect for the same conditions as count" : function(d) {
            d = d.order("a").limit(5);
            d.avg("a").then(function(sql) {
                assert.equal(sql, 'SELECT avg(a) FROM (SELECT * FROM test ORDER BY a LIMIT 5) AS t1 LIMIT 1')
            });
            d.sum("a").then(function(sql) {
                assert.equal(sql, 'SELECT sum(a) FROM (SELECT * FROM test ORDER BY a LIMIT 5) AS t1 LIMIT 1')
            });
            d.min("a").then(function(sql) {
                assert.equal(sql, 'SELECT min(a) FROM (SELECT * FROM test ORDER BY a LIMIT 5) AS t1 LIMIT 1')
            });
            d.max("a").then(function(sql) {
                assert.equal(sql, 'SELECT max(a) FROM (SELECT * FROM test ORDER BY a LIMIT 5) AS t1 LIMIT 1')
            });
        }
    },

    "Dataset.range" : {
        topic : function() {
            var c = comb.define(Dataset, {
                instance : {

                    getters : {
                        lastSql : function() {
                            return this._static.sql;
                        }
                    },

                    fetchRows : function(sql, cb) {
                        this._static.sql = sql;
                        return new comb.Promise().callback(cb({v1 : 1, v2 : 10}));
                    }
                },

                static : {
                    sql : null
                }
            });
            return new c().from("test");
        },

        "should generate a correct SQL statement" : function(d) {
            d.range("stamp").then(function() {
                assert.equal(d.lastSql, "SELECT min(stamp) AS v1, max(stamp) AS v2 FROM test LIMIT 1");
            });
            d.filter(sql.price.sqlNumber.gt(100)).range("stamp").then(function() {
                assert.equal(d.lastSql, "SELECT min(stamp) AS v1, max(stamp) AS v2 FROM test WHERE (price > 100) LIMIT 1");
            })
        },

        "should return a two values" : function(d) {
            d.range("tryme").then(function(one, two) {
                assert.equal(one, 1);
                assert.equal(two, 10);
            })
        },

        "should use a subselect for the same conditions as count" : function(d) {
            d.order("stamp").limit(5).range("stamp").then(function(one, two) {
                assert.equal(one, 1);
                assert.equal(two, 10);
                assert.equal(d.lastSql, 'SELECT min(stamp) AS v1, max(stamp) AS v2 FROM (SELECT * FROM test ORDER BY stamp LIMIT 5) AS t1 LIMIT 1');
            });
        }
    },

    "Dataset#interval" : {
        topic : function() {
            var c = comb.define(Dataset, {
                instance : {

                    getters : {
                        lastSql : function() {
                            return this._static.sql;
                        }
                    },

                    fetchRows : function(sql, cb) {
                        this._static.sql = sql;
                        return new comb.Promise().callback(cb({v : 1234}));
                    }
                },

                static : {
                    sql : null
                }
            });
            return new c().from("test");
        },

        "should generate a correct SQL statement" : function(d) {
            d.interval("stamp").then(function() {
                assert.equal(d.lastSql, "SELECT (max(stamp) - min(stamp)) FROM test LIMIT 1");
            });

            d.filter(sql.price.sqlNumber.gt(100)).interval("stamp").then(function() {
                assert.equal(d.lastSql, "SELECT (max(stamp) - min(stamp)) FROM test WHERE (price > 100) LIMIT 1");
            });
        },
        "should return an integer" : function(d) {
            d.interval("tryme").then(function(r) {
                assert.equal(r, 1234)
            });
        },

        "should use a subselect for the same conditions as count" : function(d) {
            d.order("stamp").limit(5).interval("stamp").then(function(r) {
                assert.equal(r, 1234);
                assert.equal(d.lastSql, 'SELECT (max(stamp) - min(stamp)) FROM (SELECT * FROM test ORDER BY stamp LIMIT 5) AS t1 LIMIT 1');
            });

        }
    },

    "Dataset first and last" : {
        topic : function() {
            var c = comb.define(Dataset, {
                instance : {
                    forEach : function(cb) {
                        var s = this.selectSql;
                        var x = ["a",1,"b",2,s];
                        var i = parseInt(s.match(/LIMIT (\d+)/)[1], 10);
                        var ret = new comb.Promise();
                        for (var j = 0; j < i; j++) {
                            cb(x);
                        }
                        ret.callback();
                        return ret;
                    }
                }
            });
            return new c().from("test");
        },

        "should return a single record if no argument is given" : function(d) {
            d.order("a").first().then(function(r) {
                assert.deepEqual(r, ["a",1,"b",2, 'SELECT * FROM test ORDER BY a LIMIT 1'])
            });
            d.order("a").last().then(function(r) {
                assert.deepEqual(r, ["a",1,"b",2, 'SELECT * FROM test ORDER BY a DESC LIMIT 1'])
            });
        },

        "should return the first/last matching record if argument is not an Integer" : function(d) {
            d.order("a").first({z : 26}).then(function(r) {
                assert.deepEqual(r, ["a",1,"b",2, 'SELECT * FROM test WHERE (z = 26) ORDER BY a LIMIT 1'])
            });
            d.order("a").first("z = ?", 15).then(function(r) {
                assert.deepEqual(r, ["a",1,"b",2, 'SELECT * FROM test WHERE (z = 15) ORDER BY a LIMIT 1'])
            });
            d.order("a").last({z :26}).then(function(r) {
                assert.deepEqual(r, ["a",1,"b",2, 'SELECT * FROM test WHERE (z = 26) ORDER BY a DESC LIMIT 1'])
            });
            d.order("a").last("z = ?", 15).then(function(r) {
                assert.deepEqual(r, ["a",1,"b",2, 'SELECT * FROM test WHERE (z = 15) ORDER BY a DESC LIMIT 1'])
            });

        },

        "should set the limit and return an array of records if the given number is > 1" : function(d) {
            var i = parseInt(Math.random() * 10) + 10;
            d.order("a").first(i).then(function(r) {
                assert.lengthOf(r, i);
                assert.deepEqual(r[0], ["a",1,"b",2, comb.string.format("SELECT * FROM test ORDER BY a LIMIT %d", i)]);
            });
            i = parseInt(Math.random() * 10) + 10;
            d.order("a").last(i).then(function(r) {
                assert.lengthOf(r, i);
                assert.deepEqual(r[0], ["a",1,"b",2, comb.string.format("SELECT * FROM test ORDER BY a DESC LIMIT %d", i)]);
            });
        },

        "should return the first matching record if a block is given without an argument" : function(d) {
            d.first(
                function() {
                    return this.z.sqlNumber.gt(26)
                }).then(function(r) {
                    assert.deepEqual(r, ["a",1,"b",2, 'SELECT * FROM test WHERE (z > 26) LIMIT 1']);
                });
            d.order("name").last(
                function() {
                    return this.z.sqlNumber.gt(26)
                }).then(function(r) {
                    assert.deepEqual(r, ["a",1,"b",2, 'SELECT * FROM test WHERE (z > 26) ORDER BY name DESC LIMIT 1']);
                });
        },

        "should combine block and standard argument filters if argument is not an Integer" : function(d) {
            d.first({y : 25},
                function() {
                    return this.z.sqlNumber.gt(26)
                }).then(function(r) {
                    assert.deepEqual(r, ["a",1,"b",2, 'SELECT * FROM test WHERE ((z > 26) AND (y = 25)) LIMIT 1'])
                });
            d.order("name").last("y = ?", 16,
                function() {
                    return this.z.sqlNumber.gt(26)
                }).then(function(r) {
                    assert.deepEqual(r, ["a",1,"b",2, 'SELECT * FROM test WHERE ((z > 26) AND (y = 16)) ORDER BY name DESC LIMIT 1'])
                });
        },

        "should return the first matching record if a block is given without an argument" : function(d) {
            var i = parseInt(Math.random() * 10) + 10;
            d.order("a").first(i,
                function() {
                    return this.z.sqlNumber.gt(26)
                }).then(function(r) {
                    assert.lengthOf(r, i);
                    assert.deepEqual(r[0], ["a",1,"b",2, comb.string.format("SELECT * FROM test WHERE (z > 26) ORDER BY a LIMIT %d", i)]);
                });
            i = parseInt(Math.random() * 10) + 10;
            d.order("name").last(i,
                function() {
                    return this.z.sqlNumber.gt(26)
                }).then(function(r) {
                    assert.lengthOf(r, i);
                    assert.deepEqual(r[0], ["a",1,"b",2, comb.string.format("SELECT * FROM test WHERE (z > 26) ORDER BY name DESC LIMIT %d", i)]);
                });
        },

        "last should raise an error if no order is given" : function(d) {
            assert.throws(hitch(d, "last"));
            assert.throws(hitch(d, "last", 2));
            assert.doesNotThrow(hitch(d.order("a"), "last"));
            assert.doesNotThrow(hitch(d.order("a"), "last", 2));
        },

        "last should invert the order" : function(d) {
            d.order("a").last().then(function(r) {
                assert.equal(r.pop(), 'SELECT * FROM test ORDER BY a DESC LIMIT 1');
            });
            d.order(sql.b.desc()).last().then(function(r) {
                assert.equal(r.pop(), 'SELECT * FROM test ORDER BY b ASC LIMIT 1');
            });
            d.order("c", "d").last().then(function(r) {
                assert.equal(r.pop(), 'SELECT * FROM test ORDER BY c DESC, d DESC LIMIT 1');
            });

            d.order(sql.e.desc(), "f").last().then(function(r) {
                assert.equal(r.pop(), 'SELECT * FROM test ORDER BY e ASC, f DESC LIMIT 1');
            });
        }
    },

    "Dataset.singleRecord" : {
        topic : function() {
            var c = comb.define(Dataset, {
                instance : {
                    fetchRows : function(sql, cb) {
                        return new comb.Promise().callback(cb(sql));
                    }
                }
            });

            var cc = comb.define(Dataset, {
                instance : {
                    fetchRows : function(sql, cb) {
                        return new comb.Promise().callback();
                    }
                }
            });
            return {
                d : new c().from("test"),
                e : new cc().from("test")
            };
        },

        "should call each with a limit of 1 and return the record" : function(ds) {
            var d = ds.d, e = ds.e;
            d.singleRecord().then(function(r) {
                assert.equal(r, 'SELECT * FROM test LIMIT 1');
            });
        },

        "should return nil if no record is present" : function(ds) {
            var d = ds.d, e = ds.e;
            e.singleRecord().then(function(r) {
                assert.isNull(r)
            });
        }
    },

    "Dataset.singleValue" : {
        topic : function() {
            var c = comb.define(Dataset, {
                instance : {
                    fetchRows : function(sql, cb) {
                        this.__columns = ["a"];
                        return new comb.Promise().callback(cb ? cb({1 : sql}) : {1 : sql});
                    }
                }
            });

            var cc = comb.define(Dataset, {
                instance : {
                    fetchRows : function(sql, cb) {
                        return new comb.Promise().callback();
                    }
                }
            });
            return {
                d : new c().from("test"),
                e : new cc().from("test")
            };
        },

        "should call each and return the first value of the first record" : function(ds) {
            var d = ds.d, e = ds.e;
            d.singleValue().then(function(r) {
                assert.equal(r, 'SELECT * FROM test LIMIT 1');
            });
        },

        "should return nil if no record is present" : function(ds) {
            var d = ds.d, e = ds.e;
            e.singleRecord().then(function(r) {
                assert.isNull(r)
            });
        },

        "should work on a graphed_dataset" : function(ds) {
            var d = ds.d, e = ds.e;
            d.graph(d, ["a"], {tableAlias : "test2"}).then(function(d) {
                d.singleValue().then(function(r) {
                    assert.equal(r, 'SELECT test.a, test2.a AS test2_a FROM test LEFT OUTER JOIN test AS test2 USING (a) LIMIT 1');
                });
            });
        }
    },

    "Dataset.get" : {
        topic : function() {
            var c = comb.define(Dataset, {
                instance : {
                    lastSql : null,

                    fetchRows : function(sql, cb) {
                        this.lastSql = sql;
                        return new comb.Promise().callback(cb({name : sql}));
                    }
                }
            });
            return new c().from("test");
        },

        "should select the specified column and fetch its value" : function(d) {
            d.get("name").then(function(r) {
                assert.equal(r, "SELECT name FROM test LIMIT 1");
            });
            d.get("abc").then(function(r) {
                assert.equal(r, "SELECT abc FROM test LIMIT 1")
            });
        },

        "should work with filters" : function(d) {
            d.filter({id : 1}).get("name").then(function(r) {
                assert.equal(r, "SELECT name FROM test WHERE (id = 1) LIMIT 1")
            });
        },

        "should work with aliased fields" : function(d) {
            d.get(sql.x__b.as("name")).then(function(r) {
                assert.equal(r, "SELECT x.b AS name FROM test LIMIT 1")
            });
        },

        "should accept a block that yields a virtual row" : function(d) {
            d.get(
                function(o) {
                    return o.x__b.as("name");
                }).then(function(r) {
                    assert.equal(r, "SELECT x.b AS name FROM test LIMIT 1")
                });
            d.get(
                function() {
                    return this.x(1).as("name");
                }).then(function(r) {
                    assert.equal(r, "SELECT x(1) AS name FROM test LIMIT 1")
                });
        }
    },

    "Dataset set rowCb" : {
        topic : function() {
            var c = comb.define(Dataset, {
                instance : {

                    fetchRows : function(sql, cb) {
                        // yield a hash with kind as the 1 bit of a number
                        for (var i = 0; i < 10; i++) {
                            cb({kind : i});
                        }
                        return new comb.Promise().callback();
                    }
                }
            });
            return new c().from("items");
        },

        "should cause dataset to pass all rows through the filter" : function(dataset) {
            dataset.rowCb = function(h) {
                h.der = h.kind + 2;
                return h;
            };

            dataset.all().then(function(rows) {
                assert.lengthOf(rows, 10);
                rows.forEach(function(r) {
                    assert.deepEqual(r.der, r.kind + 2);
                });
                dataset.rowCb = null;
            });
        },

        "should be copied over when dataset is cloned" : function(dataset) {
            dataset.rowCb = function(h) {
                h.der = h.kind + 2;
                return h;
            };
            dataset.filter({a : 1}).first().then(function(r) {
                assert.deepEqual(r, {kind : 0, der : 2})
            });
        }

    },

    "Dataset.columns" : {
        topic : function() {
            var i = 0;
            var arr = ["a", "b", "c"];
            var dataset = (new (comb.define(DummyDataset, { instance : {
                forEach : function() {
                    var ret = new comb.Promise().callback();
                    this.__columns = this.selectSql + arr[i++];
                    return ret;
                }
            } }))).from("items");
            return dataset;
        },

        "should return the value of @columns if __columns is not nil" : function(dataset) {
            dataset.__columns = ["a", "b", "c"];
            dataset.columns.then(function(arr) {
                assert.deepEqual(arr, ["a", "b", "c"])
            });
        },

        "should attempt to get a single record and return __columns if __columns is nil" : function(dataset) {
            dataset.__columns = null;
            dataset.columns.then(function(arr) {
                assert.deepEqual(arr, 'SELECT * FROM items LIMIT 1a')
            });
            dataset.__opts.from = ["nana"];
            dataset.columns.then(function(arr) {
                assert.deepEqual(arr, 'SELECT * FROM items LIMIT 1a')
            });
        },

        "should ignore any filters, orders, or DISTINCT clauses" : function(dataset) {
            dataset = dataset.from("items").filter({b : 100}).order("b").distinct();
            dataset.__columns = null;
            dataset.columns.then(function(arr) {
                assert.deepEqual(arr, 'SELECT * FROM items LIMIT 1b')
            });
        }
    },

    "Dataset.import" : {
        topic : function() {
            var dbc = comb.define(Database, {
                instance : {
                    sqls : null,

                    execute : function(sql, opts) {
                        var ret = new comb.Promise().callback();
                        this.sqls = this.sqls || [];
                        this.sqls.push(sql);
                        return ret;
                    },

                    transaction : function(opts, cb) {
                        if (comb.isFunction(opts)) {
                            cb = opts;
                            opts = {};
                        } else {
                            opts = opts || {};
                        }
                        var ret = new comb.Promise();
                        this.sqls = this.sqls || [];
                        this.sqls.push("BEGIN");
                        cb && cb();
                        this.sqls.push("COMMIT");
                        ret.callback();
                        return ret;
                    },

                    reset : function() {
                        this.sqls = [];
                    }
                }
            });
            var db = new dbc();
            return {
                db : db,
                ds : new Dataset(db).from("items"),
                list : [
                    {name : "abc"},
                    {name : "def"},
                    {name : "ghi"}
                ]
            };
        },

        "should accept string keys as column names" : function(d) {
            var ds = d.ds, db = d.db;
            ds.import(['x', 'y'], [
                [1, 2],
                [3, 4]
            ]).then(function() {
                    assert.deepEqual(db.sqls, [
                        'BEGIN',
                        "INSERT INTO items (x, y) VALUES (1, 2)",
                        "INSERT INTO items (x, y) VALUES (3, 4)",
                        'COMMIT'
                    ]);
                    db.reset();
                });
        },

        "should accept a columns array and a values array" : function(d) {
            var ds = d.ds, db = d.db;
            ds.import(["x", "y"], [
                [1, 2],
                [3, 4]
            ]).then(function() {
                    assert.deepEqual(db.sqls, [
                        'BEGIN',
                        "INSERT INTO items (x, y) VALUES (1, 2)",
                        "INSERT INTO items (x, y) VALUES (3, 4)",
                        'COMMIT'
                    ]);
                    db.reset();
                })
        },

        "should accept a columns array and a dataset" : function(d) {
            var ds = d.ds, db = d.db;
            var ds2 = new Dataset(db).from("cats").filter({purr : true}).select("a", "b");

            ds.import(["x", "y"], ds2).then(function() {
                assert.deepEqual(db.sqls, [
                    'BEGIN',
                    "INSERT INTO items (x, y) SELECT a, b FROM cats WHERE (purr IS TRUE)",
                    'COMMIT'
                ]);
                db.reset();
            });
        },

        "should accept a columns array and a values array with {commitEvery) option" : function(d) {
            var ds = d.ds, db = d.db;
            ds.import(["x", "y"], [
                [1, 2],
                [3, 4],
                [5, 6]
            ], {commitEvery : 3}).then(function() {
                    assert.deepEqual(db.sqls, [
                        'BEGIN',
                        "INSERT INTO items (x, y) VALUES (1, 2)",
                        "INSERT INTO items (x, y) VALUES (3, 4)",
                        "INSERT INTO items (x, y) VALUES (5, 6)",
                        'COMMIT'
                    ]);
                    db.reset();
                })
        },

        "should accept a columns array and a values array with slice option" : function(d) {
            var ds = d.ds, db = d.db;
            ds.import(["x", "y"], [
                [1, 2],
                [3, 4],
                [5, 6]
            ], {slice : 2}).then(function() {
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
        }

    },


    "Dataset.multiInsert" : {
        topic : function() {
            var dbc = comb.define(null, {
                instance : {
                    sqls : [],

                    reset : function() {
                        this.sqls.length = 0;
                    },

                    execute : function(sql, opts) {
                        this.sqls.push(sql);
                        return new comb.Promise().callback();
                    },

                    executeDui : function() {
                        return this.execute.apply(this, arguments);
                    },

                    transaction : function(opts, cb) {
                        if (comb.isFunction(opts)) {
                            cb = opts;
                            opts = {};
                        } else {
                            opts = opts || {};
                        }
                        var ret = new comb.Promise();
                        this.sqls.push("BEGIN");
                        cb && cb();
                        this.sqls.push("COMMIT");
                        ret.callback();
                        return ret;
                    }

                }
            });
            var db = new dbc();
            var ds = new Dataset(db).from("items");
            var list = [
                {name : 'abc'},
                {name : 'def'},
                {name : 'ghi'}
            ];
            return {
                db : db,
                ds : ds,
                list : list
            }
        },


        "should issue multiple insert statements inside a transaction"  : function(o) {
            var db = o.db, ds = o.ds, list = o.list;
            ds.multiInsert(list).then(function() {
                assert.deepEqual(db.sqls, [
                    'BEGIN',
                    "INSERT INTO items (name) VALUES ('abc')",
                    "INSERT INTO items (name) VALUES ('def')",
                    "INSERT INTO items (name) VALUES ('ghi')",
                    'COMMIT'
                ]);
                db.reset();
            })
        },

        "should handle different formats for tables"  : function(o) {
            var db = o.db, ds = o.ds, list = o.list;
            ds = ds.from("sch__tab");
            ds.multiInsert(list).then(function() {
                assert.deepEqual(db.sqls, [
                    'BEGIN',
                    "INSERT INTO sch.tab (name) VALUES ('abc')",
                    "INSERT INTO sch.tab (name) VALUES ('def')",
                    "INSERT INTO sch.tab (name) VALUES ('ghi')",
                    'COMMIT'
                ]);
                db.reset();
            })

            ds = ds.from(sql.tab.qualify("sch"));
            ds.multiInsert(list).then(function() {
                assert.deepEqual(db.sqls, [
                    'BEGIN',
                    "INSERT INTO sch.tab (name) VALUES ('abc')",
                    "INSERT INTO sch.tab (name) VALUES ('def')",
                    "INSERT INTO sch.tab (name) VALUES ('ghi')",
                    'COMMIT'
                ]);
                db.reset();
            });
            ds = ds.from(new Identifier("sch__tab"));
            ds.multiInsert(list).then(function() {
                assert.deepEqual(db.sqls, [
                    'BEGIN',
                    "INSERT INTO sch__tab (name) VALUES ('abc')",
                    "INSERT INTO sch__tab (name) VALUES ('def')",
                    "INSERT INTO sch__tab (name) VALUES ('ghi')",
                    'COMMIT'
                ]);
                db.reset();
            });
        },


        "should accept the :commit_every option for committing every x records"  : function(o) {
            var db = o.db, ds = o.ds, list = o.list;
            ds.multiInsert(list, {commitEvery : 1}).then(function() {
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
            });
        },


        "should accept the :slice option for committing every x records"  : function(o) {
            var db = o.db, ds = o.ds, list = o.list;
            ds.multiInsert(list, {slice : 2}).then(function() {
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
            });
        },


        "should not do anything if no hashes are provided"  : function(o) {
            var db = o.db, ds = o.ds, list = o.list;
            ds.multiInsert([]).then(function() {
                assert.deepEqual(db.sqls, []);
            });
        }
    },

    "Dataset.toCsv" : {
        topic : function() {
            var c = comb.define(Dataset, {

                instance : {
                    data : [
                        {a : 1, b : 2, c : 3},
                        {a  : 4, b  : 5, c  : 6},
                        { a  : 7, b : 8, c : 9}
                    ],

                    __columns : ["a", "b", "c"],

                    fetchRows : function(sql, cb) {
                        this.data.forEach(cb, this)
                        return new comb.Promise().callback();
                    },

                    naked : function() {
                        return this;
                    }
                }
            });
            return new c().from("items");
        },

        "should format a CSV representation of the records" : function(ds) {
            ds.toCsv().then(function(csv) {
                assert.equal(csv, "a, b, c\r\n1, 2, 3\r\n4, 5, 6\r\n7, 8, 9\r\n")
            });
        },

        "should exclude column titles if so specified" : function(ds) {
            ds.toCsv(false).then(function(csv) {
                assert.equal(csv, "1, 2, 3\r\n4, 5, 6\r\n7, 8, 9\r\n")
            });
        }
    },


    "Dataset.all" : {
        topic : function() {
            var c = comb.define(Dataset, {
                instance : {
                    fetchRows : function(sql, block) {
                        block({x : 1, y : 2});
                        block({x : 3, y : 4});
                        block(sql);
                        return new comb.Promise();
                    }
                }
            });
            return new c().from("items");
        },

        "should return an array with all records" : function(ds) {
            ds.all().then(function(ret) {
                assert.deepEqual(ret, [
                    {x : 1, y : 2},
                    {x : 3, y : 4},
                    "SELECT * FROM items"
                ]);
            });
        },

        "should iterate over the array if a block is given" : function(ds) {
            var a = [];

            ds.all(
                function(r) {
                    a.push(comb.isHash(r) ? r.x : r);
                }).then(function() {
                    assert.deepEqual(a, [1, 3, "SELECT * FROM items"]);
                });
        }
    },


    "Dataset default fetchRows, insert, update, delete, truncate, execute" : {
        topic : function() {
            var db = new Database();
            return {
                db : db,
                ds : db.from("items")
            };
        },

        "fetchRows should raise an Error" : function(o) {
            var db = o.db, ds = o.ds;
            assert.throws(ds, "fetchRows", '', function() {
            });
        },

        "delete should execute delete SQL" : function(o) {
            var db = o.db, ds = o.ds;
            var orig = db.execute;
            var s, o;
            db.execute = function(sql, opts) {
                s = sql,o = opts || {}
            };
            ds.delete();
            db.execute = orig;
            assert.equal(s, 'DELETE FROM items');
            assert.deepEqual(o, {server : "default"});
        },

        "delete should executeDui delete SQL" : function(o) {
            var db = o.db, ds = o.ds;
            var s, o;
            var orig = db.executeDui;
            db.executeDui = function(sql, opts) {
                s = sql,o = opts || {}
            };
            ds.delete();
            assert.equal(s, 'DELETE FROM items');
            assert.deepEqual(o, {server : "default"});
            db.executeDui = orig;

        },


        "insert should execute insert SQL" : function(o) {
            var db = o.db, ds = o.ds;
            var orig = db.execute;
            var s, o;
            db.execute = function(sql, opts) {
                s = sql,o = opts || {}
            };
            ds.insert([]);
            db.execute = orig;
            assert.equal(s, 'INSERT INTO items DEFAULT VALUES');
            assert.deepEqual(o, {server : "default"});
        },

        "insert should executeDui insert SQL" : function(o) {
            var db = o.db, ds = o.ds;
            var s, o;
            var orig = db.executeDui;
            db.executeDui = function(sql, opts) {
                s = sql,o = opts || {}
            };
            ds.insert([]);
            assert.equal(s, 'INSERT INTO items DEFAULT VALUES');
            assert.deepEqual(o, {server : "default"});
            db.executeDui = orig;
        },

        "update should execute update SQL" : function(o) {
            var db = o.db, ds = o.ds;
            var orig = db.execute;
            var s, o;
            db.execute = function(sql, opts) {
                s = sql,o = opts || {}
            };
            ds.update({number : 1});
            db.execute = orig;
            assert.equal(s, 'UPDATE items SET number = 1');
            assert.deepEqual(o, {server : "default"});
        },

        "update should executeDui update SQL" : function(o) {
            var db = o.db, ds = o.ds;
            var s, o;
            var orig = db.executeDui;
            db.executeDui = function(sql, opts) {
                s = sql,o = opts || {}
            };
            ds.update({number : 1});
            assert.equal(s, 'UPDATE items SET number = 1');
            assert.deepEqual(o, {server : "default"});
            db.executeDui = orig;
        },

        "truncate should execute truncate SQL" : function(o) {
            var db = o.db, ds = o.ds;
            var orig = db.execute;
            var s, o;
            db.execute = function(sql, opts) {
                s = sql,o = opts || {}
            };
            ds.truncate();
            db.execute = orig;
            assert.equal(s, 'TRUNCATE TABLE items');
            assert.deepEqual(o, {server : "default"});
        },

        "truncate should executeDui truncate SQL" : function(o) {
            var db = o.db, ds = o.ds;
            var s, o;
            var orig = db.executeDui;
            db.executeDui = function(sql, opts) {
                s = sql,o = opts || {}
            };
            ds.truncate();
            assert.equal(s, 'TRUNCATE TABLE items');
            assert.deepEqual(o, {server : "default"});
            db.executeDui = orig;
        },

        "truncate should raise an InvalidOperation exception if the dataset is filtered" : function(o) {
            var db = o.db, ds = o.ds;
            assert.throws(ds, "filter", {a : 1});
        },

        "#execute should execute the SQL on the database" : function(o) {
            var db = o.db, ds = o.ds;
            var orig = db.execute;
            var s, o;
            db.execute = function(sql, opts) {
                s = sql,o = opts || {}
            };
            ds.execute("SELECT 1");
            db.execute = orig;
            assert.equal(s, 'SELECT 1');
            assert.deepEqual(o, {server : "readOnly"});

        }
    },


    "patio.Dataset.selectMap" : {
        topic : function() {
            var DS = comb.define(MockDataset, {
                instance : {
                    fetchRows : function(sql, cb) {
                        var ret = this.db.run(sql);
                        cb({c : 1});
                        cb({c : 2});
                        return ret;
                    }
                }
            });
            var MDB = comb.define(MockDatabase, {
                instance : {
                    getters : {
                        dataset : function() {
                            return new DS(this);
                        }
                    }
                }
            });
            var ds = new MDB().from("t");
            ds.db.reset();
            return ds;
        },

        "should do select and map in one step" : function(ds) {
            ds.selectMap("a").then(function(res) {
                assert.deepEqual(res, [1,2])
                assert.deepEqual(ds.db.sqls, ['SELECT a FROM t']);
                ds.db.reset();
            });

        },

        "should handle implicit qualifiers in arguments" : function(ds) {
            ds.selectMap("a__b").then(function(res) {
                assert.deepEqual(res, [1,2])
                assert.deepEqual(ds.db.sqls, ['SELECT a.b FROM t']);
                ds.db.reset();
            });
        },

        "should handle implicit aliases in arguments" : function(ds) {
            ds.selectMap("a___b").then(function(res) {
                assert.deepEqual(res, [1,2])
                assert.deepEqual(ds.db.sqls, ['SELECT a AS b FROM t']);
                ds.db.reset();
            });
        },

        "should handle other objects" : function(ds) {
            ds.selectMap(sql.literal("a").as("b")).then(function(res) {
                assert.deepEqual(res, [1,2])
                assert.deepEqual(ds.db.sqls, ['SELECT a AS b FROM t']);
                ds.db.reset();
            });
        },

        "should accept a block" : function(ds) {
            ds.selectMap(
                function(t) {
                    return t.a(t.t__c)
                }).then(function(res) {
                    assert.deepEqual(res, [1,2])
                    assert.deepEqual(ds.db.sqls, ['SELECT a(t.c) FROM t']);
                    ds.db.reset();
                });
        }
    },

    "patio.Dataset.selectOrderMap" : {
        topic : function() {
            var DS = comb.define(MockDataset, {
                instance : {
                    fetchRows : function(sql, cb) {
                        var ret = this.db.run(sql);
                        cb({c : 1});
                        cb({c : 2});
                        return ret;
                    }
                }
            });
            var MDB = comb.define(MockDatabase, {
                instance : {
                    getters : {
                        dataset : function() {
                            return new DS(this);
                        }
                    }
                }
            });
            var ds = new MDB().from("t");
            ds.db.reset();
            return ds;
        },

        "should do select and map in one step" : function(ds) {
            ds.selectOrderMap("a").then(function(res) {
                assert.deepEqual(res, [1,2])
                assert.deepEqual(ds.db.sqls, ['SELECT a FROM t ORDER BY a']);
                ds.db.reset();
            });
        },

        "should handle implicit qualifiers in arguments" : function(ds) {
            ds.selectOrderMap("a__b").then(function(res) {
                assert.deepEqual(res, [1,2])
                assert.deepEqual(ds.db.sqls, ['SELECT a.b FROM t ORDER BY a.b']);
                ds.db.reset();
            });
        },

        "should handle implicit aliases in arguments" : function(ds) {
            ds.selectOrderMap("a___b").then(function(res) {
                assert.deepEqual(res, [1,2])
                assert.deepEqual(ds.db.sqls, ['SELECT a AS b FROM t ORDER BY a']);
                ds.db.reset();
            });
        },

        "should handle implicit qualifiers and aliases in arguments" : function(ds) {
            ds.selectOrderMap("t__a___b").then(function(res) {
                assert.deepEqual(res, [1,2])
                assert.deepEqual(ds.db.sqls, ['SELECT t.a AS b FROM t ORDER BY t.a']);
                ds.db.reset();
            });
        },

        "should handle AliasedExpressions" : function(ds) {
            ds.selectOrderMap(sql.literal("a").as("b")).then(function(res) {
                assert.deepEqual(res, [1,2])
                assert.deepEqual(ds.db.sqls, ['SELECT a AS b FROM t ORDER BY a']);
                ds.db.reset();
            });
        },

        "should accept a block" : function(ds) {
            ds.selectOrderMap(
                function(t) {
                    return t.a(t.t__c)
                }).then(function(res) {
                    assert.deepEqual(res, [1,2]);
                    assert.deepEqual(ds.db.sqls, ['SELECT a(t.c) FROM t ORDER BY a(t.c)']);
                    ds.db.reset();
                });
        }
    },

    "patio.DatasetselectHash" : {
        topic : function() {

            var DS = comb.define(MockDataset, {
                instance : {

                    fetchRows : function(sql, cb) {
                        var ret = this.db.run(sql);
                        var hs = [
                            {a : 1, b : 2},
                            {a : 3, b : 4}
                        ];
                        hs.forEach(cb, this);
                        return ret;
                    }
                }
            });
            var MDB = comb.define(MockDatabase, {
                instance : {
                    getters : {
                        dataset : function() {
                            return new DS(this);
                        }
                    }
                }
            });
            var ds = new MDB().from("t");
            ds.db.reset();
            return ds;
        },

        "should do select and map in one step" : function(ds) {
            ds.selectHash("a", "b").then(function(ret) {
                assert.deepEqual(ret, {1 : 2, 3 : 4});
                assert.deepEqual(ds.db.sqls, ['SELECT a, b FROM t']);
                ds.db.reset();
            });
        },

        "should handle implicit qualifiers in arguments" : function(ds) {
            ds.selectHash("t__a", "t__b").then(function(ret) {
                assert.deepEqual(ret, {1 : 2, 3 : 4});
                assert.deepEqual(ds.db.sqls, ['SELECT t.a, t.b FROM t']);
                ds.db.reset();
            });
        },

        "should handle implicit aliases in arguments" : function(ds) {
            ds.selectHash("c___a", "d___b").then(function(ret) {
                assert.deepEqual(ret, {1 : 2, 3 : 4});
                assert.deepEqual(ds.db.sqls, ['SELECT c AS a, d AS b FROM t']);
                ds.db.reset();
            });
        },

        "should handle implicit qualifiers and aliases in arguments" : function(ds) {
            ds.selectHash("t__c___a", "t__d___b").then(function(ret) {
                assert.deepEqual(ret, {1 : 2, 3 : 4});
                assert.deepEqual(ds.db.sqls, ['SELECT t.c AS a, t.d AS b FROM t']);
                ds.db.reset();
            });
        }
    },

    "Modifying joined datasets" : {
        topic : function() {
            var ds = new MockDatabase().from("b", "c").join("d", ["id"]).where({id : 2});
            ds.supportsModifyingJoins = true;
            ds.db.reset();
            return ds;
        },

        "should allow deleting from joined datasets" : function(ds) {
            ds.delete();
            assert.deepEqual(ds.db.sqls, ['DELETE FROM b, c WHERE (id = 2)']);
            ds.db.reset();
        },

        "should allow updating joined datasets" : function(ds) {
            ds.update({a : 1});
            assert.deepEqual(ds.db.sqls, ['UPDATE b, c INNER JOIN d USING (id) SET a = 1 WHERE (id = 2)']);
            ds.db.reset();
        }
    }

});


suite.run({reporter : vows.reporter.spec}, comb.hitch(ret, "callback"));
