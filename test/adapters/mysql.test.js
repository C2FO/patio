var vows = require('vows'),
    assert = require('assert'),
    patio = require("index"),
    sql = patio.SQL,
    comb = require("comb"),
    format = comb.string.format,
    hitch = comb.hitch;

patio.quoteIdentifiers = false;

var ret = (module.exports = exports = new comb.Promise());
var suite = vows.describe("Database");


var MYSQL_URL = format("mysql://test:testpass@localhost:3306/sandbox");
var MYSQL_DB = patio.connect(MYSQL_URL);




var INTEGRATION_DB = MYSQL_DB;
var p1 = new comb.Promise();
MYSQL_DB.forceCreateTable("test2",
    function () {
        this.name("text");
        this.value("integer");
    }).chainBoth(hitch(MYSQL_DB, "dropTable", "items")).chainBoth(hitch(MYSQL_DB, "dropTable", "dolls")).chainBoth(hitch(MYSQL_DB, "dropTable", "booltest")).both(hitch(p1, "callback"));

MYSQL_DB.__defineGetter__("sqls", function () {
    return (comb.isArray(this.__sqls) ? this.__sqls : (this.__sqls = []));
});

MYSQL_DB.__defineSetter__("sqls", function (sql) {
    return this.__sqls = sql;
});

var origExecute = MYSQL_DB.__logAndExecute;
MYSQL_DB.__logAndExecute = function(sql){
    this.sqls.push(sql.trim());
    return origExecute.apply(this, arguments);
}

var SQL_BEGIN = 'BEGIN';
var SQL_ROLLBACK = 'ROLLBACK';
var SQL_COMMIT = 'COMMIT';

p1.both(function () {
    suite.addBatch({

        "createTable":{
            topic:function () {
                MYSQL_DB.sqls.length = 0;
                return MYSQL_DB;
            },

            "should allow to specify options for mysql":{
                topic:function () {
                    MYSQL_DB.sqls.length = 0;
                    MYSQL_DB.createTable("dolls", {engine:"MyISAM", charset:"latin2"},
                        function () {
                            this.name("text");
                        }).then(hitch(this, "callback", null, MYSQL_DB), hitch(this, "callback"));
                },

                "db sql should equal CREATE TABLE dolls (name text) ENGINE=MyISAM DEFAULT CHARSET=latin2":function (db) {
                    assert.deepEqual(db.sqls, ["CREATE TABLE dolls (name text) ENGINE=MyISAM DEFAULT CHARSET=latin2"]);
                },

                "should create a temporary table":{
                    topic:function () {
                        comb.executeInOrder(MYSQL_DB,
                            function (db) {
                                db.dropTable("dolls");
                                db.sqls = [];
                                db.createTable("tmp_dolls", {temp:true, engine:"MyISAM", charset:"latin2"},
                                    function () {
                                        this.name("text");
                                    });
                                return db;
                            }).then(hitch(this, "callback", null), hitch(this, "callback"));
                    },

                    "db sql should equal CREATE TABLE dolls (name text) ENGINE=MyISAM DEFAULT CHARSET=latin2":function (db) {
                        assert.deepEqual(db.sqls, ["CREATE TEMPORARY TABLE tmp_dolls (name text) ENGINE=MyISAM DEFAULT CHARSET=latin2"]);
                    },

                    "should not use default for string {text : true}":{
                        topic:function () {
                            MYSQL_DB.sqls.length = 0;
                            MYSQL_DB.createTable("dolls",
                                function () {
                                    this.name("string", {text:true, "default":"blah"});
                                }).then(hitch(this, "callback", null, MYSQL_DB), hitch(this, "callback"));

                        },

                        "db sql should equal 'CREATE TABLE dolls (name text)'":function (db) {
                            assert.deepEqual(db.sqls, ["CREATE TABLE dolls (name text)"]);
                        },

                        "should not create the autoIcrement attribute if it is specified":{
                            topic:function () {
                                MYSQL_DB.sqls.length = 0;
                                comb.executeInOrder(MYSQL_DB,
                                    function (db) {
                                        db.dropTable("dolls");
                                        db.createTable("dolls", function () {
                                            this.n2("integer");
                                            this.n3(String);
                                            this.n4("integer", {autoIncrement:true, unique:true});
                                        });
                                        return db.schema("dolls");
                                    }).then(hitch(this, "callback", null), hitch(this, "callback"));

                            },

                            "db sql should equal 'CREATE TABLE dolls (name text)'":function (schema) {
                                assert.deepEqual([false, false, true], Object.keys(schema).map(function (k) {
                                    return schema[k].autoIncrement
                                }));
                            }
                        }
                    }


                }

            }
        }

    });

    suite.addBatch({
        "A MySQL database":{
            topic:MYSQL_DB,

            "should provide the server version ":{
                topic:function (db) {
                    db.serverVersion().then(hitch(this, "callback", null), hitch(this, "callback"));
                },

                "it should be greater than 40000":function (version) {
                    assert.isTrue(version >= 40000);
                },

                "should handle the creation and dropping of an InnoDB table with foreign keys":{
                    topic:function (i, db) {
                        db.forceCreateTable("test_innodb", {engine:"InnoDB"},
                            function () {
                                this.primaryKey("id");
                                this.foreignKey("fk", "test_innodb", {key:"id"});
                            }).then(hitch(this, "callback", null), hitch(this, "callback", "ERROR"));
                    },

                    "should not throw an error":function (res) {
                        assert.notEqual(res, "ERROR");
                    },

                    "should support forShare":{
                        topic:function (i, db) {
                            var cb = hitch(this, "callback", null), eb = hitch(this, "callback");
                            db.transaction(function () {
                                db.from("test2").forShare().all().then(cb, eb);
                            });
                        },

                        "should be empty ":function (res) {
                            assert.lengthOf(res, 0);
                        }
                    }
                }
            }
        }
    });

    suite.addBatch({
        "MySQL convertTinyIntToBool":{
            topic:function () {
                MYSQL_DB.createTable("booltest",
                    function () {
                        this.column("b", "tinyint(1)");
                        this.column("i", "tinyint(4)");
                    }).then(hitch(this, function () {
                    this.callback(null, {db:MYSQL_DB, ds:MYSQL_DB.from("booltest")});
                }), hitch(this, "callback"));
            },

            "should consider tinyint(1) datatypes as boolean if set, but not larger tinyints":{

                topic:function (topic) {
                    var db = topic.db;
                    db.schema("booltest", {reload:true}).then(hitch(this, "callback", null), hitch(this, "callback"));
                },

                "schema should have boolean for type":function (schema) {
                    assert.deepEqual(schema, {
                        b:{type:"boolean", autoIncrement:false, allowNull:true, primaryKey:false, "default":null, jsDefault:null, dbType:"tinyint(1)"},
                        i:{type:"integer", autoIncrement:false, allowNull:true, primaryKey:false, "default":null, jsDefault:null, dbType:"tinyint(4)"}
                    })
                },

                "should return tinyint(1)s as boolean values and tinyint(4) as integers ":{
                    topic:function () {
                        var resultSets = [];
                        var ds = MYSQL_DB.from("booltest");
                        comb.executeInOrder(ds,
                            function (ds) {
                                var results = [];
                                ds.delete();
                                ds.insert({b:true, i:10});
                                results.push(ds.all());
                                ds.delete();
                                ds.insert({b:false, i:10});
                                results.push(ds.all());
                                ds.delete();
                                ds.insert({b:true, i:1});
                                results.push(ds.all());
                                return results;
                            }).then(hitch(this, this.callback, null), hitch(this, "callback"));
                    },

                    "the result sets should cast properly":function (resultSet) {
                        assert.deepEqual(resultSet, [
                            [
                                {b:true, i:10}
                            ],
                            [
                                {b:false, i:10}
                            ],
                            [
                                {b:true, i:1}
                            ]
                        ]);
                    },


                    "should not consider tinyint(1) a boolean if convertTinyintToBool is false":{
                        topic:function () {
                            patio.mysql.convertTinyintToBool = false;
                            MYSQL_DB.schema("booltest", {reload:true}).then(hitch(this, "callback", null), hitch(this, "callback"));
                        },

                        "schema should have boolean for type":function (schema) {
                            assert.deepEqual(schema, {
                                b:{type:"integer", autoIncrement:false, allowNull:true, primaryKey:false, "default":null, jsDefault:null, dbType:"tinyint(1)"},
                                i:{type:"integer", autoIncrement:false, allowNull:true, primaryKey:false, "default":null, jsDefault:null, dbType:"tinyint(4)"}
                            });
                        },

                        "should return tinyint(1)s as integers values and tinyint(4) as integers ":{
                            topic:function () {
                                var resultSets = [];
                                var ds = MYSQL_DB.from("booltest");
                                comb.executeInOrder(ds,
                                    function (ds) {
                                        var results = [];
                                        ds.delete();
                                        ds.insert({b:true, i:10});
                                        results.push(ds.all());
                                        ds.delete();
                                        ds.insert({b:false, i:10});
                                        results.push(ds.all());
                                        ds.delete();
                                        ds.insert({b:true, i:1});
                                        results.push(ds.all());
                                        return results;
                                    }).then(hitch(this, this.callback, null), hitch(this, "callback"));
                            },

                            "the result sets should cast properly":function (resultSet) {
                                assert.deepEqual(resultSet, [
                                    [
                                        {b:1, i:10}
                                    ],
                                    [
                                        {b:0, i:10}
                                    ],
                                    [
                                        {b:1, i:1}
                                    ]
                                ]);
                                patio.mysql.convertTinyintToBool = true;
                            }
                        }
                    }
                }
            }
        }

    });


    suite.addBatch({

        "A MySQL dataset":{
            topic:function () {
                MYSQL_DB.createTable("items",
                    function () {
                        this.name("string");
                        this.value("integer");
                    }).then(hitch(this, function () {
                    MYSQL_DB.sqls.length = 0;
                    this.callback(null, MYSQL_DB.from("items"));
                }));
            },
            "should quote columns and tables using back-ticks if quoting identifiers":function (d) {
                d.quoteIdentifiers = true;
                assert.equal(d.select("name").sql, 'SELECT `name` FROM `items`');

                assert.equal(d.select(sql.literal('COUNT(*)')).sql, 'SELECT COUNT(*) FROM `items`');

                assert.equal(d.select(sql.max("value")).sql, 'SELECT max(`value`) FROM `items`');

                assert.equal(d.select(sql.NOW.sqlFunction).sql, 'SELECT NOW() FROM `items`');

                assert.equal(d.select(sql.max(sql.identifier("items__value"))).sql, 'SELECT max(`items`.`value`) FROM `items`');

                assert.equal(d.order(sql.name.desc()).sql, 'SELECT * FROM `items` ORDER BY `name` DESC');

                assert.equal(d.select(sql.literal('items.name AS item_name')).sql, 'SELECT items.name AS item_name FROM `items`');

                assert.equal(d.select(sql.literal('`name`')).sql, 'SELECT `name` FROM `items`');

                assert.equal(d.select(sql.literal('max(items.`name`) AS `max_name`')).sql, 'SELECT max(items.`name`) AS `max_name` FROM `items`');

                assert.equal(d.select(sql.test("abc", sql.literal("'hello'"))).sql, "SELECT test(`abc`, 'hello') FROM `items`");

                assert.equal(d.select(sql.test("abc__def", sql.literal("'hello'"))).sql, "SELECT test(`abc`.`def`, 'hello') FROM `items`");

                assert.equal(d.select(sql.test("abc__def", sql.literal("'hello'")).as("x2")).sql, "SELECT test(`abc`.`def`, 'hello') AS `x2` FROM `items`");

                assert.equal(d.insertSql({value:333}), 'INSERT INTO `items` (`value`) VALUES (333)');

                assert.equal(d.insertSql({x:sql.y}), 'INSERT INTO `items` (`x`) VALUES (`y`)');
            },

            "should quote fields correctly when reversing the order":function (d) {
                d.quoteIdentifiers = true;
                assert.equal(d.reverseOrder("name").sql, 'SELECT * FROM `items` ORDER BY `name` DESC');
                assert.equal(d.reverseOrder(sql.name.desc()).sql, 'SELECT * FROM `items` ORDER BY `name` ASC');
                assert.equal(d.reverseOrder("name", sql.test.desc()).sql, 'SELECT * FROM `items` ORDER BY `name` DESC, `test` ASC');
                assert.equal(d.reverseOrder(sql.name.desc(), "test").sql, 'SELECT * FROM `items` ORDER BY `name` ASC, `test` DESC');
            },

            "should support ORDER clause in UPDATE statements":function (d) {
                d.quoteIdentifiers = false;
                assert.equal(d.order("name").updateSql({value:1}), 'UPDATE items SET value = 1 ORDER BY name');
            },

            "should support LIMIT clause in UPDATE statements":function (d) {
                d.quoteIdentifiers = false;
                assert.equal(d.limit(10).updateSql({value:1}), 'UPDATE items SET value = 1 LIMIT 10');
            },

            "should support regexes":{
                topic:function (d) {
                    comb.executeInOrder(d,
                        function (ds) {
                            ds.insert({name:"abc", value:1});
                            ds.insert({name:"bcd", value:1});
                            return [ds.filter({name:/bc/}).count(), ds.filter({name:/^bc/}).count()];
                        }).then(hitch(this, "callback", null), hitch(this, "callback"));

                },

                "should equal [2,1]":function (res) {
                    assert.deepEqual(res, [2, 1]);
                },

                "should correctly literalize strings with comment backslashes in them":{
                    topic:function (ig, d) {
                        comb.executeInOrder(d,
                            function (d) {
                                d.delete();
                                d.insert({name:":\\"});
                                return d.first().name;
                            }).then(hitch(this, "callback", null), hitch(this, "callback"));
                    },

                    "name should equal ':\\'":function (name) {
                        assert.equal(name, ":\\")
                    }


                }

            }
        }

    });


    suite.addBatch({
        "MySQL datasets":{
            topic:MYSQL_DB.from("orders"),

            "should correctly quote column references":function (d) {
                d.quoteIdentifiers = true;
                var market = 'ICE';
                var ackStamp = new Date() - 15 * 60; // 15 minutes ago
                assert.equal(d.select("market", sql.minute(sql.from_unixtime("ack")).as("minute")).where(
                    function (o) {
                        return this.ack.sqlNumber.gt(ackStamp).and({market:market})
                    }).groupBy(sql.minute(sql.from_unixtime("ack"))).sql, "SELECT `market`, minute(from_unixtime(`ack`)) AS `minute` FROM `orders` WHERE ((`ack` > " + d.literal(ackStamp) + ") AND (`market` = 'ICE')) GROUP BY minute(from_unixtime(`ack`))");
            }
        },

        "Dataset.distinct":{
            topic:function () {
                var db = MYSQL_DB, ds = db.from("a");
                comb.executeInOrder(db, ds,
                    function (db, ds) {
                        db.forceCreateTable("a", function () {
                            this.a("integer");
                            this.b("integer");
                        });
                        ds.insert(20, 10);
                        ds.insert(30, 10);
                        var ret = [ds.order("b", "a").distinct().map("a"), ds.order("b", sql.a.desc()).distinct().map("a")];
                        db.dropTable("a");
                        return ret;

                    }).then(hitch(this, "callback", null), hitch(this, "callback"));
            },

            "should equal [20,30], [30,20]":function (res) {
                assert.deepEqual(res[0], [20, 30]);
                assert.deepEqual(res[1], [30, 20]);
            }
        },

        "MySQL join expressions":{
            topic:MYSQL_DB.from("nodes"),

            "should raise error for :full_outer join requests.":function (ds) {
                assert.throws(hitch(ds, "joinTable", "fullOuter", "nodes"));
            },
            "should support natural left joins":function (ds) {
                assert.equal(ds.joinTable("naturalLeft", "nodes").sql,
                    'SELECT * FROM nodes NATURAL LEFT JOIN nodes');
            },
            "should support natural right joins":function (ds) {
                assert.equal(ds.joinTable("naturalRight", "nodes").sql,
                    'SELECT * FROM nodes NATURAL RIGHT JOIN nodes');
            },
            "should support natural left outer joins":function (ds) {
                assert.equal(ds.joinTable("naturalLeftOuter", "nodes").sql,
                    'SELECT * FROM nodes NATURAL LEFT OUTER JOIN nodes');
            },
            "should support natural right outer joins":function (ds) {
                assert.equal(ds.joinTable("naturalRightOuter", "nodes").sql,
                    'SELECT * FROM nodes NATURAL RIGHT OUTER JOIN nodes');
            },
            "should support natural inner joins":function (ds) {
                assert.equal(ds.joinTable("naturalInner", "nodes").sql,
                    'SELECT * FROM nodes NATURAL LEFT JOIN nodes');
            },
            "should support cross joins":function (ds) {
                assert.equal(ds.joinTable("cross", "nodes").sql, 'SELECT * FROM nodes CROSS JOIN nodes');
            },
            "should support cross joins as inner joins if conditions are used":function (ds) {
                assert.equal(ds.joinTable("cross", "nodes", {id: sql.identifier("id")}).sql,
                    'SELECT * FROM nodes INNER JOIN nodes ON (nodes.id = nodes.id)');
            },
            "should support straight joins (force left table to be read before right)":function (ds) {
                assert.equal(ds.joinTable("straight", "nodes").sql,
                    'SELECT * FROM nodes STRAIGHT_JOIN nodes');
            },
            "should support natural joins on multiple tables.":function (ds) {
                assert.equal(ds.joinTable("naturalLeftOuter", ["nodes", "branches"]).sql,
                    'SELECT * FROM nodes NATURAL LEFT OUTER JOIN (nodes, branches)');
            },
            "should support straight joins on multiple tables.":function (ds) {
                assert.equal(ds.joinTable("straight", ["nodes", "branches"]).sql,
                    'SELECT * FROM nodes STRAIGHT_JOIN (nodes, branches)');
            },

            "should quote fields correctly":function (ds) {
                ds.quoteIdentifiers = true;
                assert.equal(ds.join("attributes", {nodeId:sql.id}).sql, "SELECT * FROM `nodes` INNER JOIN `attributes` ON (`attributes`.`nodeId` = `nodes`.`id`)");
            },

            "should allow a having clause on ungrouped datasets":function (ds) {
                ds.quoteIdentifiers = false;
                assert.doesNotThrow(hitch(ds, "having", "blah"));
                assert.equal(ds.having(sql.literal('blah')).sql, "SELECT * FROM nodes HAVING (blah)");
            },

            "should put a having clause before an order by clause":function (ds) {
                ds.quoteIdentifiers = false;
                assert.equal(ds.order("aaa").having({bbb:sql.identifier("ccc")}).sql, "SELECT * FROM nodes HAVING (bbb = ccc) ORDER BY aaa");
            }
        },

        "A MySQL database":{
            topic:MYSQL_DB,

            "should support addColumn operations":{
                topic:function (db) {
                    comb.executeInOrder(db,
                        function (db) {
                            db.addColumn("test2", "xyz", "text");
                            var ds = db.from("test2");
                            var columns = ds.columns;
                            ds.insert({name:"mmm", value:"111", xyz:'000'});
                            return {columns:columns, first:ds.first().xyz};
                        }).then(hitch(this, "callback", null), hitch(this, "callback"));
                },

                "it should equal ['name', 'value', 'xyz'] and '000'":function (res) {
                    assert.deepEqual(res.columns, ["name", "value", "xyz"]);
                    assert.equal(res.first, "000");
                },

                "should support dropColumn operations":{
                    topic:function (ig, db) {
                        comb.executeInOrder(db,
                            function (db) {
                                db.dropColumn("test2", "xyz", "text");
                                return {columns:db.from("test2").columns};
                            }).then(hitch(this, "callback", null), hitch(this, "callback"));
                    },

                    "it should equal ['name', 'value'']":function (res) {
                        assert.deepEqual(res.columns, ["name", "value"]);
                    },

                    "should support renameColumn operations":{
                        topic:function (ig, igg, db) {
                            comb.executeInOrder(db,
                                function (db) {
                                    db.from("test2").delete();
                                    db.addColumn("test2", "xyz", "text");
                                    db.from("test2").insert({name:"mmm", value:111, xyz:"gggg"});
                                    var col1 = db.from("test2").columns;
                                    db.renameColumn("test2", "xyz", "zyx", {type:"text"});
                                    var col2 = db.from("test2").columns;
                                    return {col1:col1, col2:col2, first:db.from("test2").first().zyx};
                                }).then(hitch(this, "callback", null), hitch(this, "callback"));
                        },

                        "it should equal ['name', 'value'']":function (res) {
                            assert.deepEqual(res.col1, ["name", "value", "xyz"]);
                            assert.deepEqual(res.col2, ["name", "value", "zyx"]);
                            assert.equal(res.first, "gggg");
                        },

                        "should support renameColumn operations with types like varchar":{
                            topic:function (ig, igg, iggg, db) {
                                comb.executeInOrder(db,
                                    function (db) {
                                        db.from("test2").delete();
                                        db.addColumn("test2", "tre", "text");
                                        db.from("test2").insert({name:"mmm", value:111, tre:"gggg"});
                                        var col1 = db.from("test2").columns;
                                        db.renameColumn("test2", "tre", "ert", {type:"varchar", size:255});
                                        var col2 = db.from("test2").columns;
                                        return {col1:col1, col2:col2, first:db.from("test2").first().ert};
                                    }).then(hitch(this, "callback", null), hitch(this, "callback"));
                            },

                            "it should equal ['name', 'value'']":function (res) {
                                assert.deepEqual(res.col1, ["name", "value", "zyx", "tre"]);
                                assert.deepEqual(res.col2, ["name", "value", "zyx", "ert"]);
                                assert.equal(res.first, "gggg");
                            },

                            "should support setColumntype operation":{
                                topic:function (ig, igg, iggg, igggg, db) {
                                    comb.executeInOrder(db,
                                        function (db) {
                                            db.from("test2").delete();
                                            db.addColumn("test2", "xyz", "float");
                                            db.from("test2").insert({name:"mmm", value:111, xyz:56.78});
                                            var before = db.from("test2").first().xyz;
                                            db.setColumnType("test2", "xyz", "integer");
                                            var after = db.from("test2").first().xyz;
                                            return {before:before, after:after};
                                        }).then(hitch(this, "callback", null), hitch(this, "callback"));
                                },

                                "it should equal ['name', 'value'']":function (res) {
                                    assert.equal(res.before, 56.78);
                                    assert.equal(res.after, 57);
                                },

                                "should support addIndex operation":{
                                    topic:function (ig, ign, igno, ignor, ignore, db) {
                                        comb.executeInOrder(db,
                                            function (db) {
                                                db.from("test2").delete();
                                                var emptyIndexes = db.indexes("test2");
                                                db.addIndex("test2", "value");
                                                var indexes = db.indexes("test2");
                                                return {indexes:indexes, emptyIndexes:emptyIndexes};
                                            }).then(hitch(this, "callback", null), hitch(this, "callback"));
                                    },

                                    "it should equal ['name', 'value'']":function (res) {
                                        assert.isNotNull(res.indexes.test2_value_index);
                                        assert.isTrue(comb.isEmpty(res.emptyIndexes));
                                    },

                                    "should support addForeignKey":{
                                        topic:function (ig, ign, igno, ignor, ignore, ignoree, db) {
                                            comb.executeInOrder(db,
                                                function (db) {
                                                    db.from("test2").delete();
                                                    db.alterTable("test2", function () {
                                                        this.addForeignKey("value2", "test2", {key:"value"});
                                                    });
                                                    return db.from("test2").columns;
                                                }).then(hitch(this, "callback", null), hitch(this, "callback"));
                                        },

                                        "it should equal ['name', 'value'']":function (columns) {
                                            assert.deepEqual(columns, ["name", "value", "zyx", "ert", "xyz", "value2"]);
                                        }
                                    }
                                }

                            }
                        }
                    }
                }
            }
        }});

    suite.addBatch({
        "A MySQL database with table options":{
            topic:function () {
                patio.mysql.defaultEngine = 'InnoDB';
                patio.mysql.defaultCharset = 'utf8';
                patio.mysql.defaultCollate = 'utf8_general_ci';
                var db = MYSQL_DB;
                comb.executeInOrder(db,
                    function (db) {
                        db.dropTable("items");
                        db.sqls = [];
                    }).then(hitch(this, "callback", null, MYSQL_DB), hitch(this, "callback"));
            },

            "should allow to pass custom options (engine, charset, collate) for table creation":{
                topic:function (db) {
                    comb.executeInOrder(db,
                        function (db) {
                            db.sqls = [];
                            db.createTable("items", {engine:'MyISAM', charset:'latin1', collate:'latin1_swedish_ci'}, function () {
                                this.size("integer");
                                this.name("text");
                            });
                            var sqls = db.sqls.slice(0);
                            db.dropTable("items");
                            return sqls;
                        }).then(hitch(this, "callback", null), hitch(this, "callback"));
                },
                'sqls should equal ["CREATE TABLE items (size integer, name text) ENGINE=MyISAM DEFAULT CHARSET=latin1 DEFAULT COLLATE=latin1_swedish_ci"]':function (sqls) {
                    assert.deepEqual(sqls, ["CREATE TABLE items (size integer, name text) ENGINE=MyISAM DEFAULT CHARSET=latin1 DEFAULT COLLATE=latin1_swedish_ci"]);
                },

                "should use default options (engine, charset, collate) for table creation":{
                    topic:function (ig, db) {
                        comb.executeInOrder(db,
                            function (db) {
                                db.sqls = [];
                                db.createTable("items", function () {
                                    this.size("integer");
                                    this.name("text");
                                });
                                var sqls = db.sqls.slice(0);
                                db.dropTable("items");
                                return sqls;
                            }).then(hitch(this, "callback", null), hitch(this, "callback"));
                    },
                    'sqls should equal ["CREATE TABLE items (size integer, name text) ENGINE=InnoDB DEFAULT CHARSET=utf8 DEFAULT COLLATE=utf8_general_ci"]':function (sqls) {
                        assert.deepEqual(sqls, ["CREATE TABLE items (size integer, name text) ENGINE=InnoDB DEFAULT CHARSET=utf8 DEFAULT COLLATE=utf8_general_ci"]);
                    },


                    "should not use default options (engine, charset, collate) for table creation":{
                        topic:function (id, igg, db) {
                            comb.executeInOrder(db,
                                function (db) {
                                    db.sqls = [];
                                    db.createTable("items", {engine:null, charset:null, collate:null}, function () {
                                        this.size("integer");
                                        this.name("text");
                                    });
                                    var sqls = db.sqls.slice(0);
                                    db.dropTable("items");
                                    return sqls;
                                }).then(hitch(this, "callback", null), hitch(this, "callback"));
                        },

                        'sqls should equal["CREATE TABLE items (size integer, name text)"]':function (sqls) {
                            assert.deepEqual(sqls, ["CREATE TABLE items (size integer, name text)"]);
                            patio.mysql.defaultEngine = null;
                            patio.mysql.defaultCharset = null;
                            patio.mysql.defaultCollate = null;
                        }
                    }
                }
            }
        }
    });

    suite.addBatch({
        "A MySQL database":{
            topic:function () {
                MYSQL_DB.dropTable("items").then(hitch(this, "callback", null, MYSQL_DB), hitch(this, "callback", null, MYSQL_DB));
                MYSQL_DB.sqls.length = 0;
            },

            "should support defaults for boolean columns":{
                topic:function (db) {
                    comb.executeInOrder(db,
                        function (db) {
                            db.sqls = [];
                            db.createTable("items", function () {
                                this.active1(Boolean, {"default":true});
                                this.active2(Boolean, {"default":false});
                            });
                            var sqls = db.sqls.slice(0);
                            db.dropTable("items");
                            return sqls;
                        }).then(hitch(this, "callback", null), hitch(this, "callback"));
                },

                'sqls should equal["CREATE TABLE items (active1 tinyint(1) DEFAULT 1, active2 tinyint(1) DEFAULT 0)"]':function (sqls) {
                    assert.deepEqual(sqls, ["CREATE TABLE items (active1 tinyint(1) DEFAULT 1, active2 tinyint(1) DEFAULT 0)"]);
                },

                "should correctly format CREATE TABLE statements with foreign keys":{
                    topic:function (ig, db) {
                        comb.executeInOrder(db,
                            function (db) {
                                db.sqls = [];
                                db.createTable("items", function () {
                                    this.primaryKey("id");
                                    this.foreignKey("p_id", "items", {key:"id", "null":false, onDelete:"cascade"});
                                });
                                var sqls = db.sqls.slice(0);
                                db.dropTable("items");
                                return sqls;
                            }).then(hitch(this, "callback", null), hitch(this, "callback"));
                    },

                    'sqls should equal["CREATE TABLE items (id integer PRIMARY KEY AUTO_INCREMENT, p_id integer NOT NULL, FOREIGN KEY (p_id) REFERENCES items(id) ON DELETE CASCADE)"]':function (sqls) {
                        assert.deepEqual(sqls, ["CREATE TABLE items (id integer PRIMARY KEY AUTO_INCREMENT, p_id integer NOT NULL, FOREIGN KEY (p_id) REFERENCES items(id) ON DELETE CASCADE)"]);
                    },

                    "should correctly format ALTER TABLE statements with foreign keys":{
                        topic:function (ig, ign, db) {
                            comb.executeInOrder(db,
                                function (db) {
                                    db.sqls = [];
                                    db.createTable("items", function () {
                                        this.primaryKey("id");
                                    });
                                    db.alterTable("items", function () {
                                        this.addForeignKey("p_id", "items", {key:"id", "null":false, onDelete:"cascade"});
                                    });
                                    var sqls = db.sqls.slice(0);
                                    db.dropTable("items");
                                    return sqls;
                                }).then(hitch(this, "callback", null), hitch(this, "callback"));
                        },

                        "sqls should equal['CREATE TABLE items (id integer PRIMARY KEY AUTO_INCREMENT)', 'ALTER TABLE items ADD COLUMN p_id integer NOT NULL', 'ALTER TABLE items ADD FOREIGN KEY (p_id) REFERENCES items(id) ON DELETE CASCADE']":function (sqls) {
                            assert.deepEqual(sqls, [
                                'CREATE TABLE items (id integer PRIMARY KEY AUTO_INCREMENT)',
                                'ALTER TABLE items ADD COLUMN p_id integer NOT NULL',
                                'ALTER TABLE items ADD FOREIGN KEY (p_id) REFERENCES items(id) ON DELETE CASCADE']);
                        },

                        "should have renameColumn support keep existing options":{
                            topic:function (ig, ign, igno, db) {
                                comb.executeInOrder(db,
                                    function (db) {
                                        db.sqls = [];
                                        db.createTable("items", function () {
                                            this.id(String, {"null":false, "default":"blah"});
                                        });
                                        db.alterTable("items", function () {
                                            this.renameColumn("id", "nid");
                                        });
                                        var sqls = db.sqls.slice(0);
                                        var ds = db.from("items");
                                        ds.insert();
                                        var items = ds.all();
                                        db.dropTable("items");
                                        return {sqls:sqls, items:items};
                                    }).then(hitch(this, "callback", null), hitch(this, "callback"));
                            },

                            'sqls should equal["CREATE TABLE items (id varchar(255) NOT NULL DEFAULT \'blah\')","DESCRIBE items","ALTER TABLE items CHANGE COLUMN id nid varchar(255) NOT NULL DEFAULT \'blah\'"]':function (sqls) {
                                assert.deepEqual(sqls.sqls, [
                                    "CREATE TABLE items (id varchar(255) NOT NULL DEFAULT 'blah')",
                                    "DESCRIBE items",
                                    "ALTER TABLE items CHANGE COLUMN id nid varchar(255) NOT NULL DEFAULT 'blah'"
                                ]);
                                assert.deepEqual(sqls.items, [
                                    {nid:"blah"}
                                ]);
                            },

                            "should have setColumnType support keep existing options":{
                                topic:function (ig, ign, igno, ignor, db) {
                                    comb.executeInOrder(db,
                                        function (db) {
                                            db.sqls = [];
                                            db.createTable("items", function () {
                                                this.id("integer", {"null":false, "default":5});
                                            });
                                            db.alterTable("items", function () {
                                                this.setColumnType("id", "bigint");
                                            });
                                            var sqls = db.sqls.slice(0);
                                            var ds = db.from("items");
                                            ds.insert(Math.pow(2, 40));
                                            var items = ds.all();
                                            db.dropTable("items");
                                            return {sqls:sqls, items:items};
                                        }).then(hitch(this, "callback", null), hitch(this, "callback"));
                                },

                                'sqls should equal["CREATE TABLE items (id integer NOT NULL DEFAULT 5)", "DESCRIBE items", "ALTER TABLE items CHANGE COLUMN id id bigint NOT NULL DEFAULT 5"]':function (sqls) {
                                    assert.deepEqual(sqls.sqls, [
                                        "CREATE TABLE items (id integer NOT NULL DEFAULT 5)",
                                        "DESCRIBE items",
                                        "ALTER TABLE items CHANGE COLUMN id id bigint NOT NULL DEFAULT 5"
                                    ]);
                                    assert.deepEqual(sqls.items, [
                                        {id:Math.pow(2, 40)}
                                    ]);
                                },

                                "should have setColumnType pass through options":{
                                    topic:function (ig, ign, igno, ignor, ignore, db) {
                                        comb.executeInOrder(db,
                                            function (db) {
                                                db.sqls = [];
                                                db.createTable("items", function () {
                                                    this.id("integer");
                                                    this.list("enum", {elements:["one"]});
                                                });
                                                db.alterTable("items", function () {
                                                    this.setColumnType("id", "int", {unsigned:true, size:8});
                                                    this.setColumnType("list", "enum", {elements:["two"]});
                                                });
                                                var sqls = db.sqls.slice(0);
                                                db.dropTable("items");
                                                return sqls;
                                            }).then(hitch(this, "callback", null), hitch(this, "callback"));
                                    },

                                    'sqls should equal["CREATE TABLE items (id integer, list enum(\'one\'))", "DESCRIBE items", "ALTER TABLE items CHANGE COLUMN id id int(8) UNSIGNED NULL", "ALTER TABLE items CHANGE COLUMN list list enum(\'two\') NULL"]':function (sqls) {
                                        assert.deepEqual(sqls, [
                                            "CREATE TABLE items (id integer, list enum('one'))",
                                            "DESCRIBE items",
                                            "DESCRIBE items",
                                            "ALTER TABLE items CHANGE COLUMN id id int(8) UNSIGNED NULL",
                                            "ALTER TABLE items CHANGE COLUMN list list enum('two') NULL"
                                        ]);
                                    },

                                    "should have setColumnDefault keep existing options":{
                                        topic:function (ig, ign, igno, ignor, ignore, ignoree, db) {
                                            comb.executeInOrder(db,
                                                function (db) {
                                                    db.sqls = [];
                                                    db.createTable("items", function () {
                                                        this.id("integer", {"null":false, "default":5});
                                                    });
                                                    db.alterTable("items", function () {
                                                        this.setColumnDefault("id", 6);
                                                    });
                                                    var sqls = db.sqls.slice(0);
                                                    var ds = db.from("items");
                                                    ds.insert();
                                                    var items = ds.all();
                                                    db.dropTable("items");
                                                    return {sqls:sqls, items:items};
                                                }).then(hitch(this, "callback", null), hitch(this, "callback"));
                                        },

                                        'sqls should equal["CREATE TABLE items (id integer NOT NULL DEFAULT 5)", "DESCRIBE items", "ALTER TABLE items CHANGE COLUMN id id int(11) NOT NULL DEFAULT 6"]':function (sqls) {
                                            assert.deepEqual(sqls.sqls, [
                                                "CREATE TABLE items (id integer NOT NULL DEFAULT 5)",
                                                "DESCRIBE items",
                                                "ALTER TABLE items CHANGE COLUMN id id int(11) NOT NULL DEFAULT 6"
                                            ]);
                                            assert.deepEqual(sqls.items, [
                                                {id:6}
                                            ]);
                                        },

                                        "should have setAllowNull keep existing options":{
                                            topic:function (ig, ign, igno, ignor, ignore, ignoree, ignoreee, db) {
                                                comb.executeInOrder(db,
                                                    function (db) {
                                                        db.sqls = [];
                                                        db.createTable("items", function () {
                                                            this.id("integer", {"null":false, "default":5});
                                                        });
                                                        db.alterTable("items", function () {
                                                            this.setAllowNull("id", true);
                                                        });
                                                        var sqls = db.sqls.slice(0);
                                                        var ds = db.from("items");
                                                        ds.insert();
                                                        var items = ds.all();
                                                        db.dropTable("items");
                                                        return {sqls:sqls, items:items};
                                                    }).then(hitch(this, "callback", null), hitch(this, "callback"));
                                            },

                                            'sqls should equal["CREATE TABLE items (id integer NOT NULL DEFAULT 5)", "DESCRIBE items", "ALTER TABLE items CHANGE COLUMN id id int(11) NULL DEFAULT 5"]':function (sqls) {
                                                assert.deepEqual(sqls.sqls, [
                                                    "CREATE TABLE items (id integer NOT NULL DEFAULT 5)",
                                                    "DESCRIBE items",
                                                    "ALTER TABLE items CHANGE COLUMN id id int(11) NULL DEFAULT 5"
                                                ]);
                                                assert.deepEqual(sqls.items, [
                                                    {id:5}
                                                ]);
                                            },

                                            "should have accept raw SQL when using db.run":{
                                                topic:function (ig, ign, igno, ignor, ignore, ignoree, ignoreee, ignoreeee, db) {
                                                    comb.executeInOrder(db,
                                                        function (db) {
                                                            var res = [];
                                                            db.createTable("items", function () {
                                                                this.name("string");
                                                                this.value("integer");
                                                            });
                                                            db.run("DELETE FROM items");
                                                            res.push(db.from("items").all());
                                                            db.run("INSERT INTO items (name, value) VALUES ('tutu', 1234)");
                                                            res.push(db.from("items").first());
                                                            db.run("DELETE FROM items");
                                                            res.push(db.from("items").first());
                                                            db.dropTable("items");
                                                            return res;
                                                        }).then(hitch(this, "callback", null), hitch(this, "callback"));
                                                },

                                                'result should be [[], {name : \'tutu\', value : 1234}, null]':function (res) {
                                                    assert.deepEqual(res, [
                                                        [],
                                                        {name:'tutu', value:1234},
                                                        null
                                                    ]);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    });

    suite.addBatch({
        "A grouped mysql MySQL dataset":{
            topic:function () {
                comb.executeInOrder(MYSQL_DB,
                    function (db) {
                        var ds = db.from("test2");
                        ds.delete();
                        ds.insert({name:11, value:10});
                        ds.insert({name:11, value:20});
                        ds.insert({name:11, value:30});
                        ds.insert({name:12, value:10});
                        ds.insert({name:12, value:20});
                        ds.insert({name:13, value:30});
                        var ds2 = db.fetch("SELECT name FROM test2 WHERE name = '11' GROUP BY name");
                        var count = ds2.count();
                        var ds3 = db.from("test2").select("name").where({name:11}).group("name");
                        return {count1:count, count2:ds3.count()};
                    }).then(hitch(this, "callback", null), hitch(this, "callback"));
            },

            "should return 1 AND 1":function (res) {
                assert.equal(res.count1, 1);
                assert.equal(res.count2, 1);
            }
        }
    });

    suite.addBatch({
        "A MySQL database":{
            topic:function () {
                var db = MYSQL_DB;
                comb.executeInOrder(db,
                    function (db) {
                        db.dropTable("posts");
                        db.sqls = [];
                    }).then(hitch(this, "callback", null, db), hitch(this, "callback", null, db));
            },

            "should support fulltext indexes and fullTextSearch":{

                topic:function (db) {
                    comb.executeInOrder(db,
                        function (db) {
                            db.sqls = [];
                            db.createTable("posts", {engine:"MyISAM"}, function () {
                                this.title("text");
                                this.body("text");
                                this.fullTextIndex("title");
                                this.fullTextIndex(["title", "body"]);
                            });
                            var ret = {};
                            ret.sqls = db.sqls.slice(0);
                            db.from("posts").insert({title:'node server', body:'y'});
                            db.from("posts").insert({title:'patio', body:'query'});
                            db.from("posts").insert({title:'node bode', body:'x'});
                            db.sqls = [];
                            ret.ret1 = db.from("posts").fullTextSearch("title", "server").all()[0];
                            ret.ret2 = db.from("posts").fullTextSearch(["title", "body"], ['patio', 'query']).all()[0];
                            ret.ret3 = db.from("posts").fullTextSearch("title", '+node -server', {boolean:true}).all()[0];
                            ret.sqls2 = db.sqls.slice(0);
                            db.dropTable("posts");
                            return ret;
                        }).then(hitch(this, "callback", null), hitch(this, "callback"));
                },

                'the sql should equal ["CREATE TABLE posts (title text, body text) ENGINE=MyISAM","CREATE FULLTEXT INDEX posts_title_index ON posts (title)","CREATE FULLTEXT INDEX posts_title_body_index ON posts (title, body)"]':function (sqls) {
                    assert.deepEqual(sqls.sqls, [
                        "CREATE TABLE posts (title text, body text) ENGINE=MyISAM",
                        "CREATE FULLTEXT INDEX posts_title_index ON posts (title)",
                        "CREATE FULLTEXT INDEX posts_title_body_index ON posts (title, body)"
                    ]);
                    assert.deepEqual(sqls.ret1, { title:'node server', body:'y' });
                    assert.deepEqual(sqls.ret2, { title:'patio', body:'query' });
                    assert.deepEqual(sqls.ret3, { title:'node bode', body:'x' });
                    assert.deepEqual(sqls.sqls2, [
                        "SELECT * FROM posts WHERE (MATCH (title) AGAINST ('server'))",
                        "SELECT * FROM posts WHERE (MATCH (title, body) AGAINST ('patio query'))",
                        "SELECT * FROM posts WHERE (MATCH (title) AGAINST ('+node -server' IN BOOLEAN MODE))"
                    ]);
                },

                "should support spatial indexes":{

                    topic:function (ig, db) {
                        comb.executeInOrder(db,
                            function (db) {
                                db.sqls = [];
                                db.createTable("posts", {engine:"MyISAM"}, function () {
                                    this.geom("point", {allowNull:false});
                                    this.spatialIndex(["geom"]);
                                });
                                var sqls = db.sqls.slice(0);
                                db.dropTable("posts");
                                return sqls;
                            }).then(hitch(this, "callback", null), hitch(this, "callback"));
                    },

                    'the sql should equal ["CREATE TABLE posts (geom point NOT NULL) ENGINE=MyISAM","CREATE SPATIAL INDEX posts_geom_index ON posts (geom)"]':function (sqls) {
                        assert.deepEqual(sqls, [
                            "CREATE TABLE posts (geom point NOT NULL) ENGINE=MyISAM",
                            "CREATE SPATIAL INDEX posts_geom_index ON posts (geom)"

                        ]);
                    },

                    "should support indexes types":{

                        topic:function (ig, ign, db) {
                            comb.executeInOrder(db,
                                function (db) {
                                    db.sqls = [];
                                    db.createTable("posts", function () {
                                        this.id("integer");
                                        this.index("id", {type:"btree"});
                                    });
                                    var sqls = db.sqls.slice(0);
                                    db.dropTable("posts");
                                    return sqls;
                                }).then(hitch(this, "callback", null), hitch(this, "callback"));
                        },

                        'the sql should equal ["CREATE TABLE posts (id integer)","CREATE INDEX posts_id_index USING btree ON posts (id)"]':function (sqls) {
                            assert.deepEqual(sqls, [
                                "CREATE TABLE posts (id integer)",
                                "CREATE INDEX posts_id_index USING btree ON posts (id)"
                            ]);
                        },

                        "should support unique indexes using types":{

                            topic:function (ig, ign, igno, db) {
                                comb.executeInOrder(db,
                                    function (db) {
                                        db.sqls = [];
                                        db.createTable("posts", function () {
                                            this.id("integer");
                                            this.index("id", {type:"btree", unique:true});
                                        });
                                        var sqls = db.sqls.slice(0);
                                        db.dropTable("posts");
                                        return sqls;
                                    }).then(hitch(this, "callback", null), hitch(this, "callback"));
                            },

                            'the sql should equal ["CREATE TABLE posts (id integer)","CREATE UNIQUE INDEX posts_id_index USING btree ON posts (id)"]':function (sqls) {
                                assert.deepEqual(sqls, [
                                    "CREATE TABLE posts (id integer)",
                                    "CREATE UNIQUE INDEX posts_id_index USING btree ON posts (id)"
                                ]);
                            },

                            "should not dump partial indexes":{

                                topic:function (ig, ign, igno, ignor, db) {
                                    comb.executeInOrder(db,
                                        function (db) {
                                            db.sqls = [];
                                            db.createTable("posts", function () {
                                                this.id("text");
                                            });
                                            db.run("CREATE INDEX posts_id_index ON posts (id(10))");
                                            var ret = db.indexes("posts");
                                            db.dropTable("posts");
                                            return ret;
                                        }).then(hitch(this, "callback", null), hitch(this, "callback"));
                                },

                                'the sql should equal ["CREATE TABLE posts (id integer)","CREATE UNIQUE INDEX posts_id_index USING btree ON posts (id)"]':function (indexes) {
                                    assert.isTrue(comb.isEmpty(indexes));
                                }
                            }
                        }
                    }
                }
            }
        }
    });

    suite.addBatch({
        "Mysql.Dataset.insert and related methods":{
            topic:function () {
                comb.executeInOrder(MYSQL_DB,
                    function (db) {
                        db.createTable("items",
                            function () {
                                this.name(String);
                                this.value("integer");
                            });
                        db.sqls = [];
                    }).then(hitch(this, "callback", null, MYSQL_DB.from("items")), hitch(this, "callback"));
            },

            "insert":{
                topic:function (d) {
                    comb.executeInOrder(MYSQL_DB, d,
                        function (db, d) {
                            db.sqls = [];
                            d.insert();
                            var sql = db.sqls.slice(0), all = d.all();
                            db.dropTable("items");
                            db.createTable("items",
                                function () {
                                    this.name(String);
                                    this.value("integer");
                                });
                            return {sql:sql, all:all};
                        }).then(hitch(this, "callback", null), hitch(this, "callback"));
                },

                " should insert record with default values when no arguments given":function (ret) {
                    assert.deepEqual(ret.all, [
                        {name:null, value:null}
                    ]);
                    assert.deepEqual(ret.sql, [
                        "INSERT INTO items () VALUES ()"
                    ]);
                },

                "insert":{
                    topic:function (i, d) {
                        comb.executeInOrder(MYSQL_DB, d,
                            function (db, d) {
                                db.sqls = [];
                                d.insert({});
                                var sql = db.sqls.slice(0), all = d.all();
                                db.dropTable("items");
                                db.createTable("items",
                                    function () {
                                        this.name(String);
                                        this.value("integer");
                                    });
                                return {sql:sql, all:all};
                            }).then(hitch(this, "callback", null), hitch(this, "callback"));
                    },

                    "  should insert record with default values when empty hash given":function (ret) {
                        assert.deepEqual(ret.all, [
                            {name:null, value:null}
                        ]);
                        assert.deepEqual(ret.sql, [
                            "INSERT INTO items () VALUES ()"
                        ]);
                    },

                    "insert":{
                        topic:function (i, ig, d) {
                            comb.executeInOrder(MYSQL_DB, d,
                                function (db, d) {
                                    db.sqls = [];
                                    d.insert([]);
                                    var sql = db.sqls.slice(0), all = d.all();
                                    db.dropTable("items");
                                    db.createTable("items",
                                        function () {
                                            this.name(String);
                                            this.value("integer");
                                        });
                                    return {sql:sql, all:all};
                                }).then(hitch(this, "callback", null), hitch(this, "callback"));
                        },

                        "  should insert record with default values when empty array given":function (ret) {
                            assert.deepEqual(ret.all, [
                                {name:null, value:null}
                            ]);
                            assert.deepEqual(ret.sql, [
                                "INSERT INTO items () VALUES ()"
                            ]);
                        },

                        "onDuplicateKeyUpdate":{
                            topic:function (i, ig, ign, d) {
                                comb.executeInOrder(MYSQL_DB, d,
                                    function (db, d) {
                                        db.addIndex("items", "name", {unique:true});
                                        db.sqls = [];
                                        d.insert({name:"abc", value:1});
                                        d.onDuplicateKeyUpdate("name", {value:6}).insert({name:"abc", value:1});
                                        d.onDuplicateKeyUpdate("name", {value:6}).insert({name:"def", value:2});
                                        var sql = db.sqls.slice(0), all = d.all();
                                        db.dropTable("items");
                                        db.createTable("items",
                                            function () {
                                                this.name(String);
                                                this.value("integer");
                                            });
                                        return {sql:sql, all:all};
                                    }).then(hitch(this, "callback", null), hitch(this, "callback"));
                            },

                            " should work with regular inserts":function (ret) {
                                assert.deepEqual(ret.all, [
                                    {name:'abc', value:6},
                                    {name:'def', value:2}
                                ]);
                                assert.deepEqual(ret.sql, [
                                    "INSERT INTO items (name, value) VALUES ('abc', 1)",
                                    "INSERT INTO items (name, value) VALUES ('abc', 1) ON DUPLICATE KEY UPDATE name=VALUES(name), value=6",
                                    "INSERT INTO items (name, value) VALUES ('def', 2) ON DUPLICATE KEY UPDATE name=VALUES(name), value=6"
                                ]);
                            },

                            "multiInsert":{
                                topic:function (i, ig, ign, igno, d) {
                                    comb.executeInOrder(MYSQL_DB, d,
                                        function (db, d) {
                                            db.sqls = [];
                                            d.multiInsert([
                                                {name:"abc"},
                                                {name:'def'}
                                            ]);
                                            var sql = db.sqls.slice(0), all = d.all();
                                            db.dropTable("items");
                                            db.createTable("items",
                                                function () {
                                                    this.name(String);
                                                    this.value("integer");
                                                });
                                            return {sql:sql, all:all};
                                        }).then(hitch(this, "callback", null), hitch(this, "callback"));
                                },

                                " should insert multiple records in a single statement":function (ret) {
                                    assert.deepEqual(ret.all, [
                                        {name:'abc', value:null},
                                        {name:'def', value:null}
                                    ]);
                                    assert.deepEqual(ret.sql, [
                                        SQL_BEGIN,
                                        "INSERT INTO items (name) VALUES ('abc'), ('def')",
                                        SQL_COMMIT
                                    ]);
                                },

                                "multiInsert":{
                                    topic:function (i, ig, ign, igno, ignor, d) {
                                        comb.executeInOrder(MYSQL_DB, d,
                                            function (db, d) {
                                                db.sqls = [];
                                                d.multiInsert([
                                                    {value:1},
                                                    {value:2},
                                                    {value:3},
                                                    {value:4}
                                                ], {commitEvery:2});
                                                var sql = db.sqls.slice(0), all = d.all();
                                                db.dropTable("items");
                                                db.createTable("items",
                                                    function () {
                                                        this.name(String);
                                                        this.value("integer");
                                                    });
                                                return {sql:sql, all:all};
                                            }).then(hitch(this, "callback", null), hitch(this, "callback"));
                                    },

                                    " should split the list of records into batches if commitEvery option is given":function (ret) {
                                        assert.deepEqual(ret.all, [
                                            {name:null, value:1},
                                            {name:null, value:2},
                                            {name:null, value:3},
                                            {name:null, value:4}
                                        ]);
                                        assert.deepEqual(ret.sql, [
                                            SQL_BEGIN,
                                            "INSERT INTO items (value) VALUES (1), (2)",
                                            SQL_COMMIT,
                                            SQL_BEGIN,
                                            "INSERT INTO items (value) VALUES (3), (4)",
                                            SQL_COMMIT
                                        ]);
                                    },

                                    "multiInsert":{
                                        topic:function (i, ig, ign, igno, ignor, ignore, d) {
                                            comb.executeInOrder(MYSQL_DB, d,
                                                function (db, d) {
                                                    db.sqls = [];
                                                    d.multiInsert([
                                                        {value:1},
                                                        {value:2},
                                                        {value:3},
                                                        {value:4}
                                                    ], {slice:2});
                                                    var sql = db.sqls.slice(0), all = d.all();
                                                    db.dropTable("items");
                                                    db.createTable("items",
                                                        function () {
                                                            this.name(String);
                                                            this.value("integer");
                                                        });
                                                    return {sql:sql, all:all};
                                                }).then(hitch(this, "callback", null), hitch(this, "callback"));
                                        },

                                        " should split the list of records into batches if slice option is given":function (ret) {
                                            assert.deepEqual(ret.all, [
                                                {name:null, value:1},
                                                {name:null, value:2},
                                                {name:null, value:3},
                                                {name:null, value:4}
                                            ]);
                                            assert.deepEqual(ret.sql, [
                                                SQL_BEGIN,
                                                "INSERT INTO items (value) VALUES (1), (2)",
                                                SQL_COMMIT,
                                                SQL_BEGIN,
                                                "INSERT INTO items (value) VALUES (3), (4)",
                                                SQL_COMMIT
                                            ]);
                                        },

                                        "import":{
                                            topic:function (i, ig, ign, igno, ignor, ignore, ignoree, d) {
                                                comb.executeInOrder(MYSQL_DB, d,
                                                    function (db, d) {
                                                        db.sqls = [];
                                                        d.import(["name", "value"], [
                                                            ["abc", 1],
                                                            ["def", 2]
                                                        ]);
                                                        var sql = db.sqls.slice(0), all = d.all();
                                                        db.dropTable("items");
                                                        db.createTable("items",
                                                            function () {
                                                                this.name(String);
                                                                this.value("integer");
                                                            });
                                                        return {sql:sql, all:all};
                                                    }).then(hitch(this, "callback", null), hitch(this, "callback"));
                                            },

                                            " should support inserting using columns and values arrays":function (ret) {
                                                assert.deepEqual(ret.all, [
                                                    {name:'abc', value:1},
                                                    {name:"def", value:2}
                                                ]);
                                                assert.deepEqual(ret.sql, [
                                                    SQL_BEGIN,
                                                    "INSERT INTO items (name, value) VALUES ('abc', 1), ('def', 2)",
                                                    SQL_COMMIT
                                                ]);
                                            },

                                            "insertIgnore ":{
                                                topic:function (i, ig, ign, igno, ignor, ignore, ignoree, ignoreee, d) {
                                                    comb.executeInOrder(MYSQL_DB, d,
                                                        function (db, d) {
                                                            db.sqls = [];
                                                            d.insertIgnore().multiInsert([
                                                                {name:"abc"},
                                                                {name:"def"}
                                                            ]);
                                                            var sql = db.sqls.slice(0), all = d.all();
                                                            db.dropTable("items");
                                                            db.createTable("items",
                                                                function () {
                                                                    this.name(String);
                                                                    this.value("integer");
                                                                });
                                                            return {sql:sql, all:all};
                                                        }).then(hitch(this, "callback", null), hitch(this, "callback"));
                                                },

                                                "should add the IGNORE keyword when inserting":function (ret) {
                                                    assert.deepEqual(ret.all, [
                                                        {name:'abc', value:null},
                                                        {name:"def", value:null}
                                                    ]);
                                                    assert.deepEqual(ret.sql, [
                                                        SQL_BEGIN,
                                                        "INSERT IGNORE INTO items (name) VALUES ('abc'), ('def')",
                                                        SQL_COMMIT
                                                    ]);
                                                },

                                                "insertIgnore":{
                                                    topic:function (i, ig, ign, igno, ignor, ignore, ignoree, ignoreee, ignoreeee, d) {
                                                        comb.executeInOrder(MYSQL_DB, d,
                                                            function (db, d) {
                                                                db.sqls = [];
                                                                d.insertIgnore().insert({name:"ghi"});
                                                                var sql = db.sqls.slice(0), all = d.all();
                                                                db.dropTable("items");
                                                                db.createTable("items", function () {
                                                                    this.name(String);
                                                                    this.value("integer");
                                                                });
                                                                return {sql:sql, all:all};
                                                            }).then(hitch(this, "callback", null), hitch(this, "callback"));
                                                    },

                                                    "should add the IGNORE keyword for single inserts":function (ret) {
                                                        assert.deepEqual(ret.all, [
                                                            {name:'ghi', value:null}
                                                        ]);
                                                        assert.deepEqual(ret.sql, ["INSERT IGNORE INTO items (name) VALUES ('ghi')"]);
                                                    },


                                                    "onDuplicateKeyUpdate ":{
                                                        topic:function (i, ig, ign, igno, ignor, ignore, ignoree, ignoreee, ignoreeee, ignoreeeee, d) {
                                                            comb.executeInOrder(MYSQL_DB, d,
                                                                function (db, d) {
                                                                    db.sqls = [];
                                                                    d.onDuplicateKeyUpdate("value").import(["name", "value"], [
                                                                        ['abc', 1],
                                                                        ['def', 2]
                                                                    ]);
                                                                    var sql = db.sqls.slice(0), all = d.all();
                                                                    db.dropTable("items");
                                                                    db.createTable("items", function () {
                                                                        this.name(String);
                                                                        this.value("integer");
                                                                    });
                                                                    return {sql:sql, all:all};
                                                                }).then(hitch(this, "callback", null), hitch(this, "callback"));
                                                        },

                                                        "should add the ON DUPLICATE KEY UPDATE and columns specified when args are given":function (ret) {
                                                            assert.deepEqual(ret.all, [
                                                                {name:'abc', value:1},
                                                                {name:'def', value:2}
                                                            ]);
                                                            assert.deepEqual(ret.sql, [
                                                                SQL_BEGIN,
                                                                "INSERT INTO items (name, value) VALUES ('abc', 1), ('def', 2) ON DUPLICATE KEY UPDATE value=VALUES(value)",
                                                                SQL_COMMIT]);
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                }
            }
        }

    });

    suite.addBatch({
        "Mysql.Dataset.replace":{
            topic:function () {
                comb.executeInOrder(MYSQL_DB,
                    function (db) {
                        db.dropTable("items");
                        db.createTable("items", function () {
                            this.id("integer", {unique:true});
                            this.value("integer");
                        });
                        db.sqls = [];
                    }).then(hitch(this, "callback", null, MYSQL_DB.from("items")), hitch(this, "callback"));

            },

            "replace ":{
                topic:function (d) {
                    comb.executeInOrder(MYSQL_DB, d,
                        function (db, d) {
                            db.sqls = [];
                            db.alterTable("items", function () {
                                this.setColumnDefault("id", 1);
                                this.setColumnDefault("value", 2);
                            });
                            var ret = [];
                            d.replace();
                            ret.push(d.all());
                            d.replace([]);
                            ret.push(d.all());
                            d.replace({});
                            ret.push(d.all());
                            db.dropTable("items");
                            db.createTable("items", function () {
                                this.id("integer", {unique:true});
                                this.value("integer");
                            });
                            return ret;
                        }).then(hitch(this, "callback", null), hitch(this, "callback"));
                },

                "should use default values if they exist":function (ret) {
                    assert.deepEqual(ret, [
                        [
                            {id:1, value:2}
                        ],
                        [
                            {id:1, value:2}
                        ],
                        [
                            {id:1, value:2}
                        ]
                    ]);
                },

                "replace ":{
                    topic:function (i, d) {
                        comb.executeInOrder(MYSQL_DB, d,
                            function (db, d) {
                                db.sqls = [];
                                var ret = [];
                                d.replace([1, 2]);
                                ret.push(d.all());
                                d.replace(1, 2);
                                ret.push(d.all());
                                d.replace(d);
                                ret.push(d.all());
                                db.dropTable("items");
                                db.createTable("items", function () {
                                    this.id("integer", {unique:true});
                                    this.value("integer");
                                });
                                return ret;
                            }).then(hitch(this, "callback", null), hitch(this, "callback"));
                    },

                    "should use default values if they exist":function (ret) {
                        assert.deepEqual(ret, [
                            [
                                {id:1, value:2}
                            ],
                            [
                                {id:1, value:2}
                            ],
                            [
                                {id:1, value:2}
                            ]
                        ]);
                    },

                    "replace ":{
                        topic:function (i, ig, d) {
                            comb.executeInOrder(MYSQL_DB, d,
                                function (db, d) {
                                    db.sqls = [];
                                    d.replace({id:111, value:333});
                                    var ret = d.all();
                                    db.dropTable("items");
                                    db.createTable("items", function () {
                                        this.id("integer", {unique:true});
                                        this.value("integer");
                                    });
                                    return ret;
                                }).then(hitch(this, "callback", null), hitch(this, "callback"));
                        },

                        "should create a record if the condition is not met":function (ret) {
                            assert.deepEqual(ret, [
                                {id:111, value:333}
                            ]);
                        },

                        "replace ":{
                            topic:function (i, ig, ign, d) {
                                comb.executeInOrder(MYSQL_DB, d,
                                    function (db, d) {
                                        db.sqls = [];
                                        var ret = [];
                                        d.replace({id:111, value:null});
                                        ret.push(d.all());
                                        d.replace({id:111, value:333});
                                        ret.push(d.all());
                                        db.dropTable("items");
                                        db.createTable("items", function () {
                                            this.id("integer", {unique:true});
                                            this.value("integer");
                                        });
                                        return ret;
                                    }).then(hitch(this, "callback", null), hitch(this, "callback"));
                            },

                            "should update a record if the condition is met":function (ret) {
                                assert.deepEqual(ret, [
                                    [
                                        {id:111, value:null}
                                    ],
                                    [
                                        {id:111, value:333}
                                    ]
                                ]);
                            }
                        }
                    }
                }
            }
        }
    });

    suite.addBatch({

        "mysql.Dataset complexExpressionSql":{
            topic:MYSQL_DB.dataset,

            "should handle pattern matches correctly":function (d) {
                assert.equal(d.literal(sql.identifier("x").like('a')), "(x LIKE BINARY 'a')");
                assert.equal(d.literal(sql.identifier("x").like("a").not()), "(x NOT LIKE BINARY 'a')");
                assert.equal(d.literal(sql.identifier("x").ilike('a')), "(x LIKE 'a')");
                assert.equal(d.literal(sql.identifier("x").ilike('a').not()), "(x NOT LIKE 'a')");
                assert.equal(d.literal(sql.identifier("x").like(/a/)), "(x REGEXP BINARY 'a')");
                assert.equal(d.literal(sql.identifier("x").like(/a/).not()), "(x NOT REGEXP BINARY 'a')");
                assert.equal(d.literal(sql.identifier("x").like(/a/i)), "(x REGEXP 'a')");
                assert.equal(d.literal(sql.identifier("x").like(/a/i).not()), "(x NOT REGEXP 'a')");
            },

            "should handle string concatenation with CONCAT if more than one record":function (d) {
                assert.equal(d.literal(sql.sqlStringJoin(["x", "y"])), "CONCAT(x, y)");
                assert.equal(d.literal(sql.sqlStringJoin(["x", "y"], ' ')), "CONCAT(x, ' ', y)");
                assert.equal(d.literal(sql.sqlStringJoin([sql.x("y"), 1, sql.literal('z')], sql.y.sqlSubscript(1))), "CONCAT(x(y), y[1], '1', y[1], z)");
            },

            "should handle string concatenation as simple string if just one record":function (d) {
                assert.equal(d.literal(sql.sqlStringJoin(["x"])), "x");
                assert.equal(d.literal(sql.sqlStringJoin(["x"], ' ')), "x");
            }
        }
    });
    suite.addBatch({
        "MySQL bad date/time conversions":{


            "should raise an exception when a bad date/time is used and convertInvalidDateTime is false":{
                topic:function () {
                    patio.mysql.convertInvalidDateTime = false;
                    var ret = new comb.Promise();
                    MYSQL_DB.fetch("SELECT CAST('0000-00-00' AS date)").singleValue().then(function (res) {
                        ret.errback("err");
                    }, function () {
                        MYSQL_DB.fetch("SELECT CAST('0000-00-00 00:00:00' AS datetime)").singleValue().then(function () {
                            ret.errback("err")
                        }, function () {
                            MYSQL_DB.fetch("SELECT CAST('25:00:00' AS time)").singleValue().then(function () {
                                ret.errback("err")
                            }, function (err) {
                                patio.mysql.convertInvalidDateTime = false;
                                ret.callback();
                            });
                        });
                    });
                    ret.then(hitch(this, "callback", null, true), hitch(this, "callback"));
                },

                "should not raise an error":function (res) {
                    assert.isTrue(res);
                },

                "should not use a null value bad date/time is used and convertInvalidDateTime is null":{
                    topic:function () {
                        comb.executeInOrder(MYSQL_DB, patio,
                            function (db, patio) {
                                patio.mysql.convertInvalidDateTime = null;
                                var ret = [];
                                ret.push(db.fetch("SELECT CAST('0000-00-00' AS date)").singleValue());
                                ret.push(db.fetch("SELECT CAST('0000-00-00 00:00:00' AS datetime)").singleValue());
                                ret.push(db.fetch("SELECT CAST('25:00:00' AS time)").singleValue());
                                patio.mysql.convertInvalidDateTime = false;
                                return ret;
                            }).then(hitch(this, "callback", null), hitch(this, "callback"));
                    },

                    "should return all null":function (res) {
                        assert.deepEqual(res, [null, null, null]);
                    },

                    "should not use a null value bad date/time is used and convertInvalidDateTime is string || String":{
                        topic:function () {
                            comb.executeInOrder(MYSQL_DB, patio,
                                function (db, patio) {
                                    patio.mysql.convertInvalidDateTime = String;
                                    var ret = [];
                                    ret.push(db.fetch("SELECT CAST('0000-00-00' AS date)").singleValue());
                                    ret.push(db.fetch("SELECT CAST('0000-00-00 00:00:00' AS datetime)").singleValue());
                                    ret.push(db.fetch("SELECT CAST('25:00:00' AS time)").singleValue());
                                    patio.mysql.convertInvalidDateTime = "string";
                                    ret.push(db.fetch("SELECT CAST('0000-00-00' AS date)").singleValue());
                                    ret.push(db.fetch("SELECT CAST('0000-00-00 00:00:00' AS datetime)").singleValue());
                                    ret.push(db.fetch("SELECT CAST('25:00:00' AS time)").singleValue());
                                    patio.mysql.convertInvalidDateTime = "String";
                                    ret.push(db.fetch("SELECT CAST('0000-00-00' AS date)").singleValue());
                                    ret.push(db.fetch("SELECT CAST('0000-00-00 00:00:00' AS datetime)").singleValue());
                                    ret.push(db.fetch("SELECT CAST('25:00:00' AS time)").singleValue());
                                    patio.mysql.convertInvalidDateTime = false;
                                    return ret;
                                }).then(hitch(this, "callback", null), hitch(this, "callback"));
                        },

                        "should return the date strings":function (res) {
                            assert.deepEqual(res, ['0000-00-00', '0000-00-00 00:00:00', '25:00:00', '0000-00-00', '0000-00-00 00:00:00', '25:00:00', '0000-00-00', '0000-00-00 00:00:00', '25:00:00']);
                        }
                    }
                }
            }



        }
    });


    suite.run({reporter:require("vows").reporter.spec}, function () {
        patio.disconnect().both(hitch(ret, "callback"));
    });
});

