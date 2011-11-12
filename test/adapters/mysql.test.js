var vows = require('vows'),
    assert = require('assert'),
    moose = require("../../lib"),
    sql = moose.SQL,
    comb = require("comb"),
    format = comb.string.format,
    hitch = comb.hitch;

moose.quoteIdentifiers = false;

new comb.logging.BasicConfigurator().configure();
var ret = (module.exports = exports = new comb.Promise());
var suite = vows.describe("Database");

var MYSQL_USER = "root";

var MYSQL_URL = format("mysql://%s@localhost:3306/sandbox", MYSQL_USER);
var MYSQL_DB = moose.connect(MYSQL_URL);


var INTEGRATION_DB = MYSQL_DB;
var p1 = new comb.Promise();
MYSQL_DB.forceCreateTable("test2",
    function() {
        this.text("name");
        this.integer("value");
    }).chainBoth(hitch(MYSQL_DB, "dropTable", "items")).chainBoth(hitch(MYSQL_DB, "dropTable", "dolls")).chainBoth(hitch(MYSQL_DB, "dropTable", "booltest")).both(hitch(p1, "callback"));

MYSQL_DB.__defineGetter__("sqls", function() {
    return (this.__sqls ? this.__sqls : (this.__sqls = []));
});

var SQL_BEGIN = 'BEGIN';
var SQL_ROLLBACK = 'ROLLBACK';
var SQL_COMMIT = 'COMMIT';
var orig = console.log;

console.log = function(m) {
    if (comb.isString(m)) {
        var parts = m.split(";");
        if (parts.length == 2) {
            MYSQL_DB.sqls.push(parts[1].trim());
        }
    }
    orig.apply(console, arguments);
};

p1.both(function() {
    suite.addBatch({

        "createTable" : {
            topic : function() {
                MYSQL_DB.sqls.length = 0;
                return MYSQL_DB;
            },

            "should allow to specify options for mysql" : {
                topic : function() {
                    MYSQL_DB.sqls.length = 0;
                    MYSQL_DB.createTable("dolls", {engine : "MyISAM", charset : "latin2"},
                        function() {
                            this.text("name");
                        }).then(hitch(this, "callback", null, MYSQL_DB), hitch(this, "callback"));
                },

                "db sql should equal CREATE TABLE dolls (name text) ENGINE=MyISAM DEFAULT CHARSET=latin2" : function(db) {
                    assert.deepEqual(db.sqls, ["CREATE TABLE dolls (name text) ENGINE=MyISAM DEFAULT CHARSET=latin2"]);
                },

                "should create a temporary table" : {
                    topic : function() {
                        comb.executeInOrder(MYSQL_DB,
                            function(db) {
                                db.dropTable("dolls");
                                db.sqls.length = 0;
                                db.createTable("tmp_dolls", {temp : true, engine : "MyISAM", charset : "latin2"},
                                    function() {
                                        this.text("name");
                                    });
                            }).then(hitch(this, "callback", null, MYSQL_DB), hitch(this, "callback"));
                    },

                    "db sql should equal CREATE TABLE dolls (name text) ENGINE=MyISAM DEFAULT CHARSET=latin2" : function(db) {
                        assert.deepEqual(db.sqls, ["CREATE TEMPORARY TABLE tmp_dolls (name text) ENGINE=MyISAM DEFAULT CHARSET=latin2"]);
                    },

                    "should not use default for string {text : true}" : {
                        topic : function() {
                            MYSQL_DB.sqls.length = 0;
                            MYSQL_DB.createTable("dolls",
                                function() {
                                    this.string("name", {text : true, "default" : "blah"});
                                }).then(hitch(this, "callback", null, MYSQL_DB), hitch(this, "callback"));

                        },

                        "db sql should equal 'CREATE TABLE dolls (name text)'" : function(db) {
                            assert.deepEqual(db.sqls, ["CREATE TABLE dolls (name text)"]);
                        },

                        "should not create the autoIcrement attribute if it is specified" : {
                            topic : function() {
                                MYSQL_DB.sqls.length = 0;
                                comb.executeInOrder(MYSQL_DB,
                                    function(db) {
                                        db.dropTable("dolls");
                                        db.createTable("dolls", function() {
                                            this.integer("n2");
                                            this.string("n3");
                                            this.integer("n4", {autoIncrement : true, unique : true});
                                        });
                                        return db.schema("dolls");
                                    }).then(hitch(this, "callback", null), hitch(this, "callback"));

                            },

                            "db sql should equal 'CREATE TABLE dolls (name text)'" : function(schema) {
                                assert.deepEqual([false, false, true], Object.keys(schema).map(function(k) {
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
        "A MySQL database" : {
            topic : MYSQL_DB,

            "should provide the server version " : {
                topic : function(db) {
                    db.serverVersion().then(hitch(this, "callback", null), hitch(this, "callback"));
                },

                "it should be greater than 40000" : function(version) {
                    assert.isTrue(version >= 40000);
                }
            },

            "should handle the creation and dropping of an InnoDB table with foreign keys" : {
                topic : function(db) {
                    db.forceCreateTable("test_innodb", {engine: "InnoDB"},
                        function() {
                            this.primaryKey("id");
                            this.foreignKey("fk", "test_innodb", {key : "id"});
                        }).then(hitch(this, "callback", null), hitch(this, "callback", "ERROR"));
                },

                "should not throw an error" : function(res) {
                    assert.notEqual(res, "ERROR");
                }
            },

            "should support forShare" : {
                topic : function(db) {
                    var cb = hitch(this, "callback", null), eb = hitch(this, "callback");
                    db.transaction(function() {
                        db.from("test2").forShare().all().then(cb, eb);
                    });
                },

                "should be empty " : function(res) {
                    assert.length(res, 0);
                }
            }
        },

        "MySQL convertTinyIntToBool" : {
            topic : function() {
                MYSQL_DB.createTable("booltest",
                    function() {
                        this.column("b", "tinyint(1)");
                        this.column("i", "tinyint(4)");
                    }).then(hitch(this, function() {
                    this.callback(null, {db : MYSQL_DB, ds : MYSQL_DB.from("booltest")});
                }), hitch(this, "callback"));
            },

            "should consider tinyint(1) datatypes as boolean if set, but not larger tinyints" : {

                topic : function(topic) {
                    var db = topic.db;
                    db.schema("booltest", {reload : true}).then(hitch(this, "callback", null), hitch(this, "callback"));
                },

                "schema should have boolean for type" : function(schema) {
                    assert.deepEqual(schema, {
                        b : {type : "boolean", autoIncrement : false, allowNull : true, primaryKey : false, "default" : null, jsDefault : null, dbType : "tinyint(1)"},
                        i : {type : "integer", autoIncrement : false, allowNull : true, primaryKey : false, "default" : null, jsDefault : null, dbType : "tinyint(4)"}
                    })
                },

                "should return tinyint(1)s as boolean values and tinyint(4) as integers " : {
                    topic : function() {
                        var resultSets = [];
                        var ds = MYSQL_DB.from("booltest");
                        comb.executeInOrder(ds,
                            function(ds) {
                                var results = [];
                                ds.delete();
                                ds.insert({b : true, i : 10});
                                results.push(ds.all());
                                ds.delete();
                                ds.insert({b : false, i : 10});
                                results.push(ds.all());
                                ds.delete();
                                ds.insert({b : true, i : 1});
                                results.push(ds.all());
                                return results;
                            }).then(hitch(this, this.callback, null), hitch(this, "callback"));
                    },

                    "the result sets should cast properly" : function(resultSet) {
                        assert.deepEqual(resultSet, [
                            [
                                {b : true, i : 10}
                            ],
                            [
                                {b : false, i : 10}
                            ],
                            [
                                {b : true, i : 1}
                            ]
                        ]);
                    },


                    "should not consider tinyint(1) a boolean if convertTinyintToBool is false" : {
                        topic : function() {
                            moose.mysql.convertTinyintToBool = false;
                            MYSQL_DB.schema("booltest", {reload : true}).then(hitch(this, "callback", null), hitch(this, "callback"));
                        },

                        "schema should have boolean for type" : function(schema) {
                            assert.deepEqual(schema, {
                                b : {type : "integer", autoIncrement : false, allowNull : true, primaryKey : false, "default" : null, jsDefault : null, dbType : "tinyint(1)"},
                                i : {type : "integer", autoIncrement : false, allowNull : true, primaryKey : false, "default" : null, jsDefault : null, dbType : "tinyint(4)"}
                            });
                        },

                        "should return tinyint(1)s as integers values and tinyint(4) as integers " : {
                            topic : function() {
                                var resultSets = [];
                                var ds = MYSQL_DB.from("booltest");
                                comb.executeInOrder(ds,
                                    function(ds) {
                                        var results = [];
                                        ds.delete();
                                        ds.insert({b : true, i : 10});
                                        results.push(ds.all());
                                        ds.delete();
                                        ds.insert({b : false, i : 10});
                                        results.push(ds.all());
                                        ds.delete();
                                        ds.insert({b : true, i : 1});
                                        results.push(ds.all());
                                        return results;
                                    }).then(hitch(this, this.callback, null), hitch(this, "callback"));
                            },

                            "the result sets should cast properly" : function(resultSet) {
                                assert.deepEqual(resultSet, [
                                    [
                                        {b : 1, i : 10}
                                    ],
                                    [
                                        {b : 0, i : 10}
                                    ],
                                    [
                                        {b : 1, i : 1}
                                    ]
                                ]);
                                moose.mysql.convertTinyintToBool = true;
                            }
                        }
                    }
                }
            }
        },

        "A MySQL dataset" : {
            topic : function() {
                MYSQL_DB.createTable("items",
                    function() {
                        this.string("name");
                        this.integer("value");
                    }).then(hitch(this, function() {
                    MYSQL_DB.sqls.length = 0;
                    this.callback(null, MYSQL_DB.from("items"));
                }));
            },
            "should quote columns and tables using back-ticks if quoting identifiers" : function(d) {
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

                assert.equal(d.insertSql({value : 333}), 'INSERT INTO `items` (`value`) VALUES (333)');

                assert.equal(d.insertSql({x : sql.y}), 'INSERT INTO `items` (`x`) VALUES (`y`)');
            },

            "should quote fields correctly when reversing the order" : function(d) {
                d.quoteIdentifiers = true;
                assert.equal(d.reverseOrder("name").sql, 'SELECT * FROM `items` ORDER BY `name` DESC');
                assert.equal(d.reverseOrder(sql.name.desc()).sql, 'SELECT * FROM `items` ORDER BY `name` ASC');
                assert.equal(d.reverseOrder("name", sql.test.desc()).sql, 'SELECT * FROM `items` ORDER BY `name` DESC, `test` ASC');
                assert.equal(d.reverseOrder(sql.name.desc(), "test").sql, 'SELECT * FROM `items` ORDER BY `name` ASC, `test` DESC');
            },

            "should support ORDER clause in UPDATE statements" : function(d) {
                d.quoteIdentifiers = false;
                assert.equal(d.order("name").updateSql({value : 1}), 'UPDATE items SET value = 1 ORDER BY name');
            },

            "should support LIMIT clause in UPDATE statements" : function(d) {
                d.quoteIdentifiers = false;
                assert.equal(d.limit(10).updateSql({value : 1}), 'UPDATE items SET value = 1 LIMIT 10');
            },

            "should support regexes" : {
                topic : function(d) {
                    comb.executeInOrder(d,
                        function(ds) {
                            ds.insert({name : "abc", value : 1});
                            ds.insert({name : "bcd", value : 1});
                            return [ds.filter({name : /bc/}).count(), ds.filter({name : /^bc/}).count()];
                        }).then(hitch(this, "callback", null), hitch(this, "callback"));

                },

                "should equal [2,1]" : function(res) {
                    assert.deepEqual(res, [2,1]);
                },

                "should correctly literalize strings with comment backslashes in them"  : {
                    topic : function(ig, d) {
                        comb.executeInOrder(d,
                            function(d) {
                                d.delete();
                                d.insert({name : ":\\"});
                                return d.first().name;
                            }).then(hitch(this, "callback", null), hitch(this, "callback"));
                    },

                    "name should equal ':\\'" : function(name) {
                        assert.equal(name, ":\\")
                    }


                }

            }
        }

    });


    suite.addBatch({
        "MySQL datasets" : {
            topic : MYSQL_DB.from("orders"),

            "should correctly quote column references" : function(d) {
                d.quoteIdentifiers = true;
                var market = 'ICE';
                var ackStamp = new Date() - 15 * 60; // 15 minutes ago
                assert.equal(d.select("market", sql.minute(sql.from_unixtime("ack")).as("minute")).where(
                    function(o) {
                        return this.ack.sqlNumber.gt(ackStamp).and({market : market})
                    }).groupBy(sql.minute(sql.from_unixtime("ack"))).sql, "SELECT `market`, minute(from_unixtime(`ack`)) AS `minute` FROM `orders` WHERE ((`ack` > " + d.literal(ackStamp) + ") AND (`market` = 'ICE')) GROUP BY minute(from_unixtime(`ack`))");
            }
        },

        "Dataset.distinct" : {
            topic : function() {
                var db = MYSQL_DB, ds = db.from("a");
                comb.executeInOrder(db, ds,
                    function(db, ds) {
                        db.forceCreateTable("a", function() {
                            this.integer("a");
                            this.integer("b");
                        });
                        ds.insert(20, 10);
                        ds.insert(30, 10);
                        var ret = [ds.order("b", "a").distinct().map("a"), ds.order("b", sql.a.desc()).distinct().map("a")];
                        db.dropTable("a");
                        return ret;

                    }).then(hitch(this, "callback", null), hitch(this, "callback"));
            },

            "should equal [20,30], [30,20]" : function(res) {
                assert.deepEqual(res[0], [20,30]);
                assert.deepEqual(res[1], [30,20]);
            }
        },

        "MySQL join expressions" : {
            topic : MYSQL_DB.from("nodes"),

            "should raise error for :full_outer join requests."  : function(ds) {
                assert.throws(hitch(ds, "joinTable", "fullOuter", "nodes"));
            },
            "should support natural left joins"  : function(ds) {
                assert.equal(ds.joinTable("naturalLeft", "nodes").sql,
                    'SELECT * FROM nodes NATURAL LEFT JOIN nodes');
            },
            "should support natural right joins"  : function(ds) {
                assert.equal(ds.joinTable("naturalRight", "nodes").sql,
                    'SELECT * FROM nodes NATURAL RIGHT JOIN nodes');
            },
            "should support natural left outer joins"  : function(ds) {
                assert.equal(ds.joinTable("naturalLeftOuter", "nodes").sql,
                    'SELECT * FROM nodes NATURAL LEFT OUTER JOIN nodes');
            },
            "should support natural right outer joins"  : function(ds) {
                assert.equal(ds.joinTable("naturalRightOuter", "nodes").sql,
                    'SELECT * FROM nodes NATURAL RIGHT OUTER JOIN nodes');
            },
            "should support natural inner joins"  : function(ds) {
                assert.equal(ds.joinTable("naturalInner", "nodes").sql,
                    'SELECT * FROM nodes NATURAL LEFT JOIN nodes');
            },
            "should support cross joins"  : function(ds) {
                assert.equal(ds.joinTable("cross", "nodes").sql, 'SELECT * FROM nodes CROSS JOIN nodes');
            },
            "should support cross joins as inner joins if conditions are used"  : function(ds) {
                assert.equal(ds.joinTable("cross", "nodes", {id : "id"}).sql,
                    'SELECT * FROM nodes INNER JOIN nodes ON (nodes.id = nodes.id)');
            },
            "should support straight joins (force left table to be read before right)"  : function(ds) {
                assert.equal(ds.joinTable("straight", "nodes").sql,
                    'SELECT * FROM nodes STRAIGHT_JOIN nodes');
            },
            "should support natural joins on multiple tables."  : function(ds) {
                assert.equal(ds.joinTable("naturalLeftOuter", ["nodes", "branches"]).sql,
                    'SELECT * FROM nodes NATURAL LEFT OUTER JOIN (nodes, branches)');
            },
            "should support straight joins on multiple tables."  : function(ds) {
                assert.equal(ds.joinTable("straight", ["nodes","branches"]).sql,
                    'SELECT * FROM nodes STRAIGHT_JOIN (nodes, branches)');
            },

            "should quote fields correctly"  : function(ds) {
                ds.quoteIdentifiers = true;
                assert.equal(ds.join("attributes", {nodeId  : "id"}).sql, "SELECT * FROM `nodes` INNER JOIN `attributes` ON (`attributes`.`nodeId` = `nodes`.`id`)");
            },

            "should allow a having clause on ungrouped datasets"  : function(ds) {
                ds.quoteIdentifiers = false;
                assert.doesNotThrow(hitch(ds, "having", "blah"));
                assert.equal(ds.having('blah').sql, "SELECT * FROM nodes HAVING (blah)");
            },

            "should put a having clause before an order by clause"  : function(ds) {
                ds.quoteIdentifiers = false;
                assert.equal(ds.order("aaa").having({bbb : sql.identifier("ccc")}).sql, "SELECT * FROM nodes HAVING (bbb = ccc) ORDER BY aaa");
            }
        },

        "A MySQL database" : {
            topic : MYSQL_DB,

            "should support addColumn operations"  : {
                topic: function(db) {
                    comb.executeInOrder(db,
                        function(db) {
                            db.addColumn("test2", "xyz", "text");
                            var ds = db.from("test2");
                            var columns = ds.columns;
                            ds.insert({name : "mmm", value : "111", xyz : '000'});
                            return {columns : columns, first : ds.first().xyz};
                        }).then(hitch(this, "callback", null), hitch(this, "callback"));
                },

                "it should equal ['name', 'value', 'xyz'] and '000'" : function(res) {
                    assert.deepEqual(res.columns, ["name", "value", "xyz"]);
                    assert.equal(res.first, "000");
                },

                "should support dropColumn operations"  : {
                    topic: function(ig, db) {
                        comb.executeInOrder(db,
                            function(db) {
                                db.dropColumn("test2", "xyz", "text");
                                return {columns : db.from("test2").columns};
                            }).then(hitch(this, "callback", null), hitch(this, "callback"));
                    },

                    "it should equal ['name', 'value'']" : function(res) {
                        assert.deepEqual(res.columns, ["name", "value"]);
                    },

                    "should support renameColumn operations"  : {
                        topic: function(ig, igg, db) {
                            comb.executeInOrder(db,
                                function(db) {
                                    db.from("test2").delete();
                                    db.addColumn("test2", "xyz", "text");
                                    db.from("test2").insert({name : "mmm", value : 111, xyz : "gggg"});
                                    var col1 = db.from("test2").columns;
                                    db.renameColumn("test2", "xyz", "zyx", {type : "text"});
                                    var col2 = db.from("test2").columns;
                                    return {col1 : col1, col2 : col2, first : db.from("test2").first().zyx};
                                }).then(hitch(this, "callback", null), hitch(this, "callback"));
                        },

                        "it should equal ['name', 'value'']" : function(res) {
                            assert.deepEqual(res.col1, ["name", "value", "xyz"]);
                            assert.deepEqual(res.col2, ["name", "value", "zyx"]);
                            assert.equal(res.first, "gggg");
                        },

                        "should support renameColumn operations with types like varchar"  : {
                            topic: function(ig, igg, iggg, db) {
                                comb.executeInOrder(db,
                                    function(db) {
                                        db.from("test2").delete();
                                        db.addColumn("test2", "tre", "text");
                                        db.from("test2").insert({name : "mmm", value : 111, tre : "gggg"});
                                        var col1 = db.from("test2").columns;
                                        db.renameColumn("test2", "tre", "ert", {type : "varchar", size : 255});
                                        var col2 = db.from("test2").columns;
                                        return {col1 : col1, col2 : col2, first : db.from("test2").first().ert};
                                    }).then(hitch(this, "callback", null), hitch(this, "callback"));
                            },

                            "it should equal ['name', 'value'']" : function(res) {
                                assert.deepEqual(res.col1, ["name", "value", "zyx", "tre"]);
                                assert.deepEqual(res.col2, ["name", "value", "zyx", "ert"]);
                                assert.equal(res.first, "gggg");
                            },

                            "should support setColumntype operation"  : {
                                topic: function(ig, igg, iggg, igggg, db) {
                                    comb.executeInOrder(db,
                                        function(db) {
                                            db.from("test2").delete();
                                            db.addColumn("test2", "xyz", "float");
                                            db.from("test2").insert({name : "mmm", value : 111, xyz : 56.78});
                                            var before = db.from("test2").first().xyz;
                                            db.setColumnType("test2", "xyz", "integer");
                                            var after = db.from("test2").first().xyz;
                                            return {before : before, after : after};
                                        }).then(hitch(this, "callback", null), hitch(this, "callback"));
                                },

                                "it should equal ['name', 'value'']" : function(res) {
                                    assert.equal(res.before, 56.78);
                                    assert.equal(res.after, 57);
                                },

                                "should support addIndex operation"  : {
                                    topic: function(ig, ign, igno, ignor, ignore, db) {
                                        comb.executeInOrder(db,
                                            function(db) {
                                                db.from("test2").delete();
                                                var emptyIndexes = db.indexes("test2");
                                                db.addIndex("test2", "value");
                                                var indexes = db.indexes("test2");
                                                return {indexes : indexes, emptyIndexes : emptyIndexes};
                                            }).then(hitch(this, "callback", null), hitch(this, "callback"));
                                    },

                                    "it should equal ['name', 'value'']" : function(res) {
                                        assert.isNotNull(res.indexes.test2_value_index);
                                        assert.isTrue(comb.isEmpty(res.emptyIndexes));
                                    },

                                    "should support addForeignKey"  : {
                                        topic: function(ig, ign, igno, ignor, ignore, ignoree, db) {
                                            comb.executeInOrder(db,
                                                function(db) {
                                                    db.from("test2").delete();
                                                    db.alterTable("test2", function() {
                                                        this.addForeignKey("value2", "test2", {key : "value"});
                                                    });
                                                    return db.from("test2").columns;
                                                }).then(hitch(this, "callback", null), hitch(this, "callback"));
                                        },

                                        "it should equal ['name', 'value'']" : function(columns) {
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
        "A MySQL database with table options" : {
            topic : function() {
                moose.mysql.defaultEngine = 'InnoDB';
                moose.mysql.defaultCharset = 'utf8';
                moose.mysql.defaultCollate = 'utf8_general_ci';
                var db = MYSQL_DB;
                comb.executeInOrder(db,
                    function(db) {
                        db.dropTable("items");
                        db.sqls.length = 0;
                    }).then(hitch(this, "callback", null, MYSQL_DB), hitch(this, "callback"));
            },

            "should allow to pass custom options (engine, charset, collate) for table creation"  : {
                topic : function(db) {
                    comb.executeInOrder(db,
                        function(db) {
                            db.sqls.length = 0;
                            db.createTable("items", {engine : 'MyISAM', charset : 'latin1', collate : 'latin1_swedish_ci'}, function() {
                                this.integer("size");
                                this.text("name");
                            });
                            var sqls = db.sqls.slice(0);
                            db.dropTable("items");
                            return sqls;
                        }).then(hitch(this, "callback", null), hitch(this, "callback"));
                },
                'sqls should equal ["CREATE TABLE items (size integer, name text) ENGINE=MyISAM DEFAULT CHARSET=latin1 DEFAULT COLLATE=latin1_swedish_ci"]' : function(sqls) {
                    assert.deepEqual(sqls, ["CREATE TABLE items (size integer, name text) ENGINE=MyISAM DEFAULT CHARSET=latin1 DEFAULT COLLATE=latin1_swedish_ci"]);
                },

                "should use default options (engine, charset, collate) for table creation"  : {
                    topic : function(ig, db) {
                        comb.executeInOrder(db,
                            function(db) {
                                db.sqls.length = 0;
                                db.createTable("items", function() {
                                    this.integer("size");
                                    this.text("name");
                                });
                                var sqls = db.sqls.slice(0);
                                db.dropTable("items");
                                return sqls;
                            }).then(hitch(this, "callback", null), hitch(this, "callback"));
                    },
                    'sqls should equal ["CREATE TABLE items (size integer, name text) ENGINE=InnoDB DEFAULT CHARSET=utf8 DEFAULT COLLATE=utf8_general_ci"]' : function(sqls) {
                        assert.deepEqual(sqls, ["CREATE TABLE items (size integer, name text) ENGINE=InnoDB DEFAULT CHARSET=utf8 DEFAULT COLLATE=utf8_general_ci"]);
                    },


                    "should not use default options (engine, charset, collate) for table creation" :{
                        topic : function(id, igg, db) {
                            comb.executeInOrder(db,
                                function(db) {
                                    db.sqls.length = 0;
                                    db.createTable("items", {engine : null, charset : null, collate : null}, function() {
                                        this.integer("size");
                                        this.text("name");
                                    });
                                    var sqls = db.sqls.slice(0);
                                    db.dropTable("items");
                                    return sqls;
                                }).then(hitch(this, "callback", null), hitch(this, "callback"));
                        },

                        'sqls should equal["CREATE TABLE items (size integer, name text)"]':function(sqls) {
                            assert.deepEqual(sqls, ["CREATE TABLE items (size integer, name text)"]);
                            moose.mysql.defaultEngine = null;
                            moose.mysql.defaultCharset = null;
                            moose.mysql.defaultCollate = null;
                        }
                    }
                }
            }
        }
    });

    suite.addBatch({
        "A MySQL database" : {
            topic : function() {
                MYSQL_DB.dropTable("items").then(hitch(this, "callback", null, MYSQL_DB), hitch(this, "callback", null, MYSQL_DB));
                MYSQL_DB.sqls.clear
            },

            "should support defaults for boolean columns" :{
                topic : function(db) {
                    comb.executeInOrder(db,
                        function(db) {
                            db.sqls.length = 0;
                            db.createTable("items", function() {
                                this.boolean("active1", {"default" : true});
                                this.boolean("active2", {"default" : false});
                            });
                            var sqls = db.sqls.slice(0);
                            db.dropTable("items");
                            return sqls;
                        }).then(hitch(this, "callback", null), hitch(this, "callback"));
                },

                'sqls should equal["CREATE TABLE items (active1 tinyint(1) DEFAULT 1, active2 tinyint(1) DEFAULT 0)"]':function(sqls) {
                    assert.deepEqual(sqls, ["CREATE TABLE items (active1 tinyint(1) DEFAULT 1, active2 tinyint(1) DEFAULT 0)"]);
                },

                "should correctly format CREATE TABLE statements with foreign keys" :{
                    topic : function(ig, db) {
                        comb.executeInOrder(db,
                            function(db) {
                                db.sqls.length = 0;
                                db.createTable("items", function() {
                                    this.primaryKey("id");
                                    this.index("id");
                                    this.foreignKey("p_id", "items", {key : "id", "null" : false, onDelete : "cascade"});
                                });
                                var sqls = db.sqls.slice(0);
                                db.dropTable("items");
                                return sqls;
                            }).then(hitch(this, "callback", null), hitch(this, "callback"));
                    },

                    'sqls should equal["CREATE TABLE items (id integer, p_id integer NOT NULL, FOREIGN KEY (p_id) REFERENCES items(id) ON DELETE CASCADE)"]':function(sqls) {
                        assert.deepEqual(sqls, ["CREATE TABLE items (id integer, p_id integer NOT NULL, FOREIGN KEY (p_id) REFERENCES items(id) ON DELETE CASCADE)"]);
                    }
                }
            }
        }
//
//
//  "should correctly format CREATE TABLE statements with foreign keys"  : function(ds){
//    @db.create_table(:items){Integer :id; foreign_key :p_id, :items, :key => :id, :null => false, :on_delete => :cascade}
//    @db.sqls.should == ["CREATE TABLE items (id integer, p_id integer NOT NULL, FOREIGN KEY (p_id) REFERENCES items(id) ON DELETE CASCADE)"]
//  },
//
//  "should correctly format ALTER TABLE statements with foreign keys"  : function(ds){
//    @db.create_table(:items){Integer :id}
//    @db.alter_table(:items){add_foreign_key :p_id, :users, :key => :id, :null => false, :on_delete => :cascade}
//    @db.sqls.should == ["CREATE TABLE items (id integer)", "ALTER TABLE items ADD COLUMN p_id integer NOT NULL", "ALTER TABLE items ADD FOREIGN KEY (p_id) REFERENCES users(id) ON DELETE CASCADE"]
//  },
//
//  "should have rename_column support keep existing options"  : function(ds){
//    @db.create_table(:items){String :id, :null=>false, :default=>'blah'}
//    @db.alter_table(:items){rename_column :id, :nid}
//    @db.sqls.should == ["CREATE TABLE items (id varchar(255) NOT NULL DEFAULT 'blah')", "DESCRIBE items", "ALTER TABLE items CHANGE COLUMN id nid varchar(255) NOT NULL DEFAULT 'blah'"]
//    @db[:items].insert
//    @db[:items].all.should == [{:nid=>'blah'}]
//    proc{@db[:items].insert(:nid=>nil)}.should raise_error(Sequel::DatabaseError)
//  },
//
//  "should have set_column_type support keep existing options"  : function(ds){
//    @db.create_table(:items){Integer :id, :null=>false, :default=>5}
//    @db.alter_table(:items){set_column_type :id, Bignum}
//    @db.sqls.should == ["CREATE TABLE items (id integer NOT NULL DEFAULT 5)", "DESCRIBE items", "ALTER TABLE items CHANGE COLUMN id id bigint NOT NULL DEFAULT 5"]
//    @db[:items].insert
//    @db[:items].all.should == [{:id=>5}]
//    proc{@db[:items].insert(:id=>nil)}.should raise_error(Sequel::DatabaseError)
//    @db[:items].delete
//    @db[:items].insert(2**40)
//    @db[:items].all.should == [{:id=>2**40}]
//  },
//
//  "should have set_column_type pass through options"  : function(ds){
//    @db.create_table(:items){integer :id; enum :list, :elements=>%w[one]}
//    @db.alter_table(:items){set_column_type :id, :int, :unsigned=>true, :size=>8; set_column_type :list, :enum, :elements=>%w[two]}
//    @db.sqls.should == ["CREATE TABLE items (id integer, list enum('one'))", "DESCRIBE items", "ALTER TABLE items CHANGE COLUMN id id int(8) UNSIGNED NULL", "ALTER TABLE items CHANGE COLUMN list list enum('two') NULL"]
//  },
//
//  "should have set_column_default support keep existing options"  : function(ds){
//    @db.create_table(:items){Integer :id, :null=>false, :default=>5}
//    @db.alter_table(:items){set_column_default :id, 6}
//    @db.sqls.should == ["CREATE TABLE items (id integer NOT NULL DEFAULT 5)", "DESCRIBE items", "ALTER TABLE items CHANGE COLUMN id id int(11) NOT NULL DEFAULT 6"]
//    @db[:items].insert
//    @db[:items].all.should == [{:id=>6}]
//    proc{@db[:items].insert(:id=>nil)}.should raise_error(Sequel::DatabaseError)
//  },
//
//  "should have set_column_allow_null support keep existing options"  : function(ds){
//    @db.create_table(:items){Integer :id, :null=>false, :default=>5}
//    @db.alter_table(:items){set_column_allow_null :id, true}
//    @db.sqls.should == ["CREATE TABLE items (id integer NOT NULL DEFAULT 5)", "DESCRIBE items", "ALTER TABLE items CHANGE COLUMN id id int(11) NULL DEFAULT 5"]
//    @db[:items].insert
//    @db[:items].all.should == [{:id=>5}]
//    proc{@db[:items].insert(:id=>nil)}.should_not
//  },
//
//  "should accept repeated raw sql statements using Database#<<"  : function(ds){
//    @db.create_table(:items){String :name; Integer :value}
//    @db << 'DELETE FROM items'
//    @db[:items].count.should == 0
//
//    @db << "INSERT INTO items (name, value) VALUES ('tutu', 1234)"
//    @db[:items].first.should == {:name => 'tutu', :value => 1234}
//
//    @db << 'DELETE FROM items'
//    @db[:items].first.should == nil
//  },
//},
    });


    suite.run({reporter : require("vows").reporter.spec}, function() {
        MYSQL_DB.disconnect();
    });
});


//
//# Socket tests should only be run if the MySQL server is on localhost
//if %w'localhost 127.0.0.1 ::1'.include?(MYSQL_URI.host) and MYSQL_DB.adapter_scheme == :mysql
//  context "A MySQL database" do
//    "should accept a socket option"  : function(ds){
//      db = Sequel.mysql(MYSQL_DB.opts[:database], :host => 'localhost', :user => MYSQL_DB.opts[:user], :password => MYSQL_DB.opts[:password], :socket => MYSQL_SOCKET_FILE)
//      proc {db.test_connection}.should_not raise_error
//    },
//
//    "should accept a socket option without host option"  : function(ds){
//      db = Sequel.mysql(MYSQL_DB.opts[:database], :user => MYSQL_DB.opts[:user], :password => MYSQL_DB.opts[:password], :socket => MYSQL_SOCKET_FILE)
//      proc {db.test_connection}.should_not raise_error
//    },
//
//    "should fail to connect with invalid socket"  : function(ds){
//      db = Sequel.mysql(MYSQL_DB.opts[:database], :user => MYSQL_DB.opts[:user], :password => MYSQL_DB.opts[:password], :socket =>'blah')
//      proc {db.test_connection}.should raise_error
//    },
//  },
//},
//
//context "A grouped MySQL dataset" do
//  before do
//    MYSQL_DB[:test2].delete
//    MYSQL_DB[:test2] << {:name => '11', :value => 10}
//    MYSQL_DB[:test2] << {:name => '11', :value => 20}
//    MYSQL_DB[:test2] << {:name => '11', :value => 30}
//    MYSQL_DB[:test2] << {:name => '12', :value => 10}
//    MYSQL_DB[:test2] << {:name => '12', :value => 20}
//    MYSQL_DB[:test2] << {:name => '13', :value => 10}
//  },
//
//  "should return the correct count for raw sql query"  : function(ds){
//    ds = MYSQL_DB["select name FROM test2 WHERE name = '11' GROUP BY name"]
//    ds.count.should == 1
//  },
//
//  "should return the correct count for a normal dataset"  : function(ds){
//    ds = MYSQL_DB[:test2].select(:name).where(:name => '11').group(:name)
//    ds.count.should == 1
//  },
//},
//
//context "A MySQL database" do
//  before do
//    @db = MYSQL_DB
//    @db.drop_table(:posts) rescue nil
//    @db.sqls.clear
//  },
//  after do
//    @db.drop_table(:posts) rescue nil
//  },
//
//  "should support fulltext indexes and full_text_search"  : function(ds){
//    @db.create_table(:posts){text :title; text :body; full_text_index :title; full_text_index [:title, :body]}
//    @db.sqls.should == [
//      "CREATE TABLE posts (title text, body text)",
//      "CREATE FULLTEXT INDEX posts_title_index ON posts (title)",
//      "CREATE FULLTEXT INDEX posts_title_body_index ON posts (title, body)"
//    ]
//
//    @db[:posts].insert(:title=>'ruby rails', :body=>'y')
//    @db[:posts].insert(:title=>'sequel', :body=>'ruby')
//    @db[:posts].insert(:title=>'ruby scooby', :body=>'x')
//    @db.sqls.clear
//
//    @db[:posts].full_text_search(:title, 'rails').all.should == [{:title=>'ruby rails', :body=>'y'}]
//    @db[:posts].full_text_search([:title, :body], ['sequel', 'ruby']).all.should == [{:title=>'sequel', :body=>'ruby'}]
//    @db[:posts].full_text_search(:title, '+ruby -rails', :boolean => true).all.should == [{:title=>'ruby scooby', :body=>'x'}]
//    @db.sqls.should == [
//      "SELECT * FROM posts WHERE (MATCH (title) AGAINST ('rails'))",
//      "SELECT * FROM posts WHERE (MATCH (title, body) AGAINST ('sequel ruby'))",
//      "SELECT * FROM posts WHERE (MATCH (title) AGAINST ('+ruby -rails' IN BOOLEAN MODE))"]
//  },
//
//  "should support spatial indexes"  : function(ds){
//    @db.create_table(:posts){point :geom, :null=>false; spatial_index [:geom]}
//    @db.sqls.should == [
//      "CREATE TABLE posts (geom point NOT NULL)",
//      "CREATE SPATIAL INDEX posts_geom_index ON posts (geom)"
//    ]
//  },
//
//  "should support indexes with index type"  : function(ds){
//    @db.create_table(:posts){Integer :id; index :id, :type => :btree}
//    @db.sqls.should == [
//      "CREATE TABLE posts (id integer)",
//      "CREATE INDEX posts_id_index USING btree ON posts (id)"
//    ]
//  },
//
//  "should support unique indexes with index type"  : function(ds){
//    @db.create_table(:posts){Integer :id; index :id, :type => :btree, :unique => true}
//    @db.sqls.should == [
//      "CREATE TABLE posts (id integer)",
//      "CREATE UNIQUE INDEX posts_id_index USING btree ON posts (id)"
//    ]
//  },
//
//  "should not dump partial indexes"  : function(ds){
//    @db.create_table(:posts){text :id}
//    @db << "CREATE INDEX posts_id_index ON posts (id(10))"
//    @db.indexes(:posts).should == {}
//  },
//},
//
//context "MySQL::Dataset#insert and related methods" do
//  before do
//    MYSQL_DB.create_table(:items){String :name; Integer :value}
//    @d = MYSQL_DB[:items]
//    MYSQL_DB.sqls.clear
//  },
//  after do
//    MYSQL_DB.drop_table(:items)
//  },
//
//  "#insert should insert record with default values when no arguments given"  : function(ds){
//    @d.insert
//
//    MYSQL_DB.sqls.should == [
//      "INSERT INTO items () VALUES ()"
//    ]
//
//    @d.all.should == [
//      {:name => nil, :value => nil}
//    ]
//  },
//
//  "#insert  should insert record with default values when empty hash given"  : function(ds){
//    @d.insert({})
//
//    MYSQL_DB.sqls.should == [
//      "INSERT INTO items () VALUES ()"
//    ]
//
//    @d.all.should == [
//      {:name => nil, :value => nil}
//    ]
//  },
//
//  "#insert should insert record with default values when empty array given"  : function(ds){
//    @d.insert []
//
//    MYSQL_DB.sqls.should == [
//      "INSERT INTO items () VALUES ()"
//    ]
//
//    @d.all.should == [
//      {:name => nil, :value => nil}
//    ]
//  },
//
//  "#on_duplicate_key_update should work with regular inserts"  : function(ds){
//    MYSQL_DB.add_index :items, :name, :unique=>true
//    MYSQL_DB.sqls.clear
//    @d.insert(:name => 'abc', :value => 1)
//    @d.on_duplicate_key_update(:name, :value => 6).insert(:name => 'abc', :value => 1)
//    @d.on_duplicate_key_update(:name, :value => 6).insert(:name => 'def', :value => 2)
//
//    MYSQL_DB.sqls.length.should == 3
//    MYSQL_DB.sqls[0].should =~ /\AINSERT INTO items \((name|value), (name|value)\) VALUES \(('abc'|1), (1|'abc')\)\z/
//    MYSQL_DB.sqls[1].should =~ /\AINSERT INTO items \((name|value), (name|value)\) VALUES \(('abc'|1), (1|'abc')\) ON DUPLICATE KEY UPDATE name=VALUES\(name\), value=6\z/
//    MYSQL_DB.sqls[2].should =~ /\AINSERT INTO items \((name|value), (name|value)\) VALUES \(('def'|2), (2|'def')\) ON DUPLICATE KEY UPDATE name=VALUES\(name\), value=6\z/
//
//    @d.all.should == [{:name => 'abc', :value => 6}, {:name => 'def', :value => 2}]
//  },
//
//  "#multi_insert should insert multiple records in a single statement"  : function(ds){
//    @d.multi_insert([{:name => 'abc'}, {:name => 'def'}])
//
//    MYSQL_DB.sqls.should == [
//      SQL_BEGIN,
//      "INSERT INTO items (name) VALUES ('abc'), ('def')",
//      SQL_COMMIT
//    ]
//
//    @d.all.should == [
//      {:name => 'abc', :value => nil}, {:name => 'def', :value => nil}
//    ]
//  },
//
//  "#multi_insert should split the list of records into batches if :commit_every option is given"  : function(ds){
//    @d.multi_insert([{:value => 1}, {:value => 2}, {:value => 3}, {:value => 4}],
//      :commit_every => 2)
//
//    MYSQL_DB.sqls.should == [
//      SQL_BEGIN,
//      "INSERT INTO items (value) VALUES (1), (2)",
//      SQL_COMMIT,
//      SQL_BEGIN,
//      "INSERT INTO items (value) VALUES (3), (4)",
//      SQL_COMMIT
//    ]
//
//    @d.all.should == [
//      {:name => nil, :value => 1},
//      {:name => nil, :value => 2},
//      {:name => nil, :value => 3},
//      {:name => nil, :value => 4}
//    ]
//  },
//
//  "#multi_insert should split the list of records into batches if :slice option is given"  : function(ds){
//    @d.multi_insert([{:value => 1}, {:value => 2}, {:value => 3}, {:value => 4}],
//      :slice => 2)
//
//    MYSQL_DB.sqls.should == [
//      SQL_BEGIN,
//      "INSERT INTO items (value) VALUES (1), (2)",
//      SQL_COMMIT,
//      SQL_BEGIN,
//      "INSERT INTO items (value) VALUES (3), (4)",
//      SQL_COMMIT
//    ]
//
//    @d.all.should == [
//      {:name => nil, :value => 1},
//      {:name => nil, :value => 2},
//      {:name => nil, :value => 3},
//      {:name => nil, :value => 4}
//    ]
//  },
//
//  "#import should support inserting using columns and values arrays"  : function(ds){
//    @d.import([:name, :value], [['abc', 1], ['def', 2]])
//
//    MYSQL_DB.sqls.should == [
//      SQL_BEGIN,
//      "INSERT INTO items (name, value) VALUES ('abc', 1), ('def', 2)",
//      SQL_COMMIT
//    ]
//
//    @d.all.should == [
//      {:name => 'abc', :value => 1},
//      {:name => 'def', :value => 2}
//    ]
//  },
//
//  "#insert_ignore should add the IGNORE keyword when inserting"  : function(ds){
//    @d.insert_ignore.multi_insert([{:name => 'abc'}, {:name => 'def'}])
//
//    MYSQL_DB.sqls.should == [
//      SQL_BEGIN,
//      "INSERT IGNORE INTO items (name) VALUES ('abc'), ('def')",
//      SQL_COMMIT
//    ]
//
//    @d.all.should == [
//      {:name => 'abc', :value => nil}, {:name => 'def', :value => nil}
//    ]
//  },
//
//  "#insert_ignore should add the IGNORE keyword for single inserts"  : function(ds){
//    @d.insert_ignore.insert(:name => 'ghi')
//    MYSQL_DB.sqls.should == ["INSERT IGNORE INTO items (name) VALUES ('ghi')"]
//    @d.all.should == [{:name => 'ghi', :value => nil}]
//  },
//
//  "#on_duplicate_key_update should add the ON DUPLICATE KEY UPDATE and ALL columns when no args given"  : function(ds){
//    @d.on_duplicate_key_update.import([:name,:value], [['abc', 1], ['def',2]])
//
//    MYSQL_DB.sqls.should == [
//      "SELECT * FROM items LIMIT 1",
//      SQL_BEGIN,
//      "INSERT INTO items (name, value) VALUES ('abc', 1), ('def', 2) ON DUPLICATE KEY UPDATE name=VALUES(name), value=VALUES(value)",
//      SQL_COMMIT
//    ]
//
//    @d.all.should == [
//      {:name => 'abc', :value => 1}, {:name => 'def', :value => 2}
//    ]
//  },
//
//  "#on_duplicate_key_update should add the ON DUPLICATE KEY UPDATE and columns specified when args are given"  : function(ds){
//    @d.on_duplicate_key_update(:value).import([:name,:value],
//      [['abc', 1], ['def',2]]
//    )
//
//    MYSQL_DB.sqls.should == [
//      SQL_BEGIN,
//      "INSERT INTO items (name, value) VALUES ('abc', 1), ('def', 2) ON DUPLICATE KEY UPDATE value=VALUES(value)",
//      SQL_COMMIT
//    ]
//
//    @d.all.should == [
//      {:name => 'abc', :value => 1}, {:name => 'def', :value => 2}
//    ]
//  },
//
//},
//
//context "MySQL::Dataset#replace" do
//  before do
//    MYSQL_DB.create_table(:items){Integer :id, :unique=>true; Integer :value}
//    @d = MYSQL_DB[:items]
//    MYSQL_DB.sqls.clear
//  },
//  after do
//    MYSQL_DB.drop_table(:items)
//  },
//
//  "should use default values if they exist"  : function(ds){
//    MYSQL_DB.alter_table(:items){set_column_default :id, 1; set_column_default :value, 2}
//    @d.replace
//    @d.all.should == [{:id=>1, :value=>2}]
//    @d.replace([])
//    @d.all.should == [{:id=>1, :value=>2}]
//    @d.replace({})
//    @d.all.should == [{:id=>1, :value=>2}]
//  },
//
//  "should use support arrays, datasets, and multiple values"  : function(ds){
//    @d.replace([1, 2])
//    @d.all.should == [{:id=>1, :value=>2}]
//    @d.replace(1, 2)
//    @d.all.should == [{:id=>1, :value=>2}]
//    @d.replace(@d)
//    @d.all.should == [{:id=>1, :value=>2}]
//  },
//
//  "should create a record if the condition is not met"  : function(ds){
//    @d.replace(:id => 111, :value => 333)
//    @d.all.should == [{:id => 111, :value => 333}]
//  },
//
//  "should update a record if the condition is met"  : function(ds){
//    @d << {:id => 111}
//    @d.all.should == [{:id => 111, :value => nil}]
//    @d.replace(:id => 111, :value => 333)
//    @d.all.should == [{:id => 111, :value => 333}]
//  },
//},
//
//context "MySQL::Dataset#complex_expression_sql" do
//  before do
//    @d = MYSQL_DB.dataset
//  },
//
//  "should handle pattern matches correctly"  : function(ds){
//    @d.literal(:x.like('a')).should == "(x LIKE BINARY 'a')"
//    @d.literal(~:x.like('a')).should == "(x NOT LIKE BINARY 'a')"
//    @d.literal(:x.ilike('a')).should == "(x LIKE 'a')"
//    @d.literal(~:x.ilike('a')).should == "(x NOT LIKE 'a')"
//    @d.literal(:x.like(/a/)).should == "(x REGEXP BINARY 'a')"
//    @d.literal(~:x.like(/a/)).should == "(x NOT REGEXP BINARY 'a')"
//    @d.literal(:x.like(/a/i)).should == "(x REGEXP 'a')"
//    @d.literal(~:x.like(/a/i)).should == "(x NOT REGEXP 'a')"
//  },
//
//  "should handle string concatenation with CONCAT if more than one record"  : function(ds){
//    @d.literal([:x, :y].sql_string_join).should == "CONCAT(x, y)"
//    @d.literal([:x, :y].sql_string_join(' ')).should == "CONCAT(x, ' ', y)"
//    @d.literal([:x.sql_function(:y), 1, 'z'.lit].sql_string_join(:y.sql_subscript(1))).should == "CONCAT(x(y), y[1], '1', y[1], z)"
//  },
//
//  "should handle string concatenation as simple string if just one record"  : function(ds){
//    @d.literal([:x].sql_string_join).should == "x"
//    @d.literal([:x].sql_string_join(' ')).should == "x"
//  },
//},
//
//if MYSQL_DB.adapter_scheme == :mysql or MYSQL_DB.adapter_scheme == :jdbc
//  context "MySQL Stored Procedures" do
//    before do
//      MYSQL_DB.create_table(:items){Integer :id; Integer :value}
//      @d = MYSQL_DB[:items]
//      MYSQL_DB.sqls.clear
//    },
//    after do
//      MYSQL_DB.drop_table(:items)
//      MYSQL_DB.execute('DROP PROCEDURE test_sproc')
//    },
//
//    "should be callable on the database object"  : function(ds){
//      MYSQL_DB.execute_ddl('CREATE PROCEDURE test_sproc() BEGIN DELETE FROM items; },')
//      MYSQL_DB[:items].delete
//      MYSQL_DB[:items].insert(:value=>1)
//      MYSQL_DB[:items].count.should == 1
//      MYSQL_DB.call_sproc(:test_sproc)
//      MYSQL_DB[:items].count.should == 0
//    },
//
//    "should be callable on the dataset object"  : function(ds){
//      MYSQL_DB.execute_ddl('CREATE PROCEDURE test_sproc(a INTEGER) BEGIN SELECT *, a AS b FROM items; },')
//      MYSQL_DB[:items].delete
//      @d = MYSQL_DB[:items]
//      @d.call_sproc(:select, :test_sproc, 3).should == []
//      @d.insert(:value=>1)
//      @d.call_sproc(:select, :test_sproc, 4).should == [{:id=>nil, :value=>1, :b=>4}]
//      @d.row_proc = proc{|r| r.keys.each{|k| r[k] *= 2 if r[k].is_a?(Integer)}; r}
//      @d.call_sproc(:select, :test_sproc, 3).should == [{:id=>nil, :value=>2, :b=>6}]
//    },
//
//    "should be callable on the dataset object with multiple arguments"  : function(ds){
//      MYSQL_DB.execute_ddl('CREATE PROCEDURE test_sproc(a INTEGER, c INTEGER) BEGIN SELECT *, a AS b, c AS d FROM items; },')
//      MYSQL_DB[:items].delete
//      @d = MYSQL_DB[:items]
//      @d.call_sproc(:select, :test_sproc, 3, 4).should == []
//      @d.insert(:value=>1)
//      @d.call_sproc(:select, :test_sproc, 4, 5).should == [{:id=>nil, :value=>1, :b=>4, :d=>5}]
//      @d.row_proc = proc{|r| r.keys.each{|k| r[k] *= 2 if r[k].is_a?(Integer)}; r}
//      @d.call_sproc(:select, :test_sproc, 3, 4).should == [{:id=>nil, :value=>2, :b=>6, :d => 8}]
//    },
//  },
//},
//
//if MYSQL_DB.adapter_scheme == :mysql
//  context "MySQL bad date/time conversions" do
//    after do
//      Sequel::MySQL.convert_invalid_date_time = false
//    },
//
//    "should raise an exception when a bad date/time is used and convert_invalid_date_time is false"  : function(ds){
//      Sequel::MySQL.convert_invalid_date_time = false
//      proc{MYSQL_DB["SELECT CAST('0000-00-00' AS date)"].single_value}.should raise_error(Sequel::InvalidValue)
//      proc{MYSQL_DB["SELECT CAST('0000-00-00 00:00:00' AS datetime)"].single_value}.should raise_error(Sequel::InvalidValue)
//      proc{MYSQL_DB["SELECT CAST('25:00:00' AS time)"].single_value}.should raise_error(Sequel::InvalidValue)
//    },
//
//    "should not use a nil value bad date/time is used and convert_invalid_date_time is nil or :nil"  : function(ds){
//      Sequel::MySQL.convert_invalid_date_time = nil
//      MYSQL_DB["SELECT CAST('0000-00-00' AS date)"].single_value.should == nil
//      MYSQL_DB["SELECT CAST('0000-00-00 00:00:00' AS datetime)"].single_value.should == nil
//      MYSQL_DB["SELECT CAST('25:00:00' AS time)"].single_value.should == nil
//      Sequel::MySQL.convert_invalid_date_time = :nil
//      MYSQL_DB["SELECT CAST('0000-00-00' AS date)"].single_value.should == nil
//      MYSQL_DB["SELECT CAST('0000-00-00 00:00:00' AS datetime)"].single_value.should == nil
//      MYSQL_DB["SELECT CAST('25:00:00' AS time)"].single_value.should == nil
//    },
//
//    "should not use a nil value bad date/time is used and convert_invalid_date_time is :string"  : function(ds){
//      Sequel::MySQL.convert_invalid_date_time = :string
//      MYSQL_DB["SELECT CAST('0000-00-00' AS date)"].single_value.should == '0000-00-00'
//      MYSQL_DB["SELECT CAST('0000-00-00 00:00:00' AS datetime)"].single_value.should == '0000-00-00 00:00:00'
//      MYSQL_DB["SELECT CAST('25:00:00' AS time)"].single_value.should == '25:00:00'
//    },
//  },
//
//  context "MySQL multiple result sets" do
//    before do
//      MYSQL_DB.create_table!(:a){Integer :a}
//      MYSQL_DB.create_table!(:b){Integer :b}
//      @ds = MYSQL_DB['SELECT * FROM a; SELECT * FROM b']
//      MYSQL_DB[:a].insert(10)
//      MYSQL_DB[:a].insert(15)
//      MYSQL_DB[:b].insert(20)
//      MYSQL_DB[:b].insert(25)
//    },
//    after do
//      MYSQL_DB.drop_table(:a, :b)
//    },
//
//    "should combine all results by default"  : function(ds){
//      @ds.all.should == [{:a=>10}, {:a=>15}, {:b=>20}, {:b=>25}]
//    },
//
//    "should work with Database#run"  : function(ds){
//      proc{MYSQL_DB.run('SELECT * FROM a; SELECT * FROM b')}.should_not raise_error
//      proc{MYSQL_DB.run('SELECT * FROM a; SELECT * FROM b')}.should_not raise_error
//    },
//
//    "should work with Database#run and other statements"  : function(ds){
//      proc{MYSQL_DB.run('UPDATE a SET a = 1; SELECT * FROM a; DELETE FROM b')}.should_not raise_error
//      MYSQL_DB[:a].select_order_map(:a).should == [1, 1]
//      MYSQL_DB[:b].all.should == []
//    },
//
//    "should split results returned into arrays if split_multiple_result_sets is used"  : function(ds){
//      @ds.split_multiple_result_sets.all.should == [[{:a=>10}, {:a=>15}], [{:b=>20}, {:b=>25}]]
//    },
//
//    "should have regular row_procs work when splitting multiple result sets"  : function(ds){
//      @ds.row_proc = proc{|x| x[x.keys.first] *= 2; x}
//      @ds.split_multiple_result_sets.all.should == [[{:a=>20}, {:a=>30}], [{:b=>40}, {:b=>50}]]
//    },
//
//    "should use the columns from the first result set when splitting result sets"  : function(ds){
//      @ds.split_multiple_result_sets.columns.should == [:a]
//    },
//
//    "should not allow graphing a dataset that splits multiple statements"  : function(ds){
//      proc{@ds.split_multiple_result_sets.graph(:b, :b=>:a)}.should raise_error(Sequel::Error)
//    },
//
//    "should not allow splitting a graphed dataset"  : function(ds){
//      proc{MYSQL_DB[:a].graph(:b, :b=>:a).split_multiple_result_sets}.should raise_error(Sequel::Error)
//    },
//  },
//},
