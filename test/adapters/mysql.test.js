/* jshint camelcase: false */
"use strict";

var it = require('it'),
    assert = require('assert'),
    patio = require("../../lib"),
    sql = patio.SQL,
    comb = require("comb"),
    config = require("../test.config.js"),
    hitch = comb.hitch;

if (process.env.PATIO_DB === "mysql") {
    it.describe("patio.adapters.Mysql", function (it) {

        var SQL_BEGIN = 'BEGIN';
        var SQL_COMMIT = 'COMMIT',
            MYSQL_DB;

        it.beforeAll(function () {
            patio.quoteIdentifiers = false;
            MYSQL_DB = patio.connect(config.MYSQL_URI + "/sandbox");

            MYSQL_DB.__defineGetter__("sqls", function () {
                return (comb.isArray(this.__sqls) ? this.__sqls : (this.__sqls = []));
            });

            MYSQL_DB.__defineSetter__("sqls", function (sql) {
                this.__sqls = sql;
                return this.__sqls;
            });

            var origExecute = MYSQL_DB.__logAndExecute;
            MYSQL_DB.__logAndExecute = function (sql) {
                this.sqls.push(sql.trim());
                return origExecute.apply(this, arguments);
            };

            function forceDrop(table) {
                return function () {
                    return MYSQL_DB.forceDropTable(table);
                };
            }

            return new Promise(function (resolve) {
                return MYSQL_DB.forceCreateTable("test2", function () {
                    this.name("text");
                    this.value("integer");
                }).chain(forceDrop("items"), forceDrop("items"))
                    .chain(forceDrop("dolls"), forceDrop("dolls"))
                    .chain(forceDrop("booltest"), forceDrop("booltest"))
                    .chain(resolve, resolve);
            });
        });


        it.describe("#createTable", function (it) {

            var db;
            it.beforeEach(function () {
                db = MYSQL_DB;
                return db.forceDropTable("dolls").chain(function () {
                    db.sqls.length = 0;
                });

            });

            it.should("allow the the specification of options", function () {
                return db.createTable("dolls", {engine: "MyISAM", charset: "latin2"}, function () {
                    this.name("text");
                }).chain(function () {
                    assert.deepEqual(db.sqls, ["CREATE TABLE dolls (name text) ENGINE=MyISAM DEFAULT CHARSET=latin2"]);
                });
            });

            it.should("create create a temporary table when temp options is set to true", function () {
                return db.createTable("tmp_dolls", {temp: true, engine: "MyISAM", charset: "latin2"}, function () {
                    this.name("text");
                }).chain(function () {
                    assert.deepEqual(db.sqls, ["CREATE TEMPORARY TABLE tmp_dolls (name text) ENGINE=MyISAM DEFAULT CHARSET=latin2"]);
                });

            });

            it.should("not use default for string {text : true}", function () {
                return db.createTable("dolls", function () {
                    this.name("string", {text: true, "default": "blah"});
                }).chain(function () {
                    assert.deepEqual(db.sqls, ["CREATE TABLE dolls (name text)"]);
                });
            });

            it.should("not create the autoIncrement attribute if it is specified", function () {
                return db
                    .createTable("dolls", function () {
                        this.n2("integer");
                        this.n3(String);
                        this.n4("integer", {autoIncrement: true, unique: true});
                    })
                    .chain(hitch(db, "schema", "dolls"))
                    .chain(function (res) {
                        var schema = res;
                        assert.deepEqual([false, false, true], Object.keys(schema).map(function (k) {
                            return schema[k].autoIncrement;
                        }));
                    });
            });

            it.should("create blob types", function () {
                return db
                    .createTable("dolls", {engine: "MyISAM", charset: "latin2"}, function () {
                        this.name(Buffer);
                    })
                    .chain(hitch(db, "schema", "dolls"))
                    .chain(function (res) {
                        var schema = res;
                        assert.deepEqual(db.sqls, ["CREATE TABLE dolls (name blob) ENGINE=MyISAM DEFAULT CHARSET=latin2", "DESCRIBE dolls"]);
                        assert.deepEqual(schema, {
                            name: {
                                autoIncrement: false,
                                allowNull: true,
                                primaryKey: false,
                                default: null,
                                dbType: 'blob',
                                type: 'blob',
                                jsDefault: null
                            }
                        });
                    });
            });
        });

        it.should("provide server version", function () {
            return MYSQL_DB.serverVersion().chain(function (version) {
                assert.isTrue(version >= 4000);
            });
        });

        it.should("handle the creation and dropping of InnoDB tables with foreigh keys", function () {
            return MYSQL_DB.forceCreateTable("test_innodb", {engine: "InnoDB"}, function () {
                this.primaryKey("id");
                this.foreignKey("fk", "test_innodb", {key: "id"});
            });
        });

        it.should("support forShare", function () {
            return MYSQL_DB.transaction(function () {
                return MYSQL_DB.from("test2").forShare().all().chain(function (res) {
                    assert.lengthOf(res, 0);
                });
            });
        });

        it.should("convert tinyint to bool", function () {
            return MYSQL_DB
                .createTable("booltest", function () {
                    this.column("b", "tinyint(1)");
                    this.column("i", "tinyint(4)");
                }).chain(function () {
                    return MYSQL_DB
                        .schema("booltest", {reload: true})
                        .chain(function (schema) {
                            assert.deepEqual(schema, {
                                b: {
                                    type: "boolean",
                                    autoIncrement: false,
                                    allowNull: true,
                                    primaryKey: false,
                                    "default": null,
                                    jsDefault: null,
                                    dbType: "tinyint(1)"
                                },
                                i: {
                                    type: "integer",
                                    autoIncrement: false,
                                    allowNull: true,
                                    primaryKey: false,
                                    "default": null,
                                    jsDefault: null,
                                    dbType: "tinyint(4)"
                                }
                            });
                        });
                });
        });

        it.should("return tinyint(1)s as boolean values and tinyint(4) as integers ", function () {
            var ds = MYSQL_DB.from("booltest");
            return ds
                .remove()
                .chain(function () {
                    return ds.insert({b: true, i: 10}).chain(function () {
                        return ds.all();
                    });
                })
                .chain(function (res) {
                    assert.deepEqual(res, [
                        {b: true, i: 10}
                    ]);
                    return ds.remove().chain(function () {
                        return ds.insert({b: false, i: 10}).chain(function () {
                            return ds.all();
                        });
                    });
                })
                .chain(function (res) {
                    assert.deepEqual(res, [
                        {b: false, i: 10}
                    ]);
                    return ds.remove().chain(function () {
                        return ds.insert({b: true, i: 1}).chain(function () {
                            return ds.all();
                        });
                    });
                })
                .chain(function (res) {
                    assert.deepEqual(res, [
                        {b: true, i: 1}
                    ]);
                });
        });

        it.context(function () {
            var db, d;
            it.beforeAll(function () {
                db = MYSQL_DB;
                return MYSQL_DB.createTable("items", function () {
                    this.name("string");
                    this.value("integer");
                }).chain(hitch(this, function () {
                    MYSQL_DB.sqls.length = 0;
                    d = MYSQL_DB.from("items");
                }));
            });

            it.should("quote columns and tables using backticks if quoting identifiers", function () {
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
                assert.equal(d.insertSql({value: 333}), 'INSERT INTO `items` (`value`) VALUES (333)');
                assert.equal(d.insertSql({x: sql.y}), 'INSERT INTO `items` (`x`) VALUES (`y`)');
            });

            it.should("quote fields correctly when reversing the order", function () {
                d.quoteIdentifiers = true;
                assert.equal(d.reverseOrder("name").sql, 'SELECT * FROM `items` ORDER BY `name` DESC');
                assert.equal(d.reverseOrder(sql.name.desc()).sql, 'SELECT * FROM `items` ORDER BY `name` ASC');
                assert.equal(d.reverseOrder("name", sql.test.desc()).sql, 'SELECT * FROM `items` ORDER BY `name` DESC, `test` ASC');
                assert.equal(d.reverseOrder(sql.name.desc(), "test").sql, 'SELECT * FROM `items` ORDER BY `name` ASC, `test` DESC');
            });

            it.should("support ORDER clause in UPDATE statements", function () {
                d.quoteIdentifiers = false;
                assert.equal(d.order("name").updateSql({value: 1}), 'UPDATE items SET value = 1 ORDER BY name');
            });

            it.should("support LIMIT clause in UPDATE statements", function () {
                d.quoteIdentifiers = false;
                assert.equal(d.limit(10).updateSql({value: 1}), 'UPDATE items SET value = 1 LIMIT 10');
            });

            it.should("support regexes", function () {
                return d.insert({name: "abc", value: 1})
                    .chain(function () {
                        return d.insert({name: "bcd", value: 1});
                    })
                    .chain(function () {
                        return comb.when([d.filter({name: /bc/}).count(), d.filter({name: /^bc/}).count()]);
                    }).chain(function (res) {
                        assert.deepEqual(res, [2, 1]);
                    });
            });

            it.should("correctly literalize strings with comment backslashes in them", function () {
                return d.remove()
                    .chain(function () {
                        return d.insert({name: ":\\"});
                    })
                    .chain(function () {
                        return d.first();
                    })
                    .chain(function (rec) {
                        assert.equal(rec.name, ":\\");
                    });
            });

        });

        it.should("correctly quote column references", function () {
            var d = MYSQL_DB.from("orders");
            d.quoteIdentifiers = true;
            var market = 'ICE';
            var ackStamp = new Date() - 15 * 60; // 15 minutes ago
            assert.equal(d.select("market", sql.minute(sql.from_unixtime("ack")).as("minute")).where(function () {
                return this.ack.sqlNumber.gt(ackStamp).and({market: market});
            }).groupBy(sql.minute(sql.from_unixtime("ack"))).sql, "SELECT `market`, minute(from_unixtime(`ack`)) AS `minute` FROM `orders` WHERE ((`ack` > " + d.literal(ackStamp) + ") AND (`market` = 'ICE')) GROUP BY minute(from_unixtime(`ack`))");
        });

        it.should("support distinct", function () {
            var db = MYSQL_DB,
                ds = db.from("a");
            return db
                .forceCreateTable("a", function () {
                    this.a("integer");
                    this.b("integer");
                }).chain(function () {
                    return comb.when([ds.insert(20, 10), ds.insert(30, 10)]);
                })
                .chain(function () {
                    return comb.when([ds.order("b", "a").distinct().map("a"), ds.order("b", sql.a.desc()).distinct().map("a")]);
                })
                .chain(function (res) {
                    return db.dropTable("a").chain(function () {
                        assert.deepEqual(res[0], [20, 30]);
                        assert.deepEqual(res[1], [30, 20]);
                    });
                });
        });

        it.describe("MySQL join expressions", function (it) {
            var ds;
            it.beforeAll(function () {
                ds = MYSQL_DB.from("nodes");
            });

            it.should("raise error for :full_outer join requests.", function () {
                assert.throws(hitch(ds, "joinTable", "fullOuter", "nodes"));
            });
            it.should("support natural left joins", function () {
                assert.equal(ds.joinTable("naturalLeft", "nodes").sql,
                    'SELECT * FROM nodes NATURAL LEFT JOIN nodes');
            });
            it.should("support natural right joins", function () {
                assert.equal(ds.joinTable("naturalRight", "nodes").sql,
                    'SELECT * FROM nodes NATURAL RIGHT JOIN nodes');
            });
            it.should("support natural left outer joins", function () {
                assert.equal(ds.joinTable("naturalLeftOuter", "nodes").sql,
                    'SELECT * FROM nodes NATURAL LEFT OUTER JOIN nodes');
            });
            it.should("support natural right outer joins", function () {
                assert.equal(ds.joinTable("naturalRightOuter", "nodes").sql,
                    'SELECT * FROM nodes NATURAL RIGHT OUTER JOIN nodes');
            });
            it.should("support natural inner joins", function () {
                assert.equal(ds.joinTable("naturalInner", "nodes").sql,
                    'SELECT * FROM nodes NATURAL LEFT JOIN nodes');
            });
            it.should("support cross joins", function () {
                assert.equal(ds.joinTable("cross", "nodes").sql, 'SELECT * FROM nodes CROSS JOIN nodes');
            });
            it.should("support cross joins as inner joins if conditions are used", function () {
                assert.equal(ds.joinTable("cross", "nodes", {id: sql.identifier("id")}).sql,
                    'SELECT * FROM nodes INNER JOIN nodes ON (nodes.id = nodes.id)');
            });
            it.should("support straight joins (force left table to be read before right)", function () {
                assert.equal(ds.joinTable("straight", "nodes").sql,
                    'SELECT * FROM nodes STRAIGHT_JOIN nodes');
            });
            it.should("support natural joins on multiple tables.", function () {
                assert.equal(ds.joinTable("naturalLeftOuter", ["nodes", "branches"]).sql,
                    'SELECT * FROM nodes NATURAL LEFT OUTER JOIN (nodes, branches)');
            });
            it.should("support straight joins on multiple tables.", function () {
                assert.equal(ds.joinTable("straight", ["nodes", "branches"]).sql,
                    'SELECT * FROM nodes STRAIGHT_JOIN (nodes, branches)');
            });

            it.should("quote fields correctly", function () {
                ds.quoteIdentifiers = true;
                assert.equal(ds.join("attributes", {nodeId: sql.id}).sql, "SELECT * FROM `nodes` INNER JOIN `attributes` ON (`attributes`.`nodeId` = `nodes`.`id`)");
            });

            it.should("allow a having clause on ungrouped datasets", function () {
                ds.quoteIdentifiers = false;
                assert.doesNotThrow(hitch(ds, "having", "blah"));
                assert.equal(ds.having(sql.literal('blah')).sql, "SELECT * FROM nodes HAVING (blah)");
            });

            it.should("put a having clause before an order by clause", function () {
                ds.quoteIdentifiers = false;
                assert.equal(ds.order("aaa").having({bbb: sql.identifier("ccc")}).sql, "SELECT * FROM nodes HAVING (bbb = ccc) ORDER BY aaa");
            });
        });

        it.should("support addColumn operations", function () {
            var ds = MYSQL_DB.from("test2");
            return MYSQL_DB.addColumn("test2", "xyz", "text")
                .chain(function () {
                    return comb.when([ds.columns, ds.insert({name: "mmm", value: "111", xyz: '000'})]);
                })
                .chain(function (res) {
                    assert.deepEqual(res[0], ["name", "value", "xyz"]);
                    return ds.first();
                })
                .chain(function (res) {
                    assert.equal(res.xyz, "000");
                });
        });

        it.should("support dropColumn operations", function () {
            return MYSQL_DB.dropColumn("test2", "xyz", "text")
                .chain(function () {
                    return MYSQL_DB.from("test2").columns;
                })
                .chain(function (columns) {
                    assert.deepEqual(columns, ["name", "value"]);
                });
        });

        it.should("support renameColumn operations", function () {
            var db = MYSQL_DB;
            return comb
                .when([
                    db.from("test2").remove(),
                    db.addColumn("test2", "xyz", "text")
                ])
                .chain(function () {
                    return db.from("test2").insert({name: "mmm", value: 111, xyz: "gggg"});
                })
                .chain(function () {
                    return db.from("test2").columns;
                })
                .chain(function (col1) {
                    assert.deepEqual(col1, ["name", "value", "xyz"]);
                    return db.renameColumn("test2", "xyz", "zyx", {type: "text"});
                })
                .chain(function () {
                    return comb.when([db.from("test2").columns, db.from("test2").first()]);
                })
                .chain(function (res) {
                    assert.deepEqual(res[0], ["name", "value", "zyx"]);
                    assert.equal(res[1].zyx, "gggg");
                });
        });

        it.should("support renameColumn operations with types like varchar", function () {
            var db = MYSQL_DB;
            return db.from("test2").remove()
                .chain(function () {
                    return db.addColumn("test2", "tre", "text");
                })
                .chain(function () {
                    return db.from("test2").insert({name: "mmm", value: 111, tre: "gggg"});
                })
                .chain(function () {
                    return db.from("test2").columns;
                })
                .chain(function (cols) {
                    assert.deepEqual(cols, ["name", "value", "zyx", "tre"]);
                    return db.renameColumn("test2", "tre", "ert", {type: "varchar", size: 255});
                })
                .chain(function () {
                    return comb.when([db.from("test2").columns, db.from("test2").first()]);
                })
                .chain(function (res) {
                    assert.deepEqual(res[0], ["name", "value", "zyx", "ert"]);
                    assert.equal(res[1].ert, "gggg");
                });

        });

        it.should("support setColumntype operation", function () {
            var db = MYSQL_DB;
            return db.from("test2").remove()
                .chain(function () {
                    return db.addColumn("test2", "xyz", "float");
                })
                .chain(function () {
                    return db.from("test2").insert({name: "mmm", value: 111, xyz: 56.78});
                })
                .chain(function () {
                    return db.from("test2").first();
                })
                .chain(function (before) {
                    assert.equal(before.xyz, 56.78);
                    return db.setColumnType("test2", "xyz", "integer");
                })
                .chain(function () {
                    return db.from("test2").first();
                })
                .chain(function (after) {
                    assert.equal(after.xyz, 57);
                });
        });

        it.should("support addIndex operation", function () {
            var db = MYSQL_DB;
            return db.from("test2").remove()
                .chain(function () {
                    return db.indexes("test2");
                })
                .chain(function (emptyIndexes) {
                    assert.isTrue(comb.isEmpty(emptyIndexes));
                    return db.addIndex("test2", "value");
                })
                .chain(function () {
                    return db.indexes("test2");
                })
                .chain(function (indexes) {
                    assert.isNotNull(indexes["test2_value_index"]);
                });
        });

        it.should("support addForeignKey", function () {
            var db = MYSQL_DB;
            return db.from("test2").remove()
                .chain(function () {
                    return db.alterTable("test2", function () {
                        this.addForeignKey("value2", "test2", {key: "value"});
                    });
                })
                .chain(function () {
                    return db.from("test2").columns;
                })
                .chain(function (columns) {
                    assert.deepEqual(columns, ["name", "value", "zyx", "ert", "xyz", "value2"]);
                });
        });

        it.describe("A MySQL database with table options", function (it) {
            var db;
            it.beforeAll(function () {
                patio.mysql.defaultEngine = 'InnoDB';
                patio.mysql.defaultCharset = 'utf8';
                patio.mysql.defaultCollate = 'utf8_general_ci';
                db = MYSQL_DB;
            });

            it.beforeEach(function () {
                return db.forceDropTable("items").chain(function () {
                    db.sqls = [];
                });
            });

            it.should("allow to pass custom options (engine, charset, collate) for table creation", function () {
                return db
                    .createTable("items", {
                        engine: 'MyISAM',
                        charset: 'latin1',
                        collate: 'latin1_swedish_ci'
                    }, function () {
                        this.size("integer");
                        this.name("text");
                    })
                    .chain(function () {
                        assert.deepEqual(db.sqls, ["CREATE TABLE items (size integer, name text) ENGINE=MyISAM DEFAULT CHARSET=latin1 DEFAULT COLLATE=latin1_swedish_ci"]);
                    });
            });

            it.should("use default options (engine, charset, collate) for table creation", function () {
                return db
                    .createTable("items", function () {
                        this.size("integer");
                        this.name("text");
                    }).
                    then(function () {
                        assert.deepEqual(db.sqls, ["CREATE TABLE items (size integer, name text) ENGINE=InnoDB DEFAULT CHARSET=utf8 DEFAULT COLLATE=utf8_general_ci"]);
                    });
            });

            it.should("not use default options (engine, charset, collate) for table creation", function () {
                return db.createTable("items", {engine: null, charset: null, collate: null}, function () {
                    this.size("integer");
                    this.name("text");
                }).chain(function () {
                    assert.deepEqual(db.sqls, ["CREATE TABLE items (size integer, name text)"]);
                });
            });

            it.afterAll(function () {
                patio.mysql.defaultEngine = null;
                patio.mysql.defaultCharset = null;
                patio.mysql.defaultCollate = null;
            });

        });

        it.context(function (it) {
            var db;
            it.beforeAll(function () {
                db = MYSQL_DB;
                db.sqls.length = 0;
            });

            it.beforeEach(function () {
                return db.forceDropTable("items").chain(function () {
                    db.sqls = [];
                });
            });

            it.should("support defaults for boolean columns", function () {
                return db.createTable("items", function () {
                    this.active1(Boolean, {"default": true});
                    this.active2(Boolean, {"default": false});
                }).chain(function () {
                    assert.deepEqual(db.sqls, ['CREATE TABLE items (active1 tinyint(1) DEFAULT 1, active2 tinyint(1) DEFAULT 0)']);
                });
            });

            it.should("correctly format CREATE TABLE statements with foreign keys", function () {
                return db.createTable("items", function () {
                    this.primaryKey("id");
                    this.foreignKey("p_id", "items", {
                        key: "id",
                        "null": false,
                        onUpdate: "cascade",
                        onDelete: "cascade"
                    });
                }).chain(function () {
                    assert.deepEqual(db.sqls, ['CREATE TABLE items (id integer PRIMARY KEY AUTO_INCREMENT, p_id integer NOT NULL, FOREIGN KEY (p_id) REFERENCES items(id) ON DELETE CASCADE ON UPDATE CASCADE)']);
                });
            });

            it.should("correctly format ALTER TABLE statements with foreign keys", function () {
                return db
                    .createTable("items", function () {
                        this.primaryKey("id");
                        console.log("1. MAKE the table...");
                    })
                    .chain(function (something) {
                        console.log("2. MADE the table...");
                        return db.alterTable("items", function () {
                            this.addForeignKey("p_id", "items", {key: "id", "null": false, onDelete: "cascade"});
                        });
                    })
                    .chain(function () {
                        console.log("3. go assert", db.sqls);
                        assert.deepEqual(db.sqls, [
                            'CREATE TABLE items (id integer PRIMARY KEY AUTO_INCREMENT)',
                            'ALTER TABLE items ADD COLUMN p_id integer NOT NULL',
                            'ALTER TABLE items ADD FOREIGN KEY (p_id) REFERENCES items(id) ON DELETE CASCADE'
                        ]);
                    });
            });

            it.should("have renameColumn support keep existing options", function () {
                return db
                    .createTable("items", function () {
                        this.id(String, {"null": false, "default": "blah"});
                    })
                    .chain(function () {
                        return db.alterTable("items", function () {
                            this.renameColumn("id", "nid");
                        });
                    })
                    .chain(function () {
                        assert.deepEqual(db.sqls, [
                            "CREATE TABLE items (id varchar(255) NOT NULL DEFAULT 'blah')",
                            "DESCRIBE items",
                            "ALTER TABLE items CHANGE COLUMN id nid varchar(255) NOT NULL DEFAULT 'blah'"
                        ]);
                        return db.from("items").insert();
                    })
                    .chain(function () {
                        return db.from("items").all();
                    })
                    .chain(function (items) {
                        assert.deepEqual(items, [
                            {nid: "blah"}
                        ]);
                    });
            });
            it.should("have setColumnType support keep existing options", function () {
                return db
                    .createTable("items", function () {
                        this.id("integer", {"null": false, "default": 5});
                    })
                    .chain(function () {
                        return db.alterTable("items", function () {
                            this.setColumnType("id", "bigint");
                        });
                    })
                    .chain(function () {
                        assert.deepEqual(db.sqls, [
                            "CREATE TABLE items (id integer NOT NULL DEFAULT 5)",
                            "DESCRIBE items",
                            "ALTER TABLE items CHANGE COLUMN id id bigint NOT NULL DEFAULT 5"
                        ]);
                        return db.from("items").insert(Math.pow(2, 40));
                    })
                    .chain(function () {
                        return db.from("items").all();
                    })
                    .chain(function (items) {
                        assert.deepEqual(items, [{id: Math.pow(2, 40)}]);
                    });
            });

            it.should("have setColumnType pass through options", function () {
                return db
                    .createTable("items", function () {
                        this.id("integer");
                        this.list("enum", {elements: ["one"]});
                    })
                    .chain(function () {
                        return db.alterTable("items", function () {
                            this.setColumnType("id", "int", {unsigned: true, size: 8});
                            this.setColumnType("list", "enum", {elements: ["two"]});
                        });
                    })
                    .chain(function () {
                        assert.deepEqual(db.sqls, [
                            "CREATE TABLE items (id integer, list enum('one'))",
                            "DESCRIBE items",
                            "DESCRIBE items",
                            "ALTER TABLE items CHANGE COLUMN id id int(8) UNSIGNED NULL",
                            "ALTER TABLE items CHANGE COLUMN list list enum('two') NULL"
                        ]);
                    });
            });

            it.should("have setColumnDefault keep existing options", function () {
                return db
                    .createTable("items", function () {
                        this.id("integer", {"null": false, "default": 5});
                    })
                    .chain(function () {
                        return db.alterTable("items", function () {
                            this.setColumnDefault("id", 6);
                        });
                    })
                    .chain(function () {
                        assert.deepEqual(db.sqls, [
                            "CREATE TABLE items (id integer NOT NULL DEFAULT 5)",
                            "DESCRIBE items",
                            "ALTER TABLE items CHANGE COLUMN id id int(11) NOT NULL DEFAULT 6"
                        ]);
                        return db.from("items").insert();
                    })
                    .chain(function () {
                        return db.from("items").all();
                    })
                    .chain(function (items) {
                        assert.deepEqual(items, [
                            {id: 6}
                        ]);
                    });
            });

            it.should("have setAllowNull keep existing options", function () {
                return db
                    .createTable("items", function () {
                        this.id("integer", {"null": false, "default": 5});
                    })
                    .chain(function () {
                        return db.alterTable("items", function () {
                            this.setAllowNull("id", true);
                        });
                    })
                    .chain(function () {
                        assert.deepEqual(db.sqls, [
                            "CREATE TABLE items (id integer NOT NULL DEFAULT 5)",
                            "DESCRIBE items",
                            "ALTER TABLE items CHANGE COLUMN id id int(11) NULL DEFAULT 5"
                        ]);
                    });
            });
            it.should("have accept raw SQL when using db.run", function () {
                return db
                    .createTable("items", function () {
                        this.name("string");
                        this.value("integer");
                    })
                    .chain(function () {
                        return db.run("DELETE FROM items");
                    })
                    .chain(function () {
                        return db.run("INSERT INTO items (name, value) VALUES ('tutu', 1234)");
                    })
                    .chain(function () {
                        return db.from("items").all();
                    })
                    .chain(function (res) {
                        assert.deepEqual(res, [{name: 'tutu', value: 1234}]);
                    });
            });
        });

        it.should("support group queries", function () {
            var db = MYSQL_DB,
                ds = db.from("test2");
            return ds.remove()
                .chain(function () {
                    return comb.when([
                        ds.insert({name: 11, value: 10}),
                        ds.insert({name: 11, value: 20}),
                        ds.insert({name: 11, value: 30}),
                        ds.insert({name: 12, value: 10}),
                        ds.insert({name: 12, value: 20}),
                        ds.insert({name: 13, value: 30})
                    ]);
                })
                .chain(function () {
                    return db.fetch("SELECT name FROM test2 WHERE name = '11' GROUP BY name").count();
                })
                .chain(function (count) {
                    assert.equal(count, 1);
                    return db.from("test2").select("name").where({name: 11}).group("name").count();
                })
                .chain(function (count) {
                    assert.equal(count, 1);
                });

        });

        it.should("support fulltext indexes and fullTextSearch", function () {
            var db = MYSQL_DB;
            return db.forceDropTable("posts")
                .chain(function () {
                    db.sqls = [];
                    return db.createTable("posts", {engine: "MyISAM"}, function () {
                        this.title("text");
                        this.body("text");
                        this.fullTextIndex("title");
                        this.fullTextIndex(["title", "body"]);
                    });
                })
                .chain(function () {
                    assert.deepEqual(db.sqls, ['CREATE TABLE posts (title text, body text) ENGINE=MyISAM',
                        'CREATE FULLTEXT INDEX posts_title_index ON posts (title)',
                        'CREATE FULLTEXT INDEX posts_title_body_index ON posts (title, body)'
                    ]);

                    return comb.when([
                        db.from("posts").insert({title: 'node server', body: 'y'}),
                        db.from("posts").insert({title: 'patio', body: 'query'}),
                        db.from("posts").insert({title: 'node bode', body: 'x'})
                    ]);
                })
                .chain(function () {
                    db.sqls = [];
                    return comb.when([
                        db.from("posts").fullTextSearch("title", "server").all(),
                        db.from("posts").fullTextSearch(["title", "body"], ['patio', 'query']).all(),
                        db.from("posts").fullTextSearch("title", '+node -server', {boolean: true}).all()
                    ]);
                })
                .chain(function (res) {
                    assert.deepEqual(res[0][0], {title: new Buffer('node server'), body: new Buffer('y')});
                    assert.deepEqual(res[1][0], {title: new Buffer('patio'), body: new Buffer('query')});
                    assert.deepEqual(res[2][0], {title: new Buffer('node bode'), body: new Buffer('x')});
                    assert.deepEqual(db.sqls, [
                        "SELECT * FROM posts WHERE (MATCH (title) AGAINST ('server'))",
                        "SELECT * FROM posts WHERE (MATCH (title, body) AGAINST ('patio query'))",
                        "SELECT * FROM posts WHERE (MATCH (title) AGAINST ('+node -server' IN BOOLEAN MODE))"
                    ]);
                    return db.dropTable("posts");
                });
        });

        it.should("support spatial indexes", function () {
            var db = MYSQL_DB;
            db.sqls = [];
            return db.createTable("posts", {engine: "MyISAM"}, function () {
                this.geom("point", {allowNull: false});
                this.spatialIndex(["geom"]);
            }).chain(function () {
                assert.deepEqual(db.sqls, [
                    "CREATE TABLE posts (geom point NOT NULL) ENGINE=MyISAM",
                    "CREATE SPATIAL INDEX posts_geom_index ON posts (geom)"
                ]);
                return db.dropTable("posts");
            });
        });

        it.should("support indexes types", function () {
            var db = MYSQL_DB;
            db.sqls = [];
            return db.createTable("posts", function () {
                this.id("integer");
                this.index("id", {type: "btree"});
            })
                .chain(function () {
                    assert.deepEqual(db.sqls, [
                        "CREATE TABLE posts (id integer)",
                        "CREATE INDEX posts_id_index USING btree ON posts (id)"
                    ]);
                    return db.dropTable("posts");
                });
        });

        it.should("support unique indexes using types", function () {
            var db = MYSQL_DB;
            db.sqls = [];
            return db
                .createTable("posts", function () {
                    this.id("integer");
                    this.index("id", {type: "btree", unique: true});
                })
                .chain(function () {
                    assert.deepEqual(db.sqls, [
                        "CREATE TABLE posts (id integer)",
                        "CREATE UNIQUE INDEX posts_id_index USING btree ON posts (id)"
                    ]);
                    return db.dropTable("posts");
                });
        });

        it.should("not dump partial indexes", function () {
            var db = MYSQL_DB;
            return db
                .createTable("posts", function () {
                    this.id("text");
                })
                .chain(function () {
                    return db.run("CREATE INDEX posts_id_index ON posts (id(10))");
                })
                .chain(function () {
                    return db.indexes("posts");
                })
                .chain(function (indexes) {
                    assert.isTrue(comb.isEmpty(indexes));
                    return db.dropTable("posts");
                });
        });

        it.context(function (it) {

            var d, db;
            it.beforeAll(function () {
                db = MYSQL_DB;
                d = MYSQL_DB.from("items");
            });
            it.beforeEach(function () {
                return db
                    .forceDropTable("items")
                    .chain(function () {
                        return db.createTable("items", function () {
                            this.id("integer", {unique: true});
                            this.name(String);
                            this.value("integer");
                            this.image(Buffer);
                        });
                    })
                    .chain(function () {
                        db.sqls = [];
                    });
            });

            it.describe("#insert", function (it) {

                it.should("insert record with default values when no arguments given", function () {
                    return d
                        .insert()
                        .chain(function () {
                            assert.deepEqual(db.sqls, [
                                "INSERT INTO items () VALUES ()"
                            ]);
                            return d.all();
                        })
                        .chain(function (ret) {
                            assert.deepEqual(ret, [
                                {id: null, name: null, value: null, image: null}
                            ]);
                        });
                });


                it.should("insert record with default values when empty hash given", function () {
                    return d
                        .insert({})
                        .chain(function () {
                            assert.deepEqual(db.sqls, [
                                "INSERT INTO items () VALUES ()"
                            ]);
                            return d.all();
                        })
                        .chain(function (ret) {
                            assert.deepEqual(ret, [
                                {id: null, name: null, value: null, image: null}
                            ]);
                        });
                });


                it.should("insert record with default values when empty array given", function () {
                    return d
                        .insert([])
                        .chain(function () {
                            assert.deepEqual(db.sqls, [
                                "INSERT INTO items () VALUES ()"
                            ]);
                            return d.all();
                        })
                        .chain(function (ret) {
                            assert.deepEqual(ret, [
                                {id: null, name: null, value: null, image: null}
                            ]);
                        });
                });

            });

            it.describe("#onDuplicateKeyUpdate", function (it) {
                it.should("work with regular inserts", function () {
                    return db
                        .addIndex("items", "name", {unique: true})
                        .chain(function () {
                            db.sqls = [];
                            return comb.when([
                                d.insert({name: "abc", value: 1}),
                                d.onDuplicateKeyUpdate("name", {value: 6}).insert({name: "abc", value: 1}),
                                d.onDuplicateKeyUpdate("name", {value: 6}).insert({name: "def", value: 2})
                            ]);
                        })
                        .chain(function () {
                            assert.deepEqual(db.sqls, [
                                "INSERT INTO items (name, value) VALUES ('abc', 1)",
                                "INSERT INTO items (name, value) VALUES ('abc', 1) ON DUPLICATE KEY UPDATE name=VALUES(name), value=6",
                                "INSERT INTO items (name, value) VALUES ('def', 2) ON DUPLICATE KEY UPDATE name=VALUES(name), value=6"
                            ]);
                            return d.all();
                        })
                        .chain(function (ret) {
                            assert.deepEqual(ret, [
                                {id: null, name: 'abc', value: 6, image: null},
                                {id: null, name: 'def', value: 2, image: null}
                            ]);
                        });
                });

                it.should("add the ON DUPLICATE KEY UPDATE and columns specified when args are given", function () {
                    return d
                        .onDuplicateKeyUpdate("value")["import"](["name", "value"], [
                        ['abc', 1],
                        ['def', 2]
                    ])
                        .chain(function () {
                            assert.deepEqual(db.sqls, [
                                SQL_BEGIN,
                                "INSERT INTO items (name, value) VALUES ('abc', 1), ('def', 2) ON DUPLICATE KEY UPDATE value=VALUES(value)",
                                SQL_COMMIT]);
                            return d.all();
                        })
                        .chain(function (ret) {
                            assert.deepEqual(ret, [
                                {id: null, name: 'abc', value: 1, image: null},
                                {id: null, name: 'def', value: 2, image: null}
                            ]);
                        });
                });

            });

            it.describe("#multiInsert", function (it) {
                it.should("insert multiple records in a single statement", function () {
                    return d
                        .multiInsert([
                            {name: "abc"},
                            {name: 'def'}
                        ])
                        .chain(function () {
                            assert.deepEqual(db.sqls, [
                                SQL_BEGIN,
                                "INSERT INTO items (name) VALUES ('abc'), ('def')",
                                SQL_COMMIT
                            ]);
                            return d.all();
                        })
                        .chain(function (ret) {
                            assert.deepEqual(ret, [
                                {id: null, name: 'abc', value: null, image: null},
                                {id: null, name: 'def', value: null, image: null}
                            ]);
                        });
                });
            });


            it.should("split the list of records into batches if commitEvery option is given", function () {
                return d.multiInsert([
                    {value: 1},
                    {value: 2},
                    {value: 3},
                    {value: 4}
                ], {commitEvery: 2})
                    .chain(function () {
                        assert.deepEqual(db.sqls, [
                            SQL_BEGIN,
                            "INSERT INTO items (value) VALUES (1), (2)",
                            SQL_COMMIT,
                            SQL_BEGIN,
                            "INSERT INTO items (value) VALUES (3), (4)",
                            SQL_COMMIT
                        ]);
                        return d.all();
                    })
                    .chain(function (ret) {
                        assert.deepEqual(ret, [
                            {id: null, name: null, value: 1, image: null},
                            {id: null, name: null, value: 2, image: null},
                            {id: null, name: null, value: 3, image: null},
                            {id: null, name: null, value: 4, image: null}
                        ]);

                    });
            });


            it.should("split the list of records into batches if slice option is given", function () {
                db.sqls = [];
                return d.multiInsert([
                    {value: 1},
                    {value: 2},
                    {value: 3},
                    {value: 4}
                ], {slice: 2})
                    .chain(function () {
                        assert.deepEqual(db.sqls, [
                            SQL_BEGIN,
                            "INSERT INTO items (value) VALUES (1), (2)",
                            SQL_COMMIT,
                            SQL_BEGIN,
                            "INSERT INTO items (value) VALUES (3), (4)",
                            SQL_COMMIT
                        ]);
                        return d.all();
                    })
                    .chain(function (ret) {
                        assert.deepEqual(ret, [
                            {id: null, name: null, value: 1, image: null},
                            {id: null, name: null, value: 2, image: null},
                            {id: null, name: null, value: 3, image: null},
                            {id: null, name: null, value: 4, image: null}
                        ]);

                    });
            });


            it.describe("#import", function (it) {
                it.should("support inserting using columns and values arrays", function () {
                    db.sqls = [];
                    return d["import"](["name", "value"], [
                        ["abc", 1],
                        ["def", 2]
                    ])
                        .chain(function () {
                            assert.deepEqual(db.sqls, [
                                SQL_BEGIN,
                                "INSERT INTO items (name, value) VALUES ('abc', 1), ('def', 2)",
                                SQL_COMMIT
                            ]);
                            return d.all();
                        })
                        .chain(function (ret) {
                            assert.deepEqual(ret, [
                                {id: null, name: 'abc', value: 1, image: null},
                                {id: null, name: "def", value: 2, image: null}
                            ]);

                        });
                });
            });

            it.describe("#insertIgnore", function (it) {
                it.should("add the IGNORE keyword when inserting", function () {
                    db.sqls = [];
                    return d.insertIgnore().multiInsert([
                        {name: "abc"},
                        {name: "def"}
                    ])
                        .chain(function () {
                            assert.deepEqual(db.sqls, [
                                SQL_BEGIN,
                                "INSERT IGNORE INTO items (name) VALUES ('abc'), ('def')",
                                SQL_COMMIT
                            ]);
                            return d.all();
                        })
                        .chain(function (ret) {
                            assert.deepEqual(ret, [
                                {id: null, name: 'abc', value: null, image: null},
                                {id: null, name: "def", value: null, image: null}
                            ]);

                        });
                });

                it.should("add the IGNORE keyword for single inserts", function () {
                    db.sqls = [];
                    return d.insertIgnore().insert({name: "ghi"})
                        .chain(function () {
                            assert.deepEqual(db.sqls, ["INSERT IGNORE INTO items (name) VALUES ('ghi')"]);
                            return d.all();
                        })
                        .chain(function (ret) {
                            assert.deepEqual(ret, [
                                {id: null, name: 'ghi', value: null, image: null}
                            ]);

                        });
                });
            });

            it.describe("#replace", function (it) {

                it.should("use default values if they exist", function () {
                    return db
                        .alterTable("items", function () {
                            this.setColumnDefault("id", 1);
                            this.setColumnDefault("value", 2);
                        })
                        .chain(function () {
                            return d.replace();
                        })
                        .chain(function () {
                            return d.all();
                        })
                        .chain(function (ret) {
                            assert.deepEqual(ret, [{id: 1, name: null, value: 2, image: null}]);
                            return d.replace([]);
                        })
                        .chain(function () {
                            return d.all();
                        })
                        .chain(function (ret) {
                            assert.deepEqual(ret, [{id: 1, name: null, value: 2, image: null}]);
                            return d.replace({});
                        })
                        .chain(function () {
                            return d.all();
                        })
                        .chain(function (ret) {
                            assert.deepEqual(ret, [{id: 1, name: null, value: 2, image: null}]);
                        });
                });


                it.should("not use default values if they dont exist", function () {
                    return d.replace([1, "hello", 2, new Buffer("test")])
                        .chain(function () {
                            return d.all();
                        })
                        .chain(function (res) {
                            assert.deepEqual(res, [{id: 1, name: "hello", value: 2, image: new Buffer("test")}]);
                            return d.replace(1, "hello", 2, new Buffer("test"));
                        })
                        .chain(function () {
                            return d.all();
                        })
                        .chain(function (res) {
                            assert.deepEqual(res, [{id: 1, name: 'hello', value: 2, image: new Buffer("test")}]);
                            return d.replace(d);
                        })
                        .chain(function () {
                            return d.all();
                        })
                        .chain(function (res) {
                            assert.deepEqual(res, [{id: 1, name: "hello", value: 2, image: new Buffer("test")}]);
                        });
                });


                it.should("create a record if the condition is not met", function () {
                    return d.replace({id: 111, value: 333, image: null})
                        .chain(function () {
                            return d.all();
                        })
                        .chain(function (ret) {
                            assert.deepEqual(ret, [{id: 111, name: null, value: 333, image: null}]);
                        });
                });


                it.should("update a record if the condition is met", function () {
                    return d.replace({id: 111, value: null})
                        .chain(function () {
                            return d.all();
                        })
                        .chain(function (ret) {
                            assert.deepEqual(ret, [{id: 111, name: null, value: null, image: null}]);
                            return d.replace({id: 111, value: 333});
                        })
                        .chain(function () {
                            return d.all();
                        })
                        .chain(function (ret) {
                            assert.deepEqual(ret, [{id: 111, name: null, value: 333, image: null}]);
                        });
                });
            });
        });

        it.describe("#stream", function (it) {

            var d;

            it.beforeAll(function () {
                d = MYSQL_DB.from("items2");
                d.identifierInputMethod = "underscore";
                d.identifierOutputMethod = "camelize";
                return MYSQL_DB.forceDropTable("items_2").chain(function () {
                    return MYSQL_DB.createTable("items_2", function () {
                        this.id("integer", {unique: true});
                        this.name(String);
                        this["test_name"](String);
                        this["test_value"](String);
                    });
                });
            });

            it.beforeEach(function () {
                return d.remove();
            });

            it.should("support streaming records", function (next) {
                comb.when([
                    d.insert({name: "hello", testName: "world", testValue: "!"}),
                    d.insert({name: "hello1", testName: "world1", testValue: "!1"})
                ]).chain(function () {
                    var called = 0;
                    d.stream()
                        .on("data", function (data) {
                            called++;
                        })
                        .on("error", next)
                        .on("end", function () {
                            assert.equal(called, 2);
                            next();
                        });
                }, next);
            });

            it.should("properly emit errors", function (next) {
                comb.when([
                    d.insert({name: "hello", testName: "world", testValue: "!"}),
                    d.insert({name: "hello1", testName: "world1", testValue: "!1"})
                ]).chain(function () {
                    d.filter({x: "y"})
                        .stream()
                        .on("data", assert.fail)
                        .on("error", function (err) {
                            assert.equal(err.message, "ER_BAD_FIELD_ERROR: Unknown column 'x' in 'where clause'");
                            next();
                        }).on("end", assert.fail);
                }, next);
            });
        });

        it.describe("#complexExpressionSql", function (it) {
            var d;
            it.beforeAll(function () {
                d = MYSQL_DB.dataset;
            });
            it.should("handle pattern matches correctly", function () {
                assert.equal(d.literal(sql.identifier("x").like('a')), "(x LIKE BINARY 'a')");
                assert.equal(d.literal(sql.identifier("x").like("a").not()), "(x NOT LIKE BINARY 'a')");
                assert.equal(d.literal(sql.identifier("x").ilike('a')), "(x LIKE 'a')");
                assert.equal(d.literal(sql.identifier("x").ilike('a').not()), "(x NOT LIKE 'a')");
                assert.equal(d.literal(sql.identifier("x").like(/a/)), "(x REGEXP BINARY 'a')");
                assert.equal(d.literal(sql.identifier("x").like(/a/).not()), "(x NOT REGEXP BINARY 'a')");
                assert.equal(d.literal(sql.identifier("x").like(/a/i)), "(x REGEXP 'a')");
                assert.equal(d.literal(sql.identifier("x").like(/a/i).not()), "(x NOT REGEXP 'a')");
            });

            it.should("handle string concatenation with CONCAT if more than one record", function () {
                assert.equal(d.literal(sql.sqlStringJoin(["x", "y"])), "CONCAT(x, y)");
                assert.equal(d.literal(sql.sqlStringJoin(["x", "y"], ' ')), "CONCAT(x, ' ', y)");
                assert.equal(d.literal(sql.sqlStringJoin([sql.x("y"), 1, sql.literal('z')], sql.y.sqlSubscript(1))), "CONCAT(x(y), y[1], '1', y[1], z)");
            });

            it.should("handle string concatenation as simple string if just one record", function () {
                assert.equal(d.literal(sql.sqlStringJoin(["x"])), "x");
                assert.equal(d.literal(sql.sqlStringJoin(["x"], ' ')), "x");
            });

        });

        it.describe("date/time conversions", function (it) {
            it.should("throw an exception when a bad date/time is used and convertInvalidDateTime is false", function () {
                patio.mysql.convertInvalidDateTime = false;
                return MYSQL_DB.fetch("SELECT CAST('0000-00-00' AS date)").singleValue().chain(assert.fail, function () {
                    return MYSQL_DB.fetch("SELECT CAST('0000-00-00 00:00:00' AS datetime)").singleValue().chain(assert.fail, function () {
                        return MYSQL_DB.fetch("SELECT CAST('25:00:00' AS time)").singleValue().chain(assert.fail, function () {
                            patio.mysql.convertInvalidDateTime = false;
                        });
                    });
                });
            });

            it.should("not use a null value bad date/time is used and convertInvalidDateTime is null", function () {
                var db = MYSQL_DB;
                patio.mysql.convertInvalidDateTime = null;
                return comb.when([
                    db.fetch("SELECT CAST('0000-00-00' AS date)").singleValue(),
                    db.fetch("SELECT CAST('0000-00-00 00:00:00' AS datetime)").singleValue(),
                    db.fetch("SELECT CAST('25:00:00' AS time)").singleValue()
                ])
                    .chain(function (res) {
                        patio.mysql.convertInvalidDateTime = false;
                        assert.deepEqual(res, [null, null, null]);
                    });
            });


            it.should("not use a null value bad date/time is used and convertInvalidDateTime is string || String", function () {
                var db = MYSQL_DB;
                patio.mysql.convertInvalidDateTime = String;
                return comb
                    .when([
                        db.fetch("SELECT CAST('2015-00-00' AS date)").singleValue(),
                        db.fetch("SELECT CAST('2015-00-00 00:00:00' AS datetime)").singleValue(),
                        db.fetch("SELECT CAST('25:00:00' AS time)").singleValue()
                    ])
                    .chain(function (res) {
                        assert.deepEqual(res, ['2015-00-00', '2015-00-00 00:00:00', '25:00:00']);

                        patio.mysql.convertInvalidDateTime = "string";
                        return comb.when([
                            db.fetch("SELECT CAST('2015-00-00' AS date)").singleValue(),
                            db.fetch("SELECT CAST('2015-00-00 00:00:00' AS datetime)").singleValue(),
                            db.fetch("SELECT CAST('25:00:00' AS time)").singleValue()
                        ]);
                    })
                    .chain(function (res) {
                        assert.deepEqual(res, ['2015-00-00', '2015-00-00 00:00:00', '25:00:00']);

                        patio.mysql.convertInvalidDateTime = "String";
                        return comb.when([
                            db.fetch("SELECT CAST('2015-00-00' AS date)").singleValue(),
                            db.fetch("SELECT CAST('2015-00-00 00:00:00' AS datetime)").singleValue(),
                            db.fetch("SELECT CAST('25:00:00' AS time)").singleValue()
                        ]);
                    })
                    .chain(function (res) {
                        assert.deepEqual(res, ['2015-00-00', '2015-00-00 00:00:00', '25:00:00']);
                        patio.mysql.convertInvalidDateTime = false;
                    });
            });

            it.should("handle CURRENT_TIMESTAMP as a default value", function () {
                return MYSQL_DB
                    .alterTable("items", function () {
                        this.addColumn("timestamp", sql.TimeStamp, {"default": sql.CURRENT_TIMESTAMP});
                    })
                    .chain(function () {
                        return MYSQL_DB
                            .schema("items")
                            .chain(function (schema) {
                                assert.equal(schema.timestamp["default"], "CURRENT_TIMESTAMP");
                                assert.isNull(schema.timestamp.jsDefault);
                            });
                    });

            });

        });

        it.afterAll(function () {
            return patio.disconnect();
        });
    });
}
