var it = require('it'),
    assert = require('assert'),
    patio = require("index"),
    sql = patio.SQL,
    comb = require("comb-proxy"),
    config = require("../test.config.js"),
    format = comb.string.format,
    hitch = comb.hitch;

if (process.env.PATIO_DB === "mysql" || process.env.NODE_ENV === 'test-coverage') {
    it.describe("patio.adapters.Mysql", function (it) {

        var SQL_BEGIN = 'BEGIN';
        var SQL_ROLLBACK = 'ROLLBACK';
        var SQL_COMMIT = 'COMMIT',
            MYSQL_DB;

        it.beforeAll(function () {
            patio.quoteIdentifiers = false;
            MYSQL_DB = patio.connect(config.MYSQL_URI + "/sandbox");

            MYSQL_DB.__defineGetter__("sqls", function () {
                return (comb.isArray(this.__sqls) ? this.__sqls : (this.__sqls = []));
            });

            MYSQL_DB.__defineSetter__("sqls", function (sql) {
                return this.__sqls = sql;
            });

            var origExecute = MYSQL_DB.__logAndExecute;
            MYSQL_DB.__logAndExecute = function (sql) {
                this.sqls.push(sql.trim());
                return origExecute.apply(this, arguments);
            };
            return MYSQL_DB.forceCreateTable("test2",function () {
                this.name("text");
                this.value("integer");
            }).chainBoth(hitch(MYSQL_DB, "forceDropTable", "items"))
                .chainBoth(hitch(MYSQL_DB, "forceDropTable", "dolls"))
                .chainBoth(hitch(MYSQL_DB, "forceDropTable", "booltest"));
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
                return db.createTable("dolls", {engine: "MyISAM", charset: "latin2"},function () {
                    this.name("text");
                }).chain(function () {
                        assert.deepEqual(db.sqls, ["CREATE TABLE dolls (name text) ENGINE=MyISAM DEFAULT CHARSET=latin2"]);
                    });
            });

            it.should("create create a temporary table when temp options is set to true", function () {
                return db.createTable("tmp_dolls", {temp: true, engine: "MyISAM", charset: "latin2"},function () {
                    this.name("text");
                }).chain(function () {
                        assert.deepEqual(db.sqls, ["CREATE TEMPORARY TABLE tmp_dolls (name text) ENGINE=MyISAM DEFAULT CHARSET=latin2"]);
                    });

            });

            it.should("not use default for string {text : true}", function () {
                return db.createTable("dolls",function () {
                    this.name("string", {text: true, "default": "blah"});
                }).chain(function () {
                        assert.deepEqual(db.sqls, ["CREATE TABLE dolls (name text)"]);
                    });
            });

            it.should("not create the autoIncrement attribute if it is specified", function () {
                return comb.serial([
                        function () {
                            return db.createTable("dolls", function () {
                                this.n2("integer");
                                this.n3(String);
                                this.n4("integer", {autoIncrement: true, unique: true});
                            });
                        },
                        hitch(db, "schema", "dolls")
                    ]).chain(function (res) {
                        var schema = res[1];
                        assert.deepEqual([false, false, true], Object.keys(schema).map(function (k) {
                            return schema[k].autoIncrement;
                        }));
                    });

            });

            it.should("create blob types", function () {
                return comb.serial([
                        function () {
                            return db.createTable("dolls", {engine: "MyISAM", charset: "latin2"}, function () {
                                this.name(Buffer);
                            });
                        },
                        hitch(db, "schema", "dolls")
                    ]).chain(function (res) {
                        var schema = res[1];
                        assert.deepEqual(db.sqls, ["CREATE TABLE dolls (name blob) ENGINE=MyISAM DEFAULT CHARSET=latin2", "DESCRIBE dolls"]);
                        assert.deepEqual(schema, {
                            name: {
                                autoIncrement: false,
                                allowNull: true,
                                primaryKey: false,
                                default: null,
                                dbType: 'blob',
                                type: 'blob',
                                jsDefault: null }
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
            return MYSQL_DB.createTable("booltest",function () {
                this.column("b", "tinyint(1)");
                this.column("i", "tinyint(4)");
            }).chain(function () {
                    return MYSQL_DB.schema("booltest", {reload: true})
                        .chain(function (schema) {
                            assert.deepEqual(schema, {
                                b: {type: "boolean", autoIncrement: false, allowNull: true, primaryKey: false, "default": null, jsDefault: null, dbType: "tinyint(1)"},
                                i: {type: "integer", autoIncrement: false, allowNull: true, primaryKey: false, "default": null, jsDefault: null, dbType: "tinyint(4)"}
                            });
                        });
                });
        });

        it.should("return tinyint(1)s as boolean values and tinyint(4) as integers ", function () {
            var ds = MYSQL_DB.from("booltest");
            return ds.remove()
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
            var DB = MYSQL_DB, d;
            it.beforeAll(function () {
                return MYSQL_DB.createTable("items",function () {
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
                        return comb.when(d.filter({name: /bc/}).count(), d.filter({name: /^bc/}).count());
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
                    })
            });

        });

        it.should("correctly quote column references", function () {
            var d = MYSQL_DB.from("orders");
            d.quoteIdentifiers = true;
            var market = 'ICE';
            var ackStamp = new Date() - 15 * 60; // 15 minutes ago
            assert.equal(d.select("market", sql.minute(sql.from_unixtime("ack")).as("minute")).where(function (o) {
                return this.ack.sqlNumber.gt(ackStamp).and({market: market});
            }).groupBy(sql.minute(sql.from_unixtime("ack"))).sql, "SELECT `market`, minute(from_unixtime(`ack`)) AS `minute` FROM `orders` WHERE ((`ack` > " + d.literal(ackStamp) + ") AND (`market` = 'ICE')) GROUP BY minute(from_unixtime(`ack`))");
        });

        it.should("support distinct", function () {
            var db = MYSQL_DB,
                ds = db.from("a");
            return db.forceCreateTable("a",function () {
                this.a("integer");
                this.b("integer");
            }).chain(function () {
                    return comb.when(ds.insert(20, 10), ds.insert(30, 10));
                })
                .chain(function () {
                    return comb.when(ds.order("b", "a").distinct().map("a"), ds.order("b", sql.a.desc()).distinct().map("a"));
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
                    return comb.when(ds.columns, ds.insert({name: "mmm", value: "111", xyz: '000'}))
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
            return comb.when(
                    db.from("test2").remove(),
                    db.addColumn("test2", "xyz", "text")
                ).chain(function () {
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
                    return comb.when(db.from("test2").columns, db.from("test2").first());
                })
                .chain(function (res) {
                    assert.deepEqual(res[0], ["name", "value", "zyx"]);
                    assert.equal(res[1].zyx, "gggg");
                });
        });

        it.should("support renameColumn operations with types like varchar", function () {
            return comb.executeInOrder(MYSQL_DB,function (db) {
                db.from("test2").remove();
                db.addColumn("test2", "tre", "text");
                db.from("test2").insert({name: "mmm", value: 111, tre: "gggg"});
                var col1 = db.from("test2").columns;
                db.renameColumn("test2", "tre", "ert", {type: "varchar", size: 255});
                var col2 = db.from("test2").columns;
                return {col1: col1, col2: col2, first: db.from("test2").first().ert};
            }).chain(function (res) {
                    assert.deepEqual(res.col1, ["name", "value", "zyx", "tre"]);
                    assert.deepEqual(res.col2, ["name", "value", "zyx", "ert"]);
                    assert.equal(res.first, "gggg");
                });
        });

        it.should("support setColumntype operation", function () {
            return comb.executeInOrder(MYSQL_DB,function (db) {
                db.from("test2").remove();
                db.addColumn("test2", "xyz", "float");
                db.from("test2").insert({name: "mmm", value: 111, xyz: 56.78});
                var before = db.from("test2").first().xyz;
                db.setColumnType("test2", "xyz", "integer");
                var after = db.from("test2").first().xyz;
                return {before: before, after: after};
            }).chain(function (res) {
                    assert.equal(res.before, 56.78);
                    assert.equal(res.after, 57);
                });
        });

        it.should("support addIndex operation", function () {
            return comb.executeInOrder(MYSQL_DB,function (db) {
                db.from("test2").remove();
                var emptyIndexes = db.indexes("test2");
                db.addIndex("test2", "value");
                var indexes = db.indexes("test2");
                return {indexes: indexes, emptyIndexes: emptyIndexes};
            }).chain(function (res) {
                    assert.isNotNull(res.indexes.test2_value_index);
                    assert.isTrue(comb.isEmpty(res.emptyIndexes));
                });


        });

        it.should("support addForeignKey", function () {
            return comb.executeInOrder(MYSQL_DB,function (db) {
                db.from("test2").remove();
                db.alterTable("test2", function () {
                    this.addForeignKey("value2", "test2", {key: "value"});
                });
                return db.from("test2").columns;
            }).chain(function (columns) {
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
                return comb.executeInOrder(db, function (db) {
                    db.forceDropTable("items");
                    db.sqls = [];
                });
            });

            it.should("allow to pass custom options (engine, charset, collate) for table creation", function () {
                return comb.executeInOrder(db,function (db) {
                    db.createTable("items", {engine: 'MyISAM', charset: 'latin1', collate: 'latin1_swedish_ci'}, function () {
                        this.size("integer");
                        this.name("text");
                    });
                    return db.sqls.slice(0);
                }).chain(function (sqls) {
                        assert.deepEqual(sqls, ["CREATE TABLE items (size integer, name text) ENGINE=MyISAM DEFAULT CHARSET=latin1 DEFAULT COLLATE=latin1_swedish_ci"]);
                    });
            });

            it.should("use default options (engine, charset, collate) for table creation", function () {
                return comb.executeInOrder(db,function (db) {
                    db.createTable("items", function () {
                        this.size("integer");
                        this.name("text");
                    });
                    return db.sqls.slice(0);
                }).chain(function (sqls) {
                        assert.deepEqual(sqls, ["CREATE TABLE items (size integer, name text) ENGINE=InnoDB DEFAULT CHARSET=utf8 DEFAULT COLLATE=utf8_general_ci"]);
                    });
            });

            it.should("not use default options (engine, charset, collate) for table creation", function () {

                return comb.executeInOrder(db,function (db) {
                    db.createTable("items", {engine: null, charset: null, collate: null}, function () {
                        this.size("integer");
                        this.name("text");
                    });
                    return db.sqls.slice(0);
                }).chain(function (sqls) {
                        assert.deepEqual(sqls, ["CREATE TABLE items (size integer, name text)"]);
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
                return comb.executeInOrder(db, function (db) {
                    db.forceDropTable("items");
                    db.sqls = [];
                });
            });

            it.should("support defaults for boolean columns", function () {
                return comb.executeInOrder(db,function (db) {
                    db.createTable("items", function () {
                        this.active1(Boolean, {"default": true});
                        this.active2(Boolean, {"default": false});
                    });
                    return db.sqls.slice(0);
                }).chain(function (sqls) {
                        assert.deepEqual(sqls, ["CREATE TABLE items (active1 tinyint(1) DEFAULT 1, active2 tinyint(1) DEFAULT 0)"]);
                    });
            });

            it.should("correctly format CREATE TABLE statements with foreign keys", function () {
                return comb.executeInOrder(db,function (db) {
                    db.createTable("items", function () {
                        this.primaryKey("id");
                        this.foreignKey("p_id", "items", {key: "id", "null": false, onUpdate: "cascade", onDelete: "cascade"});
                    });
                    return db.sqls.slice(0);
                }).chain(function (sqls) {
                        assert.deepEqual(sqls, ["CREATE TABLE items (id integer PRIMARY KEY AUTO_INCREMENT, p_id integer NOT NULL, FOREIGN KEY (p_id) REFERENCES items(id) ON DELETE CASCADE ON UPDATE CASCADE)"]);
                    });
            });

            it.should("correctly format ALTER TABLE statements with foreign keys", function () {
                return comb.executeInOrder(db,function (db) {
                    db.createTable("items", function () {
                        this.primaryKey("id");
                    });
                    db.alterTable("items", function () {
                        this.addForeignKey("p_id", "items", {key: "id", "null": false, onDelete: "cascade"});
                    });
                    return db.sqls.slice(0);
                }).chain(function (sqls) {
                        assert.deepEqual(sqls, [
                            'CREATE TABLE items (id integer PRIMARY KEY AUTO_INCREMENT)',
                            'ALTER TABLE items ADD COLUMN p_id integer NOT NULL',
                            'ALTER TABLE items ADD FOREIGN KEY (p_id) REFERENCES items(id) ON DELETE CASCADE']);
                    });
            });

            it.should("have renameColumn support keep existing options", function () {
                return comb.executeInOrder(db,function (db) {
                    db.createTable("items", function () {
                        this.id(String, {"null": false, "default": "blah"});
                    });
                    db.alterTable("items", function () {
                        this.renameColumn("id", "nid");
                    });
                    var sqls = db.sqls.slice(0);
                    var ds = db.from("items");
                    ds.insert();
                    return {sqls: sqls, items: ds.all()};
                }).chain(function (sqls) {
                        assert.deepEqual(sqls.sqls, [
                            "CREATE TABLE items (id varchar(255) NOT NULL DEFAULT 'blah')",
                            "DESCRIBE items",
                            "ALTER TABLE items CHANGE COLUMN id nid varchar(255) NOT NULL DEFAULT 'blah'"
                        ]);
                        assert.deepEqual(sqls.items, [
                            {nid: "blah"}
                        ]);
                    });
            });
            it.should("have setColumnType support keep existing options", function () {
                return comb.executeInOrder(db,function (db) {
                    db.createTable("items", function () {
                        this.id("integer", {"null": false, "default": 5});
                    });
                    db.alterTable("items", function () {
                        this.setColumnType("id", "bigint");
                    });
                    var sqls = db.sqls.slice(0);
                    var ds = db.from("items");
                    ds.insert(Math.pow(2, 40));
                    return {sqls: sqls, items: ds.all()};
                }).chain(function (sqls) {
                        assert.deepEqual(sqls.sqls, [
                            "CREATE TABLE items (id integer NOT NULL DEFAULT 5)",
                            "DESCRIBE items",
                            "ALTER TABLE items CHANGE COLUMN id id bigint NOT NULL DEFAULT 5"
                        ]);
                        assert.deepEqual(sqls.items, [
                            {id: Math.pow(2, 40)}
                        ]);
                    });
            });

            it.should("have setColumnType pass through options", function () {
                return comb.executeInOrder(db,function (db) {
                    db.createTable("items", function () {
                        this.id("integer");
                        this.list("enum", {elements: ["one"]});
                    });
                    db.alterTable("items", function () {
                        this.setColumnType("id", "int", {unsigned: true, size: 8});
                        this.setColumnType("list", "enum", {elements: ["two"]});
                    });
                    return db.sqls.slice(0);
                }).chain(function (sqls) {
                        assert.deepEqual(sqls, [
                            "CREATE TABLE items (id integer, list enum('one'))",
                            "DESCRIBE items",
                            "DESCRIBE items",
                            "ALTER TABLE items CHANGE COLUMN id id int(8) UNSIGNED NULL",
                            "ALTER TABLE items CHANGE COLUMN list list enum('two') NULL"
                        ]);
                    });
            });

            it.should("have setColumnDefault keep existing options", function () {
                return comb.executeInOrder(db,function (db) {
                    db.createTable("items", function () {
                        this.id("integer", {"null": false, "default": 5});
                    });
                    db.alterTable("items", function () {
                        this.setColumnDefault("id", 6);
                    });
                    var sqls = db.sqls.slice(0);
                    var ds = db.from("items");
                    ds.insert();
                    return {sqls: sqls, items: ds.all()};
                }).chain(function (sqls) {
                        assert.deepEqual(sqls.sqls, [
                            "CREATE TABLE items (id integer NOT NULL DEFAULT 5)",
                            "DESCRIBE items",
                            "ALTER TABLE items CHANGE COLUMN id id int(11) NOT NULL DEFAULT 6"
                        ]);
                        assert.deepEqual(sqls.items, [
                            {id: 6}
                        ]);
                    });
            });

            it.should("have setAllowNull keep existing options", function () {
                return comb.executeInOrder(db,function (db) {
                    db.sqls = [];
                    db.createTable("items", function () {
                        this.id("integer", {"null": false, "default": 5});
                    });
                    db.alterTable("items", function () {
                        this.setAllowNull("id", true);
                    });
                    var sqls = db.sqls.slice(0);
                    var ds = db.from("items");
                    ds.insert();
                    return {sqls: sqls, items: ds.all()};
                }).chain(function (sqls) {
                        assert.deepEqual(sqls.sqls, [
                            "CREATE TABLE items (id integer NOT NULL DEFAULT 5)",
                            "DESCRIBE items",
                            "ALTER TABLE items CHANGE COLUMN id id int(11) NULL DEFAULT 5"
                        ]);
                        assert.deepEqual(sqls.items, [
                            {id: 5}
                        ]);
                    });
            });
            it.should("have accept raw SQL when using db.run", function () {
                return comb.executeInOrder(db,function (db) {
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
                    return res;
                }).chain(function (res) {
                        assert.deepEqual(res, [
                            [],
                            {name: 'tutu', value: 1234},
                            null
                        ]);
                    });
            });
        });

        it.should("support group queries", function () {
            return comb.executeInOrder(MYSQL_DB,function (db) {
                var ds = db.from("test2");
                ds.remove();
                ds.insert({name: 11, value: 10});
                ds.insert({name: 11, value: 20});
                ds.insert({name: 11, value: 30});
                ds.insert({name: 12, value: 10});
                ds.insert({name: 12, value: 20});
                ds.insert({name: 13, value: 30});
                var ds2 = db.fetch("SELECT name FROM test2 WHERE name = '11' GROUP BY name");
                var count = ds2.count();
                var ds3 = db.from("test2").select("name").where({name: 11}).group("name");
                return {count1: count, count2: ds3.count()};
            }).chain(function (res) {
                    assert.equal(res.count1, 1);
                    assert.equal(res.count2, 1);
                });

        });


        it.should("support fulltext indexes and fullTextSearch", function () {
            return comb.executeInOrder(MYSQL_DB,function (db) {
                db.forceDropTable("posts");
                db.sqls = [];
                db.sqls = [];
                db.createTable("posts", {engine: "MyISAM"}, function () {
                    this.title("text");
                    this.body("text");
                    this.fullTextIndex("title");
                    this.fullTextIndex(["title", "body"]);
                });
                var ret = {};
                ret.sqls = db.sqls.slice(0);
                db.from("posts").insert({title: 'node server', body: 'y'});
                db.from("posts").insert({title: 'patio', body: 'query'});
                db.from("posts").insert({title: 'node bode', body: 'x'});
                db.sqls = [];
                ret.ret1 = db.from("posts").fullTextSearch("title", "server").all()[0];
                ret.ret2 = db.from("posts").fullTextSearch(["title", "body"], ['patio', 'query']).all()[0];
                ret.ret3 = db.from("posts").fullTextSearch("title", '+node -server', {boolean: true}).all()[0];
                ret.sqls2 = db.sqls.slice(0);
                db.dropTable("posts");
                return ret;
            }).chain(function (sqls) {
                    assert.deepEqual(sqls.sqls, [
                        "CREATE TABLE posts (title text, body text) ENGINE=MyISAM",
                        "CREATE FULLTEXT INDEX posts_title_index ON posts (title)",
                        "CREATE FULLTEXT INDEX posts_title_body_index ON posts (title, body)"
                    ]);
                    assert.deepEqual(sqls.ret1, { title: new Buffer('node server'), body: new Buffer('y') });
                    assert.deepEqual(sqls.ret2, { title: new Buffer('patio'), body: new Buffer('query') });
                    assert.deepEqual(sqls.ret3, { title: new Buffer('node bode'), body: new Buffer('x') });
                    assert.deepEqual(sqls.sqls2, [
                        "SELECT * FROM posts WHERE (MATCH (title) AGAINST ('server'))",
                        "SELECT * FROM posts WHERE (MATCH (title, body) AGAINST ('patio query'))",
                        "SELECT * FROM posts WHERE (MATCH (title) AGAINST ('+node -server' IN BOOLEAN MODE))"
                    ]);
                });
        });

        it.should("support spatial indexes", function () {
            return comb.executeInOrder(MYSQL_DB,function (db) {
                db.sqls = [];
                db.createTable("posts", {engine: "MyISAM"}, function () {
                    this.geom("point", {allowNull: false});
                    this.spatialIndex(["geom"]);
                });
                var sqls = db.sqls.slice(0);
                db.dropTable("posts");
                return sqls;
            }).chain(function (sqls) {
                    assert.deepEqual(sqls, [
                        "CREATE TABLE posts (geom point NOT NULL) ENGINE=MyISAM",
                        "CREATE SPATIAL INDEX posts_geom_index ON posts (geom)"
                    ]);
                });
        });

        it.should("support indexes types", function () {
            return comb.executeInOrder(MYSQL_DB,function (db) {
                db.sqls = [];
                db.createTable("posts", function () {
                    this.id("integer");
                    this.index("id", {type: "btree"});
                });
                var sqls = db.sqls.slice(0);
                db.dropTable("posts");
                return sqls;
            }).chain(function (sqls) {
                    assert.deepEqual(sqls, [
                        "CREATE TABLE posts (id integer)",
                        "CREATE INDEX posts_id_index USING btree ON posts (id)"
                    ]);
                });
        });

        it.should("support unique indexes using types", function () {
            return comb.executeInOrder(MYSQL_DB,function (db) {
                db.sqls = [];
                db.createTable("posts", function () {
                    this.id("integer");
                    this.index("id", {type: "btree", unique: true});
                });
                var sqls = db.sqls.slice(0);
                db.dropTable("posts");
                return sqls;
            }).chain(function (sqls) {
                    assert.deepEqual(sqls, [
                        "CREATE TABLE posts (id integer)",
                        "CREATE UNIQUE INDEX posts_id_index USING btree ON posts (id)"
                    ]);
                });
        });

        it.should("not dump partial indexes", function () {
            return comb.executeInOrder(MYSQL_DB,function (db) {
                db.sqls = [];
                db.createTable("posts", function () {
                    this.id("text");
                });
                db.run("CREATE INDEX posts_id_index ON posts (id(10))");
                var ret = db.indexes("posts");
                db.dropTable("posts");
                return ret;
            }).chain(function (indexes) {
                    assert.isTrue(comb.isEmpty(indexes));
                });
        });
        it.context(function (it) {

            var d;
            it.beforeAll(function () {
                d = MYSQL_DB.from("items");
            });
            it.beforeEach(function () {
                return comb.executeInOrder(MYSQL_DB, function (db) {
                    db.forceDropTable("items");
                    db.createTable("items", function () {
                        this.id("integer", {unique: true});
                        this.name(String);
                        this.value("integer");
                        this.image(Buffer);
                    });
                    db.sqls = [];
                });
            });

            it.describe("#insert", function (it) {

                it.should("insert record with default values when no arguments given", function () {
                    return comb.executeInOrder(MYSQL_DB, d,function (db, d) {
                        d.insert();
                        return {sql: db.sqls.slice(0), all: d.all()};
                    }).chain(function (ret) {
                            assert.deepEqual(ret.all, [
                                {id: null, name: null, value: null, image: null}
                            ]);
                            assert.deepEqual(ret.sql, [
                                "INSERT INTO items () VALUES ()"
                            ]);
                        });
                });


                it.should("insert record with default values when empty hash given", function () {
                    return comb.executeInOrder(MYSQL_DB, d,function (db, d) {
                        d.insert({});
                        return {sql: db.sqls.slice(0), all: d.all()};
                    }).chain(function (ret) {
                            assert.deepEqual(ret.all, [
                                {id: null, name: null, value: null, image: null}
                            ]);
                            assert.deepEqual(ret.sql, [
                                "INSERT INTO items () VALUES ()"
                            ]);
                        });
                });


                it.should("insert record with default values when empty array given", function () {
                    return comb.executeInOrder(MYSQL_DB, d,function (db, d) {
                        d.insert([]);
                        return {sql: db.sqls.slice(0), all: d.all()};
                    }).chain(function (ret) {
                            assert.deepEqual(ret.all, [
                                {id: null, name: null, value: null, image: null}
                            ]);
                            assert.deepEqual(ret.sql, [
                                "INSERT INTO items () VALUES ()"
                            ]);
                        });
                });

            });

            it.describe("#onDuplicateKeyUpdate", function (it) {
                it.should("work with regular inserts", function () {
                    return comb.executeInOrder(MYSQL_DB, d,function (db, d) {
                        db.addIndex("items", "name", {unique: true});
                        db.sqls = [];
                        d.insert({name: "abc", value: 1});
                        d.onDuplicateKeyUpdate("name", {value: 6}).insert({name: "abc", value: 1});
                        d.onDuplicateKeyUpdate("name", {value: 6}).insert({name: "def", value: 2});
                        return {sql: db.sqls.slice(0), all: d.all()};
                    }).chain(function (ret) {
                            assert.deepEqual(ret.all, [
                                {id: null, name: 'abc', value: 6, image: null},
                                {id: null, name: 'def', value: 2, image: null}
                            ]);
                            assert.deepEqual(ret.sql, [
                                "INSERT INTO items (name, value) VALUES ('abc', 1)",
                                "INSERT INTO items (name, value) VALUES ('abc', 1) ON DUPLICATE KEY UPDATE name=VALUES(name), value=6",
                                "INSERT INTO items (name, value) VALUES ('def', 2) ON DUPLICATE KEY UPDATE name=VALUES(name), value=6"
                            ]);
                        });
                });

                it.should("add the ON DUPLICATE KEY UPDATE and columns specified when args are given", function () {
                    return comb.executeInOrder(MYSQL_DB, d,function (db, d) {
                        d.onDuplicateKeyUpdate("value")["import"](["name", "value"], [
                            ['abc', 1],
                            ['def', 2]
                        ]);
                        return {sql: db.sqls.slice(0), all: d.all()};
                    }).chain(function (ret) {
                            assert.deepEqual(ret.all, [
                                {id: null, name: 'abc', value: 1, image: null},
                                {id: null, name: 'def', value: 2, image: null}
                            ]);
                            assert.deepEqual(ret.sql, [
                                SQL_BEGIN,
                                "INSERT INTO items (name, value) VALUES ('abc', 1), ('def', 2) ON DUPLICATE KEY UPDATE value=VALUES(value)",
                                SQL_COMMIT]);
                        });
                });

            });

            it.describe("#multiInsert", function (it) {
                it.should("insert multiple records in a single statement", function () {
                    return comb.executeInOrder(MYSQL_DB, d,function (db, d) {
                        d.multiInsert([
                            {name: "abc"},
                            {name: 'def'}
                        ]);
                        return {sql: db.sqls.slice(0), all: d.all()};
                    }).chain(function (ret) {
                            assert.deepEqual(ret.all, [
                                {id: null, name: 'abc', value: null, image: null},
                                {id: null, name: 'def', value: null, image: null}
                            ]);
                            assert.deepEqual(ret.sql, [
                                SQL_BEGIN,
                                "INSERT INTO items (name) VALUES ('abc'), ('def')",
                                SQL_COMMIT
                            ]);
                        });
                });


                it.should("split the list of records into batches if commitEvery option is given", function () {
                    return comb.executeInOrder(MYSQL_DB, d,function (db, d) {
                        d.multiInsert([
                            {value: 1},
                            {value: 2},
                            {value: 3},
                            {value: 4}
                        ], {commitEvery: 2});
                        return {sql: db.sqls.slice(0), all: d.all()};
                    }).chain(function (ret) {
                            assert.deepEqual(ret.all, [
                                {id: null, name: null, value: 1, image: null},
                                {id: null, name: null, value: 2, image: null},
                                {id: null, name: null, value: 3, image: null},
                                {id: null, name: null, value: 4, image: null}
                            ]);
                            assert.deepEqual(ret.sql, [
                                SQL_BEGIN,
                                "INSERT INTO items (value) VALUES (1), (2)",
                                SQL_COMMIT,
                                SQL_BEGIN,
                                "INSERT INTO items (value) VALUES (3), (4)",
                                SQL_COMMIT
                            ]);
                        });
                });


                it.should("split the list of records into batches if slice option is given", function () {
                    return comb.executeInOrder(MYSQL_DB, d,function (db, d) {
                        db.sqls = [];
                        d.multiInsert([
                            {value: 1},
                            {value: 2},
                            {value: 3},
                            {value: 4}
                        ], {slice: 2});
                        return {sql: db.sqls.slice(0), all: d.all()};
                    }).chain(function (ret) {
                            assert.deepEqual(ret.all, [
                                {id: null, name: null, value: 1, image: null},
                                {id: null, name: null, value: 2, image: null},
                                {id: null, name: null, value: 3, image: null},
                                {id: null, name: null, value: 4, image: null}
                            ]);
                            assert.deepEqual(ret.sql, [
                                SQL_BEGIN,
                                "INSERT INTO items (value) VALUES (1), (2)",
                                SQL_COMMIT,
                                SQL_BEGIN,
                                "INSERT INTO items (value) VALUES (3), (4)",
                                SQL_COMMIT
                            ]);
                        });
                });
            });

            it.describe("#import", function (it) {
                it.should("support inserting using columns and values arrays", function () {
                    return comb.executeInOrder(MYSQL_DB, d,function (db, d) {
                        db.sqls = [];
                        d["import"](["name", "value"], [
                            ["abc", 1],
                            ["def", 2]
                        ]);
                        return {sql: db.sqls.slice(0), all: d.all()};
                    }).chain(function (ret) {
                            assert.deepEqual(ret.all, [
                                {id: null, name: 'abc', value: 1, image: null},
                                {id: null, name: "def", value: 2, image: null}
                            ]);
                            assert.deepEqual(ret.sql, [
                                SQL_BEGIN,
                                "INSERT INTO items (name, value) VALUES ('abc', 1), ('def', 2)",
                                SQL_COMMIT
                            ]);
                        });
                });
            });

            it.describe("#insertIgnore", function (it) {
                it.should("add the IGNORE keyword when inserting", function () {
                    return comb.executeInOrder(MYSQL_DB, d,function (db, d) {
                        db.sqls = [];
                        d.insertIgnore().multiInsert([
                            {name: "abc"},
                            {name: "def"}
                        ]);
                        return {sql: db.sqls.slice(0), all: d.all()};
                    }).chain(function (ret) {
                            assert.deepEqual(ret.all, [
                                {id: null, name: 'abc', value: null, image: null},
                                {id: null, name: "def", value: null, image: null}
                            ]);
                            assert.deepEqual(ret.sql, [
                                SQL_BEGIN,
                                "INSERT IGNORE INTO items (name) VALUES ('abc'), ('def')",
                                SQL_COMMIT
                            ]);
                        });
                });

                it.should("add the IGNORE keyword for single inserts", function () {
                    return comb.executeInOrder(MYSQL_DB, d,function (db, d) {
                        db.sqls = [];
                        d.insertIgnore().insert({name: "ghi"});
                        return {sql: db.sqls.slice(0), all: d.all()};
                    }).chain(function (ret) {
                            assert.deepEqual(ret.all, [
                                {id: null, name: 'ghi', value: null, image: null}
                            ]);
                            assert.deepEqual(ret.sql, ["INSERT IGNORE INTO items (name) VALUES ('ghi')"]);
                        });
                });
            });

            it.describe("#replace", function (it) {

                it.should("use default values if they exist", function () {
                    return comb.executeInOrder(MYSQL_DB, d,function (db, d) {
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
                        return ret;
                    }).chain(function (ret) {
                            assert.deepEqual(ret, [
                                [
                                    {id: 1, name: null, value: 2, image: null}
                                ],
                                [
                                    {id: 1, name: null, value: 2, image: null}
                                ],
                                [
                                    {id: 1, name: null, value: 2, image: null}
                                ]
                            ]);
                        });
                });


                it.should("use default values if they exist", function () {
                    return comb.executeInOrder(MYSQL_DB, d,function (db, d) {
                        var ret = [];
                        d.replace([1, "hello", 2, new Buffer("test")]);
                        ret.push(d.all());
                        d.replace(1, "hello", 2, new Buffer("test"));
                        ret.push(d.all());
                        d.replace(d);
                        ret.push(d.all());
                        return ret;
                    }).chain(function (ret) {
                            assert.deepEqual(ret, [
                                [
                                    {id: 1, name: "hello", value: 2, image: new Buffer("test")}
                                ],
                                [
                                    {id: 1, name: 'hello', value: 2, image: new Buffer("test")}
                                ],
                                [
                                    {id: 1, name: "hello", value: 2, image: new Buffer("test")}
                                ]
                            ]);
                        });
                });


                it.should("create a record if the condition is not met", function () {
                    return comb.executeInOrder(MYSQL_DB, d,function (db, d) {
                        d.replace({id: 111, value: 333, image: null});
                        return d.all();
                    }).chain(function (ret) {
                            assert.deepEqual(ret, [
                                {id: 111, name: null, value: 333, image: null}
                            ]);
                        });
                });


                it.should("update a record if the condition is met", function () {
                    return comb.executeInOrder(MYSQL_DB, d,function (db, d) {
                        var ret = [];
                        d.replace({id: 111, value: null});
                        ret.push(d.all());
                        d.replace({id: 111, value: 333});
                        ret.push(d.all());
                        return ret;
                    }).chain(function (ret) {
                            assert.deepEqual(ret, [
                                [
                                    {id: 111, name: null, value: null, image: null}
                                ],
                                [
                                    {id: 111, name: null, value: 333, image: null}
                                ]
                            ]);
                        });
                });
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
                return comb.executeInOrder(MYSQL_DB, patio,function (db, patio) {
                    patio.mysql.convertInvalidDateTime = null;
                    var ret = [];
                    ret.push(db.fetch("SELECT CAST('0000-00-00' AS date)").singleValue());
                    ret.push(db.fetch("SELECT CAST('0000-00-00 00:00:00' AS datetime)").singleValue());
                    ret.push(db.fetch("SELECT CAST('25:00:00' AS time)").singleValue());
                    patio.mysql.convertInvalidDateTime = false;
                    return ret;
                }).chain(function (res) {
                        assert.deepEqual(res, [null, null, null]);
                    });
            });


            it.should("not use a null value bad date/time is used and convertInvalidDateTime is string || String", function () {
                var db = MYSQL_DB;
                return comb.serial([
                        function () {
                            patio.mysql.convertInvalidDateTime = String;
                            return comb.when(
                                db.fetch("SELECT CAST('0000-00-00' AS date)").singleValue(),
                                db.fetch("SELECT CAST('0000-00-00 00:00:00' AS datetime)").singleValue(),
                                db.fetch("SELECT CAST('25:00:00' AS time)").singleValue()
                            );
                        },
                        function () {
                            patio.mysql.convertInvalidDateTime = "string";
                            return comb.when(
                                db.fetch("SELECT CAST('0000-00-00' AS date)").singleValue(),
                                db.fetch("SELECT CAST('0000-00-00 00:00:00' AS datetime)").singleValue(),
                                db.fetch("SELECT CAST('25:00:00' AS time)").singleValue()
                            );
                        },
                        function () {
                            patio.mysql.convertInvalidDateTime = "String";
                            return comb.when(
                                db.fetch("SELECT CAST('0000-00-00' AS date)").singleValue(),
                                db.fetch("SELECT CAST('0000-00-00 00:00:00' AS datetime)").singleValue(),
                                db.fetch("SELECT CAST('25:00:00' AS time)").singleValue()
                            );
                        }
                    ]).chain(function (res) {
                        patio.mysql.convertInvalidDateTime = false;
                        res = comb.array.flatten(res);
                        assert.deepEqual(res, ['0000-00-00', '0000-00-00 00:00:00', '25:00:00', '0000-00-00', '0000-00-00 00:00:00', '25:00:00', '0000-00-00', '0000-00-00 00:00:00', '25:00:00']);
                    });

            });

            it.should("handle CURRENT_TIMESTAMP as a default value", function () {
                return comb.serial([
                    function () {
                        return MYSQL_DB.alterTable("items", function () {
                            this.addColumn("timestamp", sql.TimeStamp, {"default": sql.CURRENT_TIMESTAMP});
                        });
                    },
                    function () {
                        return MYSQL_DB.schema("items").chain(function (schema) {
                            assert.equal(schema.timestamp["default"], "CURRENT_TIMESTAMP");
                            assert.isNull(schema.timestamp.jsDefault);
                        });
                    }
                ]);

            });

        });

        it.afterAll(function () {
            return patio.disconnect();
        });
    });
}
