var it = require('it'),
    assert = require('assert'),
    patio = require("index"),
    sql = patio.SQL,
    comb = require("comb-proxy"),
    config = require("../test.config.js"),
    when = comb.when,
    serial = comb.serial,
    format = comb.string.format,
    hitch = comb.hitch;

if (process.env.PATIO_DB === "pg" || process.env.NODE_ENV === 'test-coverage') {
    it.describe("patio.adapters.Postgres", function (it) {

        var PG_DB;

        var resetDb = function () {
            PG_DB.sqls = [];
        };

        it.beforeAll(function () {
            patio.camelize = true;
            patio.quoteIdentifiers = false;
            //patio.configureLogging();
            PG_DB = patio.connect(config.PG_URI + "/sandbox");

            PG_DB.__defineGetter__("sqls", function () {
                return (comb.isArray(this.__sqls) ? this.__sqls : (this.__sqls = []));
            });

            PG_DB.__defineSetter__("sqls", function (sql) {
                return this.__sqls = sql;
            });


            var origExecute = PG_DB.__logAndExecute;
            PG_DB.__logAndExecute = function (sql) {
                this.sqls.push(sql.trim());
                return origExecute.apply(this, arguments);
            };
            return comb.serial([
                function () {
                    return PG_DB.forceCreateTable("test", function () {
                        this.name("text");
                        this.value("integer", {index: true});
                    });
                },
                function () {
                    return PG_DB.forceCreateTable("test2", function () {
                        this.name("text");
                        this.value("integer");
                    });
                },
                function () {
                    return PG_DB.forceCreateTable("test3", function () {
                        this.value("integer");
                        this.time(sql.TimeStamp);
                    });
                },
                function () {
                    return PG_DB.forceCreateTable("test4", function () {
                        this.name(String, {size: 20});
                        this.value("bytea");
                    });
                },
                function () {
                    return PG_DB.forceCreateTable("test5", function () {
                        this.value("integer");
                        this.json("json");
                    });
                },
                function () {
                    return PG_DB.forceCreateTable("test6", function () {
                        this.name("String");
                        this["test_value"]("String");
                        this["test_name"]("String");
                    });
                }
            ]);
        });

        it.should("provide the server version", function () {
            return PG_DB.serverVersion().chain(function (version) {
                assert.isTrue(version > 70000);
            });
        });

        it.should("correctly parse the schema", function () {
            return comb.when(PG_DB.schema("test3"), PG_DB.schema("test4")).chain(function (schemas) {
                assert.deepEqual(schemas[0], {
                    "value": {
                        type: "integer",
                        allowNull: true,
                        "default": null,
                        jsDefault: null,
                        dbType: "integer",
                        primaryKey: false
                    },
                    "time": {
                        type: "datetime",
                        allowNull: true,
                        "default": null,
                        jsDefault: null,
                        dbType: "timestamp without time zone",
                        primaryKey: false
                    }
                });
                assert.deepEqual(schemas[1], {
                    "name": {
                        type: "string",
                        allowNull: true,
                        "default": null,
                        jsDefault: null,
                        dbType: "character varying(20)",
                        primaryKey: false
                    },
                    "value": {
                        type: "blob",
                        allowNull: true,
                        "default": null,
                        jsDefault: null,
                        dbType: "bytea",
                        primaryKey: false
                    }
                });
            });
        });

        it.describe("A PostgreSQL dataset", function (it) {
            var d;
            it.beforeEach(function () {
                d = PG_DB.from("test");
                return d.remove().chain(function () {
                    return resetDb();
                });
            });

            it.should("quote columns and tables using double quotes if quoting identifiers", function () {
                d.quoteIdentifiers = true;
                assert.equal(d.select("name").sql, 'SELECT "name" FROM "test"');

                assert.equal(d.select(sql.literal('COUNT(*)')).sql, 'SELECT COUNT(*) FROM "test"');

                assert.equal(d.select(sql.max("value")).sql, 'SELECT max("value") FROM "test"');

                assert.equal(d.select(sql.NOW.sqlFunction).sql, 'SELECT NOW() FROM "test"');

                assert.equal(d.select(sql.max("items__value")).sql, 'SELECT max("items"."value") FROM "test"');

                assert.equal(d.order(sql.identifier("name").desc()).sql, 'SELECT * FROM "test" ORDER BY "name" DESC');

                assert.equal(d.select(sql.literal('test.name AS item_name')).sql, 'SELECT test.name AS item_name FROM "test"');

                assert.equal(d.select(sql.literal('"name"')).sql, 'SELECT "name" FROM "test"');

                assert.equal(d.select(sql.literal('max(test."name") AS "max_name"')).sql, 'SELECT max(test."name") AS "max_name" FROM "test"');

                assert.equal(d.select(sql.test("abc", sql.literal("'hello'"))).sql, "SELECT test(\"abc\", 'hello') FROM \"test\"");

                assert.equal(d.select(sql.test("abc__def", sql.literal("'hello'"))).sql, "SELECT test(\"abc\".\"def\", 'hello') FROM \"test\"");

                assert.equal(d.select(sql.test("abc__def", sql.literal("'hello'")).as("x2")).sql, "SELECT test(\"abc\".\"def\", 'hello') AS \"x_2\" FROM \"test\"");

                assert.isNotNull(d.insertSql({value: 333}).match(/^INSERT INTO "test" \("value"\) VALUES \(333\)( RETURNING NULL)?$/));
                assert.isNotNull(d.insertSql({x: sql.identifier("y")}).match(/^INSERT INTO "test" \("x"\) VALUES \("y"\)( RETURNING NULL)?$/));
            });


            it.should("convert json properly from string", function () {
                assert.equal(d.literal(sql.json(JSON.stringify({test: "SDF ASDFLALA\"\">ALERT(1)(DUWHB)"}))), "'{\"test\":\"SDF ASDFLALA\\\"\\\">ALERT(1)(DUWHB)\"}'");
            });

            it.should("convert json properly from object", function () {
                assert.equal(d.literal(sql.json({test: "SDF ASDFLALA\"\">ALERT(1)(DUWHB)"})), "'{\"test\":\"SDF ASDFLALA\\\"\\\">ALERT(1)(DUWHB)\"}'");
            });


            it.should("quote fields correctly when reversing the order if quoting identifiers", function () {
                d.quoteIdentifiers = true;
                assert.equal(d.reverseOrder("name").sql, 'SELECT * FROM "test" ORDER BY "name" DESC');
                assert.equal(d.reverseOrder(sql.identifier("name").desc()).sql, 'SELECT * FROM "test" ORDER BY "name" ASC');
                assert.equal(d.reverseOrder("name", sql.identifier("test").desc()).sql, 'SELECT * FROM "test" ORDER BY "name" DESC, "test" ASC');
                assert.equal(d.reverseOrder(sql.identifier("name").desc(), "test").sql, 'SELECT * FROM "test" ORDER BY "name" ASC, "test" DESC');
            });

            it.should("support regular expressions", function () {
                return comb.when(
                    d.insert({name: "abc", value: 1}),
                    d.insert({name: "bcd", value: 2})
                ).chain(function () {
                        return when(
                            d.filter({name: /bc/}).count(),
                            d.filter({name: /^bc/}).count()
                        ).chain(function (res) {
                                assert.equal(res[0], 2);
                                assert.equal(res[1], 1);
                            });
                    });
            });

            it.should("support NULLS FIRST and NULLS LAST", function () {
                return comb.when(
                    d.insert({name: "abc"}),
                    d.insert({name: "bcd"}),
                    d.insert({name: "bcd", value: 2})
                ).chain(function () {
                        return when(
                            d.order(sql.value.asc({nulls: "first"}), "name").selectMap("name"),
                            d.order(sql.value.asc({nulls: "last"}), "name").selectMap("name"),
                            d.order(sql.value.asc({nulls: "first"}), "name").reverse().selectMap("name")
                        ).chain(function (res) {
                                assert.deepEqual(res[0], ["abc", "bcd", "bcd"]);
                                assert.deepEqual(res[1], ["bcd", "abc", "bcd"]);
                                assert.deepEqual(res[2], ["bcd", "bcd", "abc"]);
                            });
                    });
            });

            it.describe("#lock", function (it) {
                it.should("lock tables and yield if a block is given", function () {
                    return d.lock('EXCLUSIVE', function () {
                        return d.insert({name: 'a'});
                    }).chain(function () {
                        assert.deepEqual(PG_DB.sqls, ["BEGIN",
                            "LOCK TABLE  test IN EXCLUSIVE MODE",
                            "INSERT INTO test (name) VALUES ('a') RETURNING *",
                            "COMMIT"]);
                    });
                });

                it.should("lock table if inside a transaction", function () {
                    return PG_DB.transaction(function () {
                        return serial([
                            d.lock.bind(d, 'EXCLUSIVE'),
                            d.insert.bind(d, {name: 'a'})
                        ]);
                    }).chain(function () {
                        assert.deepEqual(PG_DB.sqls, ["BEGIN",
                            "LOCK TABLE  test IN EXCLUSIVE MODE",
                            "INSERT INTO test (name) VALUES ('a') RETURNING *",
                            "COMMIT"]);
                    });
                });
            });

            it.describe("#distinct", function (it) {
                var db, ds;
                it.beforeEach(function () {
                    db = PG_DB;
                    ds = db.from("a");
                    return db.forceCreateTable("a", function () {
                        this.a("integer");
                        this.b("integer");
                    });

                });
                it.afterAll(function () {
                    return db.dropTable("a");
                });

                it.should("should return results distinct based on arguments", function () {
                    return comb.serial([
                        ds.insert.bind(ds, 20, 10),
                        ds.insert.bind(ds, 30, 10),
                        function () {
                            return when(
                                ds.order("b", "a").distinct().map("a"),
                                ds.order("b", sql.identifier("a").desc()).distinct().map("a"),
                                ds.order("b", "a").distinct("b").map("a"),
                                ds.order("b", sql.identifier("a").desc()).distinct("b").map("a")
                            );
                        }
                    ]).chain(function (res) {
                        assert.deepEqual(res[2], [
                            [20, 30],
                            [30, 20],
                            [20],
                            [30]
                        ]);
                    });
                });
            });

            it.describe("with timestamp field", function (it) {
                var d;
                it.beforeEach(function () {
                    d = PG_DB.from("test3");
                    return d.remove();
                });

                it.should("store milliseconds in the fime fields for Timestamp objects", function () {
                    var t = new sql.TimeStamp(new Date());
                    return d.insert({value: 1, time: t}).chain(function () {
                        return d.filter({value: 1}).select("time").returning("time").first().chain(function (res) {
                            assert.equal(res.time.getMilliseconds(), t.getMilliseconds());
                        });
                    });
                });

                it.should("store milliseconds in the time fields for DateTime objects", function () {
                    var t = new sql.DateTime(new Date());
                    return d.insert({value: 1, time: t}).chain(function () {
                        return d.filter({value: 1}).select("time").first().chain(function (res) {
                            assert.equal(res.time.getMilliseconds(), t.getMilliseconds());
                        });
                    });
                });
            });

            it.should("not covert strings with double _ to identifers", function () {
                var ds = PG_DB.from("test3");
                assert.deepEqual(ds.literal("a__B"), "'a__B'");
            });

            it.describe("with json field", function (it) {
                var d;
                it.beforeEach(function () {
                    d = PG_DB.from("test5");
                    return d.remove();
                });

                it.should("store json", function () {
                    var json = {test: "test\""};
                    return d.insert({value: 1, json: sql.json(json)}).chain(function () {
                        return d.filter({value: 1}).select("json").returning("json").first().chain(function (result) {
                            assert.deepEqual({json: json}, result);
                            assert.instanceOf(result.json, patio.sql.Json);
                        });
                    });
                });

                it.should("store json as an array", function () {
                    var json = [{test: "test\""}];
                    return d.insert({value: 1, json: sql.json(json)}).chain(function () {
                        return d.filter({value: 1}).select("json").returning("json").first().chain(function (result) {
                            assert.instanceOf(result.json, patio.sql.JsonArray);
                        });
                    });
                });
            });

            it.describe("#stream", function (it) {

                var d;

                it.beforeAll(function () {
                    patio.camelize = true;
                    d = PG_DB.from("test6");
                });

                it.afterAll(function () {
                    patio.camelize = false;
                });
                it.beforeEach(function () {
                    return d.remove();
                });

                it.should("support streaming records", function (next) {
                    comb.when(
                        d.insert({name: "hello", testName: "world", testValue: "!"}),
                        d.insert({name: "hello1", testName: "world1", testValue: "!1"})
                    ).chain(function () {
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
                    comb.when(
                        d.insert({name: "hello", testName: "world", testValue: "!"}),
                        d.insert({name: "hello1", testName: "world1", testValue: "!1"})
                    ).chain(function () {
                            d.filter({x: "y"})
                                .stream()
                                .on("data", assert.fail)
                                .on("error", function (err) {
                                    assert.equal(err.message, 'column "x" does not exist')
                                    next();
                                }).on("end", assert.fail);
                        }, next);
                });
            });
        });

        it.describe("A postgres database", function (it) {

            var db;

            it.beforeEach(function () {
                resetDb();
            });

            it.beforeAll(function () {
                db = PG_DB;
            });

            it.afterEach(function () {
                return db.forceDropTable("posts");
            });


            it.should("support column operations", function () {
                return serial([
                    db.forceCreateTable.bind(db, "test2", function () {
                        this.name("text");
                        this.value("integer");
                    }),
                    function () {
                        return db.from("test2").insert();
                    },
                    function () {
                        return db.from("test2").columns.chain(function (columns) {
                            assert.deepEqual(columns, ["name", "value"]);
                        });
                    },
                    db.addColumn.bind(db, "test2", "xyz", "text", {"default": '000'}),
                    function () {
                        return db.from("test2").columns.chain(function (columns) {
                            assert.deepEqual(columns, ["name", "value", "xyz"]);
                        });
                    },
                    function () {
                        return db.from("test2").insert({name: 'mmm', value: 111});
                    },
                    function () {
                        return db.from("test2").first().chain(function (res) {
                            assert.equal(res.xyz, '000');
                        });
                    },
                    function () {
                        return db.from("test2").columns.chain(function (columns) {
                            assert.deepEqual(columns, ["name", "value", "xyz"]);
                        });
                    },
                    db.dropColumn.bind(db, "test2", "xyz"),
                    function () {
                        return db.from("test2").columns.chain(function (columns) {
                            assert.deepEqual(columns, ["name", "value"]);
                        });
                    },
                    function () {
                        return db.from("test2").remove();
                    },
                    db.addColumn.bind(db, "test2", "xyz", "text", {"default": '000'}),
                    function () {
                        return db.from("test2").insert({name: 'mmm', value: 111, xyz: 'gggg'});
                    },
                    function () {
                        return db.from("test2").columns.chain(function (columns) {
                            assert.deepEqual(columns, ["name", "value", "xyz"]);
                        });
                    },
                    db.renameColumn.bind(db, "test2", "xyz", "zyx"),
                    function () {
                        return db.from("test2").columns.chain(function (columns) {
                            assert.deepEqual(columns, ["name", "value", "zyx"]);
                        });
                    },
                    function () {
                        db.from("test2").first().chain(function (row) {
                            assert.equal(row.zyx, "gggg");
                        });
                    },
                    db.addColumn.bind(db, "test2", "xyz", "float"),
                    function () {
                        return db.from("test2").remove();
                    },
                    function () {
                        return db.from("test2").insert({name: 'mmm', value: 111, xyz: 56.78});
                    },
                    db.setColumnType.bind(db, "test2", "xyz", "integer"),
                    function () {
                        return db.from("test2").first().chain(function (row) {
                            assert.equal(row.xyz, 57);
                        });
                    }
                ]);

            });

            it.should("return a dataset with locks when calling #lock ", function () {
                assert.instanceOf(db.locks(), patio.Dataset);
                return db.locks().all().chain(function (res) {
                    assert.isArray(res);
                });
            });

            it.should("support specifying integer/bigint types in primary keys and have them be auto incrementing", function () {
                resetDb();
                return serial([
                    function () {
                        return db.createTable("posts", function () {
                            this.primaryKey("a", {type: "integer"});
                        }).chain(function () {
                            assert.deepEqual(PG_DB.sqls, [
                                "CREATE TABLE posts (a serial PRIMARY KEY)"
                            ]);
                        });
                    },
                    resetDb,
                    function () {
                        return db.forceCreateTable("posts", function () {
                            this.primaryKey("a", {type: "bigint"});
                        }).chain(function () {
                            assert.deepEqual(PG_DB.sqls, [
                                "DROP TABLE posts",
                                "CREATE TABLE posts (a bigserial PRIMARY KEY)"
                            ]);
                        });
                    }
                ]);
            });

            it.should("support opclass specification", function () {
                return db.createTable("posts", function () {
                    this.title("text");
                    this.body("text");
                    this.userId("integer");
                    this.index("userId", {opclass: "int4_ops", type: "btree"});
                })
                    .chain(function () {
                        assert.deepEqual(db.sqls, [
                            'CREATE TABLE posts (title text, body text, user_id integer)',
                            'CREATE  INDEX posts_user_id_index ON posts USING btree (user_id int4_ops)'
                        ]);
                    });
            });

            it.should("support fulltext indexes and searching", function () {
                var ds = db.from("posts");
                return serial([
                    db.createTable.bind(db, "posts", function () {
                        this.title("text");
                        this.body("text");
                        this.fullTextIndex(["title", "body"]);
                        this.fullTextIndex("title", {language: 'french'});
                    }),
                    function () {
                        assert.deepEqual(db.sqls, [
                            'CREATE TABLE posts (title text, body text)',
                            'CREATE  INDEX posts_title_body_index ON posts USING gin (to_tsvector(\'simple\', (COALESCE(title, \'\') || \' \' || COALESCE(body, \'\'))))',
                            'CREATE  INDEX posts_title_index ON posts USING gin (to_tsvector(\'french\', (COALESCE(title, \'\'))))'
                        ]);
                    },
                    function () {
                        return when(
                            ds.insert({title: "node js", body: "hello"}),
                            ds.insert({title: "patio", body: "orm"}),
                            ds.insert({title: "java script", body: "world"})
                        );
                    },
                    resetDb,
                    function () {
                        return when(
                            ds.fullTextSearch("title", "node").all(),
                            ds.fullTextSearch(["title", "body"], ["hello", "node"]).all(),
                            ds.fullTextSearch("title", 'script', {language: "french"}).all()
                        ).chain(function (res) {
                                var all1 = res[0], all2 = res[1], all3 = res[2];
                                assert.deepEqual(all1, [
                                    {title: "node js", body: "hello"}
                                ]);
                                assert.deepEqual(all2, [
                                    {title: "node js", body: "hello"}
                                ]);
                                assert.deepEqual(all3, [
                                    {title: "java script", body: "world"}
                                ]);
                                assert.deepEqual(db.sqls, [
                                    "SELECT * FROM posts WHERE (to_tsvector('simple', (COALESCE(title, ''))) @@ to_tsquery('simple', 'node'))",
                                    "SELECT * FROM posts WHERE (to_tsvector('simple', (COALESCE(title, '') || ' ' || COALESCE(body, ''))) @@ to_tsquery('simple', 'hello | node'))",
                                    "SELECT * FROM posts WHERE (to_tsvector('french', (COALESCE(title, ''))) @@ to_tsquery('french', 'script'))"
                                ]);
                            });
                    }
                ]);

            });

            it.should("support spatial indexes", function () {
                return db.createTable("posts", function () {
                    this.geom("box");
                    this.spatialIndex(["geom"]);
                }).chain(function () {
                    assert.deepEqual(db.sqls, [
                        'CREATE TABLE posts (geom box)',
                        'CREATE  INDEX posts_geom_index ON posts USING gist (geom)'
                    ]);
                });
            });

            it.should("support indexes with index type", function () {
                return db.createTable("posts", function () {
                    this.title("varchar", {size: 5});
                    this.index("title", {type: 'hash'});
                }).chain(function () {
                    assert.deepEqual(db.sqls, [
                        'CREATE TABLE posts (title varchar(5))',
                        'CREATE  INDEX posts_title_index ON posts USING hash (title)'
                    ]);
                });
            });

            it.should("support unique indexes with index type", function () {
                return db.createTable("posts", function () {
                    this.title("varchar", {size: 5});
                    this.index("title", {type: 'btree', unique: true});
                }).chain(function () {
                    assert.deepEqual(db.sqls, [
                        'CREATE TABLE posts (title varchar(5))',
                        'CREATE UNIQUE INDEX posts_title_index ON posts USING btree (title)'
                    ]);
                });
            });

            it.should("support partial indexes", function () {
                return db.createTable("posts", function () {
                    this.title("varchar", {size: 5});
                    this.index("title", {where: {title: '5'}});
                }).chain(function () {
                    assert.deepEqual(db.sqls, [
                        'CREATE TABLE posts (title varchar(5))',
                        'CREATE  INDEX posts_title_index ON posts  (title) WHERE (title = \'5\')'
                    ]);
                });
            });

            it.should("support identifiers for table names in indicies", function () {
                return db.createTable(sql.identifier("posts"), function () {
                    this.title("varchar", {size: 5});
                    this.index("title", {where: {title: '5'}});
                }).chain(function () {
                    assert.deepEqual(db.sqls, [
                        'CREATE TABLE posts (title varchar(5))',
                        'CREATE  INDEX posts_title_index ON posts  (title) WHERE (title = \'5\')'
                    ]);
                });
            });

            it.should("support renaming tables", function () {
                return serial([
                    db.createTable.bind(db, "posts1", function () {
                        this.primaryKey("a");
                    }),
                    db.renameTable.bind(db, "posts1", "posts")
                ]).chain(function () {
                    assert.deepEqual(db.sqls, [
                        "CREATE TABLE posts_1 (a serial PRIMARY KEY)",
                        "ALTER TABLE posts_1 RENAME TO posts"
                    ]);
                });
            });

            it.describe("#import", function (it) {
                var ds;
                it.beforeAll(function () {
                    ds = db.from("test");
                });

                it.beforeEach(function () {
                    return serial([
                        db.forceCreateTable.bind(db, "test", function () {
                            this.primaryKey("x");
                            this.y("integer");
                        }),
                        resetDb
                    ]);
                });

                it.afterAll(function () {
                    return db.forceDropTable("test");
                });
                it.should("use single insert statement", function () {
                    return serial([
                        ds["import"].bind(ds, ["x", "y"], [
                            [1, 2],
                            [3, 4]
                        ]),
                        function () {
                            assert.deepEqual(db.sqls, ['BEGIN', 'INSERT INTO test (x, y) VALUES (1, 2), (3, 4)', 'COMMIT']);
                        },
                        function () {
                            return ds.all().chain(function (res) {
                                assert.deepEqual(res, [
                                    {x: 1, y: 2},
                                    {x: 3, y: 4}
                                ]);
                            });
                        }
                    ]);
                });
            });

            it.describe("#createMaterializedView, #dropMaterializedView and #refreshMaterializedView", function (it) {
                var shouldRun;
                it.beforeAll(function () {
                    return db.serverVersion().chain(function (v) {
                        shouldRun = v >= 90300;
                        if (shouldRun) {
                            return db.forceCreateTable("mtrTest", function () {
                                this.primaryKey("id");
                                this.a("text");
                                this.b("text");
                                this.c("text");
                            });
                        }
                    });
                });

                it.should("create a materialized view", function () {
                    if (shouldRun) {
                        return db.createMaterializedView("mtr", db.from("mtrTest").filter({a: "hello"})).chain(function () {
                            assert.deepEqual(db.sqls, ["CREATE MATERIALIZED VIEW mtr AS SELECT * FROM mtr_test WHERE (a = 'hello')"]);
                        });
                    }
                });

                it.should("refresh a materialized view", function () {
                    if (shouldRun) {
                        return db.refreshMaterializedView("mtr").chain(function () {
                            assert.deepEqual(db.sqls, ["REFRESH MATERIALIZED VIEW mtr"]);
                        }).chain(function () {
                            db.sqls = [];
                            return db.refreshMaterializedView("mtr", {noData: true}).chain(function () {
                                assert.deepEqual(db.sqls, ["REFRESH MATERIALIZED VIEW mtr WITH NO DATA"]);
                            });
                        });
                    }
                });

                it.should("drop a materializedView", function () {
                    if (shouldRun) {
                        return db.dropMaterializedView("mtr").chain(function () {
                            assert.deepEqual(db.sqls, ["DROP MATERIALIZED VIEW mtr"]);
                        });
                    }
                });

            });

            it.describe("#listen, #listenOnce, #notify, and #unListen", function (it) {

                it.should("listen to a channel", function () {
                    var ret = new comb.Promise();
                    db.listen("myChannel", function (msg) {
                        try {
                            assert.deepEqual(msg, {msg: "hello"});
                            db.unListen("myChannel").chain(function () {
                                assert.deepEqual(db.sqls, ['LISTEN my_channel', "NOTIFY my_channel , '{\"msg\":\"hello\"}'", "UNLISTEN my_channel"]);
                                assert.deepEqual(db.__listeners, {});
                                ret.callback();
                            }).addErrback(ret);
                        } catch (e) {
                            ret.errback(e);
                        }
                    }).chain(function () {
                        assert.isTrue("my_channel" in db.__listeners);
                        assert.deepEqual(db.sqls, ['LISTEN my_channel']);
                        return db.notify("myChannel", {msg: "hello"});
                    }).addErrback(ret);
                    return ret;
                });

                it.should("listen once to an event", function () {
                    var ret = new comb.Promise(), called = 0;
                    db.listenOnce("myChannel").chain(function (payload) {
                        assert.equal(payload, "hello1");
                        called++;
                        ret.callback();
                    });
                    return when(
                        db.notify("myChannel", "hello1"),
                        db.notify("myChannel", "hello2"),
                        db.notify("myChannel", "hello3"),
                        ret
                    ).chain(function () {
                            assert.equal(called, 1);
                            called = 0;
                            db.listenOnce("myChannel").chain(function (payload) {
                                assert.equal(payload, "hello1");
                                called++;
                            });
                            return db.notify("myChannel", "hello1")
                                .chain(function () {
                                    return db.notify("myChannel", "hello2");
                                })
                                .chain(function () {
                                    return db.notify("myChannel", "hello3");
                                })
                                .chain(function () {
                                    assert.equal(called, 1);
                                });
                        });
                });

                it("unlisten should not error if the channel is not found", function () {
                    return db.unListen("my_channel2").chain(function () {
                        assert.deepEqual(db.sqls, []);
                    });
                });

            });

        });

        it.afterAll(function () {
            return patio.disconnect();
        });
    });
}