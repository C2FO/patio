var it = require('it'),
    assert = require('assert'),
    patio = require("../../lib"),
    Database = patio.Database,
    ConnectionPool = require("../../lib/ConnectionPool"),
    sql = patio.sql,
    helper = require("../helpers/helper"),
    MockDatabase = helper.MockDatabase,
    MockDataset = helper.MockDataset,
    comb = require("comb"),
    hitch = comb.hitch;

it.describe("Database", function (it) {

    var DummyDatabase = comb.define(patio.Database, {
        instance: {
            constructor: function () {
                this._super(arguments);
                this.sqls = [];
                this.identifierInputMethod = null;
                this.identifierOutputMethod = null;
            },

            execute: function (sql, opts) {
                var ret = new comb.Promise().callback();
                this.sqls.push(sql);
                return ret;
            },

            executeError: function () {
                var ret = new comb.Promise();
                this.execute.apply(this, arguments).both(comb('errback').bind(ret));
                return ret;
            },

            reset: function () {
                this.sqls = [];
            },

            transaction: function (opts, cb) {
                var ret = new comb.Promise().callback();
                cb();
                return ret;
            },

            getters: {
                dataset: function () {
                    return new MockDataset(this);
                }
            },

            tables: function() {
                return new comb.Promise().callback(['b']);
            }
        }
    });

    var DummyConnection = comb.define(null, {
        instance: {
            constructor: function (db) {
                this.db = db;
            },

            execute: function () {
                return this.db.execute.apply(this.db, arguments);
            }
        }
    });

    var Dummy3Database = comb.define(Database, {
        instance: {
            constructor: function () {
                this._super(arguments);
                this.sqls = [];
                this.identifierInputMethod = null;
                this.identifierOutputMethod = null;
            },

            execute: function (sql, opts) {
                opts = opts || {};
                var ret = new comb.Promise();
                this.sqls.push(sql);
                ret[opts.error ? "errback" : "callback"](opts.error ? "ERROR" : "");
                return ret;
            },

            createConnection: function (options) {
                return new DummyConnection(this);
            },

            retCommit: function () {
                return this.transaction(function () {
                    return this.execute('DROP TABLE test;');
                });
            },

            retCommitSavePoint: function () {
                return this.transaction(function (db, done) {
                    this.transaction({savepoint: true}, function () {
                        return this.execute('DROP TABLE test;');
                    }).classic(done);
                });
            },

            closeConnection: function (conn) {
                return new comb.Promise().callback();
            },

            validate: function (conn) {
                return new comb.Promise().callback(true);
            },

            reset: function () {
                this.sqls = [];
            }
        }
    });

    it.describe("A new Database", function (it) {

        var db = new Database({1: 2});

        it.should("receive options", function () {
            assert.equal(db.opts[1], 2);
        });

        it.should("create a connection pool", function () {
            assert.instanceOf(db.pool, ConnectionPool);
            assert.equal(db.pool.maxObjects, 10);
            assert.equal(new Database({maxConnections: 4}).pool.maxObjects, 4);
        });

        it.should("respect the quoteIdentifiers option via options", function () {
            var db1 = new Database({quoteIdentifiers: false});
            var db2 = new Database({quoteIdentifiers: true});
            assert.isFalse(db1.quoteIdentifiers);
            assert.isTrue(db2.quoteIdentifiers);
        });

        it.should("toUpperCase on input and toLowerCase on output by default", function () {
            var db = new Database();
            assert.equal(db.identifierInputMethodDefault, "toUpperCase");
            assert.equal(db.identifierOutputMethodDefault, "toLowerCase");
        });

        it.describe("defaultPrimaryKeyType", function (it) {

            it.should("has a defaultPrimaryKeyType of integer", function () {
                var db = new Database();
                assert.equal(db.defaultPrimaryKeyType, "integer");
                assert.deepEqual(db.serialPrimaryKeyOptions, {
                    primaryKey: true,
                    type: "integer",
                    autoIncrement:true
                });
            });

            it.should("allow for overriding the option", function () {
                var db = new Database({defaultPrimaryKeyType: "bigint"});
                assert.equal(db.defaultPrimaryKeyType, "bigint");
                assert.deepEqual(db.serialPrimaryKeyOptions, {
                    primaryKey: true,
                    type: "bigint",
                    autoIncrement:true
                });
            });

        });

        it.should("respect the identifierInputMethod option", function () {
            var db = new Database({identifierInputMethod: null});
            assert.isNull(db.identifierInputMethod);
            db.identifierInputMethod = "toUpperCase";
            assert.equal(db.identifierInputMethod, 'toUpperCase');
            db = new Database({identifierInputMethod: 'toUpperCase'});
            assert.equal(db.identifierInputMethod, "toUpperCase");
            db.identifierInputMethod = null;
            assert.isNull(db.identifierInputMethod);
            patio.identifierInputMethod = "toLowerCase";
            assert.equal(Database.identifierInputMethod, 'toLowerCase');
            assert.equal(new Database().identifierInputMethod, 'toLowerCase');
        });

        it.should("respect the identifierOutputMethod option", function () {
            var db = new Database({identifierOutputMethod: null});
            assert.isNull(db.identifierOutputMethod);
            db.identifierOutputMethod = "toLowerCase";
            assert.equal(db.identifierOutputMethod, 'toLowerCase');
            db = new Database({identifierOutputMethod: 'toLowerCase'});
            assert.equal(db.identifierOutputMethod, "toLowerCase");
            db.identifierOutputMethod = null;
            assert.isNull(db.identifierOutputMethod);
            patio.identifierOutputMethod = "toUpperCase";
            assert.equal(Database.identifierOutputMethod, 'toUpperCase');
            assert.equal(new Database().identifierOutputMethod, 'toUpperCase');
        });

        it.should("respect setting the quoteIdentifiers option", function () {
            patio.quoteIdentifiers = true;
            assert.isTrue(new Database().quoteIdentifiers);
            patio.quoteIdentifiers = false;
            assert.isFalse(new Database().quoteIdentifiers);

            Database.quoteIdentifiers = true;
            assert.isTrue(new Database().quoteIdentifiers);
            Database.quoteIdentifiers = false;
            assert.isFalse(new Database().quoteIdentifiers);
        });

        it.should("respect the quoteIndentifiersDefault method if patio.quoteIdentifiers = null", function () {
            patio.quoteIdentifiers = null;
            assert.isTrue(new Database().quoteIdentifiers);
            var X = comb.define(Database, {
                instance: {
                    getters: {
                        quoteIdentifiersDefault: function () {
                            return false;
                        }
                    }
                }
            });
            var Y = comb.define(Database, {
                instance: {
                    getters: {
                        quoteIdentifiersDefault: function () {
                            return true;
                        }
                    }
                }
            });
            assert.isFalse(new X().quoteIdentifiers);
            assert.isTrue(new Y().quoteIdentifiers);
        });

        it.should("respect the identifierInputMethodDefault method if patio.identifierInputMethod = null", function () {
            patio.identifierInputMethod = undefined;
            assert.equal(new Database().identifierInputMethod, "toUpperCase");
            var X = comb.define(Database, {
                instance: {
                    getters: {
                        identifierInputMethodDefault: function () {
                            return "toLowerCase";
                        }
                    }
                }
            });
            var Y = comb.define(Database, {
                instance: {
                    getters: {
                        identifierInputMethodDefault: function () {
                            return "toUpperCase";
                        }
                    }
                }
            });
            assert.equal(new X().identifierInputMethod, "toLowerCase");
            assert.equal(new Y().identifierInputMethod, "toUpperCase");
        });

        it.should("respect the identifierOutputMethodDefault method if patio.identifierOutputMethod = null", function () {
            patio.identifierOutputMethod = undefined;
            assert.equal(new Database().identifierOutputMethod, "toLowerCase");
            var X = comb.define(Database, {
                instance: {
                    getters: {
                        identifierOutputMethodDefault: function () {
                            return "toLowerCase";
                        }
                    }
                }
            });
            var Y = comb.define(Database, {
                instance: {
                    getters: {
                        identifierOutputMethodDefault: function () {
                            return "toUpperCase";
                        }
                    }
                }
            });
            assert.equal(new X().identifierOutputMethod, "toLowerCase");
            assert.equal(new Y().identifierOutputMethod, "toUpperCase");
        });

        it.should("just use a uri option for mysql with the full connection string", function () {
            var db = patio.connect('mysql://host/db_name');
            assert.isTrue(comb.isInstanceOf(db, Database));
            assert.equal(db.opts.uri, 'mysql://host/db_name');
            assert.equal(db.type, "mysql");
        });
    });


    it.describe("#disconnect", function (it) {
        var db = new MockDatabase();

        it.should("call pool.disconnect", function () {
            db.pool.getConnection();
            db.disconnect();
            assert.equal(db.createdCount, 1);
            assert.equal(db.closedCount, 1);
        });
    });

    it.describe("#connect", function (it) {
        var DB = Database;

        it.should("throw an error", function () {
            assert.throws(function () {
                new DB().connect();
            });
        });
    });

    it.describe("#__logAndExecute", function (it) {
        var db = new Database();

        it.should("log message and call cb ", function () {
            var orig = console.log;
            var messages = [];
            console.log = function (str) {
                assert.isTrue(str.match(/blah/) !== null);
            };
            var a = null;
            db.__logAndExecute("blah", function () {
                var ret = new comb.Promise().callback();
                a = 1;
                return ret;
            });
            assert.equal(a, 1);
            console.log = orig;
        });

        it.should("raise an error if a block is not passed", function () {
            assert.throws(function () {
                db.__logAndExecute("blah");
            });
        });
    });

    it.describe("#uri", function (it) {
        var db = patio.connect('mau://user:pass@localhost:9876/maumau');

        it.should("return the connection URI for the database", function () {
            assert.isTrue(comb.isInstanceOf(db, MockDatabase));
            assert.equal(db.uri, 'mau://user:pass@localhost:9876/maumau');
            assert.equal(db.url, 'mau://user:pass@localhost:9876/maumau');
        });
    });

    it.describe("#type and setAdapterType", function (it) {

        it.should("return the database type", function () {
            assert.equal(Database.type, "default");
            assert.equal(MockDatabase.type, "mau");
            assert.equal(new MockDatabase().type, "mau");
        });
    });

    it.describe("#dataset", function (it) {

        var db, ds;
        it.beforeAll(function () {
            patio.identifierInputMethod = null;
            patio.identifierOutputMethod = null;
            patio.quoteIdentifiers = false;
            db = new Database();
            ds = db.dataset;
        });


        it.should("provide a blank dataset through #dataset", function () {
            assert.instanceOf(ds, patio.Dataset);
            assert.isEmpty(ds.__opts);
            assert.equal(ds.db, db);
        });

        it.should("provide a #from dataset", function () {
            var d = db.from("mau");
            assert.instanceOf(d, patio.Dataset);
            assert.equal(d.sql, "SELECT * FROM mau");
            d = db.from("miu");
            assert.instanceOf(d, patio.Dataset);
            assert.equal(d.sql, "SELECT * FROM miu");

        });

        it.should("provide a filtered #from dataset if a block is given", function () {
            var d = db.from("mau", function () {
                return this.x.sqlNumber.gt(100);
            });
            assert.instanceOf(d, patio.Dataset);
            assert.equal(d.sql, 'SELECT * FROM mau WHERE (x > 100)');
        });

        it.should("provide a #select dataset", function () {
            var d = db.select("a", "b", "c").from("mau");
            assert.instanceOf(d, patio.Dataset);
            assert.equal(d.sql, 'SELECT a, b, c FROM mau');
        });

        it.should("allow #select to take a block", function () {
            var d = db.select("a", "b",
                function () {
                    return "c";
                }).from("mau");
            assert.instanceOf(d, patio.Dataset);
            assert.equal(d.sql, 'SELECT a, b, c FROM mau');
        });
    });

    it.describe("#execute", function (it) {

        it.should("raise NotImplemented", function () {
            assert.throws(function () {
                new Database().execute("hello");
            });
        });
    });

    it.describe("#tables", function (it) {

        it.should("raise NotImplemented", function () {
            assert.throws(function () {
                new Database().tables();
            });
        });
    });

    it.describe("#indexes", function (it) {

        it.should("raise NotImplemented", function () {
            assert.throws(function () {
                new Database().indexes();
            });
        });
    });
    it.describe("#run", function (it) {

        var db = new (comb.define(Database, {
            instance: {

                constructor: function () {
                    this._super(arguments);
                    this.sqls = [];
                },

                executeDdl: function () {
                    var ret = new comb.Promise().callback();
                    this.sqls.length = 0;
                    this.sqls = this.sqls.concat(comb.argsToArray(arguments));
                    return ret;
                }
            }
        }))();


        it.should("pass the supplied arguments to executeDdl", function () {
            db.run("DELETE FROM items");
            assert.deepEqual(db.sqls, ["DELETE FROM items", {}]);
            db.run("DELETE FROM items2", {hello: "world"});
            assert.deepEqual(db.sqls, ["DELETE FROM items2", {hello: "world"}]);
        });
    });

    it.describe("#createTable", function (it) {

        var DB = DummyDatabase;


        it.should("construct the proper SQL", function () {
            patio.quoteIdentifiers = false;
            return comb.serial([
                function () {
                    var db = new DB();
                    return db.createTable("test", function (table) {
                        table.primaryKey("id", "integer", {"null": false});
                        table.column("name", "text");
                        table.column("image", Buffer, {"null": false});
                        table.column("age", "integer");
                        table.index("name", {unique: true});
                        table.check({name: "Bob"});
                        table.constraint("age", {age: {gt: 0}});
                    }).chain(function () {
                        assert.deepEqual(db.sqls, [
                            "CREATE TABLE test (id integer NOT NULL PRIMARY KEY AUTOINCREMENT, name text, image blob NOT NULL, age integer, CHECK (name = 'Bob'), CONSTRAINT age CHECK (age > 0))",
                            "CREATE UNIQUE INDEX test_name_index ON test (name)"
                        ]);
                    });
                },
                function () {
                    patio.quoteIdentifiers = true;
                    var db = new DB();
                    db.createTable("test", function (table) {
                        table.primaryKey("id", "integer", {"null": false});
                        table.column("name", "text");
                        table.index("name", {unique: true});
                    }).chain(function () {
                        assert.deepEqual(db.sqls, [
                            'CREATE TABLE "test" ("id" integer NOT NULL PRIMARY KEY AUTOINCREMENT, "name" text)',
                            'CREATE UNIQUE INDEX "test_name_index" ON "test" ("name")'
                        ]);
                    });
                }
            ]);
        });


        it.should("create a temporary table", function () {
            return comb.serial([

                function () {
                    patio.quoteIdentifiers = true;
                    var db = new DB();
                    return db.createTable("test", {temp: true}, function (table) {
                        table.primaryKey("id", "integer", {"null": false});
                        table.column("name", "text");
                        table.index("name", {unique: true});
                    }).chain(function () {
                        assert.deepEqual(db.sqls, [
                            'CREATE TEMPORARY TABLE "test" ("id" integer NOT NULL PRIMARY KEY AUTOINCREMENT, "name" text)',
                            'CREATE UNIQUE INDEX "test_name_index" ON "test" ("name")'
                        ]);
                    });
                },
                function () {
                    patio.quoteIdentifiers = false;
                    var db = new DB();
                    return db.createTable("test", {temp: true}, function (table) {
                        table.primaryKey("id", "integer", {"null": false});
                        table.column("name", "text");
                        table.index("name", {unique: true});
                    }).chain(function () {
                        assert.deepEqual(db.sqls, [
                            'CREATE TEMPORARY TABLE test (id integer NOT NULL PRIMARY KEY AUTOINCREMENT, name text)',
                            'CREATE UNIQUE INDEX test_name_index ON test (name)'
                        ]);
                    });
                }
            ]);
        });
    });

    it.describe("#alterTable", function (it) {

        var DB = DummyDatabase;
        it.beforeAll(function () {
            patio.quoteIdentifiers = false;
        });

        it.should("construct proper SQL", function () {
            var db = new DB();

            return db.alterTable("xyz", function (table) {
                table.addColumn("aaa", "text", {"null": false, unique: true});
                table.dropColumn("bbb");
                table.renameColumn("ccc", "ddd");
                table.setColumnType("eee", "integer");
                table.setColumnDefault("hhh", 'abcd');
                table.addIndex("fff", {unique: true});
                table.dropIndex("ggg");
                table.addForeignKey(["aaa"], "table");
                table.addConstraint("valid_name", sql.name.like('A%'));
                table.addConstraint("other_valid_name", function () {
                    return sql.name2.like('A%');
                });
            }).chain(function () {
                assert.deepEqual(db.sqls, [
                    'ALTER TABLE xyz ADD COLUMN aaa text UNIQUE NOT NULL',
                    'ALTER TABLE xyz DROP COLUMN bbb',
                    'ALTER TABLE xyz RENAME COLUMN ccc TO ddd',
                    'ALTER TABLE xyz ALTER COLUMN eee TYPE integer',
                    "ALTER TABLE xyz ALTER COLUMN hhh SET DEFAULT 'abcd'",
                    'CREATE UNIQUE INDEX xyz_fff_index ON xyz (fff)',
                    'DROP INDEX xyz_ggg_index',
                    "ALTER TABLE xyz ADD FOREIGN KEY (aaa) REFERENCES table",
                    "ALTER TABLE xyz ADD CONSTRAINT valid_name CHECK (name LIKE 'A%')",
                    "ALTER TABLE xyz ADD CONSTRAINT other_valid_name CHECK (name2 LIKE 'A%')"
                ]);
            });
        });
    });

    it.describe("#addColumn", function (it) {
        var db;
        it.beforeAll(function () {
            patio.quoteIdentifiers = false;
            db = new DummyDatabase();
        });

        it.should("construct proper SQL", function () {
            return db.addColumn("test", "name", "text", {unique: true}).chain(function () {
                assert.deepEqual(db.sqls, [
                    'ALTER TABLE test ADD COLUMN name text UNIQUE'
                ]);
            });
        });
    });

    it.describe("#dropColumn", function (it) {
        var db;
        it.beforeAll(function () {
            patio.quoteIdentifiers = false;
            db = new DummyDatabase();
        });

        it.should("construct proper SQL", function () {
            return db.dropColumn("test", "name").chain(function () {
                assert.deepEqual(db.sqls, [
                    'ALTER TABLE test DROP COLUMN name'
                ]);
            });
        });
    });

    it.describe("#renameColumn", function (it) {
        var db;
        it.beforeAll(function () {
            patio.quoteIdentifiers = false;
            db = new DummyDatabase();
        });

        it.should("construct proper SQL", function () {
            return db.renameColumn("test", "abc", "def").chain(function () {
                assert.deepEqual(db.sqls, [
                    'ALTER TABLE test RENAME COLUMN abc TO def'
                ]);
            });
        });
    });

    it.describe("#setColumnType", function (it) {
        var db;
        it.beforeAll(function () {
            patio.quoteIdentifiers = false;
            db = new DummyDatabase();
        });

        it.should("construct proper SQL", function () {
            return db.setColumnType("test", "name", "integer").chain(function () {
                assert.deepEqual(db.sqls, [
                    'ALTER TABLE test ALTER COLUMN name TYPE integer'
                ]);
            });
        });
    });

    it.describe("#setColumnDefault", function (it) {
        var db;
        it.beforeAll(function () {
            patio.quoteIdentifiers = false;
            db = new DummyDatabase();
        });

        it.should("construct proper SQL", function () {
            return db.setColumnDefault("test", "name", 'zyx').chain(function () {
                assert.deepEqual(db.sqls, [
                    "ALTER TABLE test ALTER COLUMN name SET DEFAULT 'zyx'"
                ]);
            });
        });
    });

    it.describe("#addIndex", function (it) {
        var db;
        it.beforeAll(function () {
            patio.quoteIdentifiers = false;
            db = new DummyDatabase();
        });

        it.should("construct proper SQL", function () {
            return db.addIndex("test", "name", {unique: true}).chain(function () {
                assert.deepEqual(db.sqls, [
                    'CREATE UNIQUE INDEX test_name_index ON test (name)'
                ]);
            });
        });

        it.should("accept multiple columns", function () {
            db.reset();
            return db.addIndex("test", ["one", "two"]).chain(function () {
                assert.deepEqual(db.sqls, [
                    'CREATE INDEX test_one_two_index ON test (one, two)'
                ]);
            });
        });
    });

    it.describe("#dropIndex", function (it) {
        var db;
        it.beforeAll(function () {
            patio.quoteIdentifiers = false;
            db = new DummyDatabase();
        });

        it.should("construct proper SQL", function () {
            return db.dropIndex("test", "name").chain(function () {
                assert.deepEqual(db.sqls, [
                    'DROP INDEX test_name_index'
                ]);
            });
        });

    });

    it.describe("#dropTable", function (it) {

        var db = new DummyDatabase();


        it.should("construct proper SQL", function () {
            return db.dropTable("test").chain(function () {
                assert.deepEqual(db.sqls, ['DROP TABLE test']);
            });
        });

        it.should("accept multiple table names", function () {
            db.reset();
            return db.dropTable("a", "bb", "ccc").chain(function () {
                assert.deepEqual(db.sqls, [
                    'DROP TABLE a',
                    'DROP TABLE bb',
                    'DROP TABLE ccc'
                ]);
            });
        });
    });

    it.describe("#renameTable", function (it) {
        var db;
        it.beforeAll(function () {
            patio.quoteIdentifiers = false;
            db = new DummyDatabase();
        });

        it.should("construct proper SQL", function () {
            return db.renameTable("abc", "xyz").chain(function () {
                assert.deepEqual(db.sqls, ['ALTER TABLE abc RENAME TO xyz']);
            });
        });
    });

    it.describe("#tableExists", function (it) {
        var db;
        it.beforeAll(function () {
            patio.quoteIdentifiers = false;
            db = new DummyDatabase();
        });
        it.should("returns true if the table is in the list of all tables", function () {
            var a, b;
            return comb.when(
                db.tableExists("a").chain(function (ret) {
                    assert.isFalse(ret);
                }),
                db.tableExists("b").chain(function (ret) {
                    assert.isTrue(ret);
                })
            );

        });
    });


    it.describe("#transaction", function (it) {
        var db;
        it.beforeAll(function () {
            patio.quoteIdentifiers = false;
            db = new Dummy3Database();
        });

        it.should("wrap the supplied block with BEGIN + COMMIT statements", function () {
            db.reset();
            return db.transaction(function (d) {
                return d.execute('DROP TABLE test;');
            }).chain(function () {
                assert.deepEqual(db.sqls, ['BEGIN', 'DROP TABLE test;', 'COMMIT']);
            });
        });

        it.should("support transaction isolation levels", function () {
            db.reset();
            db.supportsTransactionIsolationLevels = true;
            return comb.async.array(["uncommitted", "committed", "repeatable", "serializable"]).forEach(function (level) {
                return db.transaction({isolation: level}, function (d) {
                    return d.run("DROP TABLE " + level);
                });
            }).chain(function () {
                assert.deepEqual(db.sqls, ['BEGIN', 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED', 'DROP TABLE uncommitted', 'COMMIT',
                    'BEGIN', 'SET TRANSACTION ISOLATION LEVEL READ COMMITTED', 'DROP TABLE committed', 'COMMIT',
                    'BEGIN', 'SET TRANSACTION ISOLATION LEVEL REPEATABLE READ', 'DROP TABLE repeatable', 'COMMIT',
                    'BEGIN', 'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE', 'DROP TABLE serializable', 'COMMIT']);
            });

        });

        it.should("allow specifying a default transaction isolation level", function () {
            db.reset();
            db.supportsTransactionIsolationLevels = true;
            return comb.async.array(["uncommitted", "committed", "repeatable", "serializable"]).forEach(function (level) {
                db.transactionIsolationLevel = level;
                return db.transaction(function (d) {
                    return d.run("DROP TABLE " + level);
                });
            }, 1).chain(function () {
                assert.deepEqual(db.sqls, ['BEGIN', 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED', 'DROP TABLE uncommitted', 'COMMIT',
                    'BEGIN', 'SET TRANSACTION ISOLATION LEVEL READ COMMITTED', 'DROP TABLE committed', 'COMMIT',
                    'BEGIN', 'SET TRANSACTION ISOLATION LEVEL REPEATABLE READ', 'DROP TABLE repeatable', 'COMMIT',
                    'BEGIN', 'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE', 'DROP TABLE serializable', 'COMMIT']);
            });

        });

        it.describe("isolated options", function (it) {

            it.beforeAll(function () {
                db.supportsTransactionIsolationLevels = false;
            });

            function createTransaction(db, table, timeout1, timeout2, opts) {
                var ret = new comb.Promise();
                setTimeout(function () {
                    db.transaction(opts, function () {
                        var ret = new comb.Promise();
                        setTimeout(function () {
                            db.run("DROP TABLE " + table).chain(ret.callback, ret.errback);
                        }, timeout2);
                        return ret;
                    }).chain(ret.callback, ret.errback);
                }, timeout1);
                return ret;
            }

            it.should("allow specifying a transaction as isolated", function () {
                db.reset();
                return comb.when(
                    createTransaction(db, "a", 0, 1000),
                    createTransaction(db, "b", 500, 0, {isolated: true}),
                    createTransaction(db, "c", 500, 0, {isolated: true}),
                    createTransaction(db, "d", 500, 0, {isolated: true})
                ).chain(function () {
                    assert.deepEqual(db.sqls, [
                        'BEGIN', "DROP TABLE a", 'COMMIT',
                        'BEGIN', "DROP TABLE b", 'COMMIT',
                        'BEGIN', "DROP TABLE c", 'COMMIT',
                        'BEGIN', "DROP TABLE d", 'COMMIT'
                    ]);
                });
            });

            it.should("not isolate inner transaction unless specified", function () {
                db.reset();
                return comb.when(
                    createTransaction(db, "a", 0, 600, {isolated: true}),
                    createTransaction(db, "b", 500, 0),
                    createTransaction(db, "c", 0, 700, {isolated: true}),
                    createTransaction(db, "d", 800, 0)
                ).chain(function () {
                    assert.deepEqual(db.sqls, [
                        'BEGIN', "DROP TABLE b", "DROP TABLE a", 'COMMIT',
                        'BEGIN', "DROP TABLE d", "DROP TABLE c", 'COMMIT'
                    ]);
                });
            });

            it.should("isolate transactions with isolated = true returned from an inner transaction", function () {
                db.reset();
                return db.transaction(function () {
                    return db.run("DROP TABLE a").chain(function () {
                        db.transaction({isolated: true}, function () {
                            return db.run("DROP TABLE b");
                        }).chain(function () {
                            assert.deepEqual(db.sqls, [
                                'BEGIN', "DROP TABLE a", 'COMMIT',
                                'BEGIN', "DROP TABLE b", 'COMMIT'
                            ]);
                        });
                    });
                });
            });


        });

        it.should("issue ROLLBACK if an exception is raised, and re-raise", function () {
            var db = new Dummy3Database();
            return db.transaction(function (d) {
                d.execute('DROP TABLE test');
                throw "Error";
            }).chain(assert.fail, function () {
                assert.deepEqual(db.sqls, ['BEGIN', 'DROP TABLE test', 'ROLLBACK']);
                db.reset();
                return db.transaction(function (d) {
                    return d.execute('DROP TABLE test', {error: true});
                }).chain(assert.fail, function () {
                    assert.deepEqual(db.sqls, ['BEGIN', 'DROP TABLE test', 'ROLLBACK']);
                    return db.transaction(function (d) {
                        throw "Error";
                    }).chain(assert.fail, function () {
                        return true;
                    });
                });
            });


        });

        it.should("raise database call errback if there is an error commiting", function () {
            var db = new Dummy3Database();
            db.__commitTransaction = function () {
                return new comb.Promise().errback();
            };
            return db.transaction(function (d) {
                return d.run("DROP TABLE test");
            }).chain(assert.fail, function () {
                return true;
            });
        });

    });

    it.describe("#transaction with savepoints", function (it) {

        var db = new Dummy3Database();
        db.supportsSavepoints = true;

        it.should("wrap the supplied block with BEGIN + COMMIT statements", function () {
            return db.transaction(function () {
                return db.execute("DROP TABLE test;");
            }).chain(function () {
                assert.deepEqual(db.sqls, ['BEGIN', 'DROP TABLE test;', 'COMMIT']);
            });
        });

        it.should("use savepoints if given the :savepoint option", function () {
            db.reset();
            return db.transaction(function () {
                return db.transaction({savepoint: true}, function () {
                    return db.execute('DROP TABLE test;');
                });
            }).chain(function () {
                assert.deepEqual(db.sqls, ['BEGIN', 'SAVEPOINT autopoint_1', 'DROP TABLE test;', 'RELEASE SAVEPOINT autopoint_1', 'COMMIT']);
            });
        });

        it.should("not use a savepoints if no transaction is in progress", function () {
            db.reset();
            return db.transaction({savepoint: true}, function (d) {
                return d.execute('DROP TABLE test;');
            }).chain(function () {
                assert.deepEqual(db.sqls, ['BEGIN', 'DROP TABLE test;', 'COMMIT']);
            });
        });

        it.should("reuse the current transaction if no savepoint option is given", function () {
            db.reset();
            return db.transaction(function (d) {
                return d.transaction(function (d2) {
                    return d2.execute('DROP TABLE test;');
                });
            }).chain(function () {
                assert.deepEqual(db.sqls, ['BEGIN', 'DROP TABLE test;', 'COMMIT']);
            });
        });

        it.should("handle returning inside of the block by committing", function () {
            db.reset();
            return db.retCommit().chain(function () {
                assert.deepEqual(db.sqls, ['BEGIN', 'DROP TABLE test;', 'COMMIT']);
            });
        });

        it.should("handle returning inside of a savepoint by committing", function () {
            db.reset();
            return db.retCommitSavePoint().chain(function () {
                assert.deepEqual(db.sqls, ['BEGIN', 'SAVEPOINT autopoint_1', 'DROP TABLE test;', 'RELEASE SAVEPOINT autopoint_1', 'COMMIT']);
            });
        });

        it.should("issue ROLLBACK if an exception is raised, and re-raise", function () {
            var db = new Dummy3Database();
            return db.transaction(function (d) {
                d.execute('DROP TABLE test');
                throw "Error";
            }).chain(assert.fail, function () {
                assert.deepEqual(db.sqls, ['BEGIN', 'DROP TABLE test', 'ROLLBACK']);
                db.reset();
                return db.transaction(function (d) {
                    return d.execute('DROP TABLE test', {error: true});
                }).chain(assert.fail, function () {
                    assert.deepEqual(db.sqls, ['BEGIN', 'DROP TABLE test', 'ROLLBACK']);
                    return db.transaction(function (d) {
                        throw "Error";
                    }).chain(assert.fail, function () {
                        return true;
                    });
                });
            });
        });

        it.should("issue ROLLBACK if an exception is raised inside a savepoint, and re-raise", function () {
            db.reset();
            return db.transaction(function () {
                return db.transaction({savepoint: true}, function () {
                    db.execute('DROP TABLE test');
                    throw new Error("Error");
                });
            }).chain(assert.fail, function () {
                assert.deepEqual(db.sqls, ['BEGIN', 'SAVEPOINT autopoint_1', 'DROP TABLE test', 'ROLLBACK TO SAVEPOINT autopoint_1', 'ROLLBACK']);
                db.reset();
                return db.transaction(function (d) {
                    return db.transaction({savepoint: true}, function () {
                        return db.execute('DROP TABLE test', {error: true});
                    });
                }).chain(assert.fail, function () {
                    assert.deepEqual(db.sqls, ['BEGIN', 'SAVEPOINT autopoint_1', 'DROP TABLE test', 'ROLLBACK TO SAVEPOINT autopoint_1', 'ROLLBACK']);
                    return db.transaction(function (d) {
                        return db.transaction({savepoint: true}, function () {
                            throw "ERROR";
                        });
                    }).chain(assert.fail, function () {
                        return true;
                    });
                });
            });
        });

        it.should("issue ROLLBACK SAVEPOINT if 'ROLLBACK' is called is thrown in savepoint", function () {
            db.reset();
            return db.transaction(function () {
                return db.transaction({savepoint: true}, function () {
                    return db.dropTable("a").chain(function () {
                        throw "ROLLBACK";
                    });
                }).chain(comb(db).bindIgnore("dropTable", "b"));
            }).chain(function () {
                assert.deepEqual(db.sqls, ['BEGIN', 'SAVEPOINT autopoint_1', 'DROP TABLE a', 'ROLLBACK TO SAVEPOINT autopoint_1', "DROP TABLE b", 'COMMIT']);
            });
        });


        it.should("raise database errors when commiting a transaction and error is thrown", function () {
            var orig = db.__commitTransaction;
            db.__commitTransaction = function () {
                return new comb.Promise().errback("ERROR");
            };
            return db.transaction(function (db, done) {
                done();
            }).chain(assert.fail, function () {
                return db.transaction(function () {
                    return db.transaction({savepoint: true}, function (db, done) {
                        done();
                    });
                }).chain(assert.fail, function () {
                    db.__commitTransaction = orig;
                });
            });


        });

    });

    it.describe("#fetch", function (it) {
        var CDS = comb.define(patio.Dataset, {
            instance: {

                fetchRows: function (sql, cb) {
                    return comb.async.array(new comb.Promise().callback({sql: sql}).addCallback(cb));
                }
            }
        });
        var TestDB = comb.define(patio.Database, {
            instance: {
                getters: {
                    dataset: function () {
                        return new CDS(this);
                    }
                }
            }
        });
        var db = new TestDB();

        it.should("create a dataset and invoke its fetch_rows method with the given sql", function () {
            var sql = null;
            db.fetch('select * from xyz').forEach(function (r) {
                assert.equal(r.sql, 'select * from xyz');
            });
        });

        it.should("format the given sql with any additional arguments", function () {
            var sql = null;
            return comb.when(
                db.fetch('select * from xyz where x = ? and y = ?', 15, 'abc', function (r) {
                    assert.equal(r.sql, "select * from xyz where x = 15 and y = 'abc'");
                }),
                db.fetch('select name from table where name = ? or id in ?', 'aman', [3, 4, 7], function (r) {
                    assert.equal(r.sql, "select name from table where name = 'aman' or id in (3, 4, 7)");
                })
            );
        });

        it.should("format the given sql with named arguments", function () {
            var sql = null;
            return db.fetch('select * from xyz where x = {x} and y = {y}', {x: 15, y: 'abc'}, function (r) {
                assert.equal(r.sql, "select * from xyz where x = 15 and y = 'abc'");
            });
        });

        it.should("return the dataset if no block is given", function () {
            assert.instanceOf(db.fetch('select * from xyz'), patio.Dataset);
            return db.fetch('select a from b').map(
                function (r) {
                    return r.sql;
                }).chain(function (r) {
                assert.deepEqual(r, ['select a from b']);
            });
        });

        it.should("return a dataset that always uses the given sql for SELECTs", function () {
            var ds = db.fetch('select * from xyz');
            assert.equal(ds.selectSql, 'select * from xyz');
            assert.equal(ds.sql, 'select * from xyz');
            ds.filter(function () {
                return this.price.sqlNumber.lt(100);
            });
            assert.equal(ds.selectSql, 'select * from xyz');
            assert.equal(ds.sql, 'select * from xyz');
        });
    });

    it.describe("#createView", function (it) {
        var db;
        it.beforeAll(function () {
            patio.quoteIdentifiers = false;
            db = new DummyDatabase();
        });

        it.should("construct proper SQL with raw SQL", function () {
            db.createView("test", "SELECT * FROM xyz");
            assert.deepEqual(db.sqls, ['CREATE VIEW test AS SELECT * FROM xyz']);
            db.reset();
            db.createView(sql.test, "SELECT * FROM xyz");
            assert.deepEqual(db.sqls, ['CREATE VIEW test AS SELECT * FROM xyz']);
        });

        it.should("construct proper SQL with dataset", function () {
            db.reset();
            db.createView("test", db.from("items").select("a", "b").order("c"));
            assert.deepEqual(db.sqls, ['CREATE VIEW test AS SELECT a, b FROM items ORDER BY c']);
            db.reset();
            db.createView(sql.test.qualify("sch"), db.from("items").select("a", "b").order("c"));
            assert.deepEqual(db.sqls, ['CREATE VIEW sch.test AS SELECT a, b FROM items ORDER BY c']);
        });
    });

    it.describe("#createorReplaceView", function (it) {
        var db;
        it.beforeAll(function () {
            patio.quoteIdentifiers = false;
            db = new DummyDatabase();
        });

        it.should("construct proper SQL with raw SQL", function () {
            db.createOrReplaceView("test", "SELECT * FROM xyz");
            assert.deepEqual(db.sqls, ['CREATE OR REPLACE VIEW test AS SELECT * FROM xyz']);
            db.reset();
            db.createOrReplaceView(sql.test, "SELECT * FROM xyz");
            assert.deepEqual(db.sqls, ['CREATE OR REPLACE VIEW test AS SELECT * FROM xyz']);
        });

        it.should("construct proper SQL with dataset", function () {
            db.reset();
            db.createOrReplaceView("test", db.from("items").select("a", "b").order("c"));
            assert.deepEqual(db.sqls, ['CREATE OR REPLACE VIEW test AS SELECT a, b FROM items ORDER BY c']);
            db.reset();
            db.createOrReplaceView(sql.test.qualify("sch"), db.from("items").select("a", "b").order("c"));
            assert.deepEqual(db.sqls, ['CREATE OR REPLACE VIEW sch.test AS SELECT a, b FROM items ORDER BY c']);
        });
    });

    it.describe("#dropView", function (it) {
        var db;
        it.beforeAll(function () {
            patio.quoteIdentifiers = false;
            db = new DummyDatabase();
        });

        it.should("construct proper SQL", function () {
            return comb.when(
                db.dropView("test"),
                db.dropView(sql.identifier("test")),
                db.dropView('sch__test'),
                db.dropView(sql.test.qualify('sch'))
            ).chain(function () {
                assert.deepEqual(db.sqls, ['DROP VIEW test', 'DROP VIEW test', 'DROP VIEW sch.test', 'DROP VIEW sch.test']);
            }, console.log);
        });
    });

    it.describe("#__alterTableSql", function (it) {
        var db;
        it.beforeAll(function () {
            patio.quoteIdentifiers = false;
            db = new DummyDatabase();
        });

        it.should("raise error for an invalid op", function () {
            assert.throws(hitch(db, "__alterTableSql", "mau", {op: "blah"}));
        });
    });

    it.describe("#get", function (it) {


        var db = new (comb.define(DummyDatabase, {
            instance: {
                getters: {
                    dataset: function () {
                        var ds = new MockDataset(this);
                        ds.get = function () {
                            return this.db.execute(this.select.apply(this, arguments).sql);
                        };
                        return ds;
                    }
                }
            }
        }))();


        it.should("use Dataset#get to get a single value", function () {
            var ret;
            db.get(1);
            assert.equal(db.sqls.pop(), 'SELECT 1');

            db.get(sql.version.sqlFunction);
            assert.equal(db.sqls.pop(), 'SELECT version()');
        });

        it.should("accept a block", function () {
            db.get(function () {
                return 1;
            });
            assert.equal(db.sqls.pop(), 'SELECT 1');

            db.get(function () {
                return this.version(1);
            });
            assert.equal(db.sqls.pop(), 'SELECT version(1)');
        });
    });

    it.describe("#typecastValue", function (it) {
        var db = new Database();

        it.should("type cast properly", function () {
            assert.deepEqual(db.typecastValue("blob", "some blob string"), new Buffer("some blob string"));
            assert.deepEqual(db.typecastValue("blob", [1, 2, 3]), new Buffer([1, 2, 3]));
            assert.equal(db.typecastValue("boolean", "true"), true);
            assert.equal(db.typecastValue("boolean", "false"), false);
            assert.equal(db.typecastValue("integer", "5"), 5);
            assert.equal(db.typecastValue("float", "5.5"), 5.5);
            assert.equal(db.typecastValue("decimal", "5.5"), 5.5);
            assert.deepEqual(db.typecastValue("date", "2011-10-5"), new Date(2011, 9, 5));
            assert.deepEqual(db.typecastValue("time", "10:10:10"), new sql.Time(10, 10, 10));
            assert.deepEqual(db.typecastValue("datetime", "2011-10-5 10:10:10"), new sql.DateTime(2011, 9, 5, 10, 10, 10));
            assert.deepEqual(db.typecastValue("timestamp", "2011-10-5 10:10:10"), new sql.DateTime(2011, 9, 5, 10, 10, 10));
            assert.deepEqual(db.typecastValue("year", "2011"), new sql.Year(2011));
            assert.deepEqual(db.typecastValue("json", JSON.stringify({a: "b"})), {a: "b"});
            assert.deepEqual(db.typecastValue("json", {a: "b"}), {a: "b"});
            assert.instanceOf(db.typecastValue("json", JSON.stringify({a: "b"})), sql.Json);
            assert.instanceOf(db.typecastValue("json", {a: "b"}), sql.Json);
        });

        it.should("throw an InvalidValue when given an invalid value", function () {
            assert.throws(hitch(db, "typecastValue", "blob", 1));
            assert.throws(hitch(db, "typecastValue", "blob", true));
            assert.throws(hitch(db, "typecastValue", "integer", "a"));
            assert.throws(hitch(db, "typecastValue", "float", "a.a2"));
            assert.throws(hitch(db, "typecastValue", "decimal", "invalidValue"));
            assert.throws(hitch(db, "typecastValue", "date", "a"));
            assert.throws(hitch(db, "typecastValue", "date", {}));
            assert.throws(hitch(db, "typecastValue", "time", "v"));
            assert.throws(hitch(db, "typecastValue", "dateTime", "z"));
            assert.throws(hitch(db, "typecastValue", "json", "z"));
            assert.throws(hitch(db, "typecastValue", "json", true));
        });

    });

    it.describe("#typeLiteral", function (it) {

        var db = new Database();
        var typeLiteral = function (type, opts) {
            return db.typeLiteral(comb.merge({type: type}, opts));
        };

        it.should("convert Object constructors to types", function () {
            assert.equal(typeLiteral(String), "varchar(255)");
            assert.equal(typeLiteral(String, {size: 25}), "varchar(25)");
            assert.equal(typeLiteral(Buffer), "blob");
            assert.equal(typeLiteral(Number), "numeric");
            assert.equal(typeLiteral(Number, {size: 2}), "numeric(2)");
            assert.equal(typeLiteral(Number, {isInt: true}), "integer");
            assert.equal(typeLiteral(Number, {isDouble: true}), "double precision");
            assert.equal(typeLiteral(sql.Float, {isDouble: true}), "double precision");
            assert.equal(typeLiteral(sql.Decimal, {isDouble: true}), "double precision");
            assert.equal(typeLiteral(Date), "date");
            assert.equal(typeLiteral(sql.Time), "time");
            assert.equal(typeLiteral(Date, {onlyTime: true}), "time");
            assert.equal(typeLiteral(sql.TimeStamp), "timestamp");
            assert.equal(typeLiteral(Date, {timeStamp: true}), "timestamp");
            assert.equal(typeLiteral(sql.DateTime), "datetime");
            assert.equal(typeLiteral(Date, {dateTime: true}), "datetime");
            assert.equal(typeLiteral(sql.Year), "year");
            assert.equal(typeLiteral(Date, {yearOnly: true}), "year");
            assert.equal(typeLiteral(Boolean), "boolean");
            assert.equal(typeLiteral(sql.Json), "json");

        });

        it.should("convert strings", function () {
            assert.equal(typeLiteral("string"), "varchar(255)");
            assert.equal(typeLiteral("buffer"), "blob");
            assert.equal(typeLiteral("number"), "numeric");
            assert.equal(typeLiteral("number", {size: 2}), "numeric(2)");
            assert.equal(typeLiteral("number", {isInt: true}), "integer");
            assert.equal(typeLiteral("number", {isDouble: true}), "double precision");
            assert.equal(typeLiteral("float", {isDouble: true}), "double precision");
            assert.equal(typeLiteral("decimal", {isDouble: true}), "double precision");
            assert.equal(typeLiteral("date"), "date");
            assert.equal(typeLiteral("time"), "time");
            assert.equal(typeLiteral("date", {onlyTime: true}), "time");
            assert.equal(typeLiteral("timestamp"), "timestamp");
            assert.equal(typeLiteral("date", {timeStamp: true}), "timestamp");
            assert.equal(typeLiteral("datetime"), "datetime");
            assert.equal(typeLiteral("date", {dateTime: true}), "datetime");
            assert.equal(typeLiteral("year"), "year");
            assert.equal(typeLiteral("date", {yearOnly: true}), "year");
            assert.equal(typeLiteral("boolean"), "boolean");
            assert.equal(typeLiteral("json"), "json");
        });

        it.should("support user defined types", function () {
            assert.equal(typeLiteral("double"), "double precision");
            assert.equal(typeLiteral("varchar"), "varchar(255)");
            assert.equal(typeLiteral("varchar", {size: 255}), "varchar(255)");
            assert.equal(typeLiteral("tiny blob"), "tiny blob");
        });

    });

    it.describe("#__columnSchemaToJsDefault", function (it) {
        var db = new Database();

        it.should("handle converting many default formats ", function () {
            var m = hitch(db, db.__columnSchemaToJsDefault);
            assert.equal(m(null, "integer"), null);
            assert.equal(m("1", "integer"), 1);
            assert.equal(m("-1", "integer"), -1);
            assert.equal(m("1.0", "float"), 1.0);
            assert.equal(m("-1.0", "float"), -1.0);
            assert.equal(m("1.0", "decimal"), 1.0);
            assert.equal(m("-1.0", "decimal"), -1.0);
            assert.equal(m("1", "boolean"), true);
            assert.equal(m("0", "boolean"), false);
            assert.equal(m("true", "boolean"), true);
            assert.equal(m("false", "boolean"), false);
            assert.equal(m("t", "boolean"), true);
            assert.equal(m("f", "boolean"), false);
            assert.equal(m("'a'", "string"), "a");
            assert.equal(m("''", "string"), "");
            assert.equal(m("'||a''b'", "string"), "||a'b");
            assert.equal(m("'NULL'", "string"), "NULL");
            assert.deepEqual(m("'2009-10-29'", "date"), comb.date.parse('2009-10-29', 'yyyy-MM-dd'));
            assert.equal(m("CURRENT_TIMESTAMP", "date"), null);
            assert.equal(m("today()", "date"), null);
            assert.deepEqual(m("'2009-10-29 10:20:30-07:00'", "datetime"), new sql.DateTime(comb.date.parse('2009-10-29 10:20:30-07:00', 'yyyy-MM-dd HH:mm:ssZ')));
            assert.deepEqual(m("'2009-10-29 10:20:30'", "datetime"), new sql.DateTime(comb.date.parse('2009-10-29 10:20:30', 'yyyy-MM-dd HH:mm:ss')));
            assert.deepEqual(m("'2009-10-29 10:20:30'", "timestamp"), new sql.TimeStamp(comb.date.parse('2009-10-29 10:20:30', 'yyyy-MM-dd HH:mm:ss')));
            assert.deepEqual(m("'10:20:30'", "time"), new sql.Time(comb.date.parse("10:20:30", "HH:mm:ss")));
            assert.deepEqual(m("'2002'", "year"), new sql.Year(comb.date.parse("2002", "yyyy")));

            assert.deepEqual(m("'hello this is a binary string'", "blob"), new Buffer("hello this is a binary string"));
            assert.isNull(m("NaN", "float"));

            db.type = "postgres";
            assert.equal(m("''::text", "string"), "");
            assert.equal(m("'\\a''b'::character varying", "string"), "\\a'b");
            assert.equal(m("'a'::bpchar", "string"), "a");
            assert.equal(m("(-1)", "integer"), -1);
            assert.equal(m("(-1.0)", "float"), -1.0);
            assert.equal(m('(-1.0)', "decimal"), -1.0);
            assert.deepEqual(m("'2009-10-29'::date", "date"), comb.date.parse("2009-10-29", "yyyy-MM-dd"));
            assert.deepEqual(m("'2009-10-29 10:20:30.241343'::timestamp without time zone", "datetime"), new sql.DateTime(comb.date.parse("2009-10-29 10:20:30.241343", "yyyy-MM-dd HH:mm:ss.SSZ")));
            assert.deepEqual(m("'10:20:30'::time without time zone", "time"), new sql.Time(comb.date.parse("10:20:30", "HH:mm:ss")));

            db.type = "mysql";
            assert.equal(m("\\a'b", "string"), "\\a'b");
            assert.equal(m("a", "string"), "a");
            assert.equal(m("NULL", "string"), "NULL");
            assert.equal(m("-1", "float"), -1.0);
            assert.equal(m('-1', "decimal"), -1.0);
            assert.deepEqual(m("2009-10-29", "date"), comb.date.parse("2009-10-29", "yyyy-MM-dd"));
            assert.deepEqual(m("2009-10-29 10:20:30", "datetime"), new sql.DateTime(comb.date.parse('2009-10-29 10:20:30', 'yyyy-MM-dd HH:mm:ss')));
            assert.deepEqual(m("10:20:30", "time"), new sql.Time(comb.date.parse("10:20:30", "HH:mm:ss")));
            assert.equal(m("CURRENT_DATE", "date"), null);
            assert.equal(m("CURRENT_TIMESTAMP", "datetime"), null);
            assert.equal(m("CURRENT_TIMESTAMP", "timestamp"), null);
            assert.equal(m("a", "enum"), "a");

            db.type = "mssql";
            assert.equal(m("(N'a')", "string"), "a");
            assert.equal(m("((-12))", "integer"), -12);
            assert.equal(m("((12.1))", "float"), 12.1);
            assert.equal(m("((-12.1))", "decimal"), -12.1);
        });
    });


    it.describe("SQL.Constants", function (it) {
        var db = new MockDatabase();

        it.should("have CURRENT_DATE", function () {
            assert.equal(db.literal(patio.SQL.Constants.CURRENT_DATE), 'CURRENT_DATE');
            assert.equal(db.literal(patio.CURRENT_DATE), 'CURRENT_DATE');
        });

        it.should("have CURRENT_TIME", function () {
            assert.equal(db.literal(patio.SQL.Constants.CURRENT_TIME), 'CURRENT_TIME');
            assert.equal(db.literal(patio.CURRENT_TIME), 'CURRENT_TIME');
        });

        it.should("have CURRENT_TIMESTAMP", function () {
            assert.equal(db.literal(patio.SQL.Constants.CURRENT_TIMESTAMP), 'CURRENT_TIMESTAMP');
            assert.equal(db.literal(patio.CURRENT_TIMESTAMP), 'CURRENT_TIMESTAMP');
        });

        it.should("have NULL", function () {
            assert.equal(db.literal(patio.SQL.Constants.NULL), 'NULL');
            assert.equal(db.literal(patio.NULL), 'NULL');
        });

        it.should("have NOTNULL", function () {
            assert.equal(db.literal(patio.SQL.Constants.NOTNULL), 'NOT NULL');
            assert.equal(db.literal(patio.NOTNULL), 'NOT NULL');
        });

        it.should("have TRUE and SQLTRUE", function () {
            assert.equal(db.literal(patio.SQL.Constants.TRUE), '1');
            assert.equal(db.literal(patio.TRUE), '1');
            assert.equal(db.literal(patio.SQL.Constants.SQLTRUE), '1');
            assert.equal(db.literal(patio.SQLTRUE), '1');
        });

        it.should("have FALSE and SQLFALSE", function () {
            assert.equal(db.literal(patio.SQL.Constants.FALSE), '0');
            assert.equal(db.literal(patio.FALSE), '0');
            assert.equal(db.literal(patio.SQL.Constants.SQLFALSE), '0');
            assert.equal(db.literal(patio.SQLFALSE), '0');
        });
    });

    it.describe("logging features", function (it) {

        var mockAppender = new (comb.define(comb.logging.appenders.Appender, {
            instance: {

                messages: null,

                constructor: function () {
                    this.messages = [];
                },

                append: function (message) {
                    this.messages.push(message);
                },

                reset: function () {
                    this.messages.length = 0;
                }
            }
        }))();
        var logger, db;
        it.beforeAll(function () {
            db = new MockDatabase();
            logger = db.logger;
            logger.addAppender(mockAppender);

        });

        it.beforeEach(function () {
            mockAppender.reset();
        });

        it.should("have a logger on an instance", function () {
            assert.isNotNull(db.logger);
        });

        it.should("have a logger on the constructor", function () {
            assert.isNotNull(MockDatabase.logger);
        });

        ["logError", "logFatal", "logWarn", "logTrace", "logDebug", "logInfo"].forEach(function (type) {
            it.describe("#" + type, function (it) {

                it.should("log on class", function () {
                    MockDatabase[type](type);
                    var messages = mockAppender.messages;
                    assert.lengthOf(messages, 1);
                    assert.isNotNull(messages[0].message.match(type));
                    assert.equal(messages[0].levelName, type.replace("log", "").toUpperCase());
                });

                it.should("log on instance", function () {
                    db[type](type);
                    var messages = mockAppender.messages;
                    assert.lengthOf(messages, 1);
                    assert.isNotNull(messages[0].message.match(type));
                    assert.equal(messages[0].levelName, type.replace("log", "").toUpperCase());
                });
            });
        });


        it.afterAll(function () {
            logger.removeAppender(mockAppender);
        });

    });

    it.afterAll(function () {
        comb.hitch(patio, "disconnect");
    });
});
