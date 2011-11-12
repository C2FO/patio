var vows = require('vows'),
    assert = require('assert'),
    moose = require("../../lib"),
    Database = moose.Database,
    SQL = require("../../lib/sql"),
    ConnectionPool = require("../../lib/ConnectionPool"),
    sql = SQL.sql,
    Identifier = sql.Identifier,
    SQLFunction = sql.SQLFunction,
    LiteralString = SQL.LiteralString,
    helper = require("../helpers/helper"),
    MockDatabase = helper.MockDatabase,
    SchemaDatabase = helper.SchemaDatabase,
    MockDataset = helper.MockDataset,
    comb = require("comb"),
    hitch = comb.hitch;

new comb.logging.BasicConfigurator().configure();
var ret = (module.exports = exports = new comb.Promise());
var suite = vows.describe("Database");

var DummyDataset = comb.define(moose.Dataset, {
    instance : {
        first : function() {
            var ret = new comb.Promise();
            if (this.__opts.from[0] == "a") {
                ret.errback();
            } else {
                ret.callback();
            }
            return ret;
        }
    }
});
var DummyDatabase = comb.define(moose.Database, {
    instance : {
        constructor : function() {
            this.super(arguments);
            this.sqls = [];
        },

        execute : function(sql, opts) {
            var ret = new comb.Promise();
            this.sqls.push(sql);
            ret.callback();
            return ret;
        },

        executeError : function() {
            var ret = new comb.Promise();
            this.execute.apply(this, arguments).then(comb.hitch(ret, 'errback'), comb.hitch(ret, 'errback'));
            return ret;
        },

        reset : function() {
            this.sqls = [];
        },

        transaction : function(opts, cb) {
            var ret = new comb.Promise();
            cb();
            ret.callback();
            return ret;
        },

        getters : {
            dataset : function() {
                return new DummyDataset(this);
            }
        }
    }
});

var DummyConnection = comb.define(null, {
    instance : {
        constructor : function(db) {
            this.db = db;
        },

        execute : function() {
            return this.db.execute.apply(this.db, arguments);
        }
    }
});

var Dummy3Database = comb.define(Database, {
    instance : {
        constructor : function() {
            this.super(arguments);
            this.sqls = [];
        },

        execute : function(sql, opts) {
            opts = opts || {};
            var ret = new comb.Promise();
            this.sqls.push(sql);
            ret[opts.error ? "errback" : "callback"](opts.error ? "ERROR" : "");
            return ret;
        },

        createConnection : function(options) {
            return new DummyConnection(this);
        },

        retCommit : function() {
            this.transaction(function() {
                this.execute('DROP TABLE test;');
                return;
                this.execute('DROP TABLE test2;');
            });
        },

        retCommitSavePoint : function() {
            this.transaction(function() {
                this.transaction({savepoint : true}, function() {
                    this.execute('DROP TABLE test;');
                    return;
                    this.execute('DROP TABLE test2;');
                })
            });
        },

        closeConnection : function(conn) {
            return new comb.Promise().callback();
        },

        validate : function(conn) {
            return new comb.Promise().callback(true);
        },

        reset : function() {
            this.sqls = [];
        }
    }
});

suite.addBatch({

    "A new Database" : {
        topic : new Database({1 : 2}),

        "should receive options" : function(db) {
            assert.equal(db.opts[1], 2);
        },

        "should create a connection pool" : function(db) {
            assert.isTrue(comb.isInstanceOf(db.pool, ConnectionPool));
            assert.equal(db.pool.maxObjects, 10);
            assert.equal(new Database({maxConnections : 4}).pool.maxObjects, 4);
        },

        "should respect the quoteIdentifiers option" : function(db) {
            var db1 = new Database({quoteIdentifiers : false});
            var db2 = new Database({quoteIdentifiers : true});
            assert.isFalse(db1.quoteIdentifiers);
            assert.isTrue(db2.quoteIdentifiers);
        },

        "should toUpperCase on input and toLowerCase on output by default" : function() {
            var db = new Database();
            assert.equal(db.identifierInputMethodDefault, "toUpperCase");
            assert.equal(db.identifierOutputMethodDefault, "toLowerCase");
        },

        "should respect the identifierInputMethod option" : function() {
            var db = new Database({identifierInputMethod : null});
            assert.isNull(db.identifierInputMethod);
            db.identifierInputMethod = "toUpperCase";
            assert.equal(db.identifierInputMethod, 'toUpperCase');
            db = new Database({identifierInputMethod : 'toUpperCase'});
            assert.equal(db.identifierInputMethod, "toUpperCase");
            db.identifierInputMethod = null;
            assert.isNull(db.identifierInputMethod);
            moose.identifierInputMethod = "toLowerCase";
            assert.equal(Database.identifierInputMethod, 'toLowerCase');
            assert.equal(new Database().identifierInputMethod, 'toLowerCase');
        },

        "should respect the identifierOutputMethod option" : function() {
            var db = new Database({identifierOutputMethod : null});
            assert.isNull(db.identifierOutputMethod);
            db.identifierOutputMethod = "toLowerCase";
            assert.equal(db.identifierOutputMethod, 'toLowerCase');
            db = new Database({identifierOutputMethod : 'toLowerCase'});
            assert.equal(db.identifierOutputMethod, "toLowerCase");
            db.identifierOutputMethod = null;
            assert.isNull(db.identifierOutputMethod);
            moose.identifierOutputMethod = "toUpperCase";
            assert.equal(Database.identifierOutputMethod, 'toUpperCase');
            assert.equal(new Database().identifierOutputMethod, 'toUpperCase');
        },

        "should respect the quoteIdentifiers option" : function() {
            moose.quoteIdentifiers = true;
            assert.isTrue(new Database().quoteIdentifiers);
            moose.quoteIdentifiers = false;
            assert.isFalse(new Database().quoteIdentifiers);

            Database.quoteIdentifiers = true;
            assert.isTrue(new Database().quoteIdentifiers);
            Database.quoteIdentifiers = false;
            assert.isFalse(new Database().quoteIdentifiers);
        },

        "should respect the quoteIndentifiersDefault method if moose.quoteIdentifiers = null" : function() {
            moose.quoteIdentifiers = null;
            assert.isTrue(new Database().quoteIdentifiers);
            var x = comb.define(Database, {instance : {getters : {quoteIdentifiersDefault : function() {
                return false;
            }}}});
            var y = comb.define(Database, {instance : {getters : {quoteIdentifiersDefault : function() {
                return true;
            }}}});
            assert.isFalse(new x().quoteIdentifiers);
            assert.isTrue(new y().quoteIdentifiers);
        },

        "should respect the identifierInputMethodDefault method if moose.identifierInputMethod = null" : function() {
            moose.identifierInputMethod = null;
            assert.equal(new Database().identifierInputMethod, "toUpperCase");
            var x = comb.define(Database, {instance : {getters : {identifierInputMethodDefault : function() {
                return "toLowerCase";
            }}}});
            var y = comb.define(Database, {instance : {getters : {identifierInputMethodDefault : function() {
                return "toUpperCase";
            }}}});
            assert.equal(new x().identifierInputMethod, "toLowerCase");
            assert.equal(new y().identifierInputMethod, "toUpperCase");
        },

        "should respect the identifierOutputMethodDefault method if moose.identifierOutputMethod = null" : function() {
            moose.identifierOutputMethod = null;
            assert.equal(new Database().identifierOutputMethod, "toLowerCase");
            var x = comb.define(Database, {instance : {getters : {identifierOutputMethodDefault : function() {
                return "toLowerCase";
            }}}});
            var y = comb.define(Database, {instance : {getters : {identifierOutputMethodDefault : function() {
                return "toUpperCase";
            }}}});
            assert.equal(new x().identifierOutputMethod, "toLowerCase");
            assert.equal(new y().identifierOutputMethod, "toUpperCase");
        },

        "should just use a :uri option for mysql with the full connection string" : function() {
            var db = moose.connect('mysql://host/db_name')
            assert.isTrue(comb.isInstanceOf(db, Database));
            assert.equal(db.opts.uri, 'mysql://host/db_name');
            assert.equal(db.type, "mysql");
        }
    }
});

suite.addBatch({
    "Database.disconnect" : {
        topic : new MockDatabase(),

        "should call pool.disconnect" : function(db) {
            db.pool.getConnection();
            db.disconnect();
            assert.equal(db.createdCount, 1);
            assert.equal(db.closedCount, 1);
        }
    },

    "Database.connect" : {
        topic : function() {
            return Database
        },

        "should throw an error" : function(DB) {
            assert.throws(function() {
                new DB.connect();
            })
        }
    },

    "Database.logInfo" : {
        topic : new Database(),

        "should log message at info " : function(db) {
            var orig = console.log;
            var messages = [];
            console.log = function(str) {
                assert.isTrue(str.match(/blah$/) != null);
            };
            db.logInfo("blah");
            console.log = orig;

        }
    },

    "Database.logDebug" : {
        topic : new Database(),

        "should log message at debug " : function(db) {
            var orig = console.log;
            var messages = [];
            console.log = function(str) {
                assert.isTrue(str.match(/blah$/) != null);
            };
            db.logDebug("blah");
            console.log = orig;

        }
    },

    "Database.logWarn" : {
        topic : new Database(),

        "should log message at warn " : function(db) {
            var orig = console.log;
            var messages = [];
            console.log = function(str) {
                assert.isTrue(str.match(/blah/) != null);
            };
            db.logWarn("blah");
            console.log = orig;
        }
    },

    "Database.logError" : {
        topic : new Database(),

        "should log message at error " : function(db) {
            var orig = console.log;
            var messages = [];
            console.log = function(str) {
                assert.isTrue(str.match(/blah/) != null);
            };
            db.logError("blah");
            console.log = orig;
        }
    },

    "Database.logFatal" : {
        topic : new Database(),

        "should log message at fatal " : function(db) {
            var orig = console.log;
            var messages = [];
            console.log = function(str) {
                assert.isTrue(str.match(/blah/) != null);
            };
            db.logFatal("blah");
            console.log = orig;
        }
    },

    "Database.__logAndExecute" : {
        topic : new Database(),

        "should log message and call cb " : function(db) {
            var orig = console.log;
            var messages = [];
            console.log = function(str) {
                assert.isTrue(str.match(/blah/) != null);
            };
            var a = null;
            db.__logAndExecute("blah", function() {
                var ret = new comb.Promise().callback();
                a = 1;
                return ret;
            });
            assert.equal(a, 1);
            console.log = orig;
        },

        "should raise an error if a block is not passed" : function(db) {
            assert.throws(function() {
                db.__logAndExecute("blah");
            });
        }
    },

    "Database.uri" : {
        topic : moose.connect('mau://user:pass@localhost:9876/maumau'),

        "should return the connection URI for the database" : function(db) {
            assert.isTrue(comb.isInstanceOf(db, MockDatabase));
            assert.equal(db.uri, 'mau://user:pass@localhost:9876/maumau');
            assert.equal(db.url, 'mau://user:pass@localhost:9876/maumau');
        }
    },

    "Database.type and setAdapterType" : {
        topic : function() {
            return Database
        },

        "should return the database type" : function(DB) {
            assert.equal(DB.type, "default");
            assert.equal(MockDatabase.type, "mau");
            assert.equal(new MockDatabase().type, "mau");
        }
    },

    "Database.dataset" : {
        topic : function() {
            moose.quoteIdentifiers = false;
            var db = new Database();
            return {
                db : db,
                ds : db.dataset
            }
        },


        "should provide a blank dataset through #dataset" : function(o) {
            var ds = o.ds, db = o.db;
            assert.isTrue(comb.isInstanceOf(ds, moose.Dataset));
            assert.isTrue(comb.isEmpty(ds.__opts));
            assert.equal(o.ds.db, o.db);
        },

        "should provide a #from dataset"  : function(o) {
            var ds = o.ds, db = o.db;
            var d = db.from("mau");
            assert.instanceOf(d, moose.Dataset);
            assert.equal(d.sql, "SELECT * FROM mau");
            d = db.from("miu");
            assert.instanceOf(d, moose.Dataset);
            assert.equal(d.sql, "SELECT * FROM miu");

        },

        "should provide a filtered #from dataset if a block is given"  : function(o) {
            var ds = o.ds, db = o.db;
            var d = db.from("mau", function() {
                return this.x.sqlNumber.gt(100)
            });
            assert.instanceOf(d, moose.Dataset);
            assert.equal(d.sql, 'SELECT * FROM mau WHERE (x > 100)');
        },

        "should provide a #select dataset"  : function(o) {
            var ds = o.ds, db = o.db;
            var d = db.select("a", "b", "c").from("mau");
            assert.instanceOf(d, moose.Dataset);
            assert.equal(d.sql, 'SELECT a, b, c FROM mau');
        },

        "should allow #select to take a block"  : function(o) {
            var ds = o.ds, db = o.db;
            var d = db.select("a", "b",
                function() {
                    return "c"
                }).from("mau");
            assert.instanceOf(d, moose.Dataset);
            assert.equal(d.sql, 'SELECT a, b, c FROM mau');
        }
    },

    "Database.execute" : {

        topic : function() {
            return Database
        },

        "should raise NotImplemented" : function(DB) {
            assert.throws(function() {
                new DB().execute("hello");
            });
        }
    },

    "Database.tables" : {

        topic : function() {
            return Database
        },

        "should raise NotImplemented" : function(DB) {
            assert.throws(function() {
                new DB().tables();
            });
        }
    },

    "Database.indexes" : {

        topic : function() {
            return Database
        },

        "should raise NotImplemented" : function(DB) {
            assert.throws(function() {
                new DB().indexes();
            });
        }
    },

    "Database.run" : {
        topic : function() {
            return new (comb.define(Database, {
                instance : {

                    constructor : function() {
                        this.super(arguments);
                        this.sqls = [];
                    },

                    executeDdl : function() {
                        var ret = new comb.Promise().callback();
                        this.sqls.length = 0;
                        this.sqls = this.sqls.concat(comb.argsToArray(arguments));
                        return ret;
                    }
                }
            }));
        },

        "should pass the supplied arguments to executeDdl" : function(db) {
            db.run("DELETE FROM items");
            assert.deepEqual(db.sqls, ["DELETE FROM items", {}]);
            db.run("DELETE FROM items2", {hello : "world"});
            assert.deepEqual(db.sqls, ["DELETE FROM items2", {hello : "world"}]);
        }
    },

    "Database.createTable" : {
        topic : function() {
            return DummyDatabase
        },

        "should construct the proper SQL" : function(DB) {
            moose.quoteIdentifiers = false;
            var db = new DB();
            db.createTable("test", function(table) {
                table.primaryKey("id", "integer", {null : false});
                table.column("name", "text");
                table.index("name", {unique : true});
            });

            assert.deepEqual(db.sqls, [
                'CREATE TABLE test (id integer NOT NULL PRIMARY KEY AUTOINCREMENT, name text)',
                'CREATE UNIQUE INDEX test_name_index ON test (name)'
            ]);
            moose.quoteIdentifiers = true;
            db = new DB();
            db.createTable("test", function(table) {
                table.primaryKey("id", "integer", {null : false});
                table.column("name", "text");
                table.index("name", {unique : true});
            });

            assert.deepEqual(db.sqls, [
                'CREATE TABLE "test" ("id" integer NOT NULL PRIMARY KEY AUTOINCREMENT, "name" text)',
                'CREATE UNIQUE INDEX "test_name_index" ON "test" ("name")'
            ]);
        },





        "should create a temporary table" : function(DB) {
            moose.quoteIdentifiers = true;
            var db = new DB();
            db.createTable("test", {temp : true}, function(table) {
                table.primaryKey("id", "integer", {null : false});
                table.column("name", "text");
                table.index("name", {unique : true});
            });

            assert.deepEqual(db.sqls, [
                'CREATE TEMPORARY TABLE "test" ("id" integer NOT NULL PRIMARY KEY AUTOINCREMENT, "name" text)',
                'CREATE UNIQUE INDEX "test_name_index" ON "test" ("name")'
            ]);

            moose.quoteIdentifiers = false;
            db = new DB();
            db.createTable("test", {temp : true}, function(table) {
                table.primaryKey("id", "integer", {null : false});
                table.column("name", "text");
                table.index("name", {unique : true});
            });

            assert.deepEqual(db.sqls, [
                'CREATE TEMPORARY TABLE test (id integer NOT NULL PRIMARY KEY AUTOINCREMENT, name text)',
                'CREATE UNIQUE INDEX test_name_index ON test (name)'
            ]);
        }
    },

    "Database.alterTable" : {
        topic : function() {
            moose.quoteIdentifiers = false;
            return DummyDatabase
        },

        "should construct proper SQL" : function(DB) {
            var db = new DB();

            db.alterTable("xyz", function(table) {
                table.addColumn("aaa", "text", {null : false, unique : true});
                table.dropColumn("bbb");
                table.renameColumn("ccc", "ddd");
                table.setColumnType("eee", "integer");
                table.setColumnDefault("hhh", 'abcd');
                table.addIndex("fff", {unique : true});
                table.dropIndex("ggg");
            });

            assert.deepEqual(db.sqls, [
                'ALTER TABLE xyz ADD COLUMN aaa text UNIQUE NOT NULL',
                'ALTER TABLE xyz DROP COLUMN bbb',
                'ALTER TABLE xyz RENAME COLUMN ccc TO ddd',
                'ALTER TABLE xyz ALTER COLUMN eee TYPE integer',
                "ALTER TABLE xyz ALTER COLUMN hhh SET DEFAULT 'abcd'",

                'CREATE UNIQUE INDEX xyz_fff_index ON xyz (fff)',
                'DROP INDEX xyz_ggg_index'
            ]);
        }
    },

    "Database.addColumn" : {
        topic : function() {
            moose.quoteIdentifiers = false;
            return new DummyDatabase();
        },

        "should construct proper SQL" : function(db) {
            db.addColumn("test", "name", "text", {unique : true});
            assert.deepEqual(db.sqls, [
                'ALTER TABLE test ADD COLUMN name text UNIQUE'
            ]);
        }
    },

    "Database.dropColumn" : {
        topic : function() {
            moose.quoteIdentifiers = false;
            return new DummyDatabase();
        },

        "should construct proper SQL"  : function(db) {
            db.dropColumn("test", "name");
            assert.deepEqual(db.sqls, [
                'ALTER TABLE test DROP COLUMN name'
            ]);
        }
    },

    "Database.renameColumn" : {
        topic : function() {
            moose.quoteIdentifiers = false;
            return new DummyDatabase();
        },

        "should construct proper SQL"  : function(db) {
            db.renameColumn("test", "abc", "def");
            assert.deepEqual(db.sqls, [
                'ALTER TABLE test RENAME COLUMN abc TO def'
            ]);
        }
    },

    "Database.setColumnType" : {
        topic : function() {
            moose.quoteIdentifiers = false;
            return new DummyDatabase();
        },

        "should construct proper SQL"  : function(db) {
            db.setColumnType("test", "name", "integer");
            assert.deepEqual(db.sqls, [
                'ALTER TABLE test ALTER COLUMN name TYPE integer'
            ]);
        }
    },

    "Database.setColumnDefault" : {
        topic : function() {
            moose.quoteIdentifiers = false;
            return new DummyDatabase();
        },

        "should construct proper SQL"  : function(db) {
            db.setColumnDefault("test", "name", 'zyx');
            assert.deepEqual(db.sqls, [
                "ALTER TABLE test ALTER COLUMN name SET DEFAULT 'zyx'"
            ]);
        }
    },

    "Database.addIndex" : {
        topic : function() {
            moose.quoteIdentifiers = false;
            return new DummyDatabase();
        },

        "should construct proper SQL"  : function(db) {
            db.addIndex("test", "name", {unique : true});
            assert.deepEqual(db.sqls, [
                'CREATE UNIQUE INDEX test_name_index ON test (name)'
            ]);
        },

        "should accept multiple columns"  : function(db) {
            db.reset();
            db.addIndex("test", ["one", "two"]);
            assert.deepEqual(db.sqls, [
                'CREATE INDEX test_one_two_index ON test (one, two)'
            ]);
        }
    },

    "Database.dropIndex" : {
        topic : function() {
            moose.quoteIdentifiers = false;
            return new DummyDatabase();
        },

        "should construct proper SQL"  : function(db) {
            db.dropIndex("test", "name");
            assert.deepEqual(db.sqls, [
                'DROP INDEX test_name_index'
            ]);
        }

    },

    "Database.dropTable"  : {
        topic : function() {
            return new DummyDatabase();
        },

        "should construct proper SQL"  : function(db) {
            db.dropTable("test");
            assert.deepEqual(db.sqls, ['DROP TABLE test']);
        },

        "should accept multiple table names"  : function(db) {
            db.reset();
            db.dropTable("a", "bb", "ccc");
            assert.deepEqual(db.sqls, [
                'DROP TABLE a',
                'DROP TABLE bb',
                'DROP TABLE ccc'
            ]);
        }
    },

    "Database.renameTable"  : {
        topic : function() {
            moose.quoteIdentifiers = false;
            return new DummyDatabase();
        },

        "should construct proper SQL"  : function(db) {
            db.renameTable("abc", "xyz");
            assert.deepEqual(db.sqls, ['ALTER TABLE abc RENAME TO xyz']);
        }
    },

    "Database.tableExists"  : {
        topic : function() {
            moose.quoteIdentifiers = false;
            return new DummyDatabase();
        },
        "should try to select the first record from the table's dataset"  : function(db) {
            var a, b;
            db.tableExists("a").then(function(ret) {
                a = ret;
            });
            db.tableExists("b").then(function(ret) {
                b = ret;
            });
            assert.isFalse(a);
            assert.isTrue(b);
        }
    },


    "Database.transaction" : {
        topic : function() {
            moose.quoteIdentifiers = false;
            return new Dummy3Database();
        },

        "should wrap the supplied block with BEGIN + COMMIT statements" : function(db) {
            db.reset();
            db.transaction(function(d) {
                d.execute('DROP TABLE test;');
            });
            assert.deepEqual(db.sqls, ['BEGIN', 'DROP TABLE test;', 'COMMIT']);
        },

        "should support transaction isolation levels" : function(db) {
            db.reset();
            db.supportsTransactionIsolationLevels = true;
            ["uncommitted", "committed", "repeatable", "serializable"].forEach(function(level) {
                db.transaction({isolation:level}, function(d) {
                    d.run("DROP TABLE " + level);
                });
            });
            assert.deepEqual(db.sqls, ['BEGIN', 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED', 'DROP TABLE uncommitted', 'COMMIT',
                'BEGIN', 'SET TRANSACTION ISOLATION LEVEL READ COMMITTED', 'DROP TABLE committed', 'COMMIT',
                'BEGIN', 'SET TRANSACTION ISOLATION LEVEL REPEATABLE READ', 'DROP TABLE repeatable', 'COMMIT',
                'BEGIN', 'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE', 'DROP TABLE serializable', 'COMMIT']);

        },

        "should allow specifying a default transaction isolation level" : function(db) {
            db.reset();
            db.supportsTransactionIsolationLevels = true;
            ["uncommitted", "committed", "repeatable", "serializable"].forEach(function(level) {
                db.transactionIsolationLevel = level;
                db.transaction(function(d) {
                    d.run("DROP TABLE " + level);
                });
            });
            assert.deepEqual(db.sqls, ['BEGIN', 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED', 'DROP TABLE uncommitted', 'COMMIT',
                'BEGIN', 'SET TRANSACTION ISOLATION LEVEL READ COMMITTED', 'DROP TABLE committed', 'COMMIT',
                'BEGIN', 'SET TRANSACTION ISOLATION LEVEL REPEATABLE READ', 'DROP TABLE repeatable', 'COMMIT',
                'BEGIN', 'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE', 'DROP TABLE serializable', 'COMMIT']);

        },

        "should issue ROLLBACK if an exception is raised, and re-raise" : function(db) {
            var db = new Dummy3Database();
            db.transaction(function(d) {
                d.execute('DROP TABLE test');
                throw "Error";
            });
            assert.deepEqual(db.sqls, ['BEGIN', 'DROP TABLE test', 'ROLLBACK']);
            db.reset();
            db.transaction(function(d) {
                d.execute('DROP TABLE test', {error : true});
            });
            assert.deepEqual(db.sqls, ['BEGIN', 'DROP TABLE test', 'ROLLBACK']);
            var errored;
            db.transaction(
                function(d) {
                    throw "Error";
                }).then(function() {
                    errored = false
                }, function() {
                    errored = true
                });
            assert.isTrue(errored);
        },

        "should raise database call errback if there is an error commiting" : function() {
            var db = new Dummy3Database();
            db.__commitTransaction = function() {
                return new comb.Promise().errback();
            };
            var errored;
            db.transaction(
                function(d) {
                    d.run("DROP TABLE test");
                }).then(function() {
                    errored = false
                }, function() {
                    errored = true
                });
            assert.isTrue(errored);
        }

    },

    "Database#transaction with savepoints" : {
        topic : function() {
            var db = new Dummy3Database();
            db.supportsSavepoints = true;
            return db;
        },

        "should wrap the supplied block with BEGIN + COMMIT statements" : function(db) {
            db.transaction(function() {
                db.execute("DROP TABLE test;");
            });
            assert.deepEqual(db.sqls, ['BEGIN', 'DROP TABLE test;', 'COMMIT']);
        },

        "should use savepoints if given the :savepoint option" : function(db) {
            db.reset();
            db.transaction(function() {
                db.transaction({savepoint : true}, function() {
                    db.execute('DROP TABLE test;')
                })
            });
            assert.deepEqual(db.sqls, ['BEGIN', 'SAVEPOINT autopoint_1', 'DROP TABLE test;', 'RELEASE SAVEPOINT autopoint_1', 'COMMIT']);
        },

        "should not use a savepoints if no transaction is in progress" : function(db) {
            db.reset();
            db.transaction({savepoint : true}, function(d) {
                d.execute('DROP TABLE test;')
            });
            assert.deepEqual(db.sqls, ['BEGIN', 'DROP TABLE test;', 'COMMIT']);
        },

        "should reuse the current transaction if no savepoint option is given" : function(db) {
            db.reset();
            db.transaction(function(d) {
                d.transaction(function(d2) {
                    d2.execute('DROP TABLE test;')
                })
            });
            assert.deepEqual(db.sqls, ['BEGIN', 'DROP TABLE test;', 'COMMIT']);
        },

        "should handle returning inside of the block by committing" : function(db) {
            db.reset();
            db.retCommit();
            assert.deepEqual(db.sqls, ['BEGIN', 'DROP TABLE test;', 'COMMIT']);
        },

        "should handle returning inside of a savepoint by committing" : function(db) {
            db.reset();
            db.retCommitSavePoint();
            assert.deepEqual(db.sqls, ['BEGIN', 'SAVEPOINT autopoint_1', 'DROP TABLE test;', 'RELEASE SAVEPOINT autopoint_1', 'COMMIT']);
        },

        "should issue ROLLBACK if an exception is raised, and re-raise" : function(db) {
            var db = new Dummy3Database();
            db.transaction(function(d) {
                d.execute('DROP TABLE test');
                throw "Error";
            });
            assert.deepEqual(db.sqls, ['BEGIN', 'DROP TABLE test', 'ROLLBACK']);
            db.reset();
            db.transaction(function(d) {
                d.execute('DROP TABLE test', {error : true});
            });
            assert.deepEqual(db.sqls, ['BEGIN', 'DROP TABLE test', 'ROLLBACK']);
            var errored;
            db.transaction(
                function(d) {
                    throw "Error";
                }).then(function() {
                    errored = false
                }, function() {
                    errored = true
                });
            assert.isTrue(errored);
        },

        "should issue ROLLBACK if an exception is raised inside a savepoint, and re-raise" : function(db) {
            db.reset();
            db.transaction(function() {
                db.transaction({savepoint : true}, function() {
                    db.execute('DROP TABLE test');
                    throw "Error";
                });
            });
            assert.deepEqual(db.sqls, ['BEGIN', 'SAVEPOINT autopoint_1', 'DROP TABLE test', 'ROLLBACK TO SAVEPOINT autopoint_1', 'ROLLBACK']);
            db.reset();
            db.transaction(function(d) {
                db.transaction({savepoint : true}, function() {
                    d.execute('DROP TABLE test', {error : true});
                });
            });
            assert.deepEqual(db.sqls, ['BEGIN', 'SAVEPOINT autopoint_1', 'DROP TABLE test', 'ROLLBACK TO SAVEPOINT autopoint_1', 'ROLLBACK']);
            var errored;
            db.transaction(
                function(d) {
                    db.transaction({savepoint : true}, function() {
                        throw "ERROR";
                    });
                }).then(function() {
                    errored = false
                }, function() {
                    errored = true
                });
            assert.isTrue(errored);
        },

        "should issue ROLLBACK SAVEPOINT if 'ROLLBACK' is called is thrown in savepoint" : function(db) {
            db.reset();
            db.transaction(function() {
                db.transaction({savepoint:true}, function() {
                    db.dropTable("a");
                    throw "ROLLBACK";
                });
                db.dropTable("b")
            });

            assert.deepEqual(db.sqls, ['BEGIN', 'SAVEPOINT autopoint_1', 'DROP TABLE a', 'ROLLBACK TO SAVEPOINT autopoint_1', 'DROP TABLE b', 'COMMIT']);
        },


        "should raise database errors when commiting a transaction and error is thrown" : function(db) {
            var orig = db.__commitTransaction;
            db.__commitTransaction = function() {
                return new comb.Promise().errback("ERROR");
            };
            var thrown = false;
            db.transaction(
                function() {
                }).then(function() {
                    thrown = false;
                }, function() {
                    thrown = true;
                });
            assert.isTrue(thrown);
            var thrown = false;
            db.transaction(
                function() {
                    db.transaction({savepoint:true}, function() {
                    });
                }).then(function() {
                    thrown = false;
                }, function() {
                    thrown = true;
                });
            assert.isTrue(thrown);
            db.__commitTransaction = orig;
        }

    },

    "Database#fetch" : {
        topic : function() {
            var CDS = comb.define(moose.Dataset, {
                instance : {

                    fetchRows : function(sql, cb) {
                        return new comb.Promise().callback({sql : sql}).addCallback(cb);
                    }
                }
            });
            var TestDB = comb.define(moose.Database, {
                instance : {
                    getters : {
                        dataset : function() {
                            return new CDS(this);
                        }
                    }
                }
            })
            return new TestDB();
        },

        "should create a dataset and invoke its fetch_rows method with the given sql" : function(db) {
            var sql = null;
            db.fetch('select * from xyz').forEach(function(r) {
                sql = r.sql;
            });
            assert.equal(sql, 'select * from xyz');
        },

        "should format the given sql with any additional arguments" : function(db) {
            var sql = null;
            db.fetch('select * from xyz where x = ? and y = ?', 15, 'abc', function(r) {
                sql = r.sql;
            });
            assert.equal(sql, "select * from xyz where x = 15 and y = 'abc'");

            db.fetch('select name from table where name = ? or id in ?', 'aman', [3,4,7], function(r) {
                sql = r.sql;
            });
            assert.equal(sql, "select name from table where name = 'aman' or id in (3, 4, 7)");
        },

        "should format the given sql with named arguments" : function(db) {
            var sql = null;
            db.fetch('select * from xyz where x = {x} and y = {y}', {x:15, y: 'abc'}, function(r) {
                sql = r.sql;
            });
            assert.equal(sql, "select * from xyz where x = 15 and y = 'abc'");
        },

        "should return the dataset if no block is given" : function(db) {
            assert.instanceOf(db.fetch('select * from xyz'), moose.Dataset);
            var ret;
            db.fetch('select a from b').map(
                function(r) {
                    return r.sql
                }).then(function(r) {
                    ret = r;
                });
            assert.deepEqual(ret, ['select a from b']);
        },

        "should return a dataset that always uses the given sql for SELECTs" : function(db) {
            var ds = db.fetch('select * from xyz')
            assert.equal(ds.selectSql(), 'select * from xyz');
            assert.equal(ds.sql, 'select * from xyz');
            ds.filter(function() {
                return this.price.sqlNumber.lt(100);
            });
            assert.equal(ds.selectSql(), 'select * from xyz');
            assert.equal(ds.sql, 'select * from xyz');
        }
    },

    "Database.createView" : {
        topic : function() {
            moose.quoteIdentifiers = false;
            return new DummyDatabase();
        },

        "should construct proper SQL with raw SQL" : function(db) {
            db.createView("test", "SELECT * FROM xyz");
            assert.deepEqual(db.sqls, ['CREATE VIEW test AS SELECT * FROM xyz']);
            db.reset();
            db.createView(sql.test, "SELECT * FROM xyz");
            assert.deepEqual(db.sqls, ['CREATE VIEW test AS SELECT * FROM xyz']);
        },

        "should construct proper SQL with dataset" : function(db) {
            db.reset();
            db.createView("test", db.from("items").select("a", "b").order("c"));
            assert.deepEqual(db.sqls, ['CREATE VIEW test AS SELECT a, b FROM items ORDER BY c']);
            db.reset();
            db.createView(sql.test.qualify("sch"), db.from("items").select("a", "b").order("c"));
            assert.deepEqual(db.sqls, ['CREATE VIEW sch.test AS SELECT a, b FROM items ORDER BY c']);
        }
    },

    "Database.createorReplaceView" : {
        topic : function() {
            moose.quoteIdentifiers = false;
            return new DummyDatabase();
        },

        "should construct proper SQL with raw SQL" : function(db) {
            db.createOrReplaceView("test", "SELECT * FROM xyz");
            assert.deepEqual(db.sqls, ['CREATE OR REPLACE VIEW test AS SELECT * FROM xyz']);
            db.reset();
            db.createOrReplaceView(sql.test, "SELECT * FROM xyz");
            assert.deepEqual(db.sqls, ['CREATE OR REPLACE VIEW test AS SELECT * FROM xyz']);
        },

        "should construct proper SQL with dataset" : function(db) {
            db.reset();
            db.createOrReplaceView("test", db.from("items").select("a", "b").order("c"));
            assert.deepEqual(db.sqls, ['CREATE OR REPLACE VIEW test AS SELECT a, b FROM items ORDER BY c']);
            db.reset();
            db.createOrReplaceView(sql.test.qualify("sch"), db.from("items").select("a", "b").order("c"));
            assert.deepEqual(db.sqls, ['CREATE OR REPLACE VIEW sch.test AS SELECT a, b FROM items ORDER BY c']);
        }
    },

    "Database.dropView" : {
        topic : function() {
            moose.quoteIdentifiers = false;
            return new DummyDatabase();
        },

        "should construct proper SQL" : function(db) {
            db.dropView("test");
            db.dropView(sql.test);
            db.dropView('sch__test');
            db.dropView(sql.test.qualify('sch'));
            assert.deepEqual(db.sqls, ['DROP VIEW test', 'DROP VIEW test', 'DROP VIEW sch.test', 'DROP VIEW sch.test']);
        }
    },

    "Database.__alterTableSql" : {
        topic : function() {
            moose.quoteIdentifiers = false;
            return new DummyDatabase();
        },

        "should raise error for an invalid op" : function(db) {
            assert.throws(hitch(db, "__alterTableSql", "mau", {op : "blah"}));
        }
    },

    "Database.get" : {

        topic : function() {
            var C = comb.define(DummyDatabase, {
                instance : {
                    getters : {
                        dataset : function() {
                            var ds = new DummyDataset(this);
                            ds.get = function() {
                                return this.db.execute(this.select.apply(this, arguments).sql);
                            };
                            return ds;
                        }
                    }
                }
            });
            return new C();
        },

        "should use Dataset#get to get a single value" : function(db) {
            var ret;
            db.get(1);
            assert.equal(db.sqls.pop(), 'SELECT 1');

            db.get(sql.version.sqlFunction);
            assert.equal(db.sqls.pop(), 'SELECT version()');
        },

        "should accept a block" : function(db) {
            db.get(function() {
                return 1
            });
            assert.equal(db.sqls.pop(), 'SELECT 1');

            db.get(function() {
                return this.version(1)
            });
            assert.equal(db.sqls.pop(), 'SELECT version(1)');
        }
    },

    "Database.typecastValue" : {
        topic : new moose.Database(),

        "should raise an InvalidValue when given an invalid value" : function(db) {
            assert.throws(hitch(db, "typecastValue", "integer", "a"));
            assert.throws(hitch(db, "typecastValue", "float", "a.a2"));
            assert.throws(hitch(db, "typecastValue", "decimal", "invalidValue"));
            assert.throws(hitch(db, "typecastValue", "date", "a"));
            assert.throws(hitch(db, "typecastValue", "date", {}));
            assert.throws(hitch(db, "typecastValue", "time", "v"));
            assert.throws(hitch(db, "typecastValue", "dateTime", "z"));
        }
    },

    "Database.__columnSchemaToJsDefault" : {
        topic : new moose.Database(),

        "should handle converting many default formats " : function(db) {
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
            assert.deepEqual(m("'2009-10-29T10:20:30-07:00'", "datetime"), comb.date.parse('2009-10-29T10:20:30-07:00', 'yyyy-MM-ddThh:mm:ssZ'));
            assert.deepEqual(m("'2009-10-29 10:20:30'", "datetime"), comb.date.parse('2009-10-29 10:20:30', 'yyyy-MM-ddThh:mm:ss'));
            assert.deepEqual(m("'10:20:30'", "time"), comb.date.parse("10:20:30", "hh:mm:ss"));
            assert.isNull(m("NaN", "float"));

            db.type = "postgres";
            assert.equal(m("''::text", "string"), "");
            assert.equal(m("'\\a''b'::character varying", "string"), "\\a'b");
            assert.equal(m("'a'::bpchar", "string"), "a");
            assert.equal(m("(-1)", "integer"), -1);
            assert.equal(m("(-1.0)", "float"), -1.0);
            assert.equal(m('(-1.0)', "decimal"), -1.0);
            assert.deepEqual(m("'2009-10-29'::date", "date"), comb.date.parse("2009-10-29", "yyyy-MM-dd"));
            assert.deepEqual(m("'2009-10-29 10:20:30.241343'::timestamp without time zone", "datetime"), comb.date.parse("2009-10-29 10:20:30.241343", "yyyy-MM-dd hh:mm:ss.SSZ"));
            assert.deepEqual(m("'10:20:30'::time without time zone", "time"), comb.date.parse("10:20:30", "hh:mm:ss"));

            db.type = "mysql";
            assert.equal(m("\\a'b", "string"), "\\a'b");
            assert.equal(m("a", "string"), "a");
            assert.equal(m("NULL", "string"), "NULL");
            assert.equal(m("-1", "float"), -1.0);
            assert.equal(m('-1', "decimal"), -1.0);
            assert.deepEqual(m("2009-10-29", "date"), comb.date.parse("2009-10-29", "yyyy-MM-dd"));
            assert.deepEqual(m("2009-10-29 10:20:30", "datetime"), comb.date.parse('2009-10-29 10:20:30', 'yyyy-MM-ddThh:mm:ss'));
            assert.deepEqual(m("10:20:30", "time"), comb.date.parse("10:20:30", "hh:mm:ss"));
            assert.equal(m("CURRENT_DATE", "date"), null);
            assert.equal(m("CURRENT_TIMESTAMP", "datetime"), null);
            assert.equal(m("a", "enum"), "a");

            db.type = "mssql";
            assert.equal(m("(N'a')", "string"), "a");
            assert.equal(m("((-12))", "integer"), -12);
            assert.equal(m("((12.1))", "float"), 12.1);
            assert.equal(m("((-12.1))", "decimal"), -12.1);
        }
    },


    "moose.SQL.Constants" : {
        topic : new MockDatabase(),

        "should have CURRENT_DATE"  : function(db) {
            assert.equal(db.literal(moose.SQL.Constants.CURRENT_DATE), 'CURRENT_DATE');
            assert.equal(db.literal(moose.CURRENT_DATE), 'CURRENT_DATE');
        },

        "should have CURRENT_TIME"  : function(db) {
            assert.equal(db.literal(moose.SQL.Constants.CURRENT_TIME), 'CURRENT_TIME');
            assert.equal(db.literal(moose.CURRENT_TIME), 'CURRENT_TIME');
        },

        "should have CURRENT_TIMESTAMP"  : function(db) {
            assert.equal(db.literal(moose.SQL.Constants.CURRENT_TIMESTAMP), 'CURRENT_TIMESTAMP');
            assert.equal(db.literal(moose.CURRENT_TIMESTAMP), 'CURRENT_TIMESTAMP');
        },

        "should have NULL"  : function(db) {
            assert.equal(db.literal(moose.SQL.Constants.NULL), 'NULL');
            assert.equal(db.literal(moose.NULL), 'NULL');
        },

        "should have NOTNULL"  : function(db) {
            assert.equal(db.literal(moose.SQL.Constants.NOTNULL), 'NOT NULL');
            assert.equal(db.literal(moose.NOTNULL), 'NOT NULL');
        },

        "should have TRUE and SQLTRUE"  : function(db) {
            assert.equal(db.literal(moose.SQL.Constants.TRUE), '1');
            assert.equal(db.literal(moose.TRUE), '1');
            assert.equal(db.literal(moose.SQL.Constants.SQLTRUE), '1');
            assert.equal(db.literal(moose.SQLTRUE), '1');
        },

        "should have FALSE and SQLFALSE"  : function(db) {
            assert.equal(db.literal(moose.SQL.Constants.FALSE), '0');
            assert.equal(db.literal(moose.FALSE), '0');
            assert.equal(db.literal(moose.SQL.Constants.SQLFALSE), '0');
            assert.equal(db.literal(moose.SQLFALSE), '0');
        }
    }
});


suite.run({reporter : require("vows").reporter.spec}, function() {
    //helper.dropModels().then(comb.hitch(ret, "callback"), comb.hitch(ret, "errback"))
});