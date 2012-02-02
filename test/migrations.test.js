var vows = require('vows'),
    assert = require('assert'),
    comb = require("comb"),
    hitch = comb.hitch,
    patio = require("../index");


/*options : Object
 *  options.connection : @see moose.createConnection
 *  options.dir : name of directory where migrations are located
 *  options.up : boolen, if true will migrate up otherwise down
 *  options.start : the migration to start at
 *  options.end : the migration to end at*/


var ret = exports = module.exports = new comb.Promise();
var suite = vows.describe('Migrations');
var patioMigrationVersion = -1, patioMigrationFiles = [];

var getFileVersion = function(file){
    return parseInt(file.split(".")[0], 10);
}

var sortMigrationFiles = function(){
    patioMigrationFiles.sort(function(f1, f2){
        return getFileVersion(f1) - getFileVersion(f2);
    });
};

var MockDS = comb.define(patio.Dataset, {
    instance:{

        fetchRows:function(sql, cb){
            var from = this.__opts.from[0], ret = new comb.Promise();
            ret.callback(cb({version:patioMigrationVersion}));
            return ret;
        },

        insert:function(values){
            var from = this.__opts.from[0], ret = new comb.Promise().callback(0);
            if (from.toString() == "schema_info") {
                patioMigrationVersion = values[Object.keys(values)[0]];
            }
            return ret;
        },

        update:function(values){
            var from = this.__opts.from[0], ret = new comb.Promise().callback(1);
            if (from.toString() == "schema_info") {
                patioMigrationVersion = values[Object.keys(values)[0]];
            }
            return ret;
        },

        count:function(){
            return new comb.Promise().callback(1);
        },

        getters:{
            columns:function(){
                var from = this.__opts.from[0], ret = new comb.Promise();
                ret.callback(this.db.columnsCreated);
                return ret;
            }
        }
    }
});

var MockDB = comb.define(patio.Database, {

    instance:{

        constructor:function(){
            this._super(arguments);
            this.type = this._static.type;
            this.quoteIdentifiers = false;
            this.identifierInputMethod = null;
            this.identifierOutputMethod = null;
            this.connectionExecuteMethod = "query";
            this.sqls = [];
            this.tables = {};
            this.alteredTables = {};
            this.closedCount = 0;
            this.createdCount = 0;
            this.columnsCreated = [];
            this.columnsAltered = {};
            this.droppedTables = [];
        },

        createConnection:function(){
            this.createdCount++;
            return {
                query:function(sql){
                    DB.sqls.push(sql);
                    return new comb.Promise().callback(sql);
                }
            }
        },

        closeConnection:function(){
            this.closedCount++;
            return new comb.Promise().callback();
        },

        validate:function(){
            return new comb.Promise().callback(true);
        },

        execute:function(sql, opts){
            var ret = new comb.Promise();
            this.sqls.push(sql);
            ret.callback();
            return ret;
        },

        createTable:function(name, args){
            this.tables[name] = true;
            this._super(arguments);
            var match = this.sqls[this.sqls.length - 1].match(/ \(?(\w+) integer.*\)?$/);
            if (match != null) {
                this.columnsCreated.push(match[1]);
            }
            return new comb.Promise().callback();
        },

        dropTable:function(name){
            comb.argsToArray(arguments).forEach(function(name){
                this.droppedTables.push(name);
                this.tables[name] = null;
            }, this);
            return new comb.Promise().callback();
        },

        tableExists:function(name){
            return new comb.Promise().callback(!comb.isUndefinedOrNull(this.tables[name]));
        },

        alterTable:function(name){
            this.alteredTables[name] = true;
            this._super(arguments);
            var sql = this.sqls[this.sqls.length - 1]
            var match = sql.match(/ \(?(\w+) integer.*\)?$/);
            var alterMatch = sql.match(/(\w+) TO (\w+)$/);
            if (match != null) {
                this.columnsCreated.push(match[1]);
            }
            if (alterMatch != null) {
                this.columnsAltered[alterMatch[1]] = alterMatch[2];
            }
            return new comb.Promise().callback(1);
        },


        reset:function(){
            this.sqls = [];
            this.columnsCreated = [];
            this.droppedTables = [];
            this.columnsAltered = {};
            this.tables = {};
            this.alteredTables = {};
            patioMigrationVersion = -1;
            patioMigrationFiles = [];
        },

        getters:{
            dataset:function(){
                return new MockDS(this);
            }
        }
    }

});

var DB = new MockDB();

suite.addBatch({
    "An Integer migrator should ":{
        topic:DB,

        " raise and error if there is a missing integer migration version":{
            topic:function(){
                DB.reset();
                patio.migrate(DB, __dirname + "/migrations/files/missing_migration_file").then(hitch(this, "callback"), hitch(this, "callback", null));
            },

            " ":function(err){
                assert.deepEqual(err[0].message, "Migration error : Missing migration for 1");

            }
        },

        " raise and error if there is a duplicate integer migration version":{
            topic:function(){
                DB.reset();
                patio.migrate(DB, __dirname + "/migrations/files/duplicate_migration_file").then(hitch(this, "callback"), hitch(this, "callback", null));
            },

            " ":function(err){
                assert.deepEqual(err[0].message, "Migration error : Duplicate migration number 0");

            }
        }
    }
});

suite.addBatch({
    "An Integer migrator should ":{
        topic:DB,

        "add a column name if it doesn't already exist in the schemaInfo table":{
            topic:function(){
                DB.reset();
                DB.createTable("schema_info", function(){});
                patio.migrate(DB, __dirname + "/migrations/files/basic_integer_migration").then(hitch(this, "callback", null), hitch(this, "callback"));

            },

            "":function(res){
                assert.isTrue(DB.alteredTables["schema_info"]);
                assert.isTrue(DB.columnsCreated.indexOf("version") != -1);
            }

        }
    }
});

suite.addBatch({
    "An Integer migrator should ":{
        topic:DB,

        "add a create a schemaInfoTable if it doesn't already exist":{
            topic:function(){
                DB.reset();
                patio.migrate(DB, __dirname + "/migrations/files/basic_integer_migration").then(hitch(this, "callback", null), hitch(this, "callback"));

            },

            "":function(res){
                assert.isTrue(DB.tables["schema_info"])
                assert.isTrue(DB.columnsCreated.indexOf("version") != -1);
                DB.reset();
            }

        }
    }
});

suite.addBatch({
    "An Integer migrator should ":{
        topic:DB,

        "add a allow one to specify the schema and column":{
            topic:function(){
                DB.reset();
                patio.migrate(DB, __dirname + "/migrations/files/basic_integer_migration", {table:"migration_info", column:"mi"}).then(hitch(this, "callback", null), hitch(this, "callback"));

            },

            "":function(res){
                assert.isTrue(DB.tables["migration_info"])
                assert.isTrue(DB.columnsCreated.indexOf("mi") != -1);
                DB.reset();
            }

        }
    }
});

suite.addBatch({
    "An Integer migrator should ":{
        topic:DB,

        "apply a migration correctly if no target is given":{
            topic:function(){
                DB.reset();
                patio.migrate(DB, __dirname + "/migrations/files/basic_integer_migration").then(hitch(this, "callback", null), hitch(this, "callback"));

            },

            "":function(res){
                assert.isTrue(DB.tables["test1"]);
                assert.isTrue(DB.tables["test2"]);
                assert.isTrue(DB.tables["test3"]);
                assert.isTrue(DB.tables["test4"]);
                assert.isTrue(DB.alteredTables["test1"]);
                assert.isTrue(DB.alteredTables["test2"]);
                assert.isTrue(DB.alteredTables["test3"]);
                assert.isTrue(DB.alteredTables["test4"]);
                assert.deepEqual(DB.columnsAltered, {
                    column1:"column2",
                    column2:"column3",
                    column3:"column4",
                    column4:"column5"
                })

                DB.reset();
            }

        }
    }
});

suite.addBatch({
    "An Integer migrator should ":{
        topic:DB,

        "apply a migration correctly in the up direction if a target is given":{
            topic:function(){
                DB.reset();
                patio.migrate(DB, __dirname + "/migrations/files/basic_integer_migration", {target:2}).then(hitch(this, "callback", null), hitch(this, "callback"));

            },

            "":function(res){
                assert.isTrue(DB.tables["test1"]);
                assert.isTrue(DB.tables["test2"]);
                assert.isTrue(DB.tables["test3"]);
                assert.isTrue(comb.isEmpty(DB.alteredTables));
                assert.isTrue(comb.isEmpty(DB.columnsAltered));
                assert.equal(patioMigrationVersion, 2);
                DB.reset();
            }

        }
    }
});

suite.addBatch({
    "An Integer migrator should ":{
        topic:DB,

        "apply a migration correctly in the down direction if a target is given":{
            topic:function(){
                DB.reset();
                DB.createTable("schema_info", function(){
                    this.version("integer", {"default":0});
                });
                DB.from("schema_info").update({version:3});
                patio.migrate(DB, __dirname + "/migrations/files/basic_integer_migration", {target:-1}).then(hitch(this, "callback", null), hitch(this, "callback"));

            },

            "":function(res){
                assert.deepEqual(DB.droppedTables, ["test4", "test3", "test2", "test1"])
                assert.equal(patioMigrationVersion, -1);
                assert.isTrue(comb.isEmpty(DB.alteredTables));
                assert.isTrue(comb.isEmpty(DB.columnsAltered))
                DB.reset();
            }

        }
    }
});

suite.addBatch({
    "An Integer migrator should ":{
        topic:DB,

        "apply a migration correctly in the down direction if a target and current is given":{
            topic:function(){
                DB.reset();
                DB.createTable("schema_info", function(){
                    this.version("integer", {"default":0});
                });
                DB.from("schema_info").update({version:3});
                patio.migrate(DB, __dirname + "/migrations/files/basic_integer_migration", {target:-1, current:4}).then(hitch(this, "callback", null), hitch(this, "callback"));

            },

            "":function(res){
                assert.deepEqual(DB.droppedTables, ["test4", "test3", "test2", "test1"])
                assert.equal(patioMigrationVersion, -1);
                assert.isTrue(DB.alteredTables["test1"]);
                assert.isTrue(DB.alteredTables["test2"]);
                assert.isTrue(DB.alteredTables["test3"]);
                assert.isTrue(DB.alteredTables["test4"]);
                assert.deepEqual(DB.columnsAltered, {
                    column2:"column1",
                    column3:"column2",
                    column4:"column3",
                    column5:"column4"
                })

                DB.reset();
            }

        }
    }
});

suite.addBatch({
    "An Integer migrator should ":{
        topic:DB,

        "apply return the correct target number":{
            topic:function(){
                DB.reset();
                var ret = [];
                patio.migrate(DB, __dirname + "/migrations/files/basic_integer_migration", {target:4, current:2})
                    .addCallback(comb.hitch(ret, "push"))
                    .chain(comb.hitch(patio, "migrate", DB, __dirname + "/migrations/files/basic_integer_migration", {target:-1}), hitch(this, "callback"))
                    .addCallback(comb.hitch(ret, "push"))
                    .chain(comb.hitch(patio, "migrate", DB, __dirname + "/migrations/files/basic_integer_migration"), hitch(this, "callback"))
                    .addCallback(comb.hitch(ret, "push"))
                    .then(hitch(this, "callback", null, ret), hitch(this, "callback"));


            },

            "":function(res){
                assert.deepEqual(res, [4, 0, 4]);
                DB.reset();
            }

        }
    }
});

var MockTimestampDs = comb.define(MockDS, {
    instance:{

        fetchRows:function(sql, cb){
            var from = this.__opts.from[0], ret = new comb.Promise();
            if (from.toString() === "schema_info") {
                ret.callback(cb({version:patioMigrationVersion}));
            } else if (from.toString() === "schema_migrations") {
                sortMigrationFiles();
                ret.callback(patioMigrationFiles.forEach(function(f){
                    cb({filename:f});
                }));
            } else if (from.toString() === "sm") {
                ret.callback(patioMigrationFiles.forEach(function(f){
                    cb({fn:f});
                }));
            }
            return ret;
        },

        insert:function(values){
            var from = this.__opts.from[0].toString(), ret = new comb.Promise().callback(0);
            if (from == "schema_info") {
                patioMigrationVersion = values[Object.keys(values)[0]];
            } else if (from == "schema_migrations" || from === "sm") {
                patioMigrationFiles.push(values[Object.keys(values)[0]])
            }
            return ret;
        },

        remove:function(){
            var from = this.__opts.from[0].toString(), ret = new comb.Promise().callback(1);
            if (from == "schema_migrations" || from === "sm") {
                var where = this.__opts.where.args, index = patioMigrationFiles.indexOf(where[where.length - 1]);
                if (index > -1) {
                    patioMigrationFiles.splice(index, 1);
                }
            }
            return ret;
        },

        getters:{
            columns:function(){
                var from = this.__opts.from[0].toString(), ret = new comb.Promise();
                if (from === "schema_info") {
                    ret.callback(["version"]);
                } else if (from === "schema_migrations") {
                    ret.callback(["filename"]);
                } else if (from === "sm") {
                    ret.callback(["fn"]);
                }
                return ret;
            }
        }
    }
});

var MockTimestampDB = comb.define(MockDB, {
    instance:{
        getters:{
            dataset:function(){
                return new MockTimestampDs(this);
            }
        }
    }
});

var TSDB = new MockTimestampDB();
suite.addBatch({
    "An Timestamp migrator should ":{
        topic:DB,

        "migrate all the way up":{
            topic:function(){
                TSDB.reset();
                var ret = [];
                patio.migrate(TSDB, __dirname + "/migrations/files/timestamp_migration").then(hitch(this, "callback", null), hitch(this, "callback"));


            },

            "":function(res){
                assert.isTrue(TSDB.tables["test1"]);
                assert.isTrue(TSDB.tables["test2"]);
                assert.isTrue(TSDB.tables["test3"]);
                assert.isTrue(TSDB.tables["test4"]);
                assert.deepEqual(TSDB.columnsCreated, ["column1", "column2", "column3", "column4"]);
                assert.lengthOf(patioMigrationFiles, 4);
                assert.deepEqual(patioMigrationFiles, ["1327997153.create_table.js", "1327997224.create_table.js", "1327997243.migration.js", "1327997262.migration.js"]);
            },

            " and migrate all the way down":{
                topic:function(){
                    var ret = [];
                    patio.migrate(TSDB, __dirname + "/migrations/files/timestamp_migration", {target:-1}).then(hitch(this, "callback", null), hitch(this, "callback"));


                },

                "":function(res){
                    assert.isNull(TSDB.tables["test1"]);
                    assert.isNull(TSDB.tables["test2"]);
                    assert.isNull(TSDB.tables["test3"]);
                    assert.isNull(TSDB.tables["test4"]);
                    assert.deepEqual(TSDB.droppedTables, ["test4", "test3", "test2", "test1"]);
                    assert.deepEqual(TSDB.columnsCreated, ["column1", "column2", "column3", "column4"]);
                    assert.lengthOf(patioMigrationFiles, 0)
                    TSDB.reset();
                }

            }

        }
    }
});


suite.addBatch({
    "An Timestamp migrator should ":{
        topic:DB,

        "migrate all the way up to a timestamp":{
            topic:function(){
                TSDB.reset();
                var ret = [];
                patio.migrate(TSDB, __dirname + "/migrations/files/timestamp_migration", {target:1327997243}).then(hitch(this, "callback", null), hitch(this, "callback"));


            },

            "":function(res){
                assert.isTrue(TSDB.tables["test1"]);
                assert.isTrue(TSDB.tables["test2"]);
                assert.isTrue(TSDB.tables["test3"]);
                assert.deepEqual(TSDB.columnsCreated, ["column1", "column2", "column3"]);
                assert.lengthOf(patioMigrationFiles, 3);
                assert.deepEqual(patioMigrationFiles, ["1327997153.create_table.js", "1327997224.create_table.js", "1327997243.migration.js"]);
            },

            " and migrate all the way down to a timestamp":{
                topic:function(){
                    var ret = [];
                    patio.migrate(TSDB, __dirname + "/migrations/files/timestamp_migration", {target:-1}).then(hitch(this, "callback", null), hitch(this, "callback"));


                },

                "":function(res){
                    assert.isNull(TSDB.tables["test1"]);
                    assert.isNull(TSDB.tables["test2"]);
                    assert.isNull(TSDB.tables["test3"]);
                    assert.deepEqual(TSDB.droppedTables, ["test3", "test2", "test1"]);
                    assert.lengthOf(patioMigrationFiles, 0)
                    TSDB.reset();
                }

            }

        }
    }
});


suite.addBatch({
    "An Timestamp migrator should ":{
        topic:DB,

        "migrate all the way up ":{
            topic:function(){
                TSDB.reset();
                var ret = [];
                patio.migrate(TSDB, __dirname + "/migrations/files/timestamp_migration").then(hitch(this, "callback", null), hitch(this, "callback"));
            },

            "":function(res){
                assert.isTrue(TSDB.tables["test1"]);
                assert.isTrue(TSDB.tables["test2"]);
                assert.isTrue(TSDB.tables["test3"]);
                assert.deepEqual(TSDB.columnsCreated, ["column1", "column2", "column3", "column4"]);
                assert.lengthOf(patioMigrationFiles, 4);
                assert.deepEqual(patioMigrationFiles, ["1327997153.create_table.js", "1327997224.create_table.js", "1327997243.migration.js", '1327997262.migration.js']);
            },

            " and apply missing files":{
                topic:function(){
                    var ret = [];
                    patio.migrate(TSDB, __dirname + "/migrations/files/interleaved_timestamp_migrations").then(hitch(this, "callback", null), hitch(this, "callback"));


                },

                "":function(res){
                    assert.isTrue(TSDB.tables["test1"]);
                    assert.isTrue(TSDB.tables["test2"]);
                    assert.isTrue(TSDB.tables["test3"]);
                    assert.lengthOf(TSDB.droppedTables, 0);
                    assert.isTrue(TSDB.alteredTables["test1"]);
                    assert.isTrue(TSDB.alteredTables["test2"]);
                    assert.isTrue(TSDB.alteredTables["test3"]);
                    assert.isTrue(TSDB.alteredTables["test4"]);
                    assert.deepEqual(TSDB.columnsAltered, {
                        column1:"column2",
                        column2:"column3",
                        column3:"column4",
                        column4:"column5"
                    })
                    assert.lengthOf(patioMigrationFiles, 8);
                    assert.deepEqual(patioMigrationFiles, ['1327997153.create_table.js',
                        '1327997224.create_table.js',
                        '1327997243.migration.js',
                        '1327997262.migration.js',
                        '1327997155.create_table.js',
                        '1327997230.create_table.js',
                        '1327997250.migration.js',
                        '1327997265.migration.js']);
                    TSDB.reset();
                }

            }

        }
    }
});

suite.addBatch({
    "An Timestamp migrator should ":{
        topic:DB,

        "migrate all the way up ":{
            topic:function(){
                TSDB.reset();
                var ret = [];
                patio.migrate(TSDB, __dirname + "/migrations/files/timestamp_migration").then(hitch(this, "callback", null), hitch(this, "callback"));
            },

            "":function(res){
                assert.isTrue(TSDB.tables["test1"]);
                assert.isTrue(TSDB.tables["test2"]);
                assert.isTrue(TSDB.tables["test3"]);
                assert.deepEqual(TSDB.columnsCreated, ["column1", "column2", "column3", "column4"]);
                assert.lengthOf(patioMigrationFiles, 4);
                assert.deepEqual(patioMigrationFiles, ["1327997153.create_table.js", "1327997224.create_table.js", "1327997243.migration.js", '1327997262.migration.js']);
            },

            " and not apply down action when up action has not been called":{
                topic:function(){
                    var ret = [];
                    patio.migrate(TSDB, __dirname + "/migrations/files/interleaved_timestamp_migrations", {target:-1}).then(hitch(this, "callback", null), hitch(this, "callback"));


                },

                "":function(res){
                    assert.lengthOf(TSDB.droppedTables, 4);
                    assert.deepEqual(TSDB.droppedTables, ["test4", "test3", "test2", "test1"]);
                    assert.isTrue(comb.isEmpty(TSDB.alteredTables));
                    assert.deepEqual(TSDB.columnsAltered, {})
                    assert.lengthOf(patioMigrationFiles, 0);
                    TSDB.reset();
                }

            }

        }
    }
});

suite.addBatch({
    "An Timestamp migrator should ":{
        topic:DB,

        "migrate all the way up ":{
            topic:function(){
                TSDB.reset();
                var ret = [];
                patio.migrate(TSDB, __dirname + "/migrations/files/timestamp_migration").then(hitch(this, "callback", null), hitch(this, "callback"));
            },

            "":function(res){
                assert.isTrue(TSDB.tables["test1"]);
                assert.isTrue(TSDB.tables["test2"]);
                assert.isTrue(TSDB.tables["test3"]);
                assert.deepEqual(TSDB.columnsCreated, ["column1", "column2", "column3", "column4"]);
                assert.lengthOf(patioMigrationFiles, 4);
                assert.deepEqual(patioMigrationFiles, ["1327997153.create_table.js", "1327997224.create_table.js", "1327997243.migration.js", '1327997262.migration.js']);
            },

            " and apply missing files up to a certain timestamp":{
                topic:function(){
                    var ret = [];
                    patio.migrate(TSDB, __dirname + "/migrations/files/interleaved_timestamp_migrations", {target:1327997262}).then(hitch(this, "callback", null), hitch(this, "callback"));


                },

                "":function(res){
                    assert.isTrue(TSDB.tables["test1"]);
                    assert.isTrue(TSDB.tables["test2"]);
                    assert.isTrue(TSDB.tables["test3"]);
                    assert.lengthOf(TSDB.droppedTables, 0);
                    assert.isTrue(TSDB.alteredTables["test1"]);
                    assert.isTrue(TSDB.alteredTables["test2"]);
                    assert.isTrue(TSDB.alteredTables["test3"]);
                    assert.isUndefined(TSDB.alteredTables["test4"]);
                    assert.deepEqual(TSDB.columnsAltered, {
                        column1:"column2",
                        column2:"column3",
                        column3:"column4"
                    });
                    assert.lengthOf(patioMigrationFiles, 7);
                    sortMigrationFiles();
                    assert.deepEqual(patioMigrationFiles, [
                        '1327997153.create_table.js',
                        '1327997155.create_table.js',
                        '1327997224.create_table.js',
                        '1327997230.create_table.js',
                        '1327997243.migration.js',
                        '1327997250.migration.js',
                        '1327997262.migration.js']);
                    TSDB.reset();
                }

            }

        }
    }
});

suite.addBatch({
    "An Timestamp migrator should ":{
        topic:DB,

        " handle a bad migration file when migrating up":{
            topic:function(){
                TSDB.reset();
                var ret = [];
                patio.migrate(TSDB, __dirname + "/migrations/files/bad_timestamp_migration").then(hitch(this, "callback"), hitch(this, "callback", null));
            },

            "":function(res){
                assert.isTrue(TSDB.tables["test1"]);
                assert.isTrue(TSDB.tables["test2"]);
                assert.isTrue(TSDB.tables["test3"]);
                assert.deepEqual(TSDB.columnsCreated, ["column1", "column2", "column3"]);
                assert.lengthOf(patioMigrationFiles, 3);
                sortMigrationFiles();
                assert.deepEqual(patioMigrationFiles, ['1327997153.create_table.js', '1327997224.create_table.js', '1327997243.migration.js']);
            },

            " and down ":{
                topic:function(){
                    var ret = [];
                    patio.migrate(TSDB, __dirname + "/migrations/files/bad_timestamp_migration", {target:-1}).then(hitch(this, "callback"), hitch(this, "callback", null));
                },

                "":function(res){
                    assert.isTrue(TSDB.tables["test1"]);
                    assert.isNull(TSDB.tables["test2"]);
                    assert.isNull(TSDB.tables["test3"]);
                    assert.lengthOf(patioMigrationFiles, 1);
                    sortMigrationFiles();
                    assert.deepEqual(patioMigrationFiles, ['1327997153.create_table.js']);
                }
            }
        }
    }
});

suite.addBatch({
    "An Timestamp migrator should ":{
        topic:DB,

        " handle a duplicate timestamps when migrating up ":{
            topic:function(){
                TSDB.reset();
                var ret = [];
                patio.migrate(TSDB, __dirname + "/migrations/files/duplicate_timestamp_migration").then(hitch(this, "callback"), hitch(this, "callback", null));
            },

            "":function(res){
                assert.isTrue(TSDB.tables["test1"]);
                assert.isTrue(TSDB.tables["test2"]);
                assert.isTrue(TSDB.tables["test3"]);
                assert.isTrue(TSDB.tables["test4"]);
                assert.isTrue(TSDB.tables["test5"]);
                assert.deepEqual(TSDB.columnsCreated, ["column1", "column5", "column2", "column3", "column4"]);
                assert.lengthOf(patioMigrationFiles, 5);
                sortMigrationFiles();
                assert.deepEqual(patioMigrationFiles, [
                    '1327997153.create_table.js',
                    '1327997153.migration.js',
                    '1327997224.create_table.js',
                    '1327997243.migration.js',
                    '1327997262.migration.js'
                ]);
            },

            " and down ":{
                topic:function(){
                    var ret = [];
                    patio.migrate(TSDB, __dirname + "/migrations/files/duplicate_timestamp_migration", {target:-1}).then(hitch(this, "callback"), hitch(this, "callback", null));
                },

                "":function(res){
                    assert.isNull(TSDB.tables["test1"]);
                    assert.isNull(TSDB.tables["test2"]);
                    assert.isNull(TSDB.tables["test3"]);
                    assert.isNull(TSDB.tables["test4"]);
                    assert.isNull(TSDB.tables["test5"]);
                    assert.lengthOf(patioMigrationFiles, 0);
                }
            }
        }
    }
});

suite.addBatch({
    "An Timestamp migrator should ":{
        topic:DB,

        " convert integer migrations when migrating up ":{
            topic:function(){
                TSDB.reset();
                var ret = [];
                patio.migrate(TSDB, __dirname + "/migrations/files/basic_integer_migration")
                    .chain(comb.hitch(patio, "migrate", TSDB, __dirname + "/migrations/files/both_migration"), hitch(this, "callback", null))
                    .then(hitch(this, "callback"), hitch(this, "callback", null));
            },

            "":function(res){
                assert.isTrue(TSDB.tables["test1"]);
                assert.isTrue(TSDB.tables["test2"]);
                assert.isTrue(TSDB.tables["test3"]);
                assert.isTrue(TSDB.tables["test4"]);
                assert.isTrue(TSDB.tables["test5"]);
                assert.deepEqual(TSDB.columnsCreated, ["version", "column1", "column2", "column3", "column4", "column5"]);
                assert.deepEqual(TSDB.columnsAltered, {
                    column1:"column2",
                    column2:"column3",
                    column3:"column4",
                    column4:"column5",
                    column5:"column6"
                });
                assert.lengthOf(patioMigrationFiles, 7);
                sortMigrationFiles();
                assert.deepEqual(patioMigrationFiles, [
                    '0.create_tables.js',
                    '1.create_2.js',
                    '2.create_table.js',
                    '3.create_3.js',
                    '4.alter_tables.js',
                    '1327997153.create_table.js',
                    '1327997224.create_table.js']);
            },

            " and down ":{
                topic:function(){
                    var ret = [];
                    patio.migrate(TSDB, __dirname + "/migrations/files/both_migration", {target:-1}).then(hitch(this, "callback"), hitch(this, "callback", null));
                },

                "":function(res){
                    assert.isNull(TSDB.tables["test1"]);
                    assert.isNull(TSDB.tables["test2"]);
                    assert.isNull(TSDB.tables["test3"]);
                    assert.isNull(TSDB.tables["test4"]);
                    assert.isNull(TSDB.tables["test5"]);
                    assert.lengthOf(patioMigrationFiles, 0);
                }
            }
        }
    }
});


suite.run({reporter:require("vows").reporter.spec}, function(){
    ret.callback();
});



