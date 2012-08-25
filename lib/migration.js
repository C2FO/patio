var comb = require("comb"),
    hitch = comb.hitch,
    Promise = comb.Promise,
    errors = require("./errors"),
    MigrationError = errors.MigrationError,
    NotImplemented = errors.NotImplemented(),
    format = comb.string.format,
    define = comb.define,
    isFunction = comb.isFunction,
    serial = comb.serial,
    isNumber = comb.isNumber,
    when = comb.when,
    isUndefined = comb.isUndefined,
    fs = require("fs"),
    path = require("path");


var Migrator = define(null, {
    instance:{
        /**@lends patio.migrations.Migrator.prototype*/
        column:null,
        db:null,
        directory:null,
        ds:null,
        files:null,
        table:null,
        target:null,

        /**
         * Abstract Migrator class. This class should be be instantiated directly.
         *
         * @constructs
         * @param {patio.Database} db the database to migrate
         * @param {String} directory directory that the migration files reside in
         * @param {Object} [opts={}] optional parameters.
         * @param {String} [opts.column] the column in the table that version information should be stored.
         * @param {String} [opts.table] the table that version information should be stored.
         * @param {Number} [opts.target] the target migration(i.e the migration to migrate up/down to).
         * @param {String} [opts.current] the version that the database is currently at if the current version
         */
        constructor:function (db, directory, opts) {
            this.db = db;
            this.directory = directory;
            opts = opts || {};
            this.table = opts.table || this._static.DEFAULT_SCHEMA_TABLE;
            this.column = opts.column || this._static.DEFAULT_SCHEMA_COLUMN;
            this._opts = opts;
        },

        /**
         * Runs the migration and returns a promise.
         */
        run:function () {
            throw new NotImplemented("patio.migrations.Migrator#run");
        },

        getFileNames:function () {
            if (!this.__files) {
                return this._static.getFileNames(this.directory).addCallback(hitch(this, function (files) {
                    this.__files = files;
                }));
            } else {
                return new Promise().callback(this.__files).promise();
            }
        },

        getMigrationVersionFromFile:function (filename) {
            return parseInt(path.basename(filename).split(this._static.MIGRATION_SPLITTER)[0], 10);
        }
    },

    "static":{
        /**@lends patio.migrations.Migrator*/

        MIGRATION_FILE_PATTERN:/^\d+\..+\.js$/i,
        MIGRATION_SPLITTER:'.',
        MINIMUM_TIMESTAMP:20000101,

        getFileNames:function (directory) {
            var ret = new Promise();
            fs.readdir(directory, hitch(this, function (err, files) {
                if (err) {
                    ret.errback(err);
                } else {
                    files = files.filter(function (file) {
                        return file.match(this.MIGRATION_FILE_PATTERN) !== null;
                    }, this);
                    ret.callback(files.map(function (file) {
                        return path.resolve(directory, file);
                    }));
                }
            }));
            return ret.promise();
        },

        /**
         * Migrates the database using migration files found in the supplied directory.
         * See {@link patio#migrate}
         *
         * @example
         * var DB = patio.connect("my://connection/string");
         * patio. migrate(DB, __dirname + "/timestamp_migration").then(function(){
         *     console.log("done migrating!");
         * });
         *
         * patio. migrate(DB, __dirname + "/timestamp_migration", {target : 0}).then(function(){
         *     console.log("done migrating down!");
         * });
         *
         *
         * @param {patio.Database} db the database to migrate
         * @param {String} directory directory that the migration files reside in
         * @param {Object} [opts={}] optional parameters.
         * @param {String} [opts.column] the column in the table that version information should be stored.
         * @param {String} [opts.table] the table that version information should be stored.
         * @param {Number} [opts.target] the target migration(i.e the migration to migrate up/down to).
         * @param {String} [opts.current] the version that the database is currently at if the current version
         * is not provided it is retrieved from the database.
         *
         * @return {Promise} a promise that is resolved once the migration is complete.
         */
        run:function (db, directory, opts, cb) {
            if (isFunction(opts)) {
                cb = opts;
                opts = {};
            } else {
                opts = opts || {};
            }
            opts = opts || {};
            var ret = new Promise();
            this.__getMigrator(directory).then(function (migrator) {
                new migrator(db, directory, opts).run().then(ret);
            }, ret);
            ret.classic(cb);
            return ret.promise();
        },

        // Choose the Migrator subclass to use.  Uses the TimestampMigrator
        // // if the version number appears to be a unix time integer for a year
        // after 2005, otherwise uses the IntegerMigrator.
        __getMigrator:function (directory) {
            var ret = new Promise();
            var retClass = IntegerMigrator;
            this.getFileNames(directory).then(hitch(this, function (files) {
                var l = files.length;
                if (l) {
                    for (var i = 0; i < l; i++) {
                        var file = files[i];
                        if (parseInt(path.basename(file).split(this.MIGRATION_SPLITTER)[0], 10) > this.MINIMUM_TIMESTAMP) {
                            retClass = TimestampMigrator;
                            break;
                        }
                    }
                }
                ret.callback(retClass);

            }), ret);
            return ret.promise();
        }
    }
});


/**
 * @class Migrator that uses the file format {migrationName}.{version}.js, where version starts at 0.
 * <b>Missing migrations are not allowed</b>
 *
 * @augments patio.migrations.Migrator
 * @name IntegerMigrator
 * @memberOf patio.migrations
 */
var IntegerMigrator = define(Migrator, {
    instance:{
        /**@lends patio.migrations.IntegerMigrator.prototype*/
        current:null,
        direction:null,
        migrations:null,

        _migrationFiles:null,

        run:function () {
            var ret = new Promise(), DB = this.db;
            serial([this._getLatestMigrationVersion.bind(this), this._getCurrentMigrationVersion.bind(this)]).then(hitch(this, function (res) {
                var target = res[0], current = res[1];
                if (current !== target) {
                    var direction = this.direction = current < target ? "up" : "down", isUp = direction === "up", version = 0;
                    this._getMigrations(current, target, direction).then(hitch(this, function (migrations) {
                        var runMigration = hitch(this, function (index) {
                            if (index >= migrations.length) {
                                ret.callback(version);
                            } else {
                                var curr = migrations[index], migration = curr[0];
                                version = curr[1];
                                var now = new Date();
                                var lv = isUp ? version : version - 1;
                                DB.logInfo("Begin applying migration version %d, direction: %s", lv, direction);
                                DB.transaction(hitch(this, function () {
                                    var ret = new Promise();
                                    if (!isFunction(migration[direction])) {
                                        this._setMigrationVersion(lv).then(ret);
                                    } else {
                                        when(migration[direction].apply(DB, [DB])).then(hitch(this, function (args) {
                                            this._setMigrationVersion(lv).then(ret);
                                        }), ret);
                                    }
                                    return ret.promise();
                                })).then(function () {
                                        DB.logInfo("Finished applying migration version %d, direction: %s, took % 4dms seconds", lv, direction, new Date() - now);
                                        runMigration(index + 1);
                                    }, ret);
                            }

                        });
                        runMigration(0);
                    }), ret);
                } else {
                    ret.callback(target);
                }

            }), ret);
            return ret.promise();
        },

        _getMigrations:function (current, target, direction) {
            var ret = new Promise(), isUp = direction === "up", migrations = [];
            when(this._getMigrationFiles()).then(function (files) {
                try {
                    if ((isUp ? target : current - 1) < files.length) {
                        if (isUp) {
                            current++;
                        }
                        for (; isUp ? current <= target : current > target; isUp ? current++ : current--) {
                            migrations.push([require(files[current]), current]);
                        }
                    } else {
                        return ret.errback(new MigrationError("Invalid target " + target));
                    }
                } catch (e) {
                    return ret.errback(e);
                }
                ret.callback(migrations);
            }, ret);
            return ret.promise();
        },


        _getMigrationFiles:function () {
            var ret = new Promise();
            if (!this._migrationFiles) {
                var retFiles = [];
                var directory = this.directory;
                this.getFileNames().then(hitch(this, function (files) {
                    var l = files.length;
                    if (l) {
                        for (var i = 0; i < l; i++) {
                            var file = files[i];
                            var version = this.getMigrationVersionFromFile(file);
                            if (isUndefined(retFiles[version])) {
                                retFiles[version] = file;
                            } else {
                                return ret.errback(new MigrationError("Duplicate migration number " + version));
                            }
                        }
                        if (isUndefined(retFiles[0])) {
                            retFiles.shift();
                        }
                        for (var j = 0; j < l; j++) {
                            if (isUndefined(retFiles[j])) {
                                return ret.errback(new MigrationError("Missing migration for " + j));
                            }
                        }
                    }
                    this._migrationFiles = retFiles;
                    ret.callback(retFiles);
                }), ret);
            } else {
                ret.callback(this._migrationFiles);
            }
            return ret.promise();
        },

        _getLatestMigrationVersion:function () {
            var ret = new Promise();
            if (!isUndefined(this._opts.target)) {
                ret.callback(this._opts.target);
            } else {
                this._getMigrationFiles().then(hitch(this, function (files) {
                    var l = files[files.length - 1];
                    ret.callback(l ? this.getMigrationVersionFromFile(path.basename(l)) : null);
                }), ret);
            }
            return ret.promise();
        },

        _getCurrentMigrationVersion:function () {
            var ret = new Promise();
            if (!isUndefined(this._opts.current)) {
                ret.callback(this._opts.current);
            } else {
                when(this._getSchemaDataset()).then(hitch(this, function (ds) {
                    ds.get(this.column).then(ret);
                }), ret);
            }
            return ret.promise();
        },

        _setMigrationVersion:function (version) {
            var ret = new Promise(), c = this.column;
            this._getSchemaDataset().then(function (ds) {
                var item = {};
                item[c] = version;
                ds.update(item).both(ret);
            }, ret);

            return ret.promise();
        },

        _getSchemaDataset:function () {
            var c = this.column, table = this.table;
            var ret = new Promise();
            if (!this.__schemaDataset) {
                var ds = this.db.from(table);
                this.__createOrAlterMigrationTable().then(hitch(this, function () {
                    ds.isEmpty().then(hitch(this, function (empty) {
                        if (empty) {
                            var item = {};
                            item[c] = -1;
                            this.__schemaDataset = ds;
                            ds.insert(item).then(hitch(ret, "callback", ds), ret);
                        } else {
                            ds.count().then(hitch(this, function (count) {
                                if (count > 1) {
                                    ret.errback(new Error("More than one row in migrator table"));
                                } else {
                                    this.__schemaDataset = ds;
                                    ret.callback(ds);
                                }
                            }), ret);
                        }
                    }), ret);
                }), ret);
            } else {
                ret.callback(this.__schemaDataset);
            }
            return ret.promise();
        },

        __createOrAlterMigrationTable:function () {
            var c = this.column, table = this.table, db = this.db;
            var ds = this.db.from(table);
            var ret = new Promise();
            db.tableExists(table).then(hitch(this, function (exists) {
                if (!exists) {
                    db.createTable(table,
                        function () {
                            this.column(c, "integer", {"default":-1, allowNull:false});
                        }).then(ret);
                } else {
                    ds.columns.then(function (columns) {
                        if (columns.indexOf(c) === -1) {
                            db.addColumn(table, c, "integer", {"default":-1, allowNull:false})
                                .then(ret);
                        } else {
                            ret.callback();
                        }
                    });
                }
            }), ret);
            return ret.promise();
        }

    },

    static:{
        DEFAULT_SCHEMA_COLUMN:"version",
        DEFAULT_SCHEMA_TABLE:"schema_info"
    }
}).as(exports, "IntegerMigrator");


/**
 * @class Migrator that uses the file format {migrationName}.{timestamp}.js, where the timestamp
 * can be anything greater than 20000101.
 *
 * @name TimestampMigrator
 * @augments patio.migrations.Migrator
 * @memberOf patio.migrations
 */
var TimestampMigrator = define(Migrator, {
    instance:{

        constructor:function (db, directory, opts) {
            this._super(arguments);
            opts = opts || {};
            this.target = opts.target;
        },

        run:function () {
            var ret = new Promise(), DB = this.db, column = this.column;
            serial([this.__getMirationFiles.bind(this), this._getSchemaDataset.bind(this)]).then(function (res) {
                var migrations = res[0], ds = res[1];
                var runMigration = hitch(this, function (index) {
                    if (index >= migrations.length) {
                        ret.callback();
                    } else {
                        var curr = migrations[index], file = curr[0], migration = curr[1], direction = curr[2];
                        var now = new Date();
                        DB.logInfo("Begin applying migration file %s, direction: %s", file, direction);
                        DB.transaction(hitch(this, function () {
                            var ret = new Promise();
                            when(migration[direction].apply(DB, [DB])).then(hitch(this, function (args) {
                                var fileLowerCase = file.toLowerCase();
                                var query = {};
                                query[column] = fileLowerCase;
                                (direction === "up" ? ds.insert(query) : ds.filter(query).remove()).then(ret);
                            }), ret);
                            return ret.promise();
                        })).then(function () {
                                DB.logInfo("Finished applying migration file %s, direction: %s, took % 4dms seconds", file, direction, new Date() - now);
                                runMigration(index + 1);
                            }, ret);
                    }

                });
                runMigration(0);
            }, ret);
            return ret.promise();
        },

        getFileNames:function () {
            var ret = new Promise();
            var sup = this._super(arguments);
            sup.then(hitch(this, function (files) {
                ret.callback(files.sort(hitch(this, function (f1, f2) {
                    return this.getMigrationVersionFromFile(f1) - this.getMigrationVersionFromFile(f2);
                })));
            }), ret);
            return ret.promise();
        },

        __getAppliedMigrations:function () {
            var ret = new Promise();
            if (!this.__appliedMigrations) {
                this._getSchemaDataset().then(hitch(this, function (ds) {
                    when(
                        ds.selectOrderMap(this.column),
                        this.getFileNames()
                    ).then(hitch(this, function (res) {
                        var appliedMigrations = res[0], files = res[1].map(function (f) {
                            return path.basename(f).toLowerCase();
                        });
                        var l = appliedMigrations.length;
                        if (l) {
                            for (var i = 0; i < l; i++) {
                                if (files.indexOf(appliedMigrations[i]) == -1) {
                                    return ret.errback("Applied migrations file not found in directory " + appliedMigrations[i]);
                                }
                            }
                            this.__appliedMigrations = appliedMigrations;
                            ret.callback(appliedMigrations);
                        } else {
                            this.__appliedMigrations = [];
                            ret.callback([]);
                        }
                    }), ret);
                }), ret);
            } else {
                ret.callback(this.__appliedMigrations);
            }
            return ret.promise();
        },

        __getMirationFiles:function () {
            var ret = new Promise();
            var upMigrations = [], downMigrations = [], target = this.target;
            if (!this.__migrationFiles) {
                when(
                    this.getFileNames(),
                    this.__getAppliedMigrations()
                ).then(hitch(this, function (res) {
                    var files = res[0], appliedMigrations = res[1];
                    var l = files.length, inserts = [];
                    if (l > 0) {
                        try {
                            for (var i = 0; i < l; i++) {
                                var file = files[i], f = path.basename(file), fLowerCase = f.toLowerCase();
                                if (!isUndefined(target)) {
                                    var version = this.getMigrationVersionFromFile(f);
                                    if (version > target || (version === 0 && target === version)) {
                                        if (appliedMigrations.indexOf(fLowerCase) != -1) {
                                            downMigrations.push([f, require(file), "down"]);
                                        }
                                    } else if (appliedMigrations.indexOf(fLowerCase) == -1) {
                                        upMigrations.push([f, require(file), "up"]);
                                    }
                                } else if (appliedMigrations.indexOf(fLowerCase) == -1) {
                                    upMigrations.push([f, require(file), "up"]);
                                }
                            }
                        } catch (e) {
                            return ret.errback(e)
                        }
                        this.__migrationFiles = upMigrations.concat(downMigrations.reverse())
                        ret.callback(this.__migrationFiles);
                    } else {
                        return ret.callback();
                    }
                }), ret);
            } else {
                ret.callback(this.__migrationFiles);
            }
            return ret.promise();
        },


        // Returns the dataset for the schema_migrations table. If no such table
        // exists, it is automatically created.
        _getSchemaDataset:function () {
            var ret = new Promise();
            if (!this.__schemaDataset) {
                var ds = this.db.from(this.table);
                this.__createTable().then(hitch(this, function () {
                    this.__schemaDataset = ds;
                    ret.callback(ds);
                }), ret);
            } else {
                ret.callback(this.__schemaDataset);
            }
            return ret.promise();
        },

        __convertSchemaInfo:function () {
            var ret = new Promise(), c = this.column;
            var ds = this.db.from(this.table);
            this.db.from(IntegerMigrator.DEFAULT_SCHEMA_TABLE).get(IntegerMigrator.DEFAULT_SCHEMA_COLUMN).then(hitch(this, function (version) {
                this.getFileNames().then(hitch(this, function (files) {
                    var l = files.length, inserts = [];
                    if (l > 0) {
                        for (var i = 0; i < l; i++) {
                            var f = path.basename(files[i]);
                            if (this.getMigrationVersionFromFile(f) <= version) {
                                var insert = {};
                                insert[c] = f;
                                inserts.push(ds.insert(insert));
                            }
                        }
                    }
                    if (inserts.length) {
                        when.apply(comb, inserts).then(ret);
                    } else {
                        ret.callback();
                    }
                }), ret);
            }), ret);
            return ret.promise();

        },

        __createTable:function () {
            var c = this.column, table = this.table, db = this.db, intMigrationTable = IntegerMigrator.DEFAULT_SCHEMA_TABLE;
            var ds = this.db.from(table);
            var ret = new Promise();
            when(
                db.tableExists(table),
                db.tableExists(intMigrationTable)
            ).then(hitch(this, function (res) {
                var exists = res[0], intMigratorExists = res[1];
                if (!exists) {
                    db.createTable(table,
                        function () {
                            this.column(c, String, {primaryKey:true});
                        }).addErrback(ret);
                    if (intMigratorExists) {
                        db.from(intMigrationTable).all().then(hitch(this, function (versions) {
                            var version;
                            if (versions.length === 1 && (version = versions[0]) && isNumber(version[Object.keys(version)[0]])) {
                                this.__convertSchemaInfo().then(ret);
                            } else {
                                ret.callback();
                            }
                        }));
                    } else {
                        ret.callback();
                    }
                } else {
                    ds.columns.then(hitch(this, function (columns) {
                        if (columns.indexOf(c) === -1) {
                            ret.errback(new MigrationError(format("Migration table %s does not contain column %s", table, c)));
                        } else {
                            ret.callback();
                        }
                    }), ret);
                }
            }), ret);
            return ret.promise();
        }
    },

    static:{
        DEFAULT_SCHEMA_COLUMN:"filename",
        DEFAULT_SCHEMA_TABLE:"schema_migrations"
    }
}).as(exports, "TimestampMigrator");

exports.run = function () {
    return Migrator.run.apply(Migrator, arguments);
};