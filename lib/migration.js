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
    path = require("path"),
    baseName = path.basename,
    asyncArray = comb.async.array;


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
                    }, this).map(function (file) {
                            return path.resolve(directory, file);
                        });
                    files.sort();
                    ret.callback(files);
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
            return this.__getMigrator(directory).chain(function (migrator) {
                return new migrator(db, directory, opts).run();
            }).classic(cb);
        },

        // Choose the Migrator subclass to use.  Uses the TimestampMigrator
        // // if the version number appears to be a unix time integer for a year
        // after 2005, otherwise uses the IntegerMigrator.
        __getMigrator:function (directory) {
            var retClass = IntegerMigrator, ret = new Promise();
            this.getFileNames(directory).then(function (files) {
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
            }.bind(this), ret);
            return ret;
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
            return serial([this._getLatestMigrationVersion.bind(this), this._getCurrentMigrationVersion.bind(this)]).chain(function (res) {
                var target = res[0], current = res[1];
                if (current !== target) {
                    var direction = this.direction = current < target ? "up" : "down", isUp = direction === "up", version = 0;
                    return this._getMigrations(current, target, direction).chain(function (migrations) {
                        return asyncArray(migrations).forEach(function (curr) {
                            var migration = curr[0];
                            version = curr[1];
                            var now = new Date();
                            var lv = isUp ? version : version - 1;
                            DB.logInfo("Begin applying migration version %d, direction: %s", lv, direction);
                            return DB.transaction(hitch(this, function (db) {
                                if (!isFunction(migration[direction])) {
                                    return this._setMigrationVersion(lv);
                                } else {
                                    var nextP = new Promise();
                                    var dirP = migration[direction].apply(DB, [DB, nextP.resolve.bind(nextP)]);
                                    return (comb.isPromiseLike(dirP) ? dirP : nextP).chain(comb(this._setMigrationVersion).bindIgnore(this, lv));
                                }
                            })).chain(function () {
                                    DB.logInfo("Finished applying migration version %d, direction: %s, took % 4dms seconds", lv, direction, new Date() - now);
                                });
                        }, this, 1).chain(function () {
                                return version;
                            });
                    }.bind(this));
                } else {
                    return target;
                }

            }.bind(this));
        },

        _getMigrations:function (current, target, direction) {
            var ret = new Promise(), isUp = direction === "up", migrations = [];
            return when(this._getMigrationFiles()).chain(function (files) {
                if ((isUp ? target : current - 1) < files.length) {
                    if (isUp) {
                        current++;
                    }
                    for (; isUp ? current <= target : current > target; isUp ? current++ : current--) {
                        migrations.push([require(files[current]), current]);
                    }
                } else {
                    throw new MigrationError("Invalid target " + target);
                }
                return migrations;
            });
        },


        _getMigrationFiles:function () {
            if (!this._migrationFiles) {
                var retFiles = [];
                return this.getFileNames().chain(hitch(this, function (files) {
                    var l = files.length;
                    if (l) {
                        for (var i = 0; i < l; i++) {
                            var file = files[i];
                            var version = this.getMigrationVersionFromFile(file);
                            if (isUndefined(retFiles[version])) {
                                retFiles[version] = file;
                            } else {
                                throw new MigrationError("Duplicate migration number " + version);
                            }
                        }
                        if (isUndefined(retFiles[0])) {
                            retFiles.shift();
                        }
                        for (var j = 0; j < l; j++) {
                            if (isUndefined(retFiles[j])) {
                                throw new MigrationError("Missing migration for " + j);
                            }
                        }
                    }
                    this._migrationFiles = retFiles;
                    return retFiles;
                }));
            } else {
                return when(this._migrationFiles);
            }
        },

        _getLatestMigrationVersion:function () {
            if (!isUndefined(this._opts.target)) {
                return when(this._opts.target);
            } else {
                return this._getMigrationFiles().chain(hitch(this, function (files) {
                    var l = files[files.length - 1];
                    return l ? this.getMigrationVersionFromFile(path.basename(l)) : null;
                }));
            }
        },

        _getCurrentMigrationVersion:function () {
            if (!isUndefined(this._opts.current)) {
                return when(this._opts.current);
            } else {
                return when(this._getSchemaDataset()).chain(hitch(this, function (ds) {
                    return ds.get(this.column);
                }));
            }
        },

        _setMigrationVersion:function (version) {
            var c = this.column;
            return this._getSchemaDataset().chain(function (ds) {
                var item = {};
                item[c] = version;
                return ds.update(item).chainBoth();
            });

        },

        _getSchemaDataset:function () {
            var c = this.column, table = this.table;
            if (!this.__schemaDataset) {
                var ds = this.db.from(table);
                return this.__createOrAlterMigrationTable().chain(hitch(this, function () {
                    return ds.isEmpty().chain(hitch(this, function (empty) {
                        if (empty) {
                            var item = {};
                            item[c] = -1;
                            this.__schemaDataset = ds;
                            return ds.insert(item).chain(ds);
                        } else {
                            return ds.count().chain(hitch(this, function (count) {
                                if (count > 1) {
                                    throw new Error("More than one row in migrator table");
                                } else {
                                    this.__schemaDataset = ds;
                                    return ds;
                                }
                            }));
                        }
                    }));
                }));
            } else {
                return when(this.__schemaDataset);
            }
        },

        __createOrAlterMigrationTable:function () {
            var c = this.column, table = this.table, db = this.db;
            var ds = this.db.from(table);
            var ret = new Promise();
            return db.tableExists(table).chain(hitch(this, function (exists) {
                if (!exists) {
                    return db.createTable(table, function () {
                        this.column(c, "integer", {"default":-1, allowNull:false});
                    });
                } else {
                    return ds.columns.chain(function (columns) {
                        if (columns.indexOf(c) === -1) {
                            db.addColumn(table, c, "integer", {"default":-1, allowNull:false});
                        }
                    });
                }
            }));
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
            return serial([this.__getMigrationFiles.bind(this), this._getSchemaDataset.bind(this)]).chain(function (res) {
                var migrations = res[0], ds = res[1];
                return asyncArray(migrations).forEach(function (curr) {
                    var file = curr[0], migration = curr[1], direction = curr[2];
                    var now = new Date();
                    DB.logInfo("Begin applying migration file %s, direction: %s", file, direction);
                    return DB.transaction(hitch(this, function () {
                        var fileLowerCase = file.toLowerCase();
                        var query = {};
                        query[column] = fileLowerCase;
                        if (!isFunction(migration[direction])) {
                            return (direction === "up" ? ds.insert(query) : ds.filter(query).remove());
                        } else {
                            var nextP = new Promise();
                            var dirP = migration[direction].apply(DB, [DB, nextP.resolve.bind(nextP)]);
                            return (comb.isPromiseLike(dirP) ? dirP : nextP).chain(function () {
                                return (direction === "up" ? ds.insert(query) : ds.filter(query).remove());
                            }.bind(this));
                        }
                    })).chain(function () {
                            DB.logInfo("Finished applying migration file %s, direction: %s, took % 4dms seconds", file, direction, new Date() - now);
                        });
                }, this, 1);
            });
        },

        getFileNames:function () {
            return asyncArray(this._super(arguments)).sort(function (f1, f2) {
                var ret = this.getMigrationVersionFromFile(f1) - this.getMigrationVersionFromFile(f2);
                if (ret === 0) {
                    var b1 = baseName(f1, ".js").split("."),
                        b2 = baseName(f2, ".js").split(".");
                    b1 = b1[b1.length - 1];
                    b2 = b2[b2.length - 1];
                    ret = b1 > b1 ? 1 : b1 < b2 ? -1 : 0;
                }
                return ret;
            }.bind(this));
        },

        __getAppliedMigrations:function () {
            if (!this.__appliedMigrations) {
                return this._getSchemaDataset().chain(hitch(this, function (ds) {
                    return when(ds.selectOrderMap(this.column), this.getFileNames()).chain(hitch(this, function (res) {
                        var appliedMigrations = res[0], files = res[1].map(function (f) {
                            return path.basename(f).toLowerCase();
                        });
                        var l = appliedMigrations.length;
                        if (l) {
                            for (var i = 0; i < l; i++) {
                                if (files.indexOf(appliedMigrations[i]) == -1) {
                                    throw new MigrationError("Applied migrations file not found in directory " + appliedMigrations[i]);
                                }
                            }
                            this.__appliedMigrations = appliedMigrations;
                            return appliedMigrations;
                        } else {
                            this.__appliedMigrations = [];
                            return appliedMigrations;
                        }

                    }));
                }));
            } else {
                return when(this.__appliedMigrations);
            }
        },

        __getMigrationFiles:function () {
            var upMigrations = [], downMigrations = [], target = this.target;
            if (!this.__migrationFiles) {
                return when(this.getFileNames(), this.__getAppliedMigrations()).chain(hitch(this, function (res) {
                    var files = res[0], appliedMigrations = res[1];
                    var l = files.length;
                    if (l > 0) {
                        for (var i = 0; i < l; i++) {
                            var file = files[i], f = path.basename(file), fLowerCase = f.toLowerCase(), index = appliedMigrations.indexOf(fLowerCase);
                            if (!isUndefined(target)) {
                                var version = this.getMigrationVersionFromFile(f);
                                if (version > target || (version === 0 && target === version)) {
                                    if (index !== -1) {
                                        downMigrations.push([f, require(file), "down"]);
                                    }
                                } else if (index === -1) {
                                    upMigrations.push([f, require(file), "up"]);
                                }
                            } else if (index === -1) {
                                upMigrations.push([f, require(file), "up"]);
                            }
                        }
                        this.__migrationFiles = upMigrations.concat(downMigrations.reverse());
                        return this.__migrationFiles;
                    }
                }));
            } else {
                return when(this.__migrationFiles);
            }
        },


        // Returns the dataset for the schema_migrations table. If no such table
        // exists, it is automatically created.
        _getSchemaDataset:function () {
            if (!this.__schemaDataset) {
                var ds = this.db.from(this.table);
                return this.__createTable().chain(hitch(this, function () {
                    return (this.__schemaDataset = ds);
                }));
            } else {
                return when(this.__schemaDataset);
            }
        },

        __convertSchemaInfo:function () {
            var ret = new Promise(), c = this.column;
            var ds = this.db.from(this.table);
            return this.db.from(IntegerMigrator.DEFAULT_SCHEMA_TABLE).get(IntegerMigrator.DEFAULT_SCHEMA_COLUMN).chain(hitch(this, function (version) {
                return this.getFileNames().chain(hitch(this, function (files) {
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
                    return when(inserts);
                }));
            }));

        },

        __createTable:function () {
            var c = this.column, table = this.table, db = this.db, intMigrationTable = IntegerMigrator.DEFAULT_SCHEMA_TABLE;
            var ds = this.db.from(table);
            return when(db.tableExists(table), db.tableExists(intMigrationTable)).chain(hitch(this, function (res) {
                var exists = res[0], intMigratorExists = res[1];
                if (!exists) {
                    return db.createTable(table,function () {
                        this.column(c, String, {primaryKey:true});
                    }).chain(function () {
                        if (intMigratorExists) {
                            return db.from(intMigrationTable).all().chain(hitch(this, function (versions) {
                                var version;
                                if (versions.length === 1 && (version = versions[0]) && isNumber(version[Object.keys(version)[0]])) {
                                    return this.__convertSchemaInfo();
                                }
                            }));
                        }
                    }.bind(this));
                } else {
                    return ds.columns.chain(hitch(this, function (columns) {
                        if (columns.indexOf(c) === -1) {
                            throw new MigrationError(format("Migration table %s does not contain column %s", table, c));
                        }
                    }));
                }
            }));
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