var comb = require("comb"),
    Promise = comb.Promise,
    errors = require("./errors"),
    MigrationError = errors.MigrationError,
    NotImplemented = errors.NotImplemented(),
    format = comb.string.format,
    define = comb.define,
    isFunction = comb.isFunction,
    isNumber = comb.isNumber,
    when = comb.when,
    isUndefined = comb.isUndefined,
    fs = require("fs"),
    path = require("path"),
    baseName = path.basename,
    asyncArray = comb.async.array,
    IntegerMigrator,
    TimestampMigrator;


var Migrator = define(null, {
    instance: {
        /**@lends patio.migrations.Migrator.prototype*/
        column: null,
        db: null,
        directory: null,
        ds: null,
        files: null,
        table: null,
        target: null,

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
        constructor: function (db, directory, opts) {
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
        run: function () {
            throw new NotImplemented("patio.migrations.Migrator#run");
        },

        getFileNames: function () {
            if (!this.__files) {
                var self = this;
                return this._static.getFileNames(this.directory).chain(function (files) {
                    self.__files = files;
                    return files;
                });
            } else {
                return new Promise().callback(this.__files).promise();
            }
        },

        getMigrationVersionFromFile: function (filename) {
            return parseInt(path.basename(filename).split(this._static.MIGRATION_SPLITTER)[0], 10);
        }
    },

    "static": {
        /**@lends patio.migrations.Migrator*/

        MIGRATION_FILE_PATTERN: /^\d+\..+\.js$/i,
        MIGRATION_SPLITTER: '.',
        MINIMUM_TIMESTAMP: 20000101,

        getFileNames: function (directory) {
            var ret = new Promise(), self = this, pattern = this.MIGRATION_FILE_PATTERN;
            fs.readdir(directory, function (err, files) {
                if (err) {
                    ret.errback(err);
                } else {
                    files = files.filter(function (file) {
                        return file.match(pattern) !== null;
                    }).map(function (file) {
                        return path.resolve(directory, file);
                    });
                    files.sort();
                    ret.callback(files);
                }
            });
            return ret.promise();
        },

        /**
         * Migrates the database using migration files found in the supplied directory.
         * See {@link patio#migrate}
         *
         * @example
         * var DB = patio.connect("my://connection/string");
         * patio. migrate(DB, __dirname + "/timestamp_migration").chain(function(){
         *     console.log("done migrating!");
         * });
         *
         * patio. migrate(DB, __dirname + "/timestamp_migration", {target : 0}).chain(function(){
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
        run: function (db, directory, opts, cb) {
            if (isFunction(opts)) {
                cb = opts;
                opts = {};
            } else {
                opts = opts || {};
            }
            opts = opts || {};
            return this.__getMigrator(directory).chain(function (Migrator) {
                return new Migrator(db, directory, opts).run();
            }).classic(cb);
        },

        // Choose the Migrator subclass to use.  Uses the TimestampMigrator
        // // if the version number appears to be a unix time integer for a year
        // after 2005, otherwise uses the IntegerMigrator.
        __getMigrator: function (directory) {
            var retClass = IntegerMigrator, MIGRATION_SPLITTER = this.MIGRATION_SPLITTER, MINIMUM_TIMESTAMP = this.MINIMUM_TIMESTAMP;
            return this.getFileNames(directory).chain(function (files) {
                var l = files.length;
                if (l) {
                    for (var i = 0; i < l; i++) {
                        var file = files[i];
                        if (parseInt(path.basename(file).split(MIGRATION_SPLITTER)[0], 10) > MINIMUM_TIMESTAMP) {
                            retClass = TimestampMigrator;
                            break;
                        }
                    }
                }
                return retClass;
            });
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
IntegerMigrator = define(Migrator, {
    instance: {
        /**@lends patio.migrations.IntegerMigrator.prototype*/
        current: null,
        direction: null,
        migrations: null,

        _migrationFiles: null,

        run: function () {
            var DB = this.db, self = this;
            return this._getLatestMigrationVersion().chain(function (target) {
                return self._getCurrentMigrationVersion().chain(function (current) {
                    if (current !== target) {
                        var direction = self.direction = current < target ? "up" : "down", isUp = direction === "up", version = 0;
                        return self._getMigrations(current, target, direction).chain(function (migrations) {
                            return asyncArray(migrations).forEach(function (curr) {
                                var migration = curr[0];
                                version = curr[1];
                                var now = new Date();
                                var lv = isUp ? version : version - 1;
                                DB.logInfo("Begin applying migration version %d, direction: %s", lv, direction);
                                return DB.transaction(function () {
                                    if (!isFunction(migration[direction])) {
                                        return self._setMigrationVersion(lv);
                                    } else {
                                        var nextP = new Promise();
                                        var dirP = migration[direction].apply(DB, [DB, nextP.resolve.bind(nextP)]);
                                        return (comb.isPromiseLike(dirP) ? dirP : nextP).chain(function () {
                                            return self._setMigrationVersion(lv);
                                        });
                                    }
                                }).chain(function () {
                                    DB.logInfo("Finished applying migration version %d, direction: %s, took % 4dms seconds", lv, direction, new Date() - now);
                                });
                            }, 1).chain(function () {
                                return version;
                            });
                        });
                    } else {
                        return target;
                    }

                });
            })
                .chain(function (version) {
                    return version;
                });
        },

        _getMigrations: function (current, target, direction) {
            var isUp = direction === "up", migrations = [];
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


        _getMigrationFiles: function () {
            if (!this._migrationFiles) {
                var retFiles = [], self = this;
                return this.getFileNames().chain(function (files) {
                    var l = files.length;
                    if (l) {
                        for (var i = 0; i < l; i++) {
                            var file = files[i];
                            var version = self.getMigrationVersionFromFile(file);
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
                    self._migrationFiles = retFiles;
                    return retFiles;
                });
            } else {
                return when(this._migrationFiles);
            }
        },

        _getLatestMigrationVersion: function () {
            if (!isUndefined(this._opts.target)) {
                return when(this._opts.target);
            } else {
                var self = this;
                return this._getMigrationFiles().chain(function (files) {
                    var l = files[files.length - 1];
                    return l ? self.getMigrationVersionFromFile(path.basename(l)) : null;
                });
            }
        },

        _getCurrentMigrationVersion: function () {
            if (!isUndefined(this._opts.current)) {
                return when(this._opts.current);
            } else {
                var column = this.column;
                return when(this._getSchemaDataset()).chain(function (ds) {
                    return ds.get(column);
                });
            }
        },

        _setMigrationVersion: function (version) {
            var c = this.column;
            return this._getSchemaDataset().chain(function (ds) {
                var item = {};
                item[c] = version;
                return ds.update(item).chainBoth();
            });

        },

        _getSchemaDataset: function () {
            var c = this.column, table = this.table;
            if (!this.__schemaDataset) {
                var ds = this.db.from(table), self = this;
                return this.__createOrAlterMigrationTable().chain(function () {
                    return ds.isEmpty().chain(function (empty) {
                        if (empty) {
                            var item = {};
                            item[c] = -1;
                            self.__schemaDataset = ds;
                            return ds.insert(item).chain(function () {
                                return ds;
                            });
                        } else {
                            return ds.count().chain(function (count) {
                                if (count > 1) {
                                    throw new Error("More than one row in migrator table");
                                } else {
                                    self.__schemaDataset = ds;
                                    return ds;
                                }
                            });
                        }
                    });
                });
            } else {
                return when(this.__schemaDataset);
            }
        },

        __createOrAlterMigrationTable: function () {
            var c = this.column, table = this.table, db = this.db, ds = this.db.from(table), self = this;
            return db.tableExists(table).chain(function (exists) {
                if (!exists) {
                    return db.createTable(table, function () {
                        this.column(c, "integer", {"default": -1, allowNull: false});
                    });
                } else {
                    return ds.columns.chain(function (columns) {
                        if (columns.indexOf(c) === -1) {
                            db.addColumn(table, c, "integer", {"default": -1, allowNull: false});
                        }
                    });
                }
            });
        }

    },

    static: {
        DEFAULT_SCHEMA_COLUMN: "version",
        DEFAULT_SCHEMA_TABLE: "schema_info"
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
TimestampMigrator = define(Migrator, {
    instance: {

        constructor: function (db, directory, opts) {
            this._super(arguments);
            opts = opts || {};
            this.target = opts.target;
        },

        run: function () {
            var DB = this.db, column = this.column, self = this;
            return this.__getMigrationFiles().chain(function (migrations) {
                return self._getSchemaDataset().chain(function (ds) {
                    return asyncArray(migrations).forEach(function (curr) {
                        var file = curr[0], migration = curr[1], direction = curr[2];
                        var now = new Date();
                        DB.logInfo("Begin applying migration file %s, direction: %s", file, direction);
                        return DB.transaction(function () {
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
                                });
                            }
                        }).chain(function () {
                            DB.logInfo("Finished applying migration file %s, direction: %s, took % 4dms seconds", file, direction, new Date() - now);
                        });
                    }, 1);
                });
            });
        },

        getFileNames: function () {
            var self = this;
            return asyncArray(this._super(arguments)).sort(function (f1, f2) {
                var ret = self.getMigrationVersionFromFile(f1) - self.getMigrationVersionFromFile(f2);
                if (ret === 0) {
                    var b1 = baseName(f1, ".js").split("."),
                        b2 = baseName(f2, ".js").split(".");
                    b1 = b1[b1.length - 1];
                    b2 = b2[b2.length - 1];
                    ret = b1 > b1 ? 1 : b1 < b2 ? -1 : 0;
                }
                return ret;
            });
        },

        __getAppliedMigrations: function () {
            if (!this.__appliedMigrations) {
                var self = this;
                return this._getSchemaDataset().chain(function (ds) {
                    return when(ds.selectOrderMap(self.column), self.getFileNames()).chain(function (res) {
                        var appliedMigrations = res[0], files = res[1].map(function (f) {
                            return path.basename(f).toLowerCase();
                        });
                        var l = appliedMigrations.length;
                        if (l) {
                            for (var i = 0; i < l; i++) {
                                if (files.indexOf(appliedMigrations[i]) === -1) {
                                    throw new MigrationError("Applied migrations file not found in directory " + appliedMigrations[i]);
                                }
                            }
                            self.__appliedMigrations = appliedMigrations;
                            return appliedMigrations;
                        } else {
                            self.__appliedMigrations = [];
                            return appliedMigrations;
                        }

                    });
                });
            } else {
                return when(this.__appliedMigrations);
            }
        },

        __getMigrationFiles: function () {
            var upMigrations = [], downMigrations = [], target = this.target;
            if (!this.__migrationFiles) {
                var self = this;
                return when(this.getFileNames(), this.__getAppliedMigrations()).chain(function (res) {
                    var files = res[0], appliedMigrations = res[1];
                    var l = files.length;
                    if (l > 0) {
                        for (var i = 0; i < l; i++) {
                            var file = files[i], f = path.basename(file), fLowerCase = f.toLowerCase(), index = appliedMigrations.indexOf(fLowerCase);
                            if (!isUndefined(target)) {
                                var version = self.getMigrationVersionFromFile(f);
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
                        self.__migrationFiles = upMigrations.concat(downMigrations.reverse());
                        return self.__migrationFiles;
                    }
                });
            } else {
                return when(this.__migrationFiles);
            }
        },


        // Returns the dataset for the schema_migrations table. If no such table
        // exists, it is automatically created.
        _getSchemaDataset: function () {
            if (!this.__schemaDataset) {
                var ds = this.db.from(this.table), self = this;
                return this.__createTable().chain(function () {
                    return (self.__schemaDataset = ds);
                });
            } else {
                return when(this.__schemaDataset);
            }
        },

        __convertSchemaInfo: function () {
            var c = this.column, ds = this.db.from(this.table), self = this;
            return this.db.from(IntegerMigrator.DEFAULT_SCHEMA_TABLE).get(IntegerMigrator.DEFAULT_SCHEMA_COLUMN).chain(function (version) {
                return self.getFileNames().chain(function (files) {
                    var l = files.length, inserts = [];
                    if (l > 0) {
                        for (var i = 0; i < l; i++) {
                            var f = path.basename(files[i]);
                            if (self.getMigrationVersionFromFile(f) <= version) {
                                var insert = {};
                                insert[c] = f;
                                inserts.push(ds.insert(insert));
                            }
                        }
                    }
                    return when(inserts);
                });
            });

        },

        __createTable: function () {
            var c = this.column, table = this.table, db = this.db, intMigrationTable = IntegerMigrator.DEFAULT_SCHEMA_TABLE;
            var ds = this.db.from(table), self = this;
            return when(db.tableExists(table), db.tableExists(intMigrationTable)).chain(function (res) {
                var exists = res[0], intMigratorExists = res[1];
                if (!exists) {
                    return db.createTable(table, function () {
                        this.column(c, String, {primaryKey: true});
                    }).chain(function () {
                        if (intMigratorExists) {
                            return db.from(intMigrationTable).all().chain(function (versions) {
                                var version;
                                if (versions.length === 1 && (version = versions[0]) && isNumber(version[Object.keys(version)[0]])) {
                                    return self.__convertSchemaInfo();
                                }
                            });
                        }
                    });
                } else {
                    return ds.columns.chain(function (columns) {
                        if (columns.indexOf(c) === -1) {
                            throw new MigrationError(format("Migration table %s does not contain column %s", table, c));
                        }
                    });
                }
            });
        }
    },

    static: {
        DEFAULT_SCHEMA_COLUMN: "filename",
        DEFAULT_SCHEMA_TABLE: "schema_migrations"
    }
}).as(exports, "TimestampMigrator");

exports.run = function () {
    return Migrator.run.apply(Migrator, arguments);
};