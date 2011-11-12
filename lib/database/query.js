var comb = require("comb"), hitch = comb.hitch, format = comb.string.format, Promise = comb.Promise, PromiseList = comb.PromiseList, errors = require("../errors"), NotImplemented = errors.NotImplemented;


var Database = comb.define(null, {
    instance : {

        connectionExecuteMethod : "execute",
        SQL_BEGIN : 'BEGIN',
        SQL_COMMIT : 'COMMIT',
        SQL_RELEASE_SAVEPOINT : 'RELEASE SAVEPOINT autopoint_%d',
        SQL_ROLLBACK : 'ROLLBACK',
        SQL_ROLLBACK_TO_SAVEPOINT : 'ROLLBACK TO SAVEPOINT autopoint_%d',
        SQL_SAVEPOINT : 'SAVEPOINT autopoint_%d',

        TRANSACTION_BEGIN : 'Transaction.begin',
        TRANSACTION_COMMIT : 'Transaction.commit',
        TRANSACTION_ROLLBACK : 'Transaction.rollback',

        TRANSACTION_ISOLATION_LEVELS : {
            uncommitted : 'READ UNCOMMITTED',
            committed : 'READ COMMITTED',
            repeatable : 'REPEATABLE READ',
            serializable : 'SERIALIZABLE'
        },

        POSTGRES_DEFAULT_RE : /^(?:B?('.*')::[^']+|\((-?\d+(?:\.\d+)?)\))$/,
        MSSQL_DEFAULT_RE : /^(?:\(N?('.*')\)|\(\((-?\d+(?:\.\d+)?)\)\))$/,
        MYSQL_TIMESTAMP_RE : /^CURRENT_(?:DATE|TIMESTAMP)?$/,
        STRING_DEFAULT_RE : /^'(.*)'$/,
        DATE_PATTERN : "yyyy-MM-dd",
        TIME_PATTERN : "hh:mm:ss",
        POSTGRES_TIME_PATTERN : "hh:mm:ss",
        DATE_TIME_PATTERN : "yyyy-MM-ddThh:mm:ssZ",
        POSTGRES_DATE_TIME_PATTERN : "yyyy-MM-ddThh:mm:ss.SSZ",


        __transactionDepth : 0,

        constructor : function() {
            this.super(arguments);
            this.__transactionQueue = new comb.collections.Queue();
        },

        /**Executes the given SQL on the database. This method should be overridden in descendants.
         This method should not be called directly by user code.
         */
        execute : function(sql, options) {
            throw new NotImplemented("execute should be implemented by adapter");
        },

        /** Return a hash containing index information. Hash keys are index name symbols.
         * Values are subhashes with two keys, :columns and :unique.  The value of :columns
         * is an array of symbols of column names.  The value of :unique is true or false
         * depending on if the index is unique.
         *
         * Should not include the primary key index, functional indexes, or partial indexes.
         *
         *   DB.indexes(:artists) => {artists_name_ukey : {columns : [name], unique : true}}
         *   */
        indexes : function(table, opts) {
            throw new NotImplemented("indexes should be overridden by adapters");
        },

        get : function() {
            return this.dataset.get.apply(this.dataset, arguments);
        },

        /*Call the prepared statement with the given name with the given hash
         * of arguments.
         *
         *   DB[:items].filter(:id=>1).prepare(:first, :sa)
         *   DB.call(:sa) # SELECT * FROM items WHERE id = 1
         *   */
        call : function(psName, hash) {
            hash = hash | {};
            this.preparedStatements[psName](hash);
        },

        /**
         * Method that should be used when submitting any DDL (Data DefinitionLanguage) SQL, such as +create_table+.
         * By default, calls +execute_dui+.
         *This method should not be called directly by user code.
         */
        executeDdl : function(sql, opts, cb) {
            opts = opts || {};
            return this.executeDui(sql, opts, cb)
        },


        /**Method that should be used when issuing a DELETE, UPDATE, or INSERT
         *statement.  By default, calls execute.
         *This method should not be called directly by user code.
         * */
        executeDui : function(sql, opts, cb) {
            opts = opts || {};
            return this.execute(sql, opts, cb)
        },

        /**
         * Method that should be used when issuing a INSERT
         *statement.  By default, calls execute_dui.
         *This method should not be called directly by user code.
         * */
        executeInsert : function(sql, opts, cb) {
            opts = opts || {};
            return this.executeDui(sql, opts, cb);
        },


        /** Runs the supplied SQL statement string on the database server. Returns nil.
         * Options:
         * :server :: The server to run the SQL on.
         *
         *   DB.run("SET some_server_variable = 42")
         *   */
        run : function(sql, opts) {
            opts = opts || {};
            return this.executeDdl(sql, opts);
        },

        /**
         * Retreives a table from the database and returns a {@link moose.Table} from the parsed schema.
         * @param table
         */
        schema : function(table, opts) {
            if (!comb.isFunction(this.schemaParseTable)) {
                throw new Error("Schema parsing is not implemented on this database");
            }
            var ret = new Promise();
            opts = opts || {};
            var schemaParts = this.__schemaAndTable(table);
            var sch = schemaParts[0], tableName = schemaParts[1];
            var quotedName = this.__quoteSchemaTable(table);
            opts = sch && !opts.schema ? comb.merge({schema : sch}, opts) : opts;
            opts.reload && delete this.schemas[quotedName];
            if (this.schemas[quotedName]) {
                ret.callback(this.schemas[quotedName]);
                return ret;
            } else {
                this.schemaParseTable(tableName, opts).then(hitch(this, function(cols) {
                    var schema = {};
                    if (!cols || cols.length == 1) {
                        ret.errback("Error parsing schema, no columns returns, table probably doesnt exist");
                    } else {
                        for (var i in cols) {
                            var c = cols[i];
                            var name = c[0], c = c[1];
                            c.jsDefault = this.__columnSchemaToJsDefault(c["default"], c.type);
                            schema[name] = c;
                        }
                        this.schemas[quotedName] = schema;
                        ret.callback(schema);
                    }
                }), hitch(ret, "errback"));
            }
            return ret;
        },

        // Remove the cached schema for the given schema name
        removeCachedSchema : function(table) {
            if (this.schemas && !comb.isEmpty(this.schemas)) {
                delete this.schemas[this.__quoteSchemaTable(table)];
            }
        },

        /**
         * Determine if a table exists;
         * @param table
         */
        tableExists : function(table) {
            var ret = new Promise();
            this.from(table).first().then(hitch(ret, "callback", true), hitch(ret, "callback", false));
            return ret;
        },

        tables : function() {
            throw new NotImplemented("tables should be implemented by the adapter");
        },

        /* Starts a database transaction.  When a database transaction is used,
         * either all statements are successful or none of the statements are
         * successful.  Note that MySQL MyISAM tabels do not support transactions.
         *
         * The following options are respected:
         *
         * :isolation :: The transaction isolation level to use for this transaction,
         *               should be :uncommitted, :committed, :repeatable, or :serializable,
         *               used if given and the database/adapter supports customizable
         *               transaction isolation levels.
         * :prepare :: A string to use as the transaction identifier for a
         *             prepared transaction (two-phase commit), if the database/adapter
         *             supports prepared transactions.
         * :server :: The server to use for the transaction.
         * :savepoint :: Whether to create a new savepoint for this transaction,
         *               only respected if the database/adapter supports savepoints.  By
         *               default moose will reuse an existing transaction, so if you want to
         *               use a savepoint you must use this option.
         *               */
        transaction : function(opts, cb) {
            if (comb.isFunction(opts)) {
                cb = opts;
                opts = {};
            } else {
                opts = opts || {};
            }
            var ret = new comb.Promise();
            if (!this.alreadyInTransaction(opts)) {
                this.__transaction(ret, opts, cb);
            } else {
                cb.apply(this, [this]);
            }
            return ret;
        },

        __transactionProxy : function(cb) {
            var promises = [];
            var repl = [];
            (this.supportsSavepoints ? ["transaction"] : []).concat(["execute"]).forEach(function(n) {
                var orig = this[n];
                repl.push({name : n, orig : orig});
                this[n] = function(arg1, arg2) {
                    try {
                        if (n == "transaction" && (comb.isFunction(arg1) || comb.isUndefinedOrNull(arg1.savepoint))) {
                            orig.apply(this, arguments);
                        } else {
                            var ret = orig.apply(this, arguments);
                            if (comb.isInstanceOf(ret, Promise)) {
                                promises.push(ret);
                            } else {
                                promises.push(new Promise().callback(ret));
                            }
                        }
                    } catch(e) {
                        promises.push(new Promise().errback(e));
                    }
                    return ret;
                }
            }, this);
            try {
                var cbRet = cb.apply(this, [this]);
                if (cbRet && comb.isInstanceOf(cbRet, Promise)) {
                    promises.push(cbRet);
                }
            } catch(e) {
                promises.push(new Promise().errback(e));
            }
            if (promises.length == 0) {
                promises.push(new Promise().callback());
            }
            return new PromiseList(promises).both(hitch(this, function() {
                repl.forEach(function(o) {
                    this[o.name] = o.orig;
                }, this)
            }));

        },

        __transaction : function(promise, opts, cb) {
            this.__alreadyInTransaction = true;
            try {
                this.pool.getConnection().then(hitch(this, function(conn) {
                    this.__beginTransaction(conn, opts).then(hitch(this, function() {
                        this.__transactionProxy(cb).then(hitch(this, function(res) {
                            this.__commitTransaction(conn).then(comb.hitch(this, "__transactionComplete", promise, "callback", conn), comb.hitch(this, "__transactionComplete", promise, "errback", conn));
                        }), hitch(this, "__rollback", promise, conn));
                    }), comb.hitch(this, "__transactionComplete", promise, "errback", conn));
                }), comb.hitch(this, "__transactionComplete", promise, "errback"));
            } catch(e) {
                this.logError(e);
                promise.errback(e);
            }
        },

        __transactionComplete : function(promise, type, conn) {
            promise[type].apply(promise, comb.argsToArray(arguments).slice(3));
            this.__finishTransactionAndCheckForMore(conn);
        },

        __rollback : function(promise, conn, err) {
            this.__rollbackTransaction(conn, null, err).both(comb.hitch(this, "__finishTransactionAndCheckForMore", conn));
            if (this.__transactionDepth <= 1) {
                this.__transactionError(err, promise);
            } else {
                promise.errback(err);
            }
        },

        __transactionError : function(err, promise) {
            if (comb.isArray(err)) {
                for (var i in err) {
                    var e = err[i];
                    if (comb.isArray(e) && e.length == 2) {
                        var realE = e[1];
                        if (realE != "ROLLBACK") {
                            promise.errback(realE);
                            break;
                        } else {
                            promise.callback();
                        }
                    }
                }
            } else {
                if (e != "ROLLBACK") {
                    throw e;
                }
            }
        },

        __finishTransactionAndCheckForMore : function(conn) {
            if (this.alreadyInTransaction) {
                if (!this.supportsSavepoints || ((this.__transactionDepth -= 1) <= 0)) {
                    conn && this.pool.returnConnection(conn);
                    this.__alreadyInTransaction = false;
                    if (this.__transactionQueue.count) {
                        var next = this.__transactionQueue.dequeue();
                        if (next) {
                            this.__transaction.apply(this, next);
                        }
                    }
                }
            }
        },

        //SQL to start a new savepoint
        __beginSavepointSql : function(depth) {
            return format(Database.SQL_SAVEPOINT, depth);
        },

        // Start a new database connection on the given connection
        __beginNewTransaction : function(conn, opts) {
            var ret = new comb.Promise();
            this.__logConnectionExecute(conn, this.beginTransactionSql).chain(comb.hitch(this, "__setTransactionIsolation", conn, opts), hitch(ret, "errback")).then(hitch(ret, "callback"), hitch(ret, "errback"));
            return ret;
        },

        //Start a new database transaction or a new savepoint on the given connection.
        __beginTransaction : function(conn, opts) {
            var ret;
            if (this.supportsSavepoints) {
                if (this.__transactionDepth > 0) {
                    ret = this.__logConnectionExecute(conn, this.__beginSavepointSql(this.__transactionDepth));
                } else {
                    ret = this.__beginNewTransaction(conn, opts);
                }
                this.__transactionDepth += 1;
            } else {
                ret = this.__beginNewTransaction(conn, opts);
            }
            return  ret;
        },

        // SQL to commit a savepoint
        __commitSavepointSql : function(depth) {
            return format(this.SQL_RELEASE_SAVEPOINT, depth);
        },

        //Commit the active transaction on the connection
        __commitTransaction : function(conn, opts) {
            opts = opts || {};
            if (this.supportsSavepoints) {
                var depth = this.__transactionDepth;
                return this.__logConnectionExecute(conn, (depth > 1 ? this.__commitSavepointSql(depth - 1) : this.commitTransactionSql));
            } else {
                return this.__logConnectionExecute(conn, this.commitTransactionSql);
            }
        },


        //SQL to rollback to a savepoint
        __rollbackSavepointSql : function(depth) {
            return format(this.SQL_ROLLBACK_TO_SAVEPOINT, depth);
        },

        //Rollback the active transaction on the connection
        __rollbackTransaction : function(conn, opts, err) {
            opts = opts || {};
            if (this.supportsSavepoints) {
                var depth = this.__transactionDepth;
                return this.__logConnectionExecute(conn, depth > 1 ? this.__rollbackSavepointSql(depth - 1) : this.rollbackTransactionSql);
            } else {
                return this.__logConnectionExecute(conn, this.rollbackTransactionSql);
                throw err;
            }
        },

        // Set the transaction isolation level on the given connection
        __setTransactionIsolation : function(conn, opts) {
            var level;
            var ret = new Promise();
            if (this.supportsTransactionIsolationLevels && !comb.isUndefinedOrNull(level = comb.isUndefinedOrNull(opts.isolation) ? this.transactionIsolationLevel : opts.isolation)) {
                return this.__logConnectionExecute(conn, this.__setTransactionIsolationSql(level)).then(hitch(ret, "callback"), hitch(ret, "errback"));
            }else{
                ret.callback();
            }
            return ret;
        },

        // SQL to set the transaction isolation level
        __setTransactionIsolationSql : function(level) {
            return format("SET TRANSACTION ISOLATION LEVEL %s", this.TRANSACTION_ISOLATION_LEVELS[level]);
        },

        //Convert the given default, which should be a database specific string, into
        //a javascript object.
        __columnSchemaToJsDefault : function(def, type) {
            if (comb.isNull(def) || comb.isUndefined(def)) return null;
            var origDefault = def, m, datePattern = this.DATE_PATTERN, dateTimePattern = this.DATE_TIME_PATTERN, timePattern = this.TIME_PATTERN;
            if (this.type == "postgres" && (m = def.match(this.POSTGRES_DEFAULT_RE)) != null) {
                def = m[1] || m[2];
                dateTimePattern = this.POSTGRES_DATE_TIME_PATTERN, timePattern = this.POSTGRES_TIME_PATTERN;
            }
            if (this.type == "mssql" && (m = def.match(this.MSSQL_DEFAULT_RE)) != null) {
                def = m[1] || m[2];
            }
            if (["string", "blob", "date", "datetime", "time", "enum"].indexOf(type) != -1) {
                if (this.type == "mysql") {
                    if (["date", "datetime", "time"].indexOf(type) != -1 && def.match(this.MYSQL_TIMESTAMP_RE)) {
                        return null;
                    }
                    origDefault = def = "'" + def + "'".replace("\\", "\\\\");
                }
                if ((m = def.match(this.STRING_DEFAULT_RE)) == null) {
                    return null;
                }
                def = m[1].replace("''", "'")
            }
            var ret = null;
            try {
                switch (type) {
                    case "boolean":
                        if (def.match(/[f0]/i)) {
                            ret = false;
                        } else if (def.match(/[t1]/i)) {
                            ret = true;
                        } else if (comb.isBoolean(def)) {
                            ret = def;
                        }

                        break;
                    case "string":
                    case "enum":
                        ret = def;
                        break;
                    case  "integer":
                        ret = parseInt(def, 10);
                        isNaN(ret) && (ret = null);
                        break;
                    case  "float":
                    case  "decimal":
                        ret = parseFloat(def, 10);
                        isNaN(ret) && (ret = null);
                        break;
                    case "date":
                        ret = comb.date.parse(def, datePattern);
                        break;
                    case "datetime":
                        ret = comb.date.parse(def, dateTimePattern);
                        break;
                    case "time":
                        ret = comb.date.parse(def, timePattern);
                        break;

                }
            } catch(e) {
            }
            return ret;
        },

        //Match the database's column type to a ruby type via a
        // regular expression, and return the ruby type as a symbol
        // such as :integer or :string.
        schemaColumnType : function(dbType) {
            var ret = null, m;
            if (dbType.match(/^interval$/i)) {
                ret = "interval";
            } else if (dbType.match(/^(character( varying)?|n?(var)?char|n?text)/i)) {
                ret = "string";
            } else if (dbType.match(/^int(eger)?|(big|small|tiny)int/i)) {
                ret = "integer";
            } else if (dbType.match(/^date$/i)) {
                ret = "date";
            } else if (dbType.match(/^((small)?datetime|timestamp( with(out)? time zone)?)$/i)) {
                ret = "datetime";
            } else if (dbType.match(/^time( with(out)? timezone)?$/i)) {
                ret = "time";
            } else if (dbType.match(/^(bit|boolean)$/i)) {
                ret = "boolean";
            } else if (dbType.match(/$(real|float|double (precision)?)$/i)) {
                ret = "float";
            } else if ((m = dbType.match(/^$(?:(?:(?:num(?:ber|eric)?|decimal)(?:\(\d+,\s*(\d+)\))?)|(?:small)?money)/i))) {
                ret = m[1] && m[1] == '0' ? "integer" : "decimal";
            } else if (dbType.match(/^bytea|blob|image|(var)?binary/i)) {
                ret = "blob";
            } else if (dbType.match(/^enum/i)) {
                ret = "enum";
            }
            return ret;
        },

        alreadyInTransaction : function(opts) {
            return this.__alreadyInTransaction && (!this.supportsSavepoints || !opts.savepoint);
        },

        getters : {


            //SQL to BEGIN a transaction.
            beginTransactionSql : function() {
                return this.SQL_BEGIN;
            },

            commitTransactionSql : function() {
                return this.SQL_COMMIT;
            },

            rollbackTransactionSql : function() {
                return this.SQL_ROLLBACK;
            },

            //Return a Method object for the dataset's output_identifier_method.
            //Used in metadata parsing to make sure the returned information is in the
            //correct format.
            outputIdentifierMeth : function() {
                return comb.hitch(this.dataset, this.dataset.outputIdentifier);
            },

            //Return a Method object for the dataset's output_identifier_method.
            //Used in metadata parsing to make sure the returned information is in the
            //correct format.
            inputIdentifierMeth : function() {
                return comb.hitch(this.dataset, this.dataset.inputIdentifier);
            },

            // Return a dataset that uses the default identifier input and output methods
            // for this database.  Used when parsing metadata so that column symbols are
            //returned as expected.
            metadataDataset : function() {
                if (this.__metadataDataset) {
                    return this.__metadataDataset;
                }
                var ds = this.dataset;
                ds.identifierInputMethod = this.identifierInputMethod;
                ds.identifierOutputMethod = this.identifierOutputMethod;
                this.__metadataDataset = ds;
                return ds;
            }
        }
    },


    static : {
        SQL_BEGIN : 'BEGIN',
        SQL_COMMIT : 'COMMIT',
        SQL_RELEASE_SAVEPOINT : 'RELEASE SAVEPOINT autopoint_%d',
        SQL_ROLLBACK : 'ROLLBACK',
        SQL_ROLLBACK_TO_SAVEPOINT : 'ROLLBACK TO SAVEPOINT autopoint_%d',
        SQL_SAVEPOINT : 'SAVEPOINT autopoint_%d',

        TRANSACTION_BEGIN : 'Transaction.begin',
        TRANSACTION_COMMIT : 'Transaction.commit',
        TRANSACTION_ROLLBACK : 'Transaction.rollback',

        TRANSACTION_ISOLATION_LEVELS : {
            uncommitted : 'READ UNCOMMITTED',
            committed : 'READ COMMITTED',
            repeatable : 'REPEATABLE READ',
            serializable : 'SERIALIZABLE'
        },

        POSTGRES_DEFAULT_RE : /^(?:B?('.*')::[^']+|\((-?\d+(?:\.\d+)?)\))$/,
        MSSQL_DEFAULT_RE : /^(?:\(N?('.*')\)|\(\((-?\d+(?:\.\d+)?)\)\))$/,
        MYSQL_TIMESTAMP_RE : /^CURRENT_(?:DATE|TIMESTAMP)?$/,
        STRING_DEFAULT_RE : /^'(.*)'$/


    }
}).export(module);


