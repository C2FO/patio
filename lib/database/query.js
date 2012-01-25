var comb = require("comb"),
    hitch = comb.hitch,
    format = comb.string.format,
    Promise = comb.Promise,
    sql = require("../sql").sql,
    PromiseList = comb.PromiseList, errors = require("../errors"), NotImplemented = errors.NotImplemented;


var Database = comb.define(null, {
    instance:{
        /**@lends patio.Database.prototype*/

        /**
         * The method name to invoke on a connection. The method name
         * should be overrode by an adapter if the method to execute
         * a query is different for the adapter specific connection class.
         */
        connectionExecuteMethod:"execute",

        /**
         * The <b>BEGIN</b> SQL fragment used to signify the start of a transaciton.
         */
        SQL_BEGIN:'BEGIN',

        /**
         * The <b>COMMIT</b> SQL fragment used to signify the end of a transaction and the final commit.
         */
        SQL_COMMIT:'COMMIT',

        /**
         * The <b>RELEASE SAVEPOINT</b> SQL fragment used by trasactions when using save points.
         * The adapter should override this SQL fragment if the adapters SQL is different.
         * <p>
         *      <b>This fragment will not be used if {@link patio.Database#supportsSavepoints} is false.</b>
         * </p>
         */
        SQL_RELEASE_SAVEPOINT:'RELEASE SAVEPOINT autopoint_%d',

        /**
         * The <b>ROLLBACK</b> SQL fragment used to rollback a database transaction.
         * This should be overrode by adapters if the SQL for the adapters
         * database is different.
         */
        SQL_ROLLBACK:'ROLLBACK',

        /**
         * The <b>ROLLBACK TO SAVEPOINT</b> SQL fragment used to rollback a database transaction
         * to a particular save point.
         * This should be overrode by adapters if the SQL for the adapters
         * database is different.
         *
         * <p>
         *      <b>This fragment will not be used if {@link patio.Database#supportsSavepoints} is false.</b>
         * </p>
         */
        SQL_ROLLBACK_TO_SAVEPOINT:'ROLLBACK TO SAVEPOINT autopoint_%d',

        /**
         * The <b>SAVEPOINT</b> SQL fragment used for creating a save point in a
         * database transaction.
         * <p>
         *      <b>This fragment will not be used if {@link patio.Database#supportsSavepoints} is false.</b>
         * </p>
         */
        SQL_SAVEPOINT:'SAVEPOINT autopoint_%d',

        /**
         * Object containing different database transaction isolation levels.
         * This object is used to look up the proper SQL when starting a new transaction
         * and setting the isolation level in the options.
         * @field
         */
        TRANSACTION_ISOLATION_LEVELS:{
            uncommitted:'READ UNCOMMITTED',
            committed:'READ COMMITTED',
            repeatable:'REPEATABLE READ',
            serializable:'SERIALIZABLE'
        },

        /**
         * @ignore
         */
        POSTGRES_DEFAULT_RE:/^(?:B?('.*')::[^']+|\((-?\d+(?:\.\d+)?)\))$/,

        /**
         * @ignore
         */
        MSSQL_DEFAULT_RE:/^(?:\(N?('.*')\)|\(\((-?\d+(?:\.\d+)?)\)\))$/,

        /**
         * @ignore
         */
        MYSQL_TIMESTAMP_RE:/^CURRENT_(?:DATE|TIMESTAMP)?$/,

        /**
         * @ignore
         */
        STRING_DEFAULT_RE:/^'(.*)'$/,

        /**
         * @ignore
         */
        POSTGRES_TIME_PATTERN:"HH:mm:ss",

        /**
         * @ignore
         */
        POSTGRES_DATE_TIME_PATTERN:"yyyy-MM-dd HH:mm:ss.SSZ",

        /**
         * @ignore
         */
        __transactionDepth:0,

        /**
         * @ignore
         */
        constructor:function () {
            this._super(arguments);
        },

        /**
         * Executes the given SQL on the database. This method should be implemented by adapters.
         * <b>This method should not be called directly by user code.</b>
         */
        execute:function (sql, options) {
            throw new NotImplemented("execute should be implemented by adapter");
        },

        /**
         * Return a Promise that is resolved with an object containing index information.
         * <p>
         *     The keys are index names. Values are objects with two keys, columns and unique.  The value of columns
         *     is an array of column names.  The value of unique is true or false
         *     depending on if the index is unique.
         * </p>
         *
         * <b>Should not include the primary key index, functional indexes, or partial indexes.</b>
         *
         * @example
         *   DB.indexes("artists").then(function(indexes){
         *     //e.g. indexes === {artists_name_ukey : {columns : [name], unique : true}};
         *   })
         **/
        indexes:function (table, opts) {
            throw new NotImplemented("indexes should be overridden by adapters");
        },

        /**
         * Proxy for {@link patio.Dataset#get}.
         */
        get:function () {
            return this.dataset.get.apply(this.dataset, arguments);
        },

        /**
         * @ignore
         * //todo implement prepared statements
         *
         * Call the prepared statement with the given name with the given object
         * of arguments.
         *
         *   DB.from("items").filter({id : 1}).prepare("first", "sa");
         *   DB.call("sa") //=> SELECT * FROM items WHERE id = 1
         *   */
        call:function (psName, hash) {
            hash = hash | {};
            this.preparedStatements[psName](hash);
        },

        /**
         * Method that should be used when submitting any DDL (Data DefinitionLanguage) SQL,
         * such as {@link patio.Database#createTable}. By default, calls {@link patio.Database#executeDui}.
         * <b>This method should not be called directly by user code.</b>
         */
        executeDdl:function (sql, opts, cb) {
            opts = opts || {};
            return this.executeDui(sql, opts, cb)
        },


        /**
         * Method that should be used when issuing a DELETE, UPDATE, or INSERT
         * statement.  By default, calls {@link patio.Database#execute}.
         * <b>This method should not be called directly by user code.</b>
         **/
        executeDui:function (sql, opts, cb) {
            opts = opts || {};
            return this.execute(sql, opts, cb)
        },

        /**
         * Method that should be used when issuing a INSERT
         * statement.  By default, calls {@link patio.Database#executeDui}.
         * <b>This method should not be called directly by user code.</b>
         **/
        executeInsert:function (sql, opts, cb) {
            opts = opts || {};
            return this.executeDui(sql, opts, cb);
        },


        /**
         * Runs the supplied SQL statement string on the database server..
         *
         * @example
         * DB.run("SET some_server_variable = 42")
         *
         * @param {String} sql the SQL to run.
         * @return {Promise} a promise that is resolved with the result of the query.
         **/
        run:function (sql, opts) {
            opts = opts || {};
            return this.executeDdl(sql, opts);
        },

        /**
         * Parse the schema from the database.
         *
         * @example
         *
         *   DB.schema("artists").then(function(schema){
         *     //example schema
         *     {
         *      id :  {
         *          type : "integer",
         *          primaryKey : true,
         *          "default" : "nextval('artist_id_seq'::regclass)",
         *          jsDefault : null,
         *          dbType : "integer",
         *          allowNull : false
         *       },
         *      name : {
         *          type : "string",
         *          primaryKey : false,
         *          "default" : null,
         *          jsDefault  : null,
         *          dbType : "text",
         *          allowNull : false
         *       }
         *     }
         *   })
         *
         * @param {String|patio.sql.Identifier|patio.sql.QualifiedIdentifier} table the table to get the schema for.
         * @param {Object} [opts=null] Additinal options.
         * @param {boolean} [opts.reload=false] Set to true to ignore any cached results.
         * @param {String|patio.sql.Identifier} [opts.schema] An explicit schema to use.  It may also be implicitly provided
         *            via the table name.
         *
         * @return {Promise} Returns a Promise that is resolved with the schema for the given table as an object
         * where the key is the column name and the value is and object containg column information. The default
         * column information returned.
         * <ul>
         *     <li>allowNull : Whether NULL is an allowed value for the column.</li>
         *     <li>dbType : The database type for the column, as a database specific string.</li>
         *     <li>"default" : The database default for the column, as a database specific string.</li>
         *     <li>primaryKey : Whether the columns is a primary key column.  If this column is not present,
         *                 it means that primary key information is unavailable, not that the column
         *                 is not a primary key.</li>
         *     <li>jsDefault : The database default for the column, as a javascript object.  In many cases, complex
         *                  database defaults cannot be parsed into javascript objects.</li>
         *     <li>type : A string specifying the type, such as "integer" or "string".</li>
         * <ul>
         *
         */
        schema:function (table, opts) {
            if (!comb.isFunction(this.schemaParseTable)) {
                throw new Error("Schema parsing is not implemented on this database");
            }
            var ret = new Promise();
            opts = opts || {};
            var schemaParts = this.__schemaAndTable(table);
            var sch = schemaParts[0], tableName = schemaParts[1];
            var quotedName = this.__quoteSchemaTable(table);
            opts = sch && !opts.schema ? comb.merge({schema:sch}, opts) : opts;
            opts.reload && delete this.schemas[quotedName];
            if (this.schemas[quotedName]) {
                ret.callback(this.schemas[quotedName]);
                return ret;
            } else {
                this.schemaParseTable(tableName, opts).then(hitch(this, function (cols) {
                    var schema = {};
                    if (!cols || cols.length == 0) {
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

        /**
         * Remove the cached schema for the given table name
         * @example
         * DB.schema("artists").then(function(){
         *      DB.removeCachedSchema("artists");
         * });
         * @param {String|patio.sql.Identifier|patio.sql.QualifiedIdentifier} the table to remove from this
         * databases cached schemas.
         */
        removeCachedSchema:function (table) {
            if (this.schemas && !comb.isEmpty(this.schemas)) {
                delete this.schemas[this.__quoteSchemaTable(table)];
            }
        },

        /**
         * Determine if a table exists.
         * @example
         * comb.executeInOrder(DB, function(DB){
         *    return {
         *          table1Exists : DB.tableExists("table1"),
         *          table2Exists : DB.tableExists("table2")
         *    };
         * }).then(function(ret){
         *      //ret.table1Exists === true
         *      //ret.table2Exists === false
         * });
         * @param {String|patio.sql.Identifier|patio.sql.QualifiedIdentifier} the table to remove from this
         *
         * @return {Promise} a promise resolved with a boolean indicating if the table exists.
         */
        tableExists:function (table) {
            var ret = new Promise();
            this.from(table).first().then(hitch(ret, "callback", true), hitch(ret, "callback", false));
            return ret;
        },

        /**
         * Returns a promise with a list of tables names in this database. This method
         * should be implemented by the adapter.
         *
         * @example
         * DB.tables().then(function(tables){
         *    //e.g. tables === ["table1", "table2", "table3"];
         * });
         *
         * @return {Promise} a promise that is resolved with a list of tablenames.
         */
        tables:function () {
            throw new NotImplemented("tables should be implemented by the adapter");
        },

        /**
         * Starts a database transaction.  When a database transaction is used,
         * either all statements are successful or none of the statements are
         * successful.
         * <p>
         *      <b>Note</b> that MySQL MyISAM tables do not support transactions.</p>
         * </p>
         *
         * @example
         * //normal transaction
         * DB.transaction(function() {
         *       this.execute('DROP TABLE test;');
         *       this.execute('DROP TABLE test2;');
         *  });
         *
         * //transaction with a save point.
         * DB.transaction(function() {
         *      this.transaction({savepoint : true}, function() {
         *          this.execute('DROP TABLE test;');
         *          this.execute('DROP TABLE test2;');
         *      });
         *});
         *
         * //WITH ISOLATION LEVELS
         *
         * db.supportsTransactionIsolationLevels = true;
         * //BEGIN
         * //SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
         * //DROP TABLE test1'
         * //COMMIT
         * DB.transaction({isolation:"uncommited"}, function(d) {
         *      d.run("DROP TABLE test1");
         * });
         *
         * //BEGIN
         * //SET TRANSACTION ISOLATION LEVEL READ COMMITTED
         * //DROP TABLE test1
         * //COMMIT
         *  DB.transaction({isolation:"committed"}, function(d) {
         *      d.run("DROP TABLE test1");
         *  });
         *
         * //BEGIN
         * //SET TRANSACTION ISOLATION LEVEL REPEATABLE READ'
         * //DROP TABLE test1
         * //COMMIT
         *  DB.transaction({isolation:"repeatable"}, function(d) {
         *      d.run("DROP TABLE test1");
         *  });
         *
         * //BEGIN
         * //SET TRANSACTION ISOLATION LEVEL SERIALIZABLE
         * //DROP TABLE test1
         * //COMMIT
         *  DB.transaction({isolation:"serializable"}, function(d) {
         *      d.run("DROP TABLE test1");
         *  });
         *
         * //With an Error
         * //BEGIN
         * //DROP TABLE test
         * //ROLLBACK
         * DB.transaction(function(d) {
         *      d.execute('DROP TABLE test');
         *      throw "Error";
         * });
         *
         * @param {Object} [opts={}] options to use when performin the transaction.
         * @param {String} [opts.isolation] The transaction isolation level to use for this transaction,
         *               should be "uncommitted", "committed", "repeatable", or "serializable",
         *               used if given and the database/adapter supports customizable
         *               transaction isolation levels.
         * @param {String} [opts.prepare] A string to use as the transaction identifier for a
         *             prepared transaction (two-phase commit), if the database/adapter
         *             supports prepared transactions.
         * @param {Boolean} [opts.savepoint] Whether to create a new savepoint for this transaction,
         *               only respected if the database/adapter supports savepoints.  By
         *               default patio will reuse an existing transaction, so if you want to
         *               use a savepoint you must use this option.
         * @param {Function} cb a function used to perform the transaction. This function is
         * called in the scope of the database by default so one can use this. The dabase is also
         * called with the database as the first argument.
         *
         * @return {Promise} a promise that is resolved once the transaction is complete.
         **/
        transaction:function (opts, cb) {
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

        __transactionProxy:function (cb) {
            var promises = [];
            var repl = [];
            (this.supportsSavepoints ? ["transaction"] : []).concat(["execute"]).forEach(function (n) {
                var orig = this[n];
                repl.push({name:n, orig:orig});
                this[n] = function (arg1, arg2) {
                    try {
                        var ret;
                        if (n == "transaction" && (comb.isFunction(arg1) || comb.isUndefinedOrNull(arg1.savepoint))) {
                            orig.apply(this, arguments);
                        } else {
                            ret = orig.apply(this, arguments);
                            promises.push(comb.when(ret));
                        }
                    } catch (e) {
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
            } catch (e) {
                promises.push(new Promise().errback(e));
            }
            if (promises.length == 0) {
                promises.push(new Promise().callback());
            }
            return new PromiseList(promises).both(hitch(this, function () {
                repl.forEach(function (o) {
                    this[o.name] = o.orig;
                }, this)
            }));

        },

        __transaction:function (promise, opts, cb) {
            this.__alreadyInTransaction = true;
            try {
                this.pool.getConnection().then(hitch(this, function (conn) {
                    this.__beginTransaction(conn, opts).then(hitch(this, function () {
                        this.__transactionProxy(cb).then(hitch(this, function (res) {
                            this.__commitTransaction(conn).then(comb.hitch(this, "__transactionComplete", promise, "callback", conn), comb.hitch(this, "__transactionComplete", promise, "errback", conn));
                        }), hitch(this, "__rollback", promise, conn));
                    }), comb.hitch(this, "__transactionComplete", promise, "errback", conn));
                }), comb.hitch(this, "__transactionComplete", promise, "errback"));
            } catch (e) {
                this.logError(e);
                promise.errback(e);
            }
        },

        __transactionComplete:function (promise, type, conn) {
            this.__finishTransactionAndCheckForMore(conn);
            promise[type].apply(promise, comb.argsToArray(arguments).slice(3));
        },

        __rollback:function (promise, conn, err) {
            this.__rollbackTransaction(conn, null, err).both(comb.hitch(this, "__finishTransactionAndCheckForMore", conn)).then(hitch(this, function () {
                if (this.__transactionDepth <= 1) {
                    this.__transactionError(err, promise);
                } else {
                    promise.errback(err);
                }
            }));
        },

        __transactionError:function (err, promise) {
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

        __finishTransactionAndCheckForMore:function (conn) {
            if (this.alreadyInTransaction) {
                if (!this.supportsSavepoints || ((this.__transactionDepth -= 1) <= 0)) {
                    conn && this.pool.returnConnection(conn);
                    this.__alreadyInTransaction = false;
                }
            }
        },

        //SQL to start a new savepoint
        __beginSavepointSql:function (depth) {
            return format(Database.SQL_SAVEPOINT, depth);
        },

        // Start a new database connection on the given connection
        __beginNewTransaction:function (conn, opts) {
            var ret = new comb.Promise();
            this.__logConnectionExecute(conn, this.beginTransactionSql).chain(comb.hitch(this, "__setTransactionIsolation", conn, opts), hitch(ret, "errback")).then(hitch(ret, "callback"), hitch(ret, "errback"));
            return ret;
        },

        //Start a new database transaction or a new savepoint on the given connection.
        __beginTransaction:function (conn, opts) {
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
        __commitSavepointSql:function (depth) {
            return format(this.SQL_RELEASE_SAVEPOINT, depth);
        },

        //Commit the active transaction on the connection
        __commitTransaction:function (conn, opts) {
            opts = opts || {};
            if (this.supportsSavepoints) {
                var depth = this.__transactionDepth;
                return this.__logConnectionExecute(conn, (depth > 1 ? this.__commitSavepointSql(depth - 1) : this.commitTransactionSql));
            } else {
                return this.__logConnectionExecute(conn, this.commitTransactionSql);
            }
        },


        //SQL to rollback to a savepoint
        __rollbackSavepointSql:function (depth) {
            return format(this.SQL_ROLLBACK_TO_SAVEPOINT, depth);
        },

        //Rollback the active transaction on the connection
        __rollbackTransaction:function (conn, opts, err) {
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
        __setTransactionIsolation:function (conn, opts) {
            var level;
            var ret = new Promise();
            if (this.supportsTransactionIsolationLevels && !comb.isUndefinedOrNull(level = comb.isUndefinedOrNull(opts.isolation) ? this.transactionIsolationLevel : opts.isolation)) {
                return this.__logConnectionExecute(conn, this.__setTransactionIsolationSql(level)).then(hitch(ret, "callback"), hitch(ret, "errback"));
            } else {
                ret.callback();
            }
            return ret;
        },

        // SQL to set the transaction isolation level
        __setTransactionIsolationSql:function (level) {
            return format("SET TRANSACTION ISOLATION LEVEL %s", this.TRANSACTION_ISOLATION_LEVELS[level]);
        },

        //Convert the given default, which should be a database specific string, into
        //a javascript object.
        __columnSchemaToJsDefault:function (def, type) {
            if (comb.isNull(def) || comb.isUndefined(def)) {
                return null;
            }
            var origDefault = def, m, datePattern, dateTimePattern, timeStampPattern, timePattern;
            if (this.type == "postgres" && (m = def.match(this.POSTGRES_DEFAULT_RE)) != null) {
                def = m[1] || m[2];
                dateTimePattern = this.POSTGRES_DATE_TIME_PATTERN, timePattern = this.POSTGRES_TIME_PATTERN;
            }
            if (this.type == "mssql" && (m = def.match(this.MSSQL_DEFAULT_RE)) != null) {
                def = m[1] || m[2];
            }
            if (["string", "blob", "date", "datetime", "year", "timestamp", "time", "enum"].indexOf(type) != -1) {
                if (this.type == "mysql") {
                    if (["date", "datetime", "time", "timestamp"].indexOf(type) != -1 && def.match(this.MYSQL_TIMESTAMP_RE)) {
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
                    case "year" :
                        ret = this.patio.stringToYear(def);
                        break;
                    case "date":
                        ret = this.patio.stringToDate(def, datePattern);
                        break;
                    case "timestamp":
                        ret = this.patio.stringToTimeStamp(def, timeStampPattern);
                        break;
                    case "datetime":
                        ret = this.patio.stringToDateTime(def, dateTimePattern);
                        break;
                    case "time":
                        ret = this.patio.stringToTime(def, timePattern);
                        break;
                }
            } catch (e) {
            }
            return ret;
        },

        /**
         * Match the database's column type to a javascript type via a
         * regular expression, and return the javascript type as a string
         * such as "integer" or "string".
         * @private
         */
        schemaColumnType:function (dbType) {
            var ret = null, m;
            if (dbType.match(/^interval$/i)) {
                ret = "interval";
            } else if (dbType.match(/^(character( varying)?|n?(var)?char|n?text)/i)) {
                ret = "string";
            } else if (dbType.match(/^int(eger)?|(big|small|tiny)int/i)) {
                ret = "integer";
            } else if (dbType.match(/^date$/i)) {
                ret = "date";
            } else if (dbType.match(/^year/i)) {
                ret = "year";
            } else if (dbType.match(/^((small)?datetime|timestamp( with(out)? time zone)?)$/i)) {
                ret = "datetime";
            } else if (dbType.match(/^time( with(out)? timezone)?$/i)) {
                ret = "time";
            } else if (dbType.match(/^(bit|boolean)$/i)) {
                ret = "boolean";
            } else if (dbType.match(/^(real|float|double( precision)?)$/i)) {
                ret = "float";
            } else if ((m = dbType.match(/^(?:(?:(?:num(?:ber|eric)?|decimal|double)(?:\(\d+,\s*(\d+)\))?)|(?:small)?money)/i))) {
                ret = m[1] && m[1] == '0' ? "integer" : "decimal";
            } else if (dbType.match(/^bytea|blob|image|(var)?binary/i)) {
                ret = "blob";
            } else if (dbType.match(/^enum/i)) {
                ret = "enum";
            } else if (dbType.match(/^set/i)) {
                ret = "set";
            }
            return ret;
        },

        /**
         * Returns true if this DATABASE is currently in a transaction.
         *
         * @param opts
         * @return {Boolean} true if this dabase is currently in a transaction.
         */
        alreadyInTransaction:function (opts) {
            opts = opts || {};
            return this.__alreadyInTransaction && (!this.supportsSavepoints || !opts.savepoint);
        },

        /**@ignore*/
        getters:{
            /**@lends patio.Database.prototype*/

            /**
             * SQL to BEGIN a transaction.
             * See {@link patio.Database#SQL_BEGIN} for default,
             * @field
             * @type String
             */
            beginTransactionSql:function () {
                return this.SQL_BEGIN;
            },

            /**
             * SQL to COMMIT a transaction.
             * See {@link patio.Database#SQL_COMMIT} for default,
             * @field
             * @type String
             */
            commitTransactionSql:function () {
                return this.SQL_COMMIT;
            },

            /**
             * SQL to ROLLBACK a transaction.
             * See {@link patio.Database#SQL_ROLLBACK} for default,
             * @field
             * @type String
             */
            rollbackTransactionSql:function () {
                return this.SQL_ROLLBACK;
            },

            /**
             * Return a function for the dataset's {@link patio.Dataset#outputIdentifierMethod}.
             * Used in metadata parsing to make sure the returned information is in the
             * correct format.
             *
             * @field
             * @type Function
             */
            outputIdentifierFunc:function () {
                return comb.hitch(this.dataset, this.dataset.outputIdentifier);
            },

            /**
             * Return a function for the dataset's {@link patio.Dataset#inputIdentifierMethod}.
             * Used in metadata parsing to make sure the returned information is in the
             * correct format.
             *
             * @field
             * @type Function
             */
            inputIdentifierFunc:function () {
                return comb.hitch(this.dataset, this.dataset.inputIdentifier);
            },

            /**
             * Return a dataset that uses the default identifier input and output methods
             * for this database.  Used when parsing metadata so that column are
             * returned as expected.
             *
             * @field
             * @type patio.Dataset
             */
            metadataDataset:function () {
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


    static:{
        SQL_BEGIN:'BEGIN',
        SQL_COMMIT:'COMMIT',
        SQL_RELEASE_SAVEPOINT:'RELEASE SAVEPOINT autopoint_%d',
        SQL_ROLLBACK:'ROLLBACK',
        SQL_ROLLBACK_TO_SAVEPOINT:'ROLLBACK TO SAVEPOINT autopoint_%d',
        SQL_SAVEPOINT:'SAVEPOINT autopoint_%d',

        TRANSACTION_BEGIN:'Transaction.begin',
        TRANSACTION_COMMIT:'Transaction.commit',
        TRANSACTION_ROLLBACK:'Transaction.rollback',

        TRANSACTION_ISOLATION_LEVELS:{
            uncommitted:'READ UNCOMMITTED',
            committed:'READ COMMITTED',
            repeatable:'REPEATABLE READ',
            serializable:'SERIALIZABLE'
        },

        POSTGRES_DEFAULT_RE:/^(?:B?('.*')::[^']+|\((-?\d+(?:\.\d+)?)\))$/,
        MSSQL_DEFAULT_RE:/^(?:\(N?('.*')\)|\(\((-?\d+(?:\.\d+)?)\)\))$/,
        MYSQL_TIMESTAMP_RE:/^CURRENT_(?:DATE|TIMESTAMP)?$/,
        STRING_DEFAULT_RE:/^'(.*)'$/


    }
}).as(module);


