var comb = require("comb"),
    define = comb.define,
    merge = comb.merge,
    hitch = comb.hitch,
    when = comb.when,
    isBoolean = comb.isBoolean,
    isEmpty = comb.isEmpty,
    isArray = comb.isArray,
    isUndefined = comb.isUndefined,
    isPromiseLike = comb.isPromiseLike,
    isUndefinedOrNull = comb.isUndefinedOrNull,
    argsToArray = comb.argsToArray,
    isFunction = comb.isFunction,
    format = comb.string.format,
    Promise = comb.Promise,
    isNull = comb.isNull,
    Queue = comb.collections.Queue,
    sql = require("../sql").sql,
    PromiseList = comb.PromiseList,
    errors = require("../errors"),
    NotImplemented = errors.NotImplemented,
    stream = require("stream"),
    PassThroughStream = stream.PassThrough,
    utils = require("../utils"),
    pipeAll = utils.pipeAll,
    resolveOrPromisfyFunction = utils.resolveOrPromisfyFunction;


var Database = define(null, {
    instance: {
        /**@lends patio.Database.prototype*/

        /**
         * The method name to invoke on a connection. The method name
         * should be overrode by an adapter if the method to execute
         * a query is different for the adapter specific connection class.
         */
        connectionExecuteMethod: "execute",

        /**
         * The <b>BEGIN</b> SQL fragment used to signify the start of a transaciton.
         */
        SQL_BEGIN: 'BEGIN',

        /**
         * The <b>COMMIT</b> SQL fragment used to signify the end of a transaction and the final commit.
         */
        SQL_COMMIT: 'COMMIT',

        /**
         * The <b>RELEASE SAVEPOINT</b> SQL fragment used by trasactions when using save points.
         * The adapter should override this SQL fragment if the adapters SQL is different.
         * <p>
         *      <b>This fragment will not be used if {@link patio.Database#supportsSavepoints} is false.</b>
         * </p>
         */
        SQL_RELEASE_SAVEPOINT: 'RELEASE SAVEPOINT autopoint_%d',

        /**
         * The <b>ROLLBACK</b> SQL fragment used to rollback a database transaction.
         * This should be overrode by adapters if the SQL for the adapters
         * database is different.
         */
        SQL_ROLLBACK: 'ROLLBACK',

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
        SQL_ROLLBACK_TO_SAVEPOINT: 'ROLLBACK TO SAVEPOINT autopoint_%d',

        /**
         * The <b>SAVEPOINT</b> SQL fragment used for creating a save point in a
         * database transaction.
         * <p>
         *      <b>This fragment will not be used if {@link patio.Database#supportsSavepoints} is false.</b>
         * </p>
         */
        SQL_SAVEPOINT: 'SAVEPOINT autopoint_%d',

        /**
         * Object containing different database transaction isolation levels.
         * This object is used to look up the proper SQL when starting a new transaction
         * and setting the isolation level in the options.
         * @field
         */
        TRANSACTION_ISOLATION_LEVELS: {
            uncommitted: 'READ UNCOMMITTED',
            committed: 'READ COMMITTED',
            repeatable: 'REPEATABLE READ',
            serializable: 'SERIALIZABLE'
        },

        /**
         * @ignore
         */
        POSTGRES_DEFAULT_RE: /^(?:B?('.*')::[^']+|\((-?\d+(?:\.\d+)?)\))$/,

        /**
         * @ignore
         */
        MSSQL_DEFAULT_RE: /^(?:\(N?('.*')\)|\(\((-?\d+(?:\.\d+)?)\)\))$/,

        /**
         * @ignore
         */
        MYSQL_TIMESTAMP_RE: /^CURRENT_(?:DATE|TIMESTAMP)?$/,

        /**
         * @ignore
         */
        STRING_DEFAULT_RE: /^'(.*)'$/,

        /**
         * @ignore
         */
        POSTGRES_TIME_PATTERN: "HH:mm:ss",

        /**
         * @ignore
         */
        POSTGRES_DATE_TIME_PATTERN: "yyyy-MM-dd HH:mm:ss.SSZ",

        __transactions: null,


        /**
         * @ignore
         */
        constructor: function () {
            this._super(arguments);
            this.__transactions = [];
            this.__transactionQueue = new Queue();
        },

        /**
         * Executes the given SQL on the database. This method should be implemented by adapters.
         * <b>This method should not be called directly by user code.</b>
         */

        execute: function (sql, opts, conn) {
            var ret;
            if (opts.stream) {
                ret = this.__executeStreamed(sql, opts, conn);
            } else {
                ret = this.__executePromised(sql, opts, conn);
            }
            return ret;
        },

        __executeStreamed: function (sql, opts, conn) {
            var self = this, ret;
            if (conn) {
                var cleanUp = function cleanUp(err) {
                    if (err) {
                        conn.errored = true;
                    }
                    self._returnConnection(conn);
                    ret.removeListener("end", cleanUp);
                    ret.removeListener("error", cleanUp);
                    self = conn = sql = ret = null;
                };

                ret = this.__logAndExecute(sql, opts, function () {
                    return conn.stream(sql, opts);
                });
                ret.on("end", cleanUp);
                ret.on("error", cleanUp);
            } else {
                ret = new PassThroughStream({objectMode: true});
                this._getConnection().chain(function (conn) {
                    var queryStream = self.__executeStreamed(sql, opts, conn);
                    function fieldHandler(fields) {
                        ret.emit("fields", fields);
                        queryStream.removeListener("fields", fieldHandler);
                        queryStream = self = ret = null;
                    }
                    queryStream.on("fields", fieldHandler);
                    pipeAll(queryStream, ret);
                }, function (err) {
                    ret.emit("error", err);
                });
            }
            return ret;
        },

        __executePromised: function (sql, opts, conn) {
            var self = this, ret;
            if (conn) {
                ret = this.__logAndExecute(sql, opts, function () {
                    return conn[opts.stream ? "stream" : "query"](sql, opts);
                });
                ret.both(function () {
                    var ret = self._returnConnection(conn);
                    self = conn = sql = null;
                    return ret;
                });
            } else {
                ret = this._getConnection().chain(function (conn) {
                    return self.__executePromised(sql, opts, conn);
                });
            }
            return ret;
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
         *   DB.indexes("artists").chain(function(indexes){
         *     //e.g. indexes === {artists_name_ukey : {columns : [name], unique : true}};
         *   })
         **/
        indexes: function (table, opts) {
            throw new NotImplemented("indexes should be overridden by adapters");
        },

        /**
         * Proxy for {@link patio.Dataset#get}.
         */
        get: function () {
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
        call: function (psName, hash) {
            hash = hash || {};
            this.preparedStatements[psName](hash);
        },

        /**
         * Method that should be used when submitting any DDL (Data DefinitionLanguage) SQL,
         * such as {@link patio.Database#createTable}. By default, calls {@link patio.Database#executeDui}.
         * <b>This method should not be called directly by user code.</b>
         */
        executeDdl: function (sql, opts) {
            opts = opts || {};
            return this.executeDui(sql, opts);
        },


        /**
         * Method that should be used when issuing a DELETE, UPDATE, or INSERT
         * statement.  By default, calls {@link patio.Database#execute}.
         * <b>This method should not be called directly by user code.</b>
         **/
        executeDui: function (sql, opts) {
            opts = opts || {};
            return this.execute(sql, opts);
        },

        /**
         * Method that should be used when issuing a INSERT
         * statement.  By default, calls {@link patio.Database#executeDui}.
         * <b>This method should not be called directly by user code.</b>
         **/
        executeInsert: function (sql, opts) {
            opts = opts || {};
            return this.executeDui(sql, opts);
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
        run: function (sql, opts) {
            opts = opts || {};
            return this.executeDdl(sql, opts);
        },

        /**
         * Parse the schema from the database.
         *
         * @example
         *
         *   DB.schema("artists").chain(function(schema){
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
        schema: function (table, opts) {
            if (!isFunction(this.schemaParseTable)) {
                throw new Error("Schema parsing is not implemented on this database");
            }
            opts = opts || {};
            var schemaParts = this.__schemaAndTable(table);
            var sch = schemaParts[0], tableName = schemaParts[1];
            var quotedName = this.__quoteSchemaTable(table);
            opts = sch && !opts.schema ? merge({schema: sch}, opts) : opts;
            if (opts.reload) {
                delete this.schemas[quotedName];
            }
            var self = this;
            return this.schemaParseTable(tableName, opts).chain(function (cols) {
                if (!cols || cols.length === 0) {
                    throw new Error("Error parsing schema, " + table + " no columns returns, table probably doesnt exist");
                } else {
                    var schema = {};
                    cols.forEach(function (c) {
                        var name = c[0];
                        c = c[1];
                        c.jsDefault = self.__columnSchemaToJsDefault(c["default"], c.type);
                        schema[name] = c;
                    });
                    return schema;
                }
            });
        },

        /**
         * Remove the cached schema for the given table name
         * @example
         * DB.schema("artists").chain(function(){
         *      DB.removeCachedSchema("artists");
         * });
         * @param {String|patio.sql.Identifier|patio.sql.QualifiedIdentifier} the table to remove from this
         * databases cached schemas.
         */
        removeCachedSchema: function (table) {
            if (this.schemas && !isEmpty(this.schemas)) {
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
         * }).chain(function(ret){
         *      //ret.table1Exists === true
         *      //ret.table2Exists === false
         * });
         * @param {String|patio.sql.Identifier|patio.sql.QualifiedIdentifier} the table to remove from this
         *
         * @return {Promise} a promise resolved with a boolean indicating if the table exists.
         */
        tableExists: function (table, cb) {
            return this.from(table).first().chain(function () {
                return true;
            }, function () {
                return false;
            }).classic(cb).promise();
        },

        /**
         * Returns a promise with a list of tables names in this database. This method
         * should be implemented by the adapter.
         *
         * @example
         * DB.tables().chain(function(tables){
         *    //e.g. tables === ["table1", "table2", "table3"];
         * });
         *
         * @return {Promise} a promise that is resolved with a list of tablenames.
         */
        tables: function () {
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
         * ```
         * //normal transaction
         * DB.transaction(function() {
         *      return comb.when(
         *          this.execute('DROP TABLE test;'),
         *          this.execute('DROP TABLE test2;')
         *      );
         *  });
         *
         * //transaction with a save point.
         * DB.transaction(function() {
         *      return this.transaction({savepoint : true}, function() {
         *         return comb.when(
         *              this.execute('DROP TABLE test;'),
         *              this.execute('DROP TABLE test2;')
         *         );
         *      });
         *});
         * ```
         *
         * Using a promise.
         *
         * ```
         * var ds = db.from("user");
         * db.transaction(function(){
         *      return ds.insert({
         *              firstName:"Jane",
         *              lastName:"Gorgenson",
         *              password:"password",
         *              dateOfBirth:new Date(1956, 1, 3)
         *          }).chain(function(){
         *              return ds.forEach(function(user){
         *                  return ds.where({id:user.id}).update({firstName:user.firstName + 1});
         *              });
         *      });
         * });
         * ```
         *
         * Using the done method
         * ```
         * var ds = db.from("user");
         * db.transaction(function(db, done){
         *      ds.insert({
         *          firstName:"Jane",
         *          lastName:"Gorgenson",
         *          password:"password",
         *          dateOfBirth:new Date(1956, 1, 3)
         *      }).chain(function(){
         *          ds.forEach(function(user){
         *             return ds.where({id:user.id}).update({firstName:user.firstName + 1});
         *          }).classic(done)
         *      });
         * });
         * ```
         *
         * ```
         * //WITH ISOLATION LEVELS
         *
         * db.supportsTransactionIsolationLevels = true;
         * //BEGIN
         * //SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
         * //DROP TABLE test1'
         * //COMMIT
         * DB.transaction({isolation:"uncommited"}, function(d) {
         *     return d.run("DROP TABLE test1");
         * });
         *
         * //BEGIN
         * //SET TRANSACTION ISOLATION LEVEL READ COMMITTED
         * //DROP TABLE test1
         * //COMMIT
         *  DB.transaction({isolation:"committed"}, function(d) {
         *      return d.run("DROP TABLE test1");
         *  });
         *
         * //BEGIN
         * //SET TRANSACTION ISOLATION LEVEL REPEATABLE READ'
         * //DROP TABLE test1
         * //COMMIT
         *  DB.transaction({isolation:"repeatable"}, function(d) {
         *      return d.run("DROP TABLE test1");
         *  });
         *
         * //BEGIN
         * //SET TRANSACTION ISOLATION LEVEL SERIALIZABLE
         * //DROP TABLE test1
         * //COMMIT
         *  DB.transaction({isolation:"serializable"}, function(d) {
         *      return d.run("DROP TABLE test1");
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
         * ```
         *
         * @param {Object} [opts={}] options to use when performing the transaction.
         * @param {String} [opts.isolated] This will ensure that the transaction will be run on its own connection and
         *               not part of another transaction if one is already in progress.
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
         * called in the scope of the database by default so one can use this. The funciton is also
         * called with the database as the first argument, and a function to be called when the tranaction is complete.
         * If you return a promise from the transaction block then you do not need to call the done cb.
         *
         *
         * @return {Promise} a promise that is resolved once the transaction is complete.
         **/
        transaction: function (opts, cb) {
            if (isFunction(opts)) {
                cb = opts;
                opts = {};
            } else {
                opts = opts || {};
            }
            var ret;
            if (!this.__alreadyInTransaction) {
                this.__alreadyInTransaction = true;
                ret = this.__transaction(null, opts, cb);
            } else {
                ret = this.__enqueueTransaction(opts, cb);
            }
            return ret.promise();
        },

        __enqueueTransaction: function (opts, cb) {
            var ret = new Promise(), self = this;
            var transaction = function () {
                self.__transaction(null, opts, cb).chain(ret.callback, ret.errback);
            };
            this.__transactionQueue.enqueue(transaction);
            return ret.promise();
        },

        __transactionProxy: function (cb, conn) {
            var promises = [];
            var repl = [];
            //set up our proxy methos
            ["transaction", "execute"].forEach(function (n) {
                var orig = this[n];
                repl.push({name: n, orig: orig});
                this[n] = function (arg1, arg2) {
                    var ret;
                    try {
                        if (n === "transaction") {
                            if (comb.isHash(arg1) && arg1.isolated) {
                                return this.__enqueueTransaction(arg1, arg2);
                            } else {
                                //if its a transaction with no options then we just create a promise from what ever is returned
                                if (isFunction(arg1)) {
                                    ret = resolveOrPromisfyFunction(arg1, this, this);
                                } else {
                                    if (this.supportsSavepoints && arg1.savepoint) {
                                        //if we support save points there is a save point option then we
                                        //use __transaction again with the previous connection
                                        ret = this.__transaction(conn, arg1, arg2);
                                    } else {
                                        //other wise use the function passed in to get the returned promise
                                        ret = resolveOrPromisfyFunction(arg2, this, this);
                                    }
                                }
                            }
                        } else {
                            ret = orig.apply(this, argsToArray(arguments).concat(conn));
                        }
                    } catch (e) {
                        ret = new Promise().errback(e);
                    }
                    if (comb.isInstanceOf(ret, stream.Stream)) {
                        promises.push(comb.promisfyStream(ret));
                    } else {
                        promises.push((ret = when(ret)));
                        ret = ret.promise();
                    }
                    return ret;
                };
            }, this);
            try {
                promises.push(resolveOrPromisfyFunction(cb, this, this));
            } catch (e) {
                promises.push(new Promise().errback(e));
            }
            if (promises.length === 0) {
                promises.push(new Promise().callback());
            }
            var self = this;
            return new PromiseList(promises).both(function () {
                repl.forEach(function (o) {
                    self[o.name] = o.orig;
                });
            }).promise();

        },

        _getConnection: function () {
            return this.pool.getConnection();
        },

        _returnConnection: function (conn) {
            if (!this.alreadyInTransaction(conn)) {
                this.pool.returnConnection(conn);
            }
        },

        __getTransactionConnection: function (conn) {
            var self = this;
            return conn ? when(conn) : this._getConnection().chain(function (conn) {
                //add the connection to the transactions
                self.__transactions.push(conn);
                //reset transaction depth to 0, this is used for keeping track of save points.
                conn.__transactionDepth = 0;
                return conn;
            });
        },

        __transaction: function (conn, opts, cb) {
            var promise = new Promise();
            try {
                var self = this;

                return this.__getTransactionConnection(conn).chain(function (conn) {
                    return self.__beginTransaction(conn, opts).chain(function () {
                        return self.__transactionProxy(cb, conn)
                            .chain(function () {
                                return self.__commitTransaction(conn).chain(function (res) {
                                        self.__finishTransactionAndCheckForMore(conn);
                                        return res;
                                    },
                                    function errorHandler(err) {
                                        self.__finishTransactionAndCheckForMore(conn);
                                        throw err;
                                    });
                            }, function (err) {
                                return self.__rollback(conn, err);
                            });
                    });
                });
            } catch (e) {
                this.logError(e);
                promise.errback(e);
            }
            return promise.promise();
        },

        __transactionComplete: function (promise, type, conn) {
            this.__finishTransactionAndCheckForMore(conn);
            promise[type].apply(promise, argsToArray(arguments).slice(3));
        },

        __rollback: function (conn, err) {
            return this.__rollbackTransaction(conn, null, err)
                .both(hitch(this, "__finishTransactionAndCheckForMore", conn))
                .chain(hitch(this, function () {
                    if (conn.__transactionDepth <= 1) {
                        this.__transactionError(err);
                    } else {
                        throw err;
                    }
                }));
        },

        __transactionError: function (err) {
            if (isArray(err)) {
                for (var i in err) {
                    if (i in err) {
                        var e = err[i];
                        if (isArray(e) && e.length === 2) {
                            var realE = e[1];
                            if (realE !== "ROLLBACK") {
                                throw realE;
                            }
                            break;
                        } else {
                            throw e;
                        }
                    }
                }
            } else {
                if (err !== "ROLLBACK") {
                    throw err;
                }
            }
        },

        __finishTransactionAndCheckForMore: function (conn) {
            if (this.alreadyInTransaction(conn)) {
                if (!this.supportsSavepoints || ((conn.__transactionDepth -= 1) <= 0)) {
                    if (conn) {
                        this.pool.returnConnection(conn);
                    }
                    var index, transactions = this.__transactions;
                    if ((index = transactions.indexOf(conn)) > -1) {
                        transactions.splice(index, 1);
                    }
                    if (!this.__transactionQueue.isEmpty) {
                        this.__transactionQueue.dequeue()();
                    } else {
                        this.__alreadyInTransaction = false;
                    }
                }
            }
        },

        //SQL to start a new savepoint
        __beginSavepointSql: function (depth) {
            return format(this._static.SQL_SAVEPOINT, depth);
        },

        // Start a new database connection on the given connection
        __beginNewTransaction: function (conn, opts) {
            var self = this;
            return this.__logConnectionExecute(conn, this.beginTransactionSql).chain(function () {
                return self.__setTransactionIsolation(conn, opts);
            });
        },

        //Start a new database transaction or a new savepoint on the given connection.
        __beginTransaction: function (conn, opts) {
            var ret;
            if (this.supportsSavepoints) {
                if (conn.__transactionDepth > 0) {
                    ret = this.__logConnectionExecute(conn, this.__beginSavepointSql(conn.__transactionDepth));
                } else {
                    ret = this.__beginNewTransaction(conn, opts);
                }
                conn.__transactionDepth += 1;
            } else {
                ret = this.__beginNewTransaction(conn, opts);
            }
            return ret;
        },

        // SQL to commit a savepoint
        __commitSavepointSql: function (depth) {
            return format(this.SQL_RELEASE_SAVEPOINT, depth);
        },

        //Commit the active transaction on the connection
        __commitTransaction: function (conn, opts) {
            opts = opts || {};
            if (this.supportsSavepoints) {
                var depth = conn.__transactionDepth;
                var sql = null;
                if (depth > 1) {
                    sql = this.__commitSavepointSql(depth - 1);
                } else {
                    this.__commiting = true;
                    sql = this.commitTransactionSql;
                }
                return this.__logConnectionExecute(conn, (sql));
            } else {
                this.__commiting = true;
                return this.__logConnectionExecute(conn, this.commitTransactionSql);
            }
        },


        //SQL to rollback to a savepoint
        __rollbackSavepointSql: function (depth) {
            return format(this.SQL_ROLLBACK_TO_SAVEPOINT, depth);
        },

        //Rollback the active transaction on the connection
        __rollbackTransaction: function (conn, opts) {
            opts = opts || {};
            if (this.supportsSavepoints) {
                var sql, depth = conn.__transactionDepth;
                if (depth > 1) {
                    sql = this.__rollbackSavepointSql(depth - 1);
                } else {
                    this.__commiting = true;
                    sql = this.rollbackTransactionSql;
                }
                return this.__logConnectionExecute(conn, sql);
            } else {
                this.__commiting = false;
                return this.__logConnectionExecute(conn, this.rollbackTransactionSql);
            }
        },

        // Set the transaction isolation level on the given connection
        __setTransactionIsolation: function (conn, opts) {
            var level;
            var ret;
            if (this.supportsTransactionIsolationLevels && !isUndefinedOrNull(level = isUndefinedOrNull(opts.isolation) ? this.transactionIsolationLevel : opts.isolation)) {
                ret = this.__logConnectionExecute(conn, this.__setTransactionIsolationSql(level));
            } else {
                ret = new Promise().callback();
            }
            return ret.promise();
        },

        // SQL to set the transaction isolation level
        __setTransactionIsolationSql: function (level) {
            return format("SET TRANSACTION ISOLATION LEVEL %s", this.TRANSACTION_ISOLATION_LEVELS[level]);
        },

        //Convert the given default, which should be a database specific string, into
        //a javascript object.
        __columnSchemaToJsDefault: function (def, type) {
            if (isNull(def) || isUndefined(def)) {
                return null;
            }
            var origDefault = def, m, datePattern, dateTimePattern, timeStampPattern, timePattern;
            if (this.type === "postgres" && (m = def.match(this.POSTGRES_DEFAULT_RE)) !== null) {
                def = m[1] || m[2];
                dateTimePattern = this.POSTGRES_DATE_TIME_PATTERN;
                timePattern = this.POSTGRES_TIME_PATTERN;
            }
            if (this.type === "mssql" && (m = def.match(this.MSSQL_DEFAULT_RE)) !== null) {
                def = m[1] || m[2];
            }
            if (["string", "blob", "date", "datetime", "year", "timestamp", "time", "enum"].indexOf(type) !== -1) {
                if (this.type === "mysql") {
                    if (["date", "datetime", "time", "timestamp"].indexOf(type) !== -1 && def.match(this.MYSQL_TIMESTAMP_RE)) {
                        return null;
                    }
                    origDefault = def = "'" + def + "'".replace("\\", "\\\\");
                }
                if (!(m = def.match(this.STRING_DEFAULT_RE))) {
                    return null;
                }
                def = m[1].replace("''", "'");
            }
            var ret = null;
            try {
                switch (type) {
                case "boolean":
                    if (def.match(/[f0]/i)) {
                        ret = false;
                    } else if (def.match(/[t1]/i)) {
                        ret = true;
                    } else if (isBoolean(def)) {
                        ret = def;
                    }

                    break;
                case "blob":
                    ret = new Buffer(def);
                    break;
                case "string":
                case "enum":
                    ret = def;
                    break;
                case  "integer":
                    ret = parseInt(def, 10);
                    if (isNaN(ret)) {
                        ret = null;
                    }
                    break;
                case  "float":
                case  "decimal":
                    ret = parseFloat(def, 10);
                    if (isNaN(ret)) {
                        ret = null;
                    }
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
        schemaColumnType: function (dbType) {
            var ret = dbType, m;
            if (dbType.match(/^interval$/i)) {
                ret = "interval";
            } else if (dbType.match(/^(character( varying)?|n?(var)?char)/i)) {
                ret = "string";
            } else if (dbType.match(/^int(eger)?|(big|small|tiny)int/i)) {
                ret = "integer";
            } else if (dbType.match(/^date$/i)) {
                ret = "date";
            } else if (dbType.match(/^year/i)) {
                ret = "year";
            } else if (dbType.match(/^((small)?datetime|timestamp( with(out)? time zone)?)$/i)) {
                ret = "datetime";
            } else if (dbType.match(/^time( with(out)? time zone)?$/i)) {
                ret = "time";
            } else if (dbType.match(/^(bit|boolean)$/i)) {
                ret = "boolean";
            } else if (dbType.match(/^(real|float|double( precision)?)$/i)) {
                ret = "float";
            } else if ((m = dbType.match(/^(?:(?:(?:num(?:ber|eric)?|decimal|double)(?:\(\d+,\s*(\d+)\))?)|(?:small)?money)/i))) {
                ret = m[1] && m[1] === '0' ? "integer" : "decimal";
            } else if (dbType.match(/n?text/i)) {
                ret = "text";
            } else if (dbType.match(/bytea|[bc]lob|image|(var)?binary/i)) {
                ret = "blob";
            } else if (dbType.match(/^enum/i)) {
                ret = "enum";
            } else if (dbType.match(/^set/i)) {
                ret = "set";
            } else if (dbType.match(/^json/i)) {
                ret = "json";
            }
            return ret;
        },

        /**
         * Returns true if this DATABASE is currently in a transaction.
         *
         * @param opts
         * @return {Boolean} true if this dabase is currently in a transaction.
         */
        alreadyInTransaction: function (conn, opts) {
            opts = opts || {};
            return this.__transactions.indexOf(conn) !== -1 && (!this.supportsSavepoints || !opts.savepoint);
        },

        /**@ignore*/
        getters: {
            /**@lends patio.Database.prototype*/

            /**
             * SQL to BEGIN a transaction.
             * See {@link patio.Database#SQL_BEGIN} for default,
             * @field
             * @type String
             */
            beginTransactionSql: function () {
                return this.SQL_BEGIN;
            },

            /**
             * SQL to COMMIT a transaction.
             * See {@link patio.Database#SQL_COMMIT} for default,
             * @field
             * @type String
             */
            commitTransactionSql: function () {
                return this.SQL_COMMIT;
            },

            /**
             * SQL to ROLLBACK a transaction.
             * See {@link patio.Database#SQL_ROLLBACK} for default,
             * @field
             * @type String
             */
            rollbackTransactionSql: function () {
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
            outputIdentifierFunc: function () {
                var ds = this.dataset;
                return function (ident) {
                    return ds.outputIdentifier(ident);
                };
            },

            /**
             * Return a function for the dataset's {@link patio.Dataset#inputIdentifierMethod}.
             * Used in metadata parsing to make sure the returned information is in the
             * correct format.
             *
             * @field
             * @type Function
             */
            inputIdentifierFunc: function () {
                var ds = this.dataset;
                return function (ident) {
                    return ds.inputIdentifier(ident);
                };
            },

            /**
             * Return a dataset that uses the default identifier input and output methods
             * for this database.  Used when parsing metadata so that column are
             * returned as expected.
             *
             * @field
             * @type patio.Dataset
             */
            metadataDataset: function () {
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


    "static": {
        SQL_BEGIN: 'BEGIN',
        SQL_COMMIT: 'COMMIT',
        SQL_RELEASE_SAVEPOINT: 'RELEASE SAVEPOINT autopoint_%d',
        SQL_ROLLBACK: 'ROLLBACK',
        SQL_ROLLBACK_TO_SAVEPOINT: 'ROLLBACK TO SAVEPOINT autopoint_%d',
        SQL_SAVEPOINT: 'SAVEPOINT autopoint_%d',

        TRANSACTION_BEGIN: 'Transaction.begin',
        TRANSACTION_COMMIT: 'Transaction.commit',
        TRANSACTION_ROLLBACK: 'Transaction.rollback',

        TRANSACTION_ISOLATION_LEVELS: {
            uncommitted: 'READ UNCOMMITTED',
            committed: 'READ COMMITTED',
            repeatable: 'REPEATABLE READ',
            serializable: 'SERIALIZABLE'
        },

        POSTGRES_DEFAULT_RE: /^(?:B?('.*')::[^']+|\((-?\d+(?:\.\d+)?)\))$/,
        MSSQL_DEFAULT_RE: /^(?:\(N?('.*')\)|\(\((-?\d+(?:\.\d+)?)\)\))$/,
        MYSQL_TIMESTAMP_RE: /^CURRENT_(?:DATE|TIMESTAMP)?$/,
        STRING_DEFAULT_RE: /^'(.*)'$/


    }
}).as(module);
