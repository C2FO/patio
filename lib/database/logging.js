var comb = require("comb"),
    define = comb.define,
    Promise = comb.Promise,
    isFunction = comb.isFunction,
    logging = comb.logging,
    Logger = logging.Logger,
    hitch = comb.hitch,
    format = comb.string.format,
    QueryError = require("../errors").QueryError;


var LOGGER = Logger.getLogger("patio.Database");

define(null, {
    instance:{
        /**@lends patio.Database.prototype*/

        /**
         * Logs an INFO level message to the "patio.Database" logger.
         */
        logInfo:function () {
            if (LOGGER.isInfo) {
                LOGGER.info.apply(LOGGER, arguments);
            }
        },

        /**
         * Logs a DEBUG level message to the "patio.Database" logger.
         */
        logDebug:function () {
            if (LOGGER.isDebug) {
                LOGGER.debug.apply(LOGGER, arguments);
            }
        },

        /**
         * Logs an ERROR level message to the "patio.Database" logger.
         */
        logError:function (error) {
            if (LOGGER.isError) {
                LOGGER.error.apply(LOGGER, arguments);
            }
        },

        /**
         * Logs a WARN level message to the "patio.Database" logger.
         */
        logWarn:function () {
            if (LOGGER.isWarn) {
                LOGGER.warn.apply(LOGGER, arguments);
            }
        },

        /**
         * Logs a TRACE level message to the "patio.Database" logger.
         */
        logTrace:function () {
            if (LOGGER.isTrace) {
                LOGGER.trace.apply(LOGGER, arguments);
            }
        },

        /**
         * Logs a FATAL level message to the "patio.Database" logger.
         */
        logFatal:function () {
            if (LOGGER.isFatal) {
                LOGGER.fatal.apply(LOGGER, arguments);
            }
        },

        /* Yield to the block, logging any errors at error level to all loggers,
         * and all other queries with the duration at warn or info level.
         * */
        __logAndExecute:function (sql, args, cb) {
            if (isFunction(args)) {
                cb = args;
                args = null;
            }

            if (args) {
                sql = format("%s; %j", sql, args);
            }
            sql = sql.trim();
            var start = new Date();
            var ret = new Promise();
            if (isFunction(cb)) {
                this.logInfo("Executing; %s", sql);
                cb().then(hitch(this, function () {
                    this.logDebug("Duration: % 6dms; %s", new Date() - start, sql);
                    ret.callback.apply(ret, arguments);
                }), hitch(this, function (err) {
                    err = new QueryError(format("%s: %s", err.message, sql));
                    this.logError(err);
                    ret.errback.apply(ret, [err]);
                }));
            } else {
                throw new QueryError("CB is required");
            }
            return ret.promise();
        },

        /*Log the given SQL and then execute it on the connection, used by
         *the transaction code.
         * */
        __logConnectionExecute:function (conn, sql) {
            return this.__logAndExecute(sql, hitch(this, function () {
                return conn[this.connectionExecuteMethod](sql);
            }));
        },


        getters:{
            /**@lends patio.Database.prototype*/
            /**
             * The "patio.Database" logger.
             * @field
             */
            logger:function () {
                return LOGGER;
            }
        }
    },

    "static":{
        /**@lends patio.Database*/
        /**
         * Logs an INFO level message to the "patio.Database" logger.
         */
        logInfo:function () {
            if (LOGGER.isInfo) {
                LOGGER.info.apply(LOGGER, arguments);
            }
        },

        /**
         * Logs a DEBUG level message to the "patio.Database" logger.
         */
        logDebug:function () {
            if (LOGGER.isDebug) {
                LOGGER.debug.apply(LOGGER, arguments);
            }
        },

        /**
         * Logs a ERROR level message to the "patio.Database" logger.
         */
        logError:function () {
            if (LOGGER.isError) {
                LOGGER.error.apply(LOGGER, arguments);
            }
        },

        /**
         * Logs a WARN level message to the "patio.Database" logger.
         */
        logWarn:function () {
            if (LOGGER.isWarn) {
                LOGGER.warn.apply(LOGGER, arguments);
            }
        },

        /**
         * Logs a TRACE level message to the "patio.Database" logger.
         */
        logTrace:function () {
            if (LOGGER.isTrace) {
                LOGGER.trace.apply(LOGGER, arguments);
            }
        },

        /**
         * Logs a FATAL level message to the "patio.Database" logger.
         */
        logFatal:function () {
            if (LOGGER.isFatal) {
                LOGGER.fatal.apply(LOGGER, arguments);
            }
        },

        getters:{
            /**@lends patio.Database*/
            /**
             * The "patio.Database" logger.
             * @field
             */
            logger:function () {
                return LOGGER;
            }
        }
    }

}).as(module);
