var comb = require("comb"), logging = comb.logging, Logger = logging.Logger, format = comb.string.format;


var LOGGER = Logger.getLogger("patio.Database");

comb.define(null, {
    instance : {
        /**@lends patio.Database.prototype*/

        /**
         * Logs an INFO level message to the "patio.Database" logger.
         */
        logInfo : function() {
            if (LOGGER.isInfo) {
                LOGGER.info.apply(LOGGER, arguments);
            }
        },

        /**
         * Logs a DEBUG level message to the "patio.Database" logger.
         */
        logDebug : function() {
            if (LOGGER.isDebug) {
                LOGGER.debug.apply(LOGGER, arguments);
            }
        },

        /**
         * Logs an ERROR level message to the "patio.Database" logger.
         */
        logError : function() {
            if (LOGGER.isError) {
                LOGGER.error.apply(LOGGER, arguments);
            }
        },

        /**
         * Logs a WARN level message to the "patio.Database" logger.
         */
        logWarn : function() {
            if (LOGGER.isWarn) {
                LOGGER.warn.apply(LOGGER, arguments);
            }
        },

        /**
         * Logs a TRACE level message to the "patio.Database" logger.
         */
        logTrace : function() {
            if (LOGGER.isTrace) {
                LOGGER.trace.apply(LOGGER, arguments);
            }
        },

        /**
         * Logs a FATAL level message to the "patio.Database" logger.
         */
        logFatal : function() {
            if (LOGGER.isFatal) {
                LOGGER.fatal.apply(LOGGER, arguments);
            }
        },

        /* Yield to the block, logging any errors at error level to all loggers,
         * and all other queries with the duration at warn or info level.
         * */
        __logAndExecute : function(sql, args, cb) {
            if (comb.isFunction(args)) {
                cb = args;
                args = null;
            }
            if (args) {
                sql = format("%s; %j", sql, args);
            }
            var start = new Date();
            var ret = new comb.Promise();
            if (comb.isFunction(cb)) {
                cb().then(comb.hitch(this, function() {
                    this.logInfo(format("Duration: % 4dms; %s", new Date() - start, sql.trim()));
                    ret.callback.apply(ret, arguments);
                }), comb.hitch(this, function(err) {
                    this.logError(format("Error   : %s: %s: %s", err.name, err.message, sql.trim()));
                    ret.errback.apply(ret, arguments);
                }));
            } else {
                throw "CB is required";
            }
            return ret;
        },

        /*Log the given SQL and then execute it on the connection, used by
         *the transaction code.
         * */
        __logConnectionExecute : function(conn, sql) {
            return this.__logAndExecute(sql, comb.hitch(this, function() {
                return conn[this.connectionExecuteMethod](sql);
            }));
        },


        getters : {
            /**@lends patio.Database.prototype*/
            /**
             * The "patio.Database" logger.
             * @field
             */
            logger : function() {
                return LOGGER;
            }
        }
    },

    static : {
       /**@lends patio.Database*/
        /**
         * Logs an INFO level message to the "patio.Database" logger.
         */
        logInfo : function() {
            if (LOGGER.isInfo) {
                LOGGER.info.apply(LOGGER, arguments);
            }
        },

        /**
         * Logs a DEBUG level message to the "patio.Database" logger.
         */
        logDebug : function() {
            if (LOGGER.isDebug) {
                LOGGER.debug.apply(LOGGER, arguments);
            }
        },

        /**
         * Logs a ERROR level message to the "patio.Database" logger.
         */
        logError : function() {
            if (LOGGER.isError) {
                LOGGER.error.apply(LOGGER, arguments);
            }
        },

        /**
         * Logs a WARN level message to the "patio.Database" logger.
         */
        logWarn : function() {
            if (LOGGER.isWarn) {
                LOGGER.warn.apply(LOGGER, arguments);
            }
        },

        /**
         * Logs a TRACE level message to the "patio.Database" logger.
         */
        logTrace : function() {
            if (LOGGER.isTrace) {
                LOGGER.trace.apply(LOGGER, arguments);
            }
        },

        /**
         * Logs a FATAL level message to the "patio.Database" logger.
         */
        logFatal : function() {
            if (LOGGER.isFatal) {
                LOGGER.fatal.apply(LOGGER, arguments);
            }
        },

        getters : {
            /**@lends patio.Database*/
            /**
             * The "patio.Database" logger.
             * @field
             */
            logger : function() {
                return LOGGER;
            }
        }
    }

}).as(module);
