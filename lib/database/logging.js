var comb = require("comb"), logging = comb.logging, Logger = logging.Logger, format = comb.string.format;


var LOGGER = Logger.getLogger("moose.Database");

comb.define(null, {

    instance : {
        logInfo : function() {
            if (LOGGER.isInfo) {
                LOGGER.info.apply(LOGGER, arguments);
            }
        },

        logDebug : function() {
            if (LOGGER.isDebug) {
                LOGGER.debug.apply(LOGGER, arguments);
            }
        },

        logError : function() {
            if (LOGGER.isError) {
                LOGGER.error.apply(LOGGER, arguments);
            }
        },

        logWarn : function() {
            if (LOGGER.isWarn) {
                LOGGER.warn.apply(LOGGER, arguments);
            }
        },

        logTrace : function() {
            if (LOGGER.isTrace) {
                LOGGER.trace.apply(LOGGER, arguments);
            }
        },

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
            logger : function() {
                return LOGGER;
            }
        }
    },

    static : {
        logInfo : function() {
            if (LOGGER.isInfo) {
                LOGGER.info.apply(LOGGER, arguments);
            }
        },

        logDebug : function() {
            if (LOGGER.isDebug) {
                LOGGER.debug.apply(LOGGER, arguments);
            }
        },

        logError : function() {
            if (LOGGER.isError) {
                LOGGER.error.apply(LOGGER, arguments);
            }
        },

        logWarn : function() {
            if (LOGGER.isWarn) {
                LOGGER.warn.apply(LOGGER, arguments);
            }
        },

        logTrace : function() {
            if (LOGGER.isTrace) {
                LOGGER.trace.apply(LOGGER, arguments);
            }
        },

        logFatal : function() {
            if (LOGGER.isFatal) {
                LOGGER.fatal.apply(LOGGER, arguments);
            }
        },

        getters : {
            logger : function() {
                return LOGGER;
            }
        }
    }

}).as(module);
