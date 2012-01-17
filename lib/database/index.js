var comb = require("comb"),
    format = comb.string.format,
    hitch = comb.hitch,
    sql = require("../sql").sql,
    ConnectionPool = require("../ConnectionPool"),
    DatabaseError = require("../errors").DatabaseError,
    ConnectDB = require("./connect"),
    DatasetDB = require("./dataset"),
    DefaultsDB = require("./defaults"),
    LoggingDB = require("./logging"),
    QueryDB = require("./query"),
    SchemaDB = require("./schema"), moose;

var DATABASES = [];
var Database = comb.define([ConnectDB, DatasetDB, DefaultsDB, LoggingDB, QueryDB, SchemaDB], {

    instance : {
        /**@lends moose.Database.prototype*/

        /**
         * A Database object represents a virtual connection to a database.
         * The Database class is meant to be subclassed by database adapters in order
         * to provide the functionality needed for executing queries.
         *
         * @param {Object} opts options used to create the database
         */
        constructor : function(opts) {
            opts = opts || {};
            !moose && (moose = require("../index"));
            this.moose = moose;
            this._super(arguments, [opts]);
            opts = comb.merge(this.connectionPoolDefaultOptions, opts);
            this.schemas = {};
            this.type = opts.type;
            this.defaultSchema = opts.defaultSchema || this.defaultSchemaDefault;
            this.preparedStatements = {};
            this.opts = opts;
            this.pool = ConnectionPool.getPool(opts, hitch(this, "createConnection"), hitch(this, "closeConnection"), hitch(this, "validate"));
            DATABASES.push(this);
        },

        /**
         * Casts the given type to a literal type
         *
         * @example
         *   DB.castTypeLiteral(Float) # double precision
         *   DB.cast_type_literal(:foo) # foo
         *   */
        castTypeLiteral : function(type) {
            return this.typeLiteral({type : type});
        },


        /*
         Proxy the literal call to the dataset.
         *
         *   DB.literal(1) # 1
         *   DB.literal(:a) # a
         *   DB.literal('a') # 'a'
         *   */
        literal : function(v) {
            return this.dataset.literal(v)
        },

        /*
         * Typecast the value to the given column_type. Calls
         * typecast_value_#{column_type} if the method exists,
         * otherwise returns the value.
         * This method should raise Sequel::InvalidValue if assigned value
         * is invalid.
         * */
        typecastValue : function(columnType, value) {
            if (comb.isNull(value) || comb.isUndefined(value)) return null;
            var meth = "typecastValue" + columnType.charAt(0).toUpperCase() + columnType.substr(1).toLowerCase();
            try {
                if (comb.isFunction(this[meth])) {
                    return this[meth](value);
                } else {
                    return value;
                }
            } catch(e) {
                throw new DatabaseError("Unable to convert " + columnType);
            }
        },


        // Typecast the value to true, false, or nil
        typecastValueBoolean : function(value) {
            if (value == false || 0 || "0" || value.match(/^f(alse)?$/i)) {
                return false;
            } else {
                return comb.isEmpty(value) ? null : true;
            }
        },

        // Typecast the value to a Date
        typecastValueDate : function(value) {
            if (comb.isDate(value)) {
                return value;
            } else if (comb.isString(value)) {
                var ret = comb.date.parse(value, this.DATE_PATTERN);
                if (ret == null) {
                    throw new DatabaseError(format("Invalid value for date %j", [value]));
                }
                return ret;
            } else if (comb.isHash(value) && !comb.isEmpty(value)) {
                return new Date(value.year, value.month, value.day);
            } else {
                throw new DatabaseError(format("Invalid value for date %j", [value]));
            }
        },

        // Typecast the value to a DateTime or Time depending on Sequel.datetime_class
        typecastValueDatetime : function(value) {
            var ret;
            if(comb.isInstanceOf(value, sql.DateTime)){
                return value;
            }else if (comb.isDate(value)) {
                ret = value;
            }else if (comb.isHash(value) && !comb.isEmpty(value)) {
                ret = new Date(value.year, value.month, value.day, value.hour, value.minute, value.second);

            } else if (comb.isString(value)) {
                ret = comb.date.parse(value, this.DATE_TIME_PATTERN);
                if (ret == null) {
                    throw new DatabaseError(format("Invalid value for datetime %j", [value]));
                }
            } else {
                throw new DatabaseError(format("Invalid value for datetime %j", [value]));
            }
            return new sql.DateTime(ret);
        },

        // Typecast the value to a DateTime or Time depending on Sequel.datetime_class
        typecastValueTimestamp : function(value) {
            var ret;
            if(comb.isInstanceOf(value, sql.TimeStamp)){
                return ret;
            }else if (comb.isDate(value)) {
                ret = value;
            }else if (comb.isHash(value) && !comb.isEmpty(value)) {
                ret = new Date(value.year, value.month, value.day, value.hour, value.minute, value.second);

            } else if (comb.isString(value)) {
                ret = comb.date.parse(value, this.DATE_TIME_PATTERN);
                if (ret == null) {
                    throw new DatabaseError(format("Invalid value for timestamp %j", [value]));
                }
            } else {
                throw new DatabaseError(format("Invalid value for timestamp %j", [value]));
            }
            return new sql.TimeStamp(ret);
        },

        // Typecast the value to a Time
        typecastValueTime : function(value) {
            var ret;
            if(comb.isInstanceOf(value, sql.Time)){
                return value;
            }else if (comb.isDate(value)) {
                ret = value;
            } else if (comb.isString(value)) {
                ret = comb.date.parse(value, this.TIME_PATTERN);
                if (ret == null) {
                    throw new DatabaseError(format("Invalid value for time %j", [value]));
                }
            } else if (comb.isHash(value) && !comb.isEmpty(value)) {
                ret = new Date(0, 0, 0, value.hour, value.minute, value.second);
            } else {
                throw new DatabaseError(format("Invalid value for time %j", [value]));
            }
            return new sql.Time(ret);
        },

        // Typecast the value to a BigDecimal
        typecastValueDecimal : function(value) {
            if (comb.isNumber(value)) {
                return value * 1.0;
            } else {
                throw new DatabaseError(format("Invalid value for decimal %j", [value]));
            }

        },

        // Typecast the value to a Float
        typecastValueFloat : function(value) {
            var ret = parseFloat(value);
            if (isNaN(ret)) {
                throw new DatabaseError(format("Invalid value for float %j", [value]));
            }
            return ret;
        },

        // Typecast the value to an Integer
        typecastValueInteger : function(value) {
            var ret = parseInt(value, 10);
            if (isNaN(ret)) {
                throw new DatabaseError(format("Invalid value for integer %j", [value]));
            }
            return ret;
        },

        // Typecast the value to a String
        typecastValueString : function(value) {
            return "" + value;
        }

    },

    static : {
        DATABASES : DATABASES
    }

}).as(module);