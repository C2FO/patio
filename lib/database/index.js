var comb = require("comb"),
    format = comb.string.format,
    merge = comb.merge,
    hitch = comb.hitch,
    isNull = comb.isNull,
    define = comb.define,
    isBoolean = comb.isBoolean,
    isUndefined = comb.isUndefined,
    isFunction = comb.isFunction,
    isString = comb.isString,
    isObject = comb.isObject,
    isDate = comb.isDate,
    isArray = Array.isArray,
    isHash = comb.isHash,
    isNumber = comb.isNumber,
    isInstanceOf = comb.isInstanceOf,
    isEmpty = comb.isEmpty,
    sql = require("../sql").sql,
    DateTime = sql.DateTime,
    TimeStamp = sql.TimeStamp,
    Json = sql.Json,
    Year = sql.Year,
    Time = sql.Time,
    json = sql.json,
    ConnectionPool = require("../ConnectionPool"),
    DatabaseError = require("../errors").DatabaseError,
    ConnectDB = require("./connect"),
    DatasetDB = require("./dataset"),
    DefaultsDB = require("./defaults"),
    LoggingDB = require("./logging"),
    QueryDB = require("./query"),
    SchemaDB = require("./schema"), patio;

var DATABASES = [];
var Database = define([ConnectDB, DatasetDB, DefaultsDB, LoggingDB, QueryDB, SchemaDB], {

    instance: {
        /**@lends patio.Database.prototype*/

        /**
         * A Database object represents a virtual connection to a database.
         * The Database class is meant to be subclassed by database adapters in order
         * to provide the functionality needed for executing queries.
         *
         * @constructs
         * @param {Object} opts options used to create the database
         *
         * @property {String} uri A database URI used to create the database connection. This property is
         * available even if an object was used to create the database connection.
         * @property {patio.Dataset} dataset returns an empty adapter specific {@link patio.Dataset} that can
         * be used to query the {@link patio.Database} with.
         */
        constructor: function (opts) {
            opts = opts || {};
            if (!patio) {
                patio = require("../index");
            }
            this.patio = patio;
            this._super(arguments, [opts]);
            opts = merge(this.connectionPoolDefaultOptions, opts);
            this.schemas = {};
            this.type = opts.type;
            this.defaultSchema = opts.defaultSchema || this.defaultSchemaDefault;
            this.preparedStatements = {};
            this.opts = opts;
            this.pool = ConnectionPool.getPool(opts, hitch(this, this.createConnection), hitch(this, this.closeConnection), hitch(this, this.validate));
        },

        /**
         * Casts the given type to a SQL type.
         *
         * @example
         *   DB.castTypeLiteral(Number) //=> numeric
         *   DB.castTypeLiteral("foo") //=> foo
         *   DB.castTypeLiteral(String) //=> varchar(255)
         *   DB.castTypeLiteral(Boolean) //=> boolean
         *
         *@param type the javascript type to cast to a SQL type.
         *
         * @return {String} the SQL data type.
         **/
        castTypeLiteral: function (type) {
            return this.typeLiteral({type: type});
        },


        /**
         * This function acts as a proxy to {@link patio.Dataset#literal}.
         *
         * See {@link patio.Dataset#literal}.
         **/
        literal: function (v) {
            return this.dataset.literal(v);
        },

        /**
         * Typecast the value to the given columnType. Calls
         * typecastValue{ColumnType} if the method exists,
         * otherwise returns the value.
         *
         * @example
         * DB.typeCastValue("boolean", 0) //=> false
         * DB.typeCastValue("boolean", 1) //=> true
         * DB.typeCastValue("timestamp",  '2004-02-01 12:12:12')
         *      //=> new patio.sql.TimeStamp(2004, 1, 1, 12, 12, 12);
         *
         * @throws {patio.DatabaseError} if there is an error converting the value to the column type.
         *
         * @param {String} columnType the SQL datatype of the column
         * @param value the value to typecast.
         *
         * @return the typecasted value.
         * */
        typecastValue: function (columnType, value) {
            if (isNull(value) || isUndefined(value)) {
                return null;
            }
            var meth = "__typecastValue" + columnType.charAt(0).toUpperCase() + columnType.substr(1).toLowerCase();
            try {
                if (isFunction(this[meth])) {
                    return this[meth](value);
                } else {
                    return value;
                }
            } catch (e) {
                throw e;
            }
        },

        // Typecast the value to true, false, or null
        __typecastValueJson: function (value) {
            var ret = value;
            if (!isInstanceOf(value, Json)) {
                ret = json(value);
            }
            return ret;
        },


        // Typecast the value to true, false, or null
        __typecastValueBoolean: function (value) {
            if (isBoolean(value)) {
                return value;
            } else if (value === 0 || value === "0" || (isString(value) && value.match(/^f(alse)?$/i) !== null)) {
                return false;
            } else {
                return (isObject(value) && isEmpty(value)) || !value ? null : true;
            }
        },

        // Typecast the value to blob, false, or null
        __typecastValueBlob: function (value) {
            if (isInstanceOf(value, Buffer)) {
                return value;
            } else if (isArray(value) || isString(value)) {
                return new Buffer(value);
            } else {
                throw new DatabaseError("Invalid value for blob " + value);
            }
        },

        // Typecast the value to true, false, or null
        __typecastValueText: function (value) {
            return value.toString();
        },

        // Typecast the value to a Date
        __typecastValueDate: function (value) {
            if (isDate(value)) {
                return value;
            } else if (isString(value)) {
                var ret = patio.stringToDate(value);
                if (!ret) {
                    throw new DatabaseError(format("Invalid value for date %j", [value]));
                }
                return ret;
            } else if (isHash(value) && !isEmpty(value)) {
                return new Date(value.year, value.month, value.day);
            } else {
                throw new DatabaseError(format("Invalid value for date %j", [value]));
            }
        },

        // Typecast the value to a patio.sql.DateTime.
        __typecastValueDatetime: function (value) {
            var ret;
            if (isInstanceOf(value, DateTime)) {
                return value;
            } else if (isDate(value)) {
                ret = value;
            } else if (isHash(value) && !isEmpty(value)) {
                ret = new Date(value.year, value.month, value.day, value.hour, value.minute, value.second);
            } else if (isString(value)) {
                ret = patio.stringToDateTime(value);
                if (!ret) {
                    throw new DatabaseError(format("Invalid value for datetime %j", [value]));
                }
                ret = ret.date;
            } else {
                throw new DatabaseError(format("Invalid value for datetime %j", [value]));
            }
            return new DateTime(ret);
        },

        // Typecast the value to a patio.sql.DateTime
        __typecastValueTimestamp: function (value) {
            var ret;
            if (isInstanceOf(value, TimeStamp)) {
                return ret;
            } else if (isDate(value)) {
                ret = value;
            } else if (isHash(value) && !isEmpty(value)) {
                ret = new Date(value.year, value.month, value.day, value.hour, value.minute, value.second);
            } else if (isString(value)) {
                ret = patio.stringToTimeStamp(value);
                if (!ret) {
                    throw new DatabaseError(format("Invalid value for timestamp %j", [value]));
                }
                ret = ret.date;
            } else {
                throw new DatabaseError(format("Invalid value for timestamp %j", [value]));
            }
            return new TimeStamp(ret);
        },

        // Typecast the value to a patio.sql.Year
        __typecastValueYear: function (value) {
            if (isInstanceOf(value, Year)) {
                return value;
            } else if (isNumber(value)) {
                return new Year(value);
            } else if (isString(value)) {
                var ret = patio.stringToYear(value);
                if (!ret) {
                    throw new DatabaseError(format("Invalid value for date %j", [value]));
                }
                return ret;
            } else if (isHash(value) && !isEmpty(value)) {
                return new Date(value.year, value.month, value.day);
            } else {
                throw new DatabaseError(format("Invalid value for date %j", [value]));
            }
        },


        // Typecast the value to a patio.sql.Time
        __typecastValueTime: function (value) {
            var ret;
            if (isInstanceOf(value, Time)) {
                return value;
            } else if (isDate(value)) {
                ret = value;
            } else if (isString(value)) {
                ret = patio.stringToTime(value);
                if (!ret) {
                    throw new DatabaseError(format("Invalid value for time %j", [value]));
                }
                ret = ret.date;
            } else if (isHash(value) && !isEmpty(value)) {
                ret = new Date(0, 0, 0, value.hour, value.minute, value.second);
            } else {
                throw new DatabaseError(format("Invalid value for time %j", [value]));
            }
            return new Time(ret);
        },

        // Typecast the value to a Number
        __typecastValueDecimal: function (value) {
            var ret = parseFloat(value);
            if (isNaN(ret)) {
                throw new DatabaseError(format("Invalid value for decimal %j", [value]));
            }
            return ret;
        },

        // Typecast the value to a Number
        __typecastValueFloat: function (value) {
            var ret = parseFloat(value);
            if (isNaN(ret)) {
                throw new DatabaseError(format("Invalid value for float %j", [value]));
            }
            return ret;
        },

        // Typecast the value to a Number
        __typecastValueInteger: function (value) {
            var ret = parseInt(value, 10);
            if (isNaN(ret)) {
                throw new DatabaseError(format("Invalid value for integer %j", [value]));
            }
            return ret;
        },

        // Typecast the value to a String
        __typecastValueString: function (value) {
            return "" + value;
        }

    },

    "static": {
        /**@lends patio.Database*/

        /**
         * A list of currently connected Databases.
         * @type patio.Database[]
         */
        DATABASES: DATABASES
    }

}).as(module);