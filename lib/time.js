var comb = require("comb"),
    PatioError = require("./errors").PatioError,
    date = comb.date,
    SQL = require("./sql").sql,
    define = comb.define,
    dateFormat = date.format,
    isDate = comb.isDate,
    isUndefined = comb.isUndefined,
    isInstanceOf = comb.isInstanceOf;

var DEFAULT_DATE_FORMAT = "yyyy-MM-dd";
var TWO_YEAR_DATE_FORMAT = "yy-MM-dd";
var DEFAULT_YEAR_FORMAT = "yyyy";
var DEFAULT_TIME_FORMAT = "HH:mm:ss";
var DEFAULT_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ssZ";
var DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ssZ";


/**
 * Mixin that provides time formatting/coversion functions.
 *
 * @constructor
 * @name Time
 * @memberOf patio
 *
 * @property {String}  [dateFormat={@link patio.Time#DEFAULT_DATE_FORMAT}] the format to use to formatting/converting dates.
 * @property {String}  [yearFormat={@link patio.Time#DEFAULT_YEAR_FORMAT}] the format to use to formatting/converting dates.
 * @property {String}  [timeFormat={@link patio.Time#DEFAULT_TIME_FORMAT}] the format to use to formatting/converting dates.
 * @property {String}  [timeStampFormat={@link patio.Time#DEFAULT_TIMESTAMP_FORMAT}] the format to use to formatting/converting dates.
 * @property {String}  [dateTimeFormat={@link patio.Time#DEFAULT_DATETIME_FORMAT}] the format to use to formatting/converting dates.
 *
 */
define(null, {
    instance:{
        /**
         * @lends patio.Time.prototype
         */

        /**
         *
         * @constant
         * @type String
         *
         * @description default date format.
         *
         * @default  yyyy-MM-dd
         *
         * @example
         *
         *  patio.DEFAULT_DATE_FORMAT = "yyyy MM, dd";
         */
        DEFAULT_DATE_FORMAT:"yyyy-MM-dd",

        /**
         * @constant
         * @type String
         *
         * @description Two year date format
         * This is used in date coversions when convertTwoDigitYears is used.
         *
         * If this format fails then dateFormat|DEFAULT_DATE_FORMAT is used.
         *
         * @default yy-MM-dd
         *
         * @example
         *
         * patio.TWO_YEAR_DATE_FORMAT = "yy MM, dd";
         */
        TWO_YEAR_DATE_FORMAT:"yy-MM-dd",

        /**
         * @constant
         * @type String
         *
         * @description Default year format
         * @default yyyy
         *
         * @example
         *
         * patio.DEFAULT_YEAR_FORMAT = "yy";
         */
        DEFAULT_YEAR_FORMAT:"yyyy",

        /**
         * @constant
         * @type String
         *
         * @description Default time format
         *
         * @default HH:mm:ss
         *
         * @example
         *
         * patio.DEFAULT_TIME_FORMAT = "HH:mm:ss:SS";
         */
        DEFAULT_TIME_FORMAT:"HH:mm:ss",

        /**
         * @constant
         * @type String
         *
         * @description Default timestamp format
         *
         * @default yyyy-MM-dd HH:mm:ss
         *
         * @example
         *
         * patio.DEFAULT_TIMESTAMP_FORMAT = "yyyy-MM-dd hh:mm:ss:SS a";
         */
        DEFAULT_TIMESTAMP_FORMAT:"yyyy-MM-dd HH:mm:ss",

        /**
         * @constant
         * @type String
         *
         * @description Default datetime format
         *
         * @default yyyy-MM-dd HH:mm:ss
         *
         * @example
         *
         * patio.DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd hh:mm:ss:SS a";
         */
        DEFAULT_DATETIME_FORMAT:"yyyy-MM-dd HH:mm:ss",


        /**
         * @constant
         * @type String
         *
         * @description Timestamp format used if the default fails. This format includes timezone info.
         *
         * @default yyyy-MM-dd HH:mm:ssZ
         */
        TIMESTAMP_FORMAT_TZ:"yyyy-MM-dd HH:mm:ssZ",

        /**
         * @constant
         * @type String
         *
         * @description Two year timestamp format. If convertTwoDigitYear is set to true and the timeStampFormat
         * fails this format will be tried.
         *
         * @default yy-MM-dd HH:mm:ss
         *
         */
        TIMESTAMP_TWO_YEAR_FORMAT:"yy-MM-dd HH:mm:ss",

        /**
         * @constant
         * @type String
         *
         * @description Datetime format used if the default fails. This format includes timezone info.
         *
         * @default yyyy-MM-dd HH:mm:ssZ
         */
        DATETIME_FORMAT_TZ:"yyyy-MM-dd HH:mm:ssZ",

        /**
         * @constant
         * @type String
         *
         * @description Two year datetime format. If convertTwoDigitYear is set to true and the timeStampFormat
         * fails this format will be tried.
         *
         * @default yy-MM-dd HH:mm:ss
         *
         */
        DATETIME_TWO_YEAR_FORMAT:"yy-MM-dd HH:mm:ss",

        /**
         * @constant
         * @type String
         *
         * @description ISO-8601 format
         *
         * @default yyyy-MM-ddTHH:mm:ssZ
         */
        ISO_8601:"yyyy-MM-ddTHH:mm:ssZ",

        /**
         * @constant
         * @type String
         *
         * @description Two year ISO-8601 format
         * @default yy-MM-ddTHH:mm:ssZ
         */
        ISO_8601_TWO_YEAR:"yy-MM-ddTHH:mm:ssZ",

        /**
         * @type Boolean
         *
         * @description By default patio will try to covert all two digit years.
         * To turn this off:
         *
         * @default true
         *
         * @example
         * patio.convertTwoDigitYears = false;
         */
        convertTwoDigitYears:true,

        __dateFormat:undefined,
        __yearFormat:undefined,
        __timeFormat:undefined,
        __timeStampFormat:undefined,
        __dateTimeFormat:undefined,

        /**
         * Converts a {@link patio.sql.Year} to a string.
         * The format used is {@link patio.Time#yearFormat},
         * which defaults to {@link patio.Time#DEFAULT_YEAR_FORMAT}
         *
         * @example
         *
         * var date = new Date(2004, 1, 1, 1, 1, 1),
         *     year = new sql.Year(2004);
         * patio.yearToString(date); //=> '2004'
         * patio.yearToString(year); //=> '2004'
         * patio.yearFormat = "yy";
         * patio.yearToString(date); //=> '04'
         * patio.yearToString(year); //=> '04'
         *
         *
         * @param {Date\sql.Year} dt the year to covert to to a string.
         *
         * @returns {String} the date string.
         */
        yearToString:function (dt, format) {
            return dateFormat(isInstanceOf(dt, SQL.Year) ? dt.date : dt, format || this.yearFormat);
        },

        /**
         * Converts a {@link sql.Time} to a string.
         * The format used is {@link patio.Time#timeFormat},
         * which defaults to {@link patio.Time#DEFAULT_TIME_FORMAT}
         *
         * @example
         *
         * var date =  new Date(null, null, null, 13, 12, 12),
         *     time = new sql.Time(13,12,12);
         * patio.timeToString(date); //=> '13:12:12'
         * patio.timeToString(time); //=> '13:12:12'
         * patio.timeFormat = "hh:mm:ss";
         * patio.timeToString(date); //=> '01:12:12'
         * patio.timeToString(time); //=> '01:12:12'
         *
         * @param {Date\patio.sql.Time} dt the time to covert to to a string.
         *
         * @returns {String} the date string.
         */
        timeToString:function (dt, format) {
            return dateFormat(isInstanceOf(dt, SQL.Time) ? dt.date : dt, format || this.timeFormat);
        },

        /**
         * Converts a @link{patio.sql.DateTime} to a string.
         * The format used is {@link patio.Time#dateTimeFormat},
         * which defaults to {@link patio.Time#DEFAULT_DATETIME_FORMAT}
         *
         * @example
         *
         * var date = new Date(2004, 1, 1, 12, 12, 12),
         * dateTime = new sql.DateTime(2004, 1, 1, 12, 12, 12),
         * offset = "-0600";
         * patio.dateTimeToString(date); //=> '2004-02-01 12:12:12'
         * patio.dateTimeToString(dateTime); //=> '2004-02-01 12:12:12'
         *
         * patio.dateTimeFormat = patio.DATETIME_TWO_YEAR_FORMAT;
         * patio.dateTimeToString(date); //=> '04-02-01 12:12:12'
         * patio.dateTimeToString(dateTime); //=> '04-02-01 12:12:12'
         *
         * patio.dateTimeFormat = patio.DATETIME_FORMAT_TZ;
         * patio.dateTimeToString(date); //=> '2004-02-01 12:12:12-0600'
         * patio.dateTimeToString(dateTime); //=> '2004-02-01 12:12:12-0600'
         *
         * patio.dateTimeFormat = patio.ISO_8601;
         * patio.dateTimeToString(date); //=> '2004-02-01T12:12:12-0600'
         * patio.dateTimeToString(dateTime); //=> '2004-02-01T12:12:12-0600'
         *
         * patio.dateTimeFormat = patio.ISO_8601_TWO_YEAR;
         * patio.dateTimeToString(date); //=> '04-02-01T12:12:12-0600'
         * patio.dateTimeToString(dateTime); //=> '04-02-01T12:12:12-0600'
         *
         * @param {Date\patio.sql.DateTime} dt the datetime to covert to to a string.
         *
         * @returns {String} the date string.
         */
        dateTimeToString:function (dt, format) {
            return dateFormat(isInstanceOf(dt, SQL.DateTime) ? dt.date : dt, format || this.dateTimeFormat);
        },


        /**
         * Converts a {@link patio.sql.TimeStamp} to a string.
         * The format used is {@link patio.Time#timeStampFormat},
         * which defaults to {@link patio.Time#DEFAULT_TIMESTAMP_FORMAT}
         *
         * @example
         *
         * var date = new Date(2004, 1, 1, 12, 12, 12),
         * dateTime = new sql.TimeStamp(2004, 1, 1, 12, 12, 12),
         * offset = "-0600";
         * patio.timeStampToString(date); //=> '2004-02-01 12:12:12'
         * patio.timeStampToString(dateTime); //=> '2004-02-01 12:12:12'
         *
         * patio.timeStampFormat = patio.TIMESTAMP_TWO_YEAR_FORMAT;
         * patio.timeStampToString(date); //=> '04-02-01 12:12:12'
         * patio.timeStampToString(dateTime); //=> '04-02-01 12:12:12'
         *
         * patio.timeStampFormat = patio.TIMESTAMP_FORMAT_TZ;
         * patio.timeStampToString(date); //=> '2004-02-01 12:12:12-0600'
         * patio.timeStampToString(dateTime); //=> '2004-02-01 12:12:12-0600'
         *
         * patio.timeStamp = patio.ISO_8601;
         * patio.timeStampToString(date); //=> '2004-02-01T12:12:12-0600'
         * patio.timeStampToString(dateTime); //=> '2004-02-01T12:12:12-0600'
         *
         * patio.timeStamp = patio.ISO_8601_TWO_YEAR;
         * patio.timeStampToString(date); //=> '04-02-01T12:12:12-0600'
         * patio.timeStampToString(dateTime); //=> '04-02-01T12:12:12-0600'
         *
         *
         * @param {Date\patio.sql.TimeStamp} dt the timestamp to convert to to a string.
         *
         * @returns {String} the date string.
         */
        timeStampToString:function (dt, format) {
            return dateFormat(isInstanceOf(dt, SQL.TimeStamp) ? dt.date : dt, format || this.timeStampFormat);
        },

        /**
         * Converts a date to a string.
         *
         * @example
         *
         * var date = new Date(2004, 1, 1),
         * timeStamp = new sql.TimeStamp(2004, 1, 1, 12, 12, 12),
         * dateTime = new sql.DateTime(2004, 1, 1, 12, 12, 12),
         * year = new sql.Year(2004),
         * time = new sql.Time(12,12,12),
         *
         * //convert years
         * patio.dateToString(year); //=> '2004'
         * patio.yearFormat = "yy";
         * patio.dateToString(year); //=> '04'
         * patio.yearFormat = patio.DEFAULT_YEAR_FORMAT;
         * patio.dateToString(year); //=> '2004'
         *
         * //convert times
         * patio.dateToString(time); //=> '12:12:12'
         *
         * //convert dates
         * patio.dateToString(date); //=> '2004-02-01'
         * patio.dateFormat = patio.TWO_YEAR_DATE_FORMAT;
         * patio.dateToString(date); //=> '04-02-01'
         * patio.dateFormat = patio.DEFAULT_DATE_FORMAT;
         * patio.dateToString(date); //=> '2004-02-01'

         * //convert dateTime
         * patio.dateToString(dateTime); //=> '2004-02-01 12:12:12'
         * patio.dateTimeFormat = patio.DATETIME_TWO_YEAR_FORMAT;
         * patio.dateToString(dateTime); //=> '04-02-01 12:12:12'
         * patio.dateTimeFormat = patio.DATETIME_FORMAT_TZ;
         * patio.dateToString(dateTime); //=> '2004-02-01 12:12:12-0600'
         * patio.dateTimeFormat = patio.ISO_8601;
         * patio.dateToString(dateTime); //=> '2004-02-01T12:12:12-0600'
         * patio.dateTimeFormat = patio.ISO_8601_TWO_YEAR;
         * patio.dateToString(dateTime); //=> '04-02-01T12:12:12-0600'
         * patio.dateTimeFormat = patio.DEFAULT_DATETIME_FORMAT;
         * patio.dateToString(dateTime); //=> '2004-02-01 12:12:12'

         * //convert timestamps
         * patio.dateToString(timeStamp); //=> '2004-02-01 12:12:12'
         * patio.timeStampFormat = patio.TIMESTAMP_TWO_YEAR_FORMAT;
         * patio.dateToString(timeStamp); //=> '04-02-01 12:12:12'
         * patio.timeStampFormat = patio.TIMESTAMP_FORMAT_TZ;
         * patio.dateToString(timeStamp); //=> '2004-02-01 12:12:12-0600'
         * patio.timeStampFormat = patio.ISO_8601;
         * patio.dateToString(timeStamp); //=> '2004-02-01T12:12:12-0600'
         * patio.timeStampFormat = patio.ISO_8601_TWO_YEAR;
         * patio.dateToString(timeStamp); //=> '04-02-01T12:12:12-0600'
         * patio.timeStampFormat = patio.DEFAULT_TIMESTAMP_FORMAT;
         * patio.dateToString(timeStamp); //=> '2004-02-01 12:12:12'
         *
         * @param {Date\patio.sql.Time|patio.sql.Year|patio.sql.DateTime|patio.sql.TimeStamp} dt the date to covert to to a string.
         *
         * @returns {String} the date string.
         */
        dateToString:function (dt, format) {
            var ret = "";
            if (isInstanceOf(dt, SQL.Time)) {
                ret = this.timeToString(dt, format);
            } else if (isInstanceOf(dt, SQL.Year)) {
                ret = this.yearToString(dt, format);
            } else if (isInstanceOf(dt, SQL.DateTime)) {
                ret = this.dateTimeToString(dt, format);
            } else if (isInstanceOf(dt, SQL.TimeStamp)) {
                ret = this.timeStampToString(dt, format);
            } else if (isDate(dt)) {
                ret = dateFormat(dt, format || this.dateFormat);
            }
            return ret;
        },

        /**
         * Converts a year date string to a {@link patio.sql.Year}
         *
         * @example
         *
         * var year = new sql.Year(2004);
         * patio.stringToYear("2004"); //=> year
         * patio.yearFormat = "yy";
         * patio.stringToYear("04"); //=> year
         *
         * @param {String} dt the string to covert to a {@link patio.sql.Year}
         * @param {String} [format=patio.Time#yearFormat] the format to use when converting the date.
         *
         * @throws {PatioError} thrown if the conversion fails.
         * @return {patio.sql.Year} the {@link patio.sql.Year}
         */
        stringToYear:function (dt, format) {
            var ret = date.parse(dt, format || this.yearFormat);
            if (!ret) {
                throw new PatioError("Unable to convert year: " + dt);
            }
            return new SQL.Year(ret);
        },

        /**
         * Converts a time date string to a {@link patio.sql.Time}
         *
         * @example
         *
         * var time = new sql.Time(12,12,12);
         * patio.stringToTime("12:12:12"); //=> time
         *
         * @param {String} dt the string to convert to a {@link patio.sql.Time}
         * @param {String} [format=patio.Time#timeFormat] the format to use when converting the date.
         *
         * @throws {PatioError} thrown if the conversion fails.
         * @return {patio.sql.Time} the {@link patio.sql.Time}
         */
        stringToTime:function (dt, format) {
            var ret = date.parse(dt, format || this.timeFormat);
            if (!ret) {
                throw new PatioError("Unable to convert time: " + dt);
            }
            return new SQL.Time(ret);
        },

        /**
         * Converts a date string to a Date
         *
         * @example
         *
         * var date = new Date(2004, 1,1,0,0,0);
         * patio.stringToDate('2004-02-01'); //=> date
         *
         * patio.dateFormat = patio.TWO_YEAR_DATE_FORMAT;
         * patio.stringToDate('04-02-01'); //=> date
         *
         * @param {String} dt the string to convert to a Date
         * @param {String} [format=patio.Time#dateFormat] the format to use when converting the date.
         *
         * @throws {PatioError} thrown if the conversion fails.
         * @return {Date} the {@link Date}
         */
        stringToDate:function (dt, format) {
            var ret;
            if (this.convertTwoDigitYears) {
                ret = date.parse(dt, this.TWO_YEAR_DATE_FORMAT);
            }
            if (!ret) {
                ret = date.parse(dt, format || this.dateFormat);
            }
            if (!ret) {
                throw new PatioError("Unable to convert date: " + dt);
            }
            return ret;
        },

        /**
         * Converts a datetime date string to a {@link patio.sql.DateTime}
         *
         * @example
         *
         *  var dateTime = new sql.DateTime(2004, 1, 1, 12, 12, 12),
         * offset = getTimeZoneOffset();
         * patio.stringToDateTime('2004-02-01 12:12:12'); //=> dateTime
         *
         * patio.dateTimeFormat = patio.DATETIME_TWO_YEAR_FORMAT;
         * patio.stringToDateTime('04-02-01 12:12:12-0600'); //=> dateTime
         *
         * patio.dateTimeFormat = patio.DATETIME_FORMAT_TZ;
         * patio.stringToDateTime('2004-02-01 12:12:12-0600'); //=> dateTime
         *
         * patio.dateTimeFormat = patio.ISO_8601;
         * patio.stringToDateTime('2004-02-01T12:12:12-0600'); //=> dateTime
         *
         * patio.dateTimeFormat = patio.ISO_8601_TWO_YEAR;
         * patio.stringToDateTime('04-02-01T12:12:12-0600'); //=> dateTime
         *
         * @param {String} dt the string to convert to a {@link patio.sql.DateTime}
         * @param {String} [format=patio.Time#dateTimeFormat] the format to use when converting the date.
         *
         * @throws {PatioError} thrown if the conversion fails.
         * @return {patio.sql.DateTime} the {@linkpatio.sql.DateTime}
         */
        stringToDateTime:function (dt, fmt) {
            var useT = dt.indexOf("T") !== -1;
            //test if there is a T in the string so we can try to properly convert it
            var format = fmt ? fmt : useT ? this.ISO_8601 : this.DEFAULT_DATETIME_FORMAT;
            var ret = date.parse(dt, format);
            //if the coversion failed try it with a time zone
            !ret && (ret = date.parse(dt, this.DATETIME_FORMAT_TZ));
            if (!ret && this.convertTwoDigitYears) {
                //if we still fail and we need to convert two digit years try the twoYearFormat
                var twoYearFormat = fmt ? fmt : useT ? this.ISO_8601_TWO_YEAR : this.DATETIME_TWO_YEAR_FORMAT;
                ret = date.parse(dt, twoYearFormat);
                //try with time zone
                !ret && (ret = date.parse(dt, twoYearFormat + "Z"));
            }
            if (!ret) {
                throw new PatioError("Unable to convert datetime: " + dt);
            }
            return new SQL.DateTime(ret);
        },

        /**
         * Converts a timestamp date string to a {@link patio.sql.TimeStamp}
         *
         * @example
         *
         *  var timeStamp = new sql.TimeStamp(2004, 1, 1, 12, 12, 12);
         * patio.stringToTimeStamp('2004-02-01 12:12:12'); //=> timeStamp
         *
         * patio.timeStampFormat = patio.TIMESTAMP_TWO_YEAR_FORMAT;
         * patio.stringToTimeStamp('04-02-01 12:12:12-0600'); //=> timeStamp
         *
         * patio.timeStampFormat = patio.TIMESTAMP_FORMAT_TZ;
         * patio.stringToTimeStamp('2004-02-01 12:12:12-0600'); //=> timeStamp
         *
         * patio.timeStampFormat = patio.ISO_8601;
         * patio.stringToTimeStamp('2004-02-01T12:12:12-0600'); //=> timeStamp
         *
         * patio.timeStampFormat = patio.ISO_8601_TWO_YEAR;
         * patio.stringToTimeStamp('04-02-01T12:12:12-0600'); //=> timeStamp
         *
         * @param {String} dt the string to convert to a {@link patio.sql.TimeStamp}
         * @param {String} [format=patio.Time#timeStampFormat] the format to use when converting the date.
         *
         * @throws {PatioError} thrown if the conversion fails.
         * @return {patio.sql.TimeStamp} the {@link patio.sql.TimeStamp}
         */
        stringToTimeStamp:function (dt, fmt) {
            var useT = dt.indexOf("T") !== -1;
            //test if there is a T in the string so we can try to properly convert it
            var format = fmt ? fmt : useT ? this.ISO_8601 : this.DEFAULT_TIMESTAMP_FORMAT;
            var ret = date.parse(dt, format);
            //if the coversion failed try it with a time zone
            !ret && (ret = date.parse(dt, this.TIMESTAMP_FORMAT_TZ));
            if (!ret && this.convertTwoDigitYears) {
                //if we still fail and we need to convert two digit years try the twoYearFormat
                var twoYearFormat = fmt ? fmt : useT ? this.ISO_8601_TWO_YEAR : this.TIMESTAMP_TWO_YEAR_FORMAT;
                ret = date.parse(dt, twoYearFormat);
                //try with time zone
                !ret && (ret = date.parse(dt, twoYearFormat + "Z"));
            }
            if (!ret) {
                throw new PatioError("Unable to convert timestamp: " + dt);
            }
            return new SQL.TimeStamp(ret);
        },


        /**@ignore*/
        getters:{
            /**@ignore*/

            /**@ignore*/
            dateFormat:function (format) {
                return isUndefined(this.__dateFormat) ? this.DEFAULT_DATE_FORMAT : this.__dateFormat;
            },
            /**@ignore*/
            yearFormat:function (format) {
                return isUndefined(this.__yearFormat) ? this.DEFAULT_YEAR_FORMAT : this.__yearFormat;
            },
            /**@ignore*/
            timeFormat:function (format) {
                return isUndefined(this.__timeFormat) ? this.DEFAULT_TIME_FORMAT : this.__timeFormat;
            },
            /**@ignore*/
            timeStampFormat:function (format) {
                return isUndefined(this.__timeStampFormat) ? this.DEFAULT_TIMESTAMP_FORMAT : this.__timeStampFormat;
            },
            /**@ignore*/
            dateTimeFormat:function (format) {
                return isUndefined(this.__dateTimeFormat) ? this.DEFAULT_DATETIME_FORMAT : this.__dateTimeFormat;
            }
        },


        /**@ignore*/
        setters:{
            /**@ignore*/

            /**@ignore*/
            dateFormat:function (format) {
                this.__dateFormat = format;
            },
            /**@ignore*/
            yearFormat:function (format) {
                this.__yearFormat = format;
            },
            /**@ignore*/
            timeFormat:function (format) {
                this.__timeFormat = format;
            },
            /**@ignore*/
            timeStampFormat:function (format) {
                this.__timeStampFormat = format;
            },
            /**@ignore*/
            dateTimeFormat:function (format) {
                this.__dateTimeFormat = format;
            }
        }
    }
}).as(module);