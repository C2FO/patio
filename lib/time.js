var comb = require("comb"),
    MooseError = require("./errors").MooseError,
    date = comb.date,
    SQL = require("./sql").sql;

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
 * @memberOf moose
 *
 * @property {String}  [dateFormat={@link moose.Time#DEFAULT_DATE_FORMAT}] the format to use to formatting/converting dates.
 * @property {String}  [yearFormat={@link moose.Time#DEFAULT_YEAR_FORMAT}] the format to use to formatting/converting dates.
 * @property {String}  [timeFormat={@link moose.Time#DEFAULT_TIME_FORMAT}] the format to use to formatting/converting dates.
 * @property {String}  [timeStampFormat={@link moose.Time#DEFAULT_TIMESTAMP_FORMAT}] the format to use to formatting/converting dates.
 * @property {String}  [dateTimeFormat={@link moose.Time#DEFAULT_DATETIME_FORMAT}] the format to use to formatting/converting dates.
 *
 */
comb.define(null, {
    instance:{
        /**
         * @lends moose.Time.prototype
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
         *  moose.DEFAULT_DATE_FORMAT = "yyyy MM, dd";
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
         * moose.TWO_YEAR_DATE_FORMAT = "yy MM, dd";
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
         * moose.DEFAULT_YEAR_FORMAT = "yy";
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
         * moose.DEFAULT_TIME_FORMAT = "HH:mm:ss:SS";
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
         * moose.DEFAULT_TIMESTAMP_FORMAT = "yyyy-MM-dd hh:mm:ss:SS a";
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
         * moose.DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd hh:mm:ss:SS a";
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
         * @description By default moose will try to covert all two digit years.
         * To turn this off:
         *
         * @default true
         *
         * @example
         * moose.convertTwoDigitYears = false;
         */
        convertTwoDigitYears:true,

        __dateFormat:undefined,
        __yearFormat:undefined,
        __timeFormat:undefined,
        __timeStampFormat:undefined,
        __dateTimeFormat:undefined,

        /**
         * Converts a {@link sql.Year} to a string.
         * The format used is {@link time.yearFormat},
         * which defaults to {@link time.DEFAULT_YEAR_FORMAT}
         *
         * @example
         *
         * var date = new Date(2004, 1, 1, 1, 1, 1),
         *     year = new sql.Year(2004);
         * moose.yearToString(date); //=> '2004'
         * moose.yearToString(year); //=> '2004'
         * moose.yearFormat = "yy";
         * moose.yearToString(date); //=> '04'
         * moose.yearToString(year); //=> '04'
         *
         *
         * @param {Date\sql.Year} dt the year to covert to to a string.
         *
         * @returns {String} the date string.
         */
        yearToString:function (dt) {
            return date.format(comb.isInstanceOf(dt, SQL.Year) ? dt.date : dt, this.yearFormat);
        },

        /**
         * Converts a {@link sql.Time} to a string.
         * The format used is {@link time.timeFormat},
         * which defaults to {@link time.DEFAULT_TIME_FORMAT}
         *
         * @example
         *
         * var date =  new Date(null, null, null, 13, 12, 12),
         *     time = new sql.Time(13,12,12);
         * moose.timeToString(date); //=> '13:12:12'
         * moose.timeToString(time); //=> '13:12:12'
         * moose.timeFormat = "hh:mm:ss";
         * moose.timeToString(date); //=> '01:12:12'
         * moose.timeToString(time); //=> '01:12:12'
         *
         * @param {Date\sql.Time} dt the time to covert to to a string.
         *
         * @returns {String} the date string.
         */
        timeToString:function (dt) {
            return date.format(comb.isInstanceOf(dt, SQL.Time) ? dt.date : dt, this.timeFormat);
        },

        /**
         * Converts a @link{sql.DateTime} to a string.
         * The format used is {@link time.dateTimeFormat},
         * which defaults to {@link time.DEFAULT_DATETIME_FORMAT}
         *
         * @example
         *
         * var date = new Date(2004, 1, 1, 12, 12, 12),
         * dateTime = new sql.DateTime(2004, 1, 1, 12, 12, 12),
         * offset = "-0600";
         * moose.dateTimeToString(date); //=> '2004-02-01 12:12:12'
         * moose.dateTimeToString(dateTime); //=> '2004-02-01 12:12:12'
         *
         * moose.dateTimeFormat = moose.DATETIME_TWO_YEAR_FORMAT;
         * moose.dateTimeToString(date); //=> '04-02-01 12:12:12'
         * moose.dateTimeToString(dateTime); //=> '04-02-01 12:12:12'
         *
         * moose.dateTimeFormat = moose.DATETIME_FORMAT_TZ;
         * moose.dateTimeToString(date); //=> '2004-02-01 12:12:12-0600'
         * moose.dateTimeToString(dateTime); //=> '2004-02-01 12:12:12-0600'
         *
         * moose.dateTimeFormat = moose.ISO_8601;
         * moose.dateTimeToString(date); //=> '2004-02-01T12:12:12-0600'
         * moose.dateTimeToString(dateTime); //=> '2004-02-01T12:12:12-0600'
         *
         * moose.dateTimeFormat = moose.ISO_8601_TWO_YEAR;
         * moose.dateTimeToString(date); //=> '04-02-01T12:12:12-0600'
         * moose.dateTimeToString(dateTime); //=> '04-02-01T12:12:12-0600'
         *
         * @param {Date\sql.DateTime} dt the datetime to covert to to a string.
         *
         * @returns {String} the date string.
         */
        dateTimeToString:function (dt) {
            return date.format(comb.isInstanceOf(dt, SQL.DateTime) ? dt.date : dt, this.dateTimeFormat);
        },


        /**
         * Converts a {@link sql.TimeStamp} to a string.
         * The format used is {@link time.timeStampFormat},
         * which defaults to {@link time.DEFAULT_TIMESTAMP_FORMAT}
         *
         * @example
         *
         * var date = new Date(2004, 1, 1, 12, 12, 12),
         * dateTime = new sql.TimeStamp(2004, 1, 1, 12, 12, 12),
         * offset = "-0600";
         * moose.timeStampToString(date); //=> '2004-02-01 12:12:12'
         * moose.timeStampToString(dateTime); //=> '2004-02-01 12:12:12'
         *
         * moose.timeStampFormat = moose.TIMESTAMP_TWO_YEAR_FORMAT;
         * moose.timeStampToString(date); //=> '04-02-01 12:12:12'
         * moose.timeStampToString(dateTime); //=> '04-02-01 12:12:12'
         *
         * moose.timeStampFormat = moose.TIMESTAMP_FORMAT_TZ;
         * moose.timeStampToString(date); //=> '2004-02-01 12:12:12-0600'
         * moose.timeStampToString(dateTime); //=> '2004-02-01 12:12:12-0600'
         *
         * moose.timeStamp = moose.ISO_8601;
         * moose.timeStampToString(date); //=> '2004-02-01T12:12:12-0600'
         * moose.timeStampToString(dateTime); //=> '2004-02-01T12:12:12-0600'
         *
         * moose.timeStamp = moose.ISO_8601_TWO_YEAR;
         * moose.timeStampToString(date); //=> '04-02-01T12:12:12-0600'
         * moose.timeStampToString(dateTime); //=> '04-02-01T12:12:12-0600'
         *
         *
         * @param {Date\sql.TimeStamp} dt the timestamp to convert to to a string.
         *
         * @returns {String} the date string.
         */
        timeStampToString:function (dt) {
            return date.format(comb.isInstanceOf(dt, SQL.TimeStamp) ? dt.date : dt, this.timeStampFormat);
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
         * moose.dateToString(year); //=> '2004'
         * moose.yearFormat = "yy";
         * moose.dateToString(year); //=> '04'
         * moose.yearFormat = moose.DEFAULT_YEAR_FORMAT;
         * moose.dateToString(year); //=> '2004'
         *
         * //convert times
         * moose.dateToString(time); //=> '12:12:12'
         *
         * //convert dates
         * moose.dateToString(date); //=> '2004-02-01'
         * moose.dateFormat = moose.TWO_YEAR_DATE_FORMAT;
         * moose.dateToString(date); //=> '04-02-01'
         * moose.dateFormat = moose.DEFAULT_DATE_FORMAT;
         * moose.dateToString(date); //=> '2004-02-01'

         * //convert dateTime
         * moose.dateToString(dateTime); //=> '2004-02-01 12:12:12'
         * moose.dateTimeFormat = moose.DATETIME_TWO_YEAR_FORMAT;
         * moose.dateToString(dateTime); //=> '04-02-01 12:12:12'
         * moose.dateTimeFormat = moose.DATETIME_FORMAT_TZ;
         * moose.dateToString(dateTime); //=> '2004-02-01 12:12:12-0600'
         * moose.dateTimeFormat = moose.ISO_8601;
         * moose.dateToString(dateTime); //=> '2004-02-01T12:12:12-0600'
         * moose.dateTimeFormat = moose.ISO_8601_TWO_YEAR;
         * moose.dateToString(dateTime); //=> '04-02-01T12:12:12-0600'
         * moose.dateTimeFormat = moose.DEFAULT_DATETIME_FORMAT;
         * moose.dateToString(dateTime); //=> '2004-02-01 12:12:12'

         * //convert timestamps
         * moose.dateToString(timeStamp); //=> '2004-02-01 12:12:12'
         * moose.timeStampFormat = moose.TIMESTAMP_TWO_YEAR_FORMAT;
         * moose.dateToString(timeStamp); //=> '04-02-01 12:12:12'
         * moose.timeStampFormat = moose.TIMESTAMP_FORMAT_TZ;
         * moose.dateToString(timeStamp); //=> '2004-02-01 12:12:12-0600'
         * moose.timeStampFormat = moose.ISO_8601;
         * moose.dateToString(timeStamp); //=> '2004-02-01T12:12:12-0600'
         * moose.timeStampFormat = moose.ISO_8601_TWO_YEAR;
         * moose.dateToString(timeStamp); //=> '04-02-01T12:12:12-0600'
         * moose.timeStampFormat = moose.DEFAULT_TIMESTAMP_FORMAT;
         * moose.dateToString(timeStamp); //=> '2004-02-01 12:12:12'
         *
         * @param {Date\sql.Time|sql.Year|sql.DateTime|sql.TimeStamp} dt the date to covert to to a string.
         *
         * @returns {String} the date string.
         */
        dateToString:function (dt) {
            var ret = "";
            if (comb.isInstanceOf(dt, SQL.Time)) {
                ret = this.timeToString(dt);
            } else if (comb.isInstanceOf(dt, SQL.Year)) {
                ret = this.yearToString(dt);
            } else if (comb.isInstanceOf(dt, SQL.DateTime)) {
                ret = this.dateTimeToString(dt);
            } else if (comb.isInstanceOf(dt, SQL.TimeStamp)) {
                ret = this.timeStampToString(dt);
            } else if (comb.isDate(dt)) {
                ret = date.format(dt, this.dateFormat);
            }
            return ret;
        },

        /**
         * Converts a year date string to a {@link sql.Year}
         * 
         * @example
         * 
         * var year = new sql.Year(2004);
         * moose.stringToYear("2004"); //=> year
         * moose.yearFormat = "yy";
         * moose.stringToYear("04"); //=> year
         * 
         * @param {String} dt the string to covert to a {@link sql.Year}
         * @param {String} [format=time.yearFormat] the format to use when converting the date.
         *         
         * @throws {MooseError} thrown if the conversion fails.
         * @return {sql.Year} the {@link sql.Year}
         */
        stringToYear:function (dt, format) {
            var ret = date.parse(dt, format || this.yearFormat);
            if (!ret) {
                throw new MooseError("Unable to convert year: " + dt);
            }
            return new SQL.Year(ret);
        },

        /**
         * Converts a time date string to a {@link sql.Time}
         * 
         * @example
         * 
         * var time = new sql.Time(12,12,12);
         * moose.stringToTime("12:12:12"); //=> time
         * 
         * @param {String} dt the string to convert to a {@link sql.Time}
         * @param {String} [format=time.timeFormat] the format to use when converting the date.
         *
         * @throws {MooseError} thrown if the conversion fails.
         * @return {sql.Time} the {@link sql.Time}
         */
        stringToTime:function (dt, format) {
            var ret = date.parse(dt, format || this.timeFormat);
            if (!ret) {
                throw new MooseError("Unable to convert time: " + dt);
            }
            return new SQL.Time(ret);
        },

        /**
         * Converts a date string to a Date
         * 
         * @example
         * 
         * var date = new Date(2004, 1,1,0,0,0);
         * moose.stringToDate('2004-02-01'); //=> date
         *
         * moose.dateFormat = moose.TWO_YEAR_DATE_FORMAT;
         * moose.stringToDate('04-02-01'); //=> date
         * 
         * @param {String} dt the string to convert to a Date
         * @param {String} [format=time.dateFormat] the format to use when converting the date.
         *
         * @throws {MooseError} thrown if the conversion fails.
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
                throw new MooseError("Unable to convert date: " + dt);
            }
            return ret;
        },

        /**
         * Converts a datetime date string to a {@link sql.DateTime}
         * 
         * @example
         * 
         *  var dateTime = new sql.DateTime(2004, 1, 1, 12, 12, 12),
         * offset = getTimeZoneOffset();
         * moose.stringToDateTime('2004-02-01 12:12:12'); //=> dateTime
         *
         * moose.dateTimeFormat = moose.DATETIME_TWO_YEAR_FORMAT;
         * moose.stringToDateTime('04-02-01 12:12:12-0600'); //=> dateTime
         *
         * moose.dateTimeFormat = moose.DATETIME_FORMAT_TZ;
         * moose.stringToDateTime('2004-02-01 12:12:12-0600'); //=> dateTime
         *
         * moose.dateTimeFormat = moose.ISO_8601;
         * moose.stringToDateTime('2004-02-01T12:12:12-0600'); //=> dateTime
         *
         * moose.dateTimeFormat = moose.ISO_8601_TWO_YEAR;
         * moose.stringToDateTime('04-02-01T12:12:12-0600'); //=> dateTime
         *
         * @param {String} dt the string to convert to a {@link sql.DateTime}
         * @param {String} [format=time.dateTimeFormat] the format to use when converting the date.
         *
         * @throws {MooseError} thrown if the conversion fails.
         * @return {sql.DateTime} the {@link sql.DateTime}
         */
        stringToDateTime:function (dt, fmt) {
            var useT = dt.indexOf("T") != -1;
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
                throw new MooseError("Unable to convert datetime: " + dt);
            }
            return new SQL.DateTime(ret);
        },

        /**
         * Converts a timestamp date string to a {@link sql.TimeStamp}
         * 
         * @example
         * 
         *  var timeStamp = new sql.TimeStamp(2004, 1, 1, 12, 12, 12);
         * moose.stringToTimeStamp('2004-02-01 12:12:12'); //=> timeStamp
         *
         * moose.timeStampFormat = moose.TIMESTAMP_TWO_YEAR_FORMAT;
         * moose.stringToTimeStamp('04-02-01 12:12:12-0600'); //=> timeStamp
         *
         * moose.timeStampFormat = moose.TIMESTAMP_FORMAT_TZ;
         * moose.stringToTimeStamp('2004-02-01 12:12:12-0600'); //=> timeStamp
         *
         * moose.timeStampFormat = moose.ISO_8601;
         * moose.stringToTimeStamp('2004-02-01T12:12:12-0600'); //=> timeStamp
         *
         * moose.timeStampFormat = moose.ISO_8601_TWO_YEAR;
         * moose.stringToTimeStamp('04-02-01T12:12:12-0600'); //=> timeStamp
         *
         * @param {String} dt the string to convert to a {@link sql.TimeStamp}
         * @param {String} [format=time.timeStampFormat] the format to use when converting the date.
         *
         * @throws {MooseError} thrown if the conversion fails.
         * @return {sql.TimeStamp} the {@link sql.TimeStamp}
         */
        stringToTimeStamp:function (dt, fmt) {
            var useT = dt.indexOf("T") != -1;
            //test if there is a T in the string so we can try to properly convert it
            var format = fmt ? fmt : useT ? this.ISO_8601 : this.DEFAULT_TIME_FORMAT;
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
                throw new MooseError("Unable to convert timestamp: " + dt);
            }
            return new SQL.TimeStamp(ret);
        },


        /**@ignore*/
        getters:{
            /**@ignore*/

            /**@ignore*/
            dateFormat:function (format) {
                return comb.isUndefined(this.__dateFormat) ? this.DEFAULT_DATE_FORMAT : this.__dateFormat;
            },
            /**@ignore*/
            yearFormat:function (format) {
                return comb.isUndefined(this.__yearFormat) ? this.DEFAULT_YEAR_FORMAT : this.__yearFormat;
            },
            /**@ignore*/
            timeFormat:function (format) {
                return comb.isUndefined(this.__timeFormat) ? this.DEFAULT_TIME_FORMAT : this.__timeFormat;
            },
            /**@ignore*/
            timeStampFormat:function (format) {
                return comb.isUndefined(this.__timeStampFormat) ? this.DEFAULT_TIMESTAMP_FORMAT : this.__timeStampFormat;
            },
            /**@ignore*/
            dateTimeFormat:function (format) {
                return comb.isUndefined(this.__dateTimeFormat) ? this.DEFAULT_DATETIME_FORMAT : this.__dateTimeFormat;
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