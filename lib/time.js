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

comb.define(null, {
    instance:{

        convertTwoDigitYears:true,

        databaseTimezone:null,

        applicationTimezone:null,

        typecastTimezone:null,

        DEFAULT_DATE_FORMAT:"yyyy-MM-dd",
        TWO_YEAR_DATE_FORMAT:"yy-MM-dd",
        DEFAULT_YEAR_FORMAT:"yyyy",
        DEFAULT_TIME_FORMAT:"HH:mm:ss",
        DEFAULT_TIMESTAMP_FORMAT:"yyyy-MM-dd HH:mm:ssZ",
        DEFAULT_DATETIME_FORMAT:"yyyy-MM-dd HH:mm:ssZ",

        __dateFormat:undefined,
        __yearFormat:undefined,
        __timeFormat:undefined,
        __timeStampFormat:undefined,
        __dateTimeFormat:undefined,

        yearToString:function (dt) {
            return date.format(comb.isInstanceOf(date, SQL.Year) ? dt.date : dt, this.yearFormat);
        },

        timeToString:function (dt) {
            return date.format(comb.isInstanceOf(date, SQL.Time) ? dt.date : dt, this.timeFormat);
        },

        dateTimeToString:function (dt) {
            return date.format(comb.isInstanceOf(date, SQL.DateTime) ? dt.date : dt, this.dateTimeFormat);
        },

        timeStampToString:function (dt) {
            return date.format(comb.isInstanceOf(dt, SQL.TimeStamp) ? dt.date : dt, this.timeStampFormat);
        },

        dateToString:function (date) {
            var ret = "";
            if (comb.isInstanceOf(date, SQL.Time)) {
                ret = this.timeToString(date);
            } else if (comb.isInstanceOf(date, SQL.Year)) {
                ret = this.yearToString(date);
            } else if (comb.isInstanceOf(date, SQL.DateTime)) {
                ret = this.dateTimeToString(date);
            } else if (comb.isInstanceOf(date, SQL.TimeStamp)) {
                ret = this.timeStampToString(date);
            } else if (comb.isDate(date)) {
                ret = this.dateTimeToString(date.date);
            }
            return ret;
        },

        stringToYear:function (dt, format) {
            var ret = date.parse(dt, format || this.yearFormat);
            if (!ret) {
                throw new MooseError("Unable to convert year: " + dt);
            }
            return new SQL.Year(ret);
        },


        stringToTime:function (dt, format) {
            var ret = date.parse(dt, format || this.timeFormat);
            if (!ret) {
                throw new MooseError("Unable to convert time: " + dt);
            }
            return new SQL.Time(ret);
        },

        stringToDate:function (dt, format) {
            var ret = date.parse(dt, format || this.dateFormat);
            if (!ret && this.convertTwoDigitYears) {
                ret = date.parse(dt, "yy-MM-dd");
            }
            if (!ret) {
                throw new MooseError("Unable to convert date: " + dt);
            }
            return ret;
        },

        stringToDateTime:function (dt, fmt) {
            var useT = dt.indexOf("T") != -1;
            var format = fmt ? fmt : useT ? "yyyy-MM-ddTHH:mm:ssZ" : "yyyy-MM-dd HH:mm:ss";
            var twoYearFormat = fmt ? fmt : useT ? "yy-MM-ddTHH:mm:ssZ" : "yy-MM-dd HH:mm:ss";
            var ret = date.parse(dt, format);
            !ret && (ret = date.parse(dt, format + "Z"));
            if (!ret && this.convertTwoDigitYears) {
                ret = date.parse(dt, twoYearFormat);
                !ret && (ret = date.parse(dt, twoYearFormat + "Z"));
            }
            if (!ret) {
                throw new MooseError("Unable to convert datetime: " + dt);
            }
            return new SQL.DateTime(ret);
        },

        stringToTimeStamp:function (dt, fmt) {
            var useT = dt.indexOf("T") != -1;
            var format = fmt ? fmt : useT ? "yyyy-MM-ddTHH:mm:ssZ" : "yyyy-MM-dd HH:mm:ss";
            var twoYearFormat = fmt ? fmt : useT ? "yy-MM-ddTHH:mm:ssZ" : "yy-MM-dd HH:mm:ss";
            var ret = date.parse(dt, format);
            !ret && (ret = date.parse(dt, format + "Z"));
            if (!ret && this.convertTwoDigitYears) {
                ret = date.parse(dt, twoYearFormat);
                !ret && (ret = date.parse(dt, twoYearFormat + "Z"));
            }
            if (!ret) {
                throw new MooseError("Unable to convert timestamp: " + dt);
            }
            return new SQL.TimeStamp(ret);
        },


        getters:{
            dateFormat:function (format) {
                return comb.isUndefined(this.__dateFormat) ? this.DEFAULT_DATE_FORMAT : this.__dateFormat;
            },
            yearFormat:function (format) {
                return comb.isUndefined(this.__yearFormat) ? this.DEFAULT_YEAR_FORMAT : this.__yearFormat;
            },
            timeFormat:function (format) {
                return comb.isUndefined(this.__timeFormat) ? this.DEFAULT_TIME_FORMAT : this.__timeFormat;
            },
            timeStampFormat:function (format) {
                return comb.isUndefined(this.__timeStampFormat) ? this.DEFAULT_TIMESTAMP_FORMAT : this.__timeStampFormat;
            },
            dateTimeFormat:function (format) {
                return comb.isUndefined(this.__dateTimeFormat) ? this.DEFAULT_DATETIME_FORMAT : this.__dateTimeFormat;
            }
        },

        setters:{
            defaultTimezone:function (tz) {
                this.databaseTimezone = tz;
                this.applicationTimezone = tz;
                this.typecastTimezone = tz;
            },

            dateFormat:function (format) {
                this.__dateFormat = format;
            },
            yearFormat:function (format) {
                this.__yearFormat = format;
            },
            timeFormat:function (format) {
                this.__timeFormat = format;
            },
            timeStampFormat:function (format) {
                this.__timeStampFormat = format;
            },
            dateTimeFormat:function (format) {
                this.__dateTimeFormat = format;
            }
        }
    }
}).as(module);