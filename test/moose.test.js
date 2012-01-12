var vows = require('vows'),
    assert = require('assert'),
    moose = new require("index"),
    Database = require("database"),
    Dataset = require("dataset"),
    sql = require("sql").sql,
    Constant = sql.Constant,
    BooleanConstant = sql.BooleanConstant,
    NegativeBooleanConstant = sql.NegativeBooleanConstant,
    Identifier = sql.Identifier,
    SQLFunction = sql.SQLFunction,
    LiteralString = sql.LiteralString,
    comb = require("comb"),
    hitch = comb.hitch;
moose.DATABASES.length = 0;
moose.quoteIdentifiers = false;
var ret = (module.exports = exports = new comb.Promise());
var suite = vows.describe("Database");

var getTimeZoneOffset = function () {
    var offset = new Date().getTimezoneOffset();
    var tz = [
        (offset >= 0 ? "-" : "+"),
        comb.string.pad(Math.floor(Math.abs(offset) / 60), 2, "0"),
        comb.string.pad(Math.abs(offset) % 60, 2, "0")
    ];
    return tz.join("");
}

var DummyDataset = comb.define(moose.Dataset, {
    instance:{
        first:function () {
            var ret = new comb.Promise();
            if (this.__opts.from[0] == "a") {
                ret.errback();
            } else {
                ret.callback();
            }
            return ret;
        }
    }
});
var DummyDatabase = comb.define(moose.Database, {
    instance:{
        constructor:function () {
            this._super(arguments);
            this.sqls = [];
            this.identifierInputMethod = null;
            this.identifierOutputMethod = null;
        },

        createConnection:function (options) {
            this.connected = true;
            return new comb.Promise().callback({});
        },

        closeConnection:function (conn) {
            this.connected = false;
            return new comb.Promise().callback();
        },

        validate:function (conn) {
            return new Promise().callback(true);
        },

        execute:function (sql, opts) {
            this.pool.getConnection();
            var ret = new comb.Promise();
            this.sqls.push(sql);
            ret.callback();
            return ret;
        },

        executeError:function () {
            var ret = new comb.Promise();
            this.execute.apply(this, arguments).then(comb.hitch(ret, 'errback'), comb.hitch(ret, 'errback'));
            return ret;
        },

        reset:function () {
            this.sqls = [];
        },

        transaction:function (opts, cb) {
            var ret = new comb.Promise();
            cb();
            ret.callback();
            return ret;
        },

        getters:{
            dataset:function () {
                return new DummyDataset(this);
            }
        }
    },

    static:{
        init:function () {
            this.setAdapterType("dummydb");
        }
    }
});

suite.addBatch({
    "Moose":{

        topic:moose,

        "should have constants":function (moose) {
            assert.deepEqual(moose.CURRENT_DATE, new Constant("CURRENT_DATE"));
            assert.deepEqual(moose.CURRENT_TIME, new Constant("CURRENT_TIME"));
            assert.deepEqual(moose.CURRENT_TIMESTAMP, new Constant("CURRENT_TIMESTAMP"));
            assert.deepEqual(moose.SQLTRUE, new BooleanConstant(1));
            assert.deepEqual(moose.TRUE, new BooleanConstant(1));
            assert.deepEqual(moose.SQLFALSE, new BooleanConstant(0));
            assert.deepEqual(moose.FALSE, new BooleanConstant(0));
            assert.deepEqual(moose.NULL, new BooleanConstant(null));
            assert.deepEqual(moose.NOTNULL, new NegativeBooleanConstant(null));
            assert.equal(moose.identifierInputMethod, null);
            assert.equal(moose.identifierOutputMethod, null);
            assert.equal(moose.quoteIdentifiers, false);
        },

        "should connect to a database ":function (moose) {
            var DB1 = moose.connect("dummyDB://test:testpass@localhost/dummySchema");
            assert.instanceOf(DB1, DummyDatabase);
            assert.strictEqual(DB1, moose.defaultDatabase);
            var DB2 = moose.createConnection("dummyDB://test:testpass@localhost/dummySchema");
            assert.instanceOf(DB2, DummyDatabase);
            var DB3;
            moose.connectAndExecute("dummyDB://test:testpass@localhost/dummySchema",
                function (db) {
                    db.dropTable("test");
                    db.createTable("test", function () {
                        this.primaryKey("id");
                        this.name(String);
                        this.age(Number);
                    });
                }).then(function (db) {
                    DB3 = db;
                    assert.instanceOf(db, DummyDatabase);
                    assert.isTrue(db.connected);
                    assert.deepEqual(db.sqls, [ 'DROP TABLE test', 'CREATE TABLE test (id integer PRIMARY KEY AUTOINCREMENT, name varchar(255), age numeric)' ]);
                });
            assert.deepEqual(moose.DATABASES, [DB1, DB2, DB3]);
        },

        "should disconnect from a db":function (moose) {
            var DB = moose.connect("dummyDB://test:testpass@localhost/dummySchema");
            DB.createTable("test", function () {
                this.primaryKey("id");
                this.name(String);
                this.age(Number);
            });
            assert.isTrue(DB.connected);
            moose.disconnect();
            assert.isFalse(DB.connected);
        },

        "should expose core classes":function () {
            assert.strictEqual(moose.Dataset, Dataset);
            assert.strictEqual(moose.Database, Database);
            assert.strictEqual(moose.SQL, sql);
            assert.strictEqual(moose.sql, sql);
        },

        "should format years ":function (moose) {
            var date = new Date(2004, 1, 1, 1, 1, 1), year = new sql.Year(2004);
            assert.equal(moose.yearToString(date), '2004');
            assert.equal(moose.yearToString(year), '2004');
            moose.yearFormat = "yy";
            assert.equal(moose.yearToString(date), '04');
            assert.equal(moose.yearToString(year), '04');
            moose.yearFormat = moose.DEFAULT_YEAR_FORMAT;
        },

        "should format times ":function (moose) {
            var date =  new Date(null, null, null, 13, 12, 12), time = new sql.Time(13,12,12);
            assert.equal(moose.timeToString(date), '13:12:12');
            assert.equal(moose.timeToString(time), '13:12:12');
            moose.timeFormat = "hh:mm:ss";
            assert.equal(moose.timeToString(date), '01:12:12');
            assert.equal(moose.timeToString(time), '01:12:12');
            moose.timeFormat = moose.DEFAULT_TIME_FORMAT;
        },

        "should format dates ":function (moose) {
            var date = new Date(2004, 1,1);
            assert.equal(moose.dateToString(date), '2004-02-01');
            moose.dateFormat = moose.TWO_YEAR_DATE_FORMAT;
            assert.equal(moose.dateToString(date), '04-02-01');
            moose.dateFormat = moose.DEFAULT_DATE_FORMAT;
        },

        "should format datetimes ":function (moose) {
            var date = new Date(2004, 1, 1, 12, 12, 12),
                dateTime = new sql.DateTime(2004, 1, 1, 12, 12, 12),
                offset = getTimeZoneOffset();
            assert.equal(moose.dateTimeToString(date), '2004-02-01 12:12:12');
            assert.equal(moose.dateTimeToString(dateTime), '2004-02-01 12:12:12');
            moose.dateTimeFormat = moose.DATETIME_TWO_YEAR_FORMAT;
            assert.equal(moose.dateTimeToString(date), '04-02-01 12:12:12');
            assert.equal(moose.dateTimeToString(dateTime), '04-02-01 12:12:12');
            moose.dateTimeFormat = moose.DATETIME_FORMAT_TZ;
            assert.equal(moose.dateTimeToString(date), '2004-02-01 12:12:12' + offset);
            assert.equal(moose.dateTimeToString(dateTime), '2004-02-01 12:12:12' + offset);
            moose.dateTimeFormat = moose.ISO_8601;
            assert.equal(moose.dateTimeToString(date), '2004-02-01T12:12:12' + offset);
            assert.equal(moose.dateTimeToString(dateTime), '2004-02-01T12:12:12' + offset);
            moose.dateTimeFormat = moose.ISO_8601_TWO_YEAR;
            assert.equal(moose.dateTimeToString(date), '04-02-01T12:12:12' + offset);
            assert.equal(moose.dateTimeToString(dateTime), '04-02-01T12:12:12' + offset);
            moose.dateTimeFormat = moose.DEFAULT_DATETIME_FORMAT;
            assert.equal(moose.dateTimeToString(date), '2004-02-01 12:12:12');
            assert.equal(moose.dateTimeToString(dateTime), '2004-02-01 12:12:12');


        },

        "should format timestamps ":function (moose) {
            var date = new Date(2004, 1, 1, 12, 12, 12),
                dateTime = new sql.TimeStamp(2004, 1, 1, 12, 12, 12),
                offset = getTimeZoneOffset();
            assert.equal(moose.timeStampToString(date), '2004-02-01 12:12:12');
            assert.equal(moose.timeStampToString(dateTime), '2004-02-01 12:12:12');
            moose.timeStampFormat = moose.TIMESTAMP_TWO_YEAR_FORMAT;
            assert.equal(moose.timeStampToString(date), '04-02-01 12:12:12');
            assert.equal(moose.timeStampToString(dateTime), '04-02-01 12:12:12');
            moose.timeStampFormat = moose.TIMESTAMP_FORMAT_TZ;
            assert.equal(moose.timeStampToString(date), '2004-02-01 12:12:12' + offset);
            assert.equal(moose.timeStampToString(dateTime), '2004-02-01 12:12:12' + offset);
            moose.timeStampFormat = moose.ISO_8601;
            assert.equal(moose.timeStampToString(date), '2004-02-01T12:12:12' + offset);
            assert.equal(moose.timeStampToString(dateTime), '2004-02-01T12:12:12' + offset);
            moose.timeStampFormat = moose.ISO_8601_TWO_YEAR;
            assert.equal(moose.timeStampToString(date), '04-02-01T12:12:12' + offset);
            assert.equal(moose.timeStampToString(dateTime), '04-02-01T12:12:12' + offset);
            moose.timeStampFormat = moose.DEFAULT_TIMESTAMP_FORMAT;
            assert.equal(moose.timeStampToString(date), '2004-02-01 12:12:12');
            assert.equal(moose.timeStampToString(dateTime), '2004-02-01 12:12:12');

        },

        "should format arbitrary dates ":function (moose) {
            var date = new Date(2004, 1, 1),
                timeStamp = new sql.TimeStamp(2004, 1, 1, 12, 12, 12),
                dateTime = new sql.DateTime(2004, 1, 1, 12, 12, 12),
                year = new sql.Year(2004),
                time = new sql.Time(12,12,12),
                offset = getTimeZoneOffset();

            //convert years
            assert.equal(moose.dateToString(year), '2004');
            moose.yearFormat = "yy";
            assert.equal(moose.dateToString(year), '04');
            moose.yearFormat = moose.DEFAULT_YEAR_FORMAT;
            assert.equal(moose.dateToString(year), '2004');


            //convert times
            assert.equal(moose.dateToString(time), '12:12:12');

            //convert dates
            assert.equal(moose.dateToString(date), '2004-02-01');
            moose.dateFormat = moose.TWO_YEAR_DATE_FORMAT;
            assert.equal(moose.dateToString(date), '04-02-01');
            moose.dateFormat = moose.DEFAULT_DATE_FORMAT;
            assert.equal(moose.dateToString(date), '2004-02-01');

            //convert dateTime
            assert.equal(moose.dateToString(dateTime), '2004-02-01 12:12:12');
            moose.dateTimeFormat = moose.DATETIME_TWO_YEAR_FORMAT;
            assert.equal(moose.dateToString(dateTime), '04-02-01 12:12:12');
            moose.dateTimeFormat = moose.DATETIME_FORMAT_TZ;
            assert.equal(moose.dateToString(dateTime), '2004-02-01 12:12:12' + offset);
            moose.dateTimeFormat = moose.ISO_8601;
            assert.equal(moose.dateToString(dateTime), '2004-02-01T12:12:12' + offset);
            moose.dateTimeFormat = moose.ISO_8601_TWO_YEAR;
            assert.equal(moose.dateToString(dateTime), '04-02-01T12:12:12' + offset);
            moose.dateTimeFormat = moose.DEFAULT_DATETIME_FORMAT;
            assert.equal(moose.dateToString(dateTime), '2004-02-01 12:12:12');

            //convert timestamps
            assert.equal(moose.dateToString(timeStamp), '2004-02-01 12:12:12');
            moose.timeStampFormat = moose.TIMESTAMP_TWO_YEAR_FORMAT;
            assert.equal(moose.dateToString(timeStamp), '04-02-01 12:12:12');
            moose.timeStampFormat = moose.TIMESTAMP_FORMAT_TZ;
            assert.equal(moose.dateToString(timeStamp), '2004-02-01 12:12:12' + offset);
            moose.timeStampFormat = moose.ISO_8601;
            assert.equal(moose.dateToString(timeStamp), '2004-02-01T12:12:12' + offset);
            moose.timeStampFormat = moose.ISO_8601_TWO_YEAR;
            assert.equal(moose.dateToString(timeStamp), '04-02-01T12:12:12' + offset);
            moose.timeStampFormat = moose.DEFAULT_TIMESTAMP_FORMAT;
            assert.equal(moose.dateToString(timeStamp), '2004-02-01 12:12:12');
        },

        "should convert years" : function(moose){
            var year = new sql.Year(2004);
            assert.deepEqual(moose.stringToYear("2004"), year);
            moose.yearFormat = "yy";
            assert.deepEqual(moose.stringToYear("04"), year);
            moose.yearFormat = moose.DEFAULT_YEAR_FORMAT;
            assert.throws(comb.hitch(moose, "stringToYear", "aaaa"));
        },

        "should convert times" : function(moose){
            var time = new sql.Time(12,12,12);
            assert.deepEqual(moose.stringToTime("12:12:12"), time);
            assert.throws(comb.hitch(moose, "stringToTime", "9999:9999:9999"));
        },

        "should convert dates" : function(moose){
            var date = new Date(2004, 1,1,0,0,0);
            assert.deepEqual(moose.stringToDate('2004-02-01'), date);
            moose.dateFormat = moose.TWO_YEAR_DATE_FORMAT;
            assert.deepEqual(moose.stringToDate('04-02-01'), date);
            moose.dateFormat = moose.DEFAULT_DATE_FORMAT;
            assert.throws(comb.hitch(moose, "stringToDate", "2004-12--2"));
            assert.throws(comb.hitch(moose, "stringToDate", "a"));
            assert.throws(comb.hitch(moose, "stringToDate", "2004-25-2"));
        },

        "should convert dateTimes" : function(moose){
                var dateTime = new sql.DateTime(2004, 1, 1, 12, 12, 12),
                offset = getTimeZoneOffset();
            assert.deepEqual(moose.stringToDateTime('2004-02-01 12:12:12'), dateTime);
            moose.dateTimeFormat = moose.DATETIME_TWO_YEAR_FORMAT;
            assert.deepEqual(moose.stringToDateTime('04-02-01 12:12:12'  + offset), dateTime);
            moose.dateTimeFormat = moose.DATETIME_FORMAT_TZ;
            assert.deepEqual(moose.stringToDateTime('2004-02-01 12:12:12' + offset), dateTime);
            moose.dateTimeFormat = moose.ISO_8601;
            assert.deepEqual(moose.stringToDateTime('2004-02-01T12:12:12' + offset), dateTime);
            moose.dateTimeFormat = moose.ISO_8601_TWO_YEAR;
            assert.deepEqual(moose.stringToDateTime('04-02-01T12:12:12' + offset), dateTime);
            moose.dateTimeFormat = moose.DEFAULT_DATETIME_FORMAT;
            assert.deepEqual(moose.stringToDateTime('2004-02-01 12:12:12'), dateTime);
            assert.throws(comb.hitch(moose, "stringToDateTime", "2004-12--2"));
            assert.throws(comb.hitch(moose, "stringToDateTime", "a"));
            assert.throws(comb.hitch(moose, "stringToDateTime", "2004-25-2"));
            assert.throws(comb.hitch(moose, "stringToDateTime", "2004-25-2THHMM"));
        },

        "should convert timestamps" : function(moose){
            var dateTime = new sql.TimeStamp(2004, 1, 1, 12, 12, 12),
                offset = getTimeZoneOffset();
            assert.deepEqual(moose.stringToTimeStamp('2004-02-01 12:12:12'), dateTime);
            moose.timeStampFormat = moose.TIMESTAMP_TWO_YEAR_FORMAT;
            assert.deepEqual(moose.stringToTimeStamp('04-02-01 12:12:12'  + offset), dateTime);
            moose.timeStampFormat = moose.TIMESTAMP_FORMAT_TZ;
            assert.deepEqual(moose.stringToTimeStamp('2004-02-01 12:12:12' + offset), dateTime);
            moose.timeStampFormat = moose.ISO_8601;
            assert.deepEqual(moose.stringToTimeStamp('2004-02-01T12:12:12' + offset), dateTime);
            moose.timeStampFormat = moose.ISO_8601_TWO_YEAR;
            assert.deepEqual(moose.stringToTimeStamp('04-02-01T12:12:12' + offset), dateTime);
            moose.timeStampFormat = moose.DEFAULT_TIMESTAMP_FORMAT;
            assert.deepEqual(moose.stringToTimeStamp('2004-02-01 12:12:12'), dateTime);
            assert.throws(comb.hitch(moose, "stringToTimeStamp", "2004-12--2"));
            assert.throws(comb.hitch(moose, "stringToTimeStamp", "a"));
            assert.throws(comb.hitch(moose, "stringToTimeStamp", "2004-25-2"));
            assert.throws(comb.hitch(moose, "stringToTimeStamp", "2004-25-2THHMM"));
        }
    }
});

suite.run({reporter:require("vows").reporter.spec}, hitch(ret, "callback"));