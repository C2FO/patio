var it = require('it'),
    assert = require('assert'),
    patio = require("../../lib"),
    sql = patio.SQL,
    comb = require("comb"),
    config = require("../test.config.js"),
    when = comb.when,
    serial = comb.serial,
    format = comb.string.format,
    hitch = comb.hitch;

it.describe("patio.adapters.Redshift", function (it) {

    var PG_DB;

    var resetDb = function () {
        PG_DB.sqls = [];
    };

    it.beforeAll(function () {
        patio.resetIdentifierMethods();
        patio.camelize = false;
        patio.quoteIdentifiers = false;
        //patio.configureLogging();
        PG_DB = patio.connect(config.REDSHIFT_URI + "/sandbox");

        PG_DB["__defineGetter__"]("sqls", function () {
            return (comb.isArray(this.__sqls) ? this.__sqls : (this.__sqls = []));
        });

        PG_DB["__defineSetter__"]("sqls", function (sql) {
            this.__sqls = sql;
            return sql;
        });


        var origExecute = PG_DB.__logAndExecute;
        PG_DB.__logAndExecute = function (sql) {
            this.sqls.push(sql.trim());
            return when([]);
        };
    });

    it.afterEach(resetDb);

    it.describe("DDL", function (it) {

        it.should("use identity when using a primary key", function () {
            return PG_DB.createTable("testTable", function () {
                this.primaryKey("id");
            }).chain(function () {
                assert.deepEqual(PG_DB.sqls, ["CREATE TABLE testTable (id bigint identity(0, 1) primary key)"]);
            });
        });

        it.should("support distKey option on columns", function () {
            return PG_DB.createTable("testTable", function () {
                this.primaryKey("id");
                this.testCol(String, {distKey: true});
            }).chain(function () {
                assert.deepEqual(PG_DB.sqls, ["CREATE TABLE testTable (id bigint identity(0, 1) primary key, testCol text distkey)"]);
            });
        });

        it.should("support distkey option on columns", function () {
            return PG_DB.createTable("testTable", function () {
                this.primaryKey("id");
                this.testCol(String, {sortKey: true});
            }).chain(function () {
                assert.deepEqual(PG_DB.sqls, ["CREATE TABLE testTable (id bigint identity(0, 1) primary key, testCol text sortkey)"]);
            });
        });

        it.should("support distStyle option on create table", function () {
            return PG_DB.createTable("testTable", {distStyle: "all"}, function () {
                this.primaryKey("id");
                this.testCol(String, {sortKey: true});
            }).chain(function () {
                assert.deepEqual(PG_DB.sqls, ["CREATE TABLE testTable (id bigint identity(0, 1) primary key, testCol text sortkey) diststyle all"]);
            });
        });

        it.should("not allow using returning statements", function () {
            return PG_DB.from("test").returning("id").update({hello: "world"}).chain(function () {
                assert.deepEqual(PG_DB.sqls, ["UPDATE  test SET hello = 'world'"]);
            }, (err) => {
                console.log(err);
                throw err;
            });
        });
    });




    it.afterAll(function () {
        try {
            patio.resetIdentifierMethods();
            return patio.disconnect().chain(() => {
                console.log('diconnected');
            }, (err) => {
                console.log(err);
                throw err;
            });
        }catch(err) {
            console.log('got this caught');
            console.log(err);
        }
    });
});