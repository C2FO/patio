/*jshint -W003*/
"use strict";

var patio = require("../../lib"),
    config = require("../test.config.js"),
    comb = require("comb"),
    DB;

module.exports = {
    createSchemaAndSync: createSchemaAndSync,
    dropModels: dropModels
};

function createSchemaAndSync(underscore) {
    return createTables(underscore).chain(comb.hitch(patio, "syncModels"));
}

function dropModels() {
    return dropTableAndDisconnect();
}

function dropTableAndDisconnect() {
    return DB.forceDropTable("validator")
        .chain(function () {
            patio.disconnect();
        })
        .chain(function () {
            patio.resetIdentifierMethods();
        });
}

function createTables() {
    patio.resetIdentifierMethods();
    DB = patio.connect(config.DB_URI + "/sandbox");
    return DB.forceCreateTable("validator", function () {
        this.primaryKey("id");
        this.str(String);
        this.col1(String);
        this.col2(String);
        this.emailAddress(String);
        this.str2(String);
        this.macAddress(String);
        this.ipAddress(String);
        this.uuid(String);
        this.num(Number);
        this.num2(Number);
        this.date(Date);
    });
}
