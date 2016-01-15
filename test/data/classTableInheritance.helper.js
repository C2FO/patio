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


function dropTableAndDisconnect() {
    return DB.forceDropTable(["staff", "executive", "manager", "employee"])
        .chain(function () {
            return patio.disconnect();
        })
        .chain(function () {
            patio.resetIdentifierMethods();
        });
}

function createSchemaAndSync(underscore) {
    return createTables(underscore).chain(comb.hitch(patio, "syncModels"));
}

function dropModels() {
    return dropTableAndDisconnect();
}

function createTables(underscore) {
    underscore = underscore === true;
    if (underscore) {
        patio.camelize = underscore;
    } else {
        patio.resetIdentifierMethods();
        patio.quoteIdentifiers = false;
    }
    DB = patio.connect(config.DB_URI + "/sandbox");

    return DB.forceDropTable(["staff", "executive", "manager", "employee"])
        .chain(function () {
            return DB.createTable("employee", function () {
                this.primaryKey("id");
                this.name(String);
                this.kind(String);
            });
        })
        .chain(function () {
            return DB.createTable("manager", function () {
                this.primaryKey("id");
                this.foreignKey(["id"], "employee", {key: "id"});
                this.numstaff("integer");
            });
        })
        .chain(function () {
            return DB.createTable("executive", function () {
                this.primaryKey("id");
                this.foreignKey(["id"], "manager", {key: "id"});
                this.nummanagers("integer");
            });
        })
        .chain(function () {
            return DB.createTable("staff", function () {
                this.primaryKey("id");
                this.foreignKey(["id"], "employee", {key: "id"});
                this.foreignKey("managerid", "manager", {key: "id"});
            });
        });
}