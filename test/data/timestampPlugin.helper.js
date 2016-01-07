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


function createSchemaAndSync(useAt) {
    return createTables(useAt).chain(comb.hitch(patio, "syncModels"));
}

function dropModels() {
    return dropTableAndDisconnect();
}

function dropTableAndDisconnect() {
    return DB.forceDropTable("employee")
        .chain(function () {
            return patio.disconnect();
        })
        .chain(function () {
            patio.resetIdentifierMethods();
        });
}

function createTables(useAt) {
    useAt = comb.isBoolean(useAt) ? useAt : false;
    patio.resetIdentifierMethods();

    DB = patio.connect(config.DB_URI + "/sandbox");

    return DB.forceDropTable(["employee"])
        .chain(function () {
            return DB.createTable("employee", function () {
                this.primaryKey("id");
                this.firstname("string", {size: 20, allowNull: false});
                this.lastname("string", {size: 20, allowNull: false});
                this.midinitial("char", {size: 1});
                this.position("integer");
                this.gender("char", {size: 1});
                this.street("string", {size: 50, allowNull: false});
                this.city("string", {size: 20, allowNull: false});
                this[useAt ? "updatedAt" : "updated"]("datetime");
                this[useAt ? "createdAt" : "created"]("datetime");
            });
        });
}