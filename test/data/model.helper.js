/*jshint -W003*/
"use strict";

var patio = require("../../lib"),
    config = require("../test.config.js"),
    DB;

module.exports = {
    createSchemaAndSync: createSchemaAndSync,
    dropModels: dropModels
};


function createSchemaAndSync(underscore) {
    return createTables(underscore).chain(function () {
        return patio.syncModels();
    });
}

function createTables(underscore) {
    underscore = underscore === true;

    if (underscore) {
        patio.camelize = underscore;
    } else {
        patio.resetIdentifierMethods();
    }

    DB = patio.connect(config.DB_URI + "/sandbox");
    return DB.forceDropTable("employee")
        .chain(function () {
            return DB.forceCreateTable("employee", function () {
                this.primaryKey("id");
                config.DB_TYPE === "pg" && this[underscore ? "json_type" : "jsontype"]("json");
                config.DB_TYPE === "pg" && this[underscore ? "json_array" : "jsonarray"]("json");
                this[underscore ? "first_name" : "firstname"]("string", {size: 20, allowNull: false});
                this[underscore ? "last_name" : "lastname"]("string", {size: 20, allowNull: false});
                this[underscore ? "mid_initial" : "midinitial"]("char", {size: 1});
                this.position("integer");
                this.gender("char", {size: 1});
                this.street("string", {size: 50, allowNull: false});
                this.city("string", {size: 20, allowNull: false});
                this[underscore ? "buffer_type" : "buffertype"](Buffer);
                this[underscore ? "text_type" : "texttype"]("text");
                this[underscore ? "blob_type" : "blobtype"]("blob");
            });
        });
}

function dropModels() {
    return dropTableAndDisconnect();
}

function dropTableAndDisconnect() {
    return DB.dropTable("employee")
        .chain(function () {
            return patio.disconnect();
        })
        .chain(function () {
            patio.resetIdentifierMethods();
        });
}