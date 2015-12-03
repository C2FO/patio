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


function dropModels() {
    return dropTableAndDisconnect();
}

function createTables(underscore) {
    underscore = underscore === true;

    if (underscore) {
        patio.camelize = underscore;
    } else {
        patio.resetIdentifierMethods();
    }

    DB = patio.connect(config.DB_URI + "/sandbox");
    return DB.forceDropTable(["employee", "company"])
        .chain(function () {
            return DB.createTable("company", function () {
                this.primaryKey("id");
                this[underscore ? "company_name" : "companyName"]("string", {size: 20, allowNull: false});
            });
        })
        .chain(function () {
            return DB.createTable("employee", function () {
                this.primaryKey("id");
                this[underscore ? "first_name" : "firstname"]("string", {size: 20, allowNull: false});
                this[underscore ? "last_name" : "lastname"]("string", {size: 20, allowNull: false});
                this[underscore ? "mid_initial" : "midInitial"]("char", {size: 1});
                this.position("integer");
                this.gender("char", {size: 1});
                this.street("string", {size: 50, allowNull: false});
                this.city("string", {size: 20, allowNull: false});
                this.foreignKey(underscore ? "company_id" : "companyId", "company", {key: "id", onDelete: "cascade"});
            });
        });
}

function dropTableAndDisconnect() {
    return DB.dropTable(["employee", "company"])
        .chain(function () {
            return patio.disconnect();
        })
        .chain(function () {
            patio.resetIdentifierMethods();
        });
}
