/*jshint -W003*/
'use strict';

var patio = require("../../lib"),
    config = require("../test.config.js"),
    comb = require("comb"),
    DB;


module.exports = {
    createSchemaAndSync: createSchemaAndSync,
    dropModels: dropModels
};

function createSchemaAndSync(underscore) {
    return createTables(underscore).chain(function(){
        return patio.syncModels();
    });
}

function dropModels() {
    return dropTableAndDisconnect();
}

function dropTableAndDisconnect() {
    return DB.dropTable(["works", "employee"])
        .chain(function () {
            return patio.disconnect();
        })
        .chain(function () {
            patio.resetIdentifierMethods();
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

    return DB.forceDropTable(["works", "employee"])
        .chain(function () {
            return DB.createTable("employee", function () {
                this.primaryKey("id");
                this[underscore ? "first_name" : "firstname"]("string", {size: 20, allowNull: false});
                this[underscore ? "last_name" : "lastname"]("string", {size: 20, allowNull: false});
                this[underscore ? "mid_initial" : "midinitial"]("char", {size: 1});
                this.position("integer");
                this.gender("char", {size: 1});
                this.street("string", {size: 50, allowNull: false});
                this.city("string", {size: 20, allowNull: false});
            });
        }).chain(function () {
            return DB.createTable("works", function () {
                this.primaryKey("id");
                this[underscore ? "company_name" : "companyName"]("string", {size: 20, allowNull: false});
                this.salary("float", {size: [20, 8], allowNull: false});
                this.foreignKey(underscore ? "employee_id" : "employeeId", "employee", {key: "id"});
            });
        });
}