var patio = require("../../lib"),
    config = require("../test.config.js"),
    comb = require("comb-proxy");

var DB;
var createTables = function (underscore) {
    underscore = underscore === true;
    if (underscore) {
        patio.camelize = underscore;
    } else {
        patio.resetIdentifierMethods();
    }
    return patio.connectAndExecute(config.DB_URI + "/sandbox",
        function (db) {
            db.forceDropTable(["works", "employee"]);
            db.createTable("employee", function () {
                this.primaryKey("id");
                this[underscore ? "first_name" : "firstname"]("string", {size: 20, allowNull: false});
                this[underscore ? "last_name" : "lastname"]("string", {size: 20, allowNull: false});
                this[underscore ? "mid_initial" : "midinitial"]("char", {size: 1});
                this.position("integer");
                this.gender("char", {size: 1});
                this.street("string", {size: 50, allowNull: false});
                this.city("string", {size: 20, allowNull: false});
            });
            db.createTable("works", function (table) {
                this.primaryKey("id");
                this[underscore ? "company_name" : "companyName"]("string", {size: 20, allowNull: false});
                this.salary("float", {size: [20, 8], allowNull: false})
                this.salaryTwo("float", {size: [20, 8], allowNull: false})
                this.salaryThree("float", {size: [20, 8], allowNull: false})
                this.salaryInner("float", {size: [20, 8], allowNull: false})
                this.foreignKey(underscore ? "employee_id" : "employeeId", "employee", {key: "id"});
            });
        }).addCallback(function (db) {
            DB = db;
        });
};


var dropTableAndDisconnect = function () {
    return comb.executeInOrder(patio, DB, function (patio, db) {
        db.dropTable(["works", "employee"]);
        patio.disconnect();
        patio.resetIdentifierMethods();
    });
};

exports.createSchemaAndSync = function (underscore) {
    return createTables(underscore).chain(comb.hitch(patio, "syncModels"));
};


exports.dropModels = function () {
    return dropTableAndDisconnect();
};