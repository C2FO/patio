var patio = require("index"),
    config = require("../test.config.js"),
    comb = require("comb-proxy");

var DB;
var createTables = function (underscore) {
    underscore = !!underscore;
    patio.resetIdentifierMethods();
    if (underscore) {
        patio.camelize = underscore;
    }
    return patio.connectAndExecute(config.DB_URI + "/sandbox",
        function (db) {
            db.forceDropTable(["employee", "company"]);
            db.createTable("company", function (table) {
                this.primaryKey("id");
                this[underscore ? "company_name" : "companyName"]("string", {size: 20, allowNull: false});
            });
            db.createTable("employee", function () {
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
        }).addCallback(function (db) {
            DB = db;
        });
};


var dropTableAndDisconnect = function () {
    return comb.executeInOrder(patio, DB, function (patio, db) {
        db.dropTable(["employee", "company"]);
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