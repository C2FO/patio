var patio = require("index"),
    config = require("../test.config.js"),
    comb = require("comb-proxy");

var DB;
var createTables = function (useAt) {
    useAt = comb.isBoolean(useAt) ? useAt : false;
    patio.resetIdentifierMethods();
    return patio.connectAndExecute(config.DB_URI + "/sandbox",
        function (db) {
            db.forceDropTable(["employee"]);
            db.createTable("employee", function () {
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
        }).chain(function (db) {
            DB = db;
        });
};


var dropTableAndDisconnect = function () {
    return comb.executeInOrder(patio, DB, function (patio, db) {
        db.forceDropTable("employee");
        patio.disconnect();
        patio.resetIdentifierMethods();
    });
};

exports.createSchemaAndSync = function (useAt) {
    return createTables(useAt).chain(comb.hitch(patio, "syncModels"));
};


exports.dropModels = function () {
    return dropTableAndDisconnect();
};