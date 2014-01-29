var patio = require("index"),
    config = require("../test.config.js"),
    comb = require("comb-proxy");

var DB;
var createTables = function () {
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
};


var dropTableAndDisconnect = function () {
    return comb.executeInOrder(patio, DB, function (patio, db) {
        db.forceDropTable("validator");
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