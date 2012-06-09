var patio = require("index"),
    comb = require("comb");

var DB;
var createTables = function (underscore) {
    underscore = underscore === true;
    if (underscore) {
        patio.camelize = underscore;
    } else {
        patio.resetIdentifierMethods();
    }
    return patio.connectAndExecute("mysql://test:testpass@localhost:3306/sandbox",
        function (db) {
            db.forceDropTable(["staff", "executive", "manager", "employee"]);
            db.createTable("employee", function () {
                this.primaryKey("id")
                this.name(String);
                this.kind(String);
            });
            db.createTable("manager", function () {
                this.foreignKey("id", "employee", {key:"id"});
                this.numStaff("integer");
            });
            db.createTable("executive", function () {
                this.foreignKey("id", "manager", {key:"id"});
                this.numManagers("integer");
            });
            db.createTable("staff", function () {
                this.foreignKey("id", "employee", {key:"id"});
                this.foreignKey("managerId", "manager", {key:"id"});
            });
        }).addCallback(function (db) {
            DB = db;
        });
};


var dropTableAndDisconnect = function () {
    return comb.executeInOrder(patio, DB, function (patio, db) {
        db.forceDropTable(["staff", "executive", "manager", "employee"]);
        patio.disconnect();
        patio.resetIdentifierMethods();
    });
};

exports.createSchemaAndSync = function (underscore) {
    var ret = new comb.Promise();
    createTables(underscore).chain(comb.hitch(patio, "syncModels"), ret).then(ret);
    return ret;
};


exports.dropModels = function () {
    return dropTableAndDisconnect();
};