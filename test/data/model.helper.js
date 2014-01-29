var patio = require("index"),
    config = require("../test.config.js"),
    comb = require("comb-proxy");

var DB;
var createTables = function (underscore) {
    underscore = !!underscore;
    patio.resetIdentifierMethods();
    DB = patio.connect(config.DB_URI + "/sandbox");
    return DB.forceCreateTable("employee", function () {
        this.primaryKey("id");
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
};


var dropTableAndDisconnect = function () {
    return comb.executeInOrder(patio, DB, function (patio, db) {
        db.forceDropTable("employee");
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