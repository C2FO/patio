var patio = require("../../lib"),
    config = require("../test.config"),
    comb = require("comb-proxy");

var DB;
var createTables = function (underscore) {
    underscore = !!underscore;
    patio.resetIdentifierMethods();
    DB = patio.connect(config.DB_URI + "/sandbox");
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
};


var dropTableAndDisconnect = function () {
    return DB.forceDropTable("employee")
        .chain(function () {
            return patio.disconnect();
        })
        .chain(function () {
            patio.resetIdentifierMethods();
        });
};

exports.createSchemaAndSync = function (underscore) {
    return createTables(underscore).chain(comb.hitch(patio, "syncModels"));
};


exports.dropModels = function () {
    return dropTableAndDisconnect();
};