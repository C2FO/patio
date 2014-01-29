var patio = require("../index"), comb = require("comb");
var DB;
exports.createTableAndModel = function (connect) {
    DB = patio.connect(connect);
    return DB.forceCreateTable("patioEntry",
        function () {
            this.primaryKey("id");
            this.column("number", "integer");
            this.column("string", String);
        }).chain(function () {
            patio.addModel("patioEntry");
            return patio.syncModels();
        });
};

exports.disconnect = function () {
    return patio.disconnect();
}

exports.disconnectErr = function (err) {
    console.error(err);
    return patio.disconnect();
}