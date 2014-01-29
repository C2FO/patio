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
            var PatioEntry = patio.addModel("patioEntry");
            PatioEntry.useTransactions = false;
            PatioEntry.reloadOnSave = false;
            PatioEntry.reloadOnUpdate = false;
            PatioEntry.typecastOnAssignment = false;
            PatioEntry.typecastOnLoad = false;
            return patio.syncModels();
        });
};

exports.disconnect = function () {
    patio.disconnect();
}

exports.disconnectErr = function (err) {
    console.error(err);
    patio.disconnect();
}