var patio = require("../index"), comb = require("comb");
var DB;
exports.createTableAndModel = function (connect) {
    DB = patio.connect(connect);
    var ret = new comb.Promise();
    DB.forceCreateTable("patioEntry",
        function () {
            this.primaryKey("id");
            this.column("number", "integer");
            this.column("string", String);
        }).then(function () {
            patio.addModel("patioEntry").then(function (entry) {
                entry.useTransactions = false;
                entry.reloadOnSave = false;
                entry.reloadOnUpdate = false;
                entry.typecastOnAssignment = false;
                entry.typecastOnLoad = false;
                ret.callback(entry);
            });
        });
    return ret;
};

exports.disconnect = function(){
    patio.disconnect();
}

exports.disconnectErr = function(err){
    console.error(err);
    patio.disconnect();
}