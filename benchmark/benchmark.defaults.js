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
            patio.addModel("patioEntry");
            patio.syncModels().then(ret);
        }, ret);
    return ret;
};

exports.disconnect = function(){
    patio.disconnect();
}

exports.disconnectErr = function(err){
    console.error(err);
    patio.disconnect();
}