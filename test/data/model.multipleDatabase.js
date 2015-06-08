var patio = require("../../lib"),
    config = require("../test.config.js"),
    comb = require("comb-proxy");
var DB1, DB2;

exports.loadModels = function() {
    patio.resetIdentifierMethods();

    return comb.executeInOrder(DB1, DB2, patio, function(db1, db2, patio) {
        db1.forceCreateTable("employee", function() {
            this.primaryKey("id");
            this.firstname("string", {length : 20, allowNull : false});
            this.lastname("string", {length : 20, allowNull : false});
            this.midinitial("char", {length : 1});
            this.position("integer");
            this.gender("enum", {elements : ["M", "F"]});
            this.street("string", {length : 50, allowNull : false});
            this.city("string", {length : 20, allowNull : false});
        });
        db2.forceCreateTable("employee", function() {
            this.primaryKey("id");
            this.firstname("string", {length : 20, allowNull : false});
            this.lastname("string", {length : 20, allowNull : false});
            this.midinitial("char", {length : 1});
            this.position("integer");
            this.gender("char", {size : 1});
            this.street("string", {length : 50, allowNull : false});
            this.city("string", {length : 20, allowNull : false});
        });

        patio.syncModels();
        return [DB1, DB2];
    });
};

exports.dropModels = function () {
    return  DB1.forceDropTable("employee")
        .chain(function(){
            return DB2.forceDropTable("employee")
        })
        .chain(function () {
            return patio.disconnect();
        })
        .chain(function () {
            patio.resetIdentifierMethods();
        });
};