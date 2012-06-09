var patio = require("index"),
    comb = require("comb");

var DB;
exports.loadModels = function() {
    var ret = new comb.Promise();
    patio.resetIdentifierMethods();
    DB = patio.connect("mysql://test:testpass@localhost:3306/sandbox");
    return comb.executeInOrder(DB, patio, function(db, patio) {
        db.forceCreateTable("employee", function() {
            this.primaryKey("id");
            this.firstname("string", {size : 20, allowNull : false});
            this.lastname("string", {size : 20, allowNull : false});
            this.midinitial("char", {size : 1});
            this.position("integer");
            this.gender("enum", {elements : ["M", "F"]});
            this.street("string", {size : 50, allowNull : false});
            this.city("string", {size : 20, allowNull : false});
        });
        patio.addModel(DB.from("employee"), {
            static : {
                //class methods
                findByGender : function(gender, callback, errback) {
                    return this.filter({gender : gender}).all();
                }
            }
        });
        return patio.syncModels();
    });
};


exports.dropModels = function () {
    return comb.executeInOrder(patio, DB, function (patio, db) {
        db.forceDropTable("employee");
        patio.disconnect();
        patio.identifierInputMethod = null;
        patio.identifierOutputMethod = null;
    });
};