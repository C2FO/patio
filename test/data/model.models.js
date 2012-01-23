var patio = require("index"),
    comb = require("comb");

var DB;
exports.loadModels = function() {
    var ret = new comb.Promise();
    DB = patio.connect("mysql://test:testpass@localhost:3306/test");
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
        return patio.addModel("employee", {
            static : {
                //class methods
                findByGender : function(gender, callback, errback) {
                    return this.filter({gender : gender}).all();
                }
            }
        });
    });
};


exports.dropModels = function() {
    return patio.disconnect();
};