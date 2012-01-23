var patio = require("index"),
    comb = require("comb");

var DB;
exports.loadModels = function() {
    var ret = new comb.Promise();
    DB = patio.connect("mysql://test:testpass@localhost:3306/test");
    return comb.executeInOrder(DB, patio, function(db, patio) {
        db.forceCreateTable("employee", function() {
            this.primaryKey("id");
            this.first_name("string", {size : 20, allowNull : false});
            this.last_name("string", {size : 20, allowNull : false});
            this.mid_initial("char", {size : 1});
            this.position("integer");
            this.gender("enum", {elements : ["M", "F"]});
            this.street("string", {size : 50, allowNull : false});
            this.city("string", {size : 20, allowNull : false});
        });
        return patio.addModel("employee", {
            static : {
                camelize : true,
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