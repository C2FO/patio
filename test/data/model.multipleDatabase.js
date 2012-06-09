var patio = require("index"),
    comb = require("comb");
var DB1, DB2;

exports.loadModels = function() {
    patio.resetIdentifierMethods();
    DB1 = patio.connect("mysql://test:testpass@localhost:3306/sandbox");
    DB2 = patio.connect("mysql://test:testpass@localhost:3306/sandbox2");
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
            this.gender("enum", {elements : ["M", "F"]});
            this.street("string", {length : 50, allowNull : false});
            this.city("string", {length : 20, allowNull : false});
        });
        patio.addModel(db1.from("employee"), {
            static : {
                //class methods
                findByGender : function(gender, callback, errback) {
                    return this.filter({gender : gender}).all();
                }
            }
        });
        patio.addModel(db2.from("employee"), {
            static : {
                //class methods
                findByGender : function(gender, callback, errback) {
                    return this.filter({gender : gender}).all();
                }
            }
        });
        patio.syncModels();
        return [DB1, DB2];
    });
};

exports.dropModels = function () {
    return comb.executeInOrder(patio, DB1, DB2, function (patio, db1, db2) {
        db1.forceDropTable("employee");
        db2.forceDropTable("employee");
        patio.disconnect();
        patio.identifierInputMethod = null;
        patio.identifierOutputMethod = null;
    });
};