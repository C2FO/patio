var patio = require("index"),
    comb = require("comb");

var DB;
var createTables = function (underscore) {
    underscore = !!underscore;
    patio.resetIdentifierMethods();
    DB = patio.connect("mysql://test:testpass@localhost:3306/sandbox");
    return DB.forceCreateTable("employee", function () {
        this.primaryKey("id");
        this[underscore ? "first_name" : "firstname"]("string", {size:20, allowNull:false});
        this[underscore ? "last_name" : "lastname"]("string", {size:20, allowNull:false});
        this[underscore ? "mid_initial" : "midinitial"]("char", {size:1});
        this.position("integer");
        this.gender("enum", {elements:["M", "F"]});
        this.street("string", {size:50, allowNull:false});
        this.city("string", {size:20, allowNull:false});
    });
};


var dropTableAndDisconnect = function () {
    return comb.executeInOrder(patio, DB, function (patio, db) {
        db.forceDropTable("employee");
        patio.disconnect();
        patio.resetIdentifierMethods();
    });
};

exports.createSchemaAndSync = function (underscore) {
    var ret = new comb.Promise();
    createTables(underscore).chain(comb.hitch(patio, "syncModels"), ret).then(ret);
    return ret;
};


exports.dropModels = function () {
    return dropTableAndDisconnect();
};