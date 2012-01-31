var comb = require("comb");

//Up function used to migrate up a version
exports.up = function(db){
    //create a new table
    var ret = new comb.Promise();
    db.renameTable("employees", "employeesOld");
    db.createTable("employees", function(table){
        this.primaryKey("id");
        this.firstName(String);
        this.lastName(String);
        this.hireDate(Date);
        this.middleInitial("char", {size:1});
    });
    db.from("employeesOld").map(function(employee){
            return comb.merge(employee, {hireDate:new Date()});
    }).then(function(employees){
            db.from("employees").multiInsert(employees)
                .then(comb.hitch(ret, "callback"), comb.hitch(ret, "errback"));
        }, comb.hitch(ret, "errback"));
    return ret;
};

//Down function used to migrate down version
exports.down = function(db){
    db.dropTable("employeesOld");
    db.alterTable("employees", function(){
        this.dropColumn("hireDate");
    });
};