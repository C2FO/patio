var patio = require("../../index"), comb = require("comb");
patio.camelize = true;

var DB = patio.createConnection("mysql://test:testpass@localhost:3306/sandbox");

var disconnectErr = function(err){
    patio.logError(err);
    patio.disconnect();
};

var checkTables = function(){
    var ret = new comb.Promise();
    comb.when(DB.tableExists("class"),DB.tableExists("student"),DB.tableExists("classesStudents"), DB.from("schema_info").selectMap("version"), function(res){
        console.log("The class table %s exist!", res[0] ? "does" : "does not");
        console.log("The student table %s exist!", res[1] ? "does" : "does not");
        console.log("the classes_students table %s exist!", res[2] ? "does" : "does not");
        console.log("The schema version is currently : %d ", res[3]);
        ret.callback();
    }, comb.hitch(ret, "errback"));
    return ret;
}

var directory =  __dirname + "/integer_migration";
patio.migrate(DB, directory).then(function(){
    console.log("Done migrating up");
    checkTables().then(function(){
        patio.migrate(DB, directory, {target : -1}).then(function(){
            console.log("\nDone migrating down");
            checkTables().then(comb.hitch(patio, "disconnect"), disconnectErr);
        }, disconnectErr);
    }, disconnectErr);
}, disconnectErr);
