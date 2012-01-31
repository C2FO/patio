var patio = require("../../index"), comb = require("comb"), format = comb.string.format, assert = require("assert");
patio.camelize = true;

var DB = patio.createConnection("mysql://test:testpass@localhost:3306/sandbox");
new comb.logging.BasicConfigurator().configure();
comb.logging.Logger.getRootLogger().level = "info";
var disconnectErr = function(err){
    patio.logError(err);
    patio.disconnect();
};

var checkTables = function(){
    return DB.from("employees").forEach(
        function(employee){
            console.log(format("{id} {firstName} {middleInitial} {lastName}  was hired {[yyy-MM-dd]hireDate}", employee));
            return employee;
        }).then(function(employees){
            assert.equal(employees.length, 5);
        })
}

var migrate = function(){
    var directory = __dirname + "/promise_migration";
    patio.migrate(DB, directory).then(function(){
        console.log("Done migrating up");
        checkTables().then(function(){
            patio.migrate(DB, directory, {target:0}).then(function(){
                console.log("\nDone migrating down");
                migrate();
            }, disconnectErr);
        }, disconnectErr);
    }, disconnectErr);
};
migrate();
