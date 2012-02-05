var patio = require("index"),
    comb = require("comb");

var DB;
patio.configureLogging();
var createTables = function () {
    return patio.connectAndExecute("mysql://test:testpass@localhost:3306/sandbox",
        function (db) {
            db.forceDropTable(["staff", "executive", "manager", "employee"]);
            db.createTable("employee", function () {
                this.primaryKey("id")
                this.name(String);
                this.kind(String);
            });
            db.createTable("manager", function () {
                this.foreignKey("id", "employee", {key:"id"});
                this.numStaff("integer");
            });
            db.createTable("executive", function () {
                this.foreignKey("id", "manager", {key:"id"});
                this.numManagers("integer");
            });
            db.createTable("staff", function () {
                this.foreignKey("id", "employee", {key:"id"});
                this.foreignKey("managerId", "manager", {key:"id"});
            });
        }).addCallback(function (db) {
            DB = db;
        });
};


var dropTableAndDisconnect = function () {
    return comb.executeInOrder(patio, DB, function (patio, db) {
        db.forceDropTable(["staff", "executive", "manager", "employee"]);
        patio.disconnect();
    });
};

var dropTableAndDisconnectErr = function (err) {
    patio.logError(err);
    return comb.executeInOrder(patio, DB, function (patio, db) {
        db.forceDropTable(["staff", "executive", "manager", "employee"]);
        patio.disconnect();
    });
};

createTables().then(function () {
    patio.import(__dirname + "/models/inheritance").then(function () {
        var Employee = patio.getModel("employee"),
            Staff = patio.getModel("staff"),
            Manager = patio.getModel("manager"),
            Executive = patio.getModel("executive");

        comb.when(
            new Employee({name:"Bob"}).save(),
            new Staff({name:"Greg"}).save(),
            new Manager({name:"Jane"}).save(),
            new Executive({name:"Sue"}).save()
        ).then(function () {
                Employee.forEach(
                    function (emp) {
                        console.log("Employees %d", emp.id);
                        console.log("\tname - ", emp.name);
                        console.log("\tkind - ", emp.kind);
                        console.log("\tinstanceof Employee? ", emp instanceof Employee);
                        console.log("\tinstanceof Staff? ", emp instanceof Staff);
                        console.log("\tinstanceof Manager? ", emp instanceof Manager);
                        console.log("\tinstanceof Executive? ", emp instanceof Executive);
                    }).then(dropTableAndDisconnect, dropTableAndDisconnectErr);
            }, dropTableAndDisconnectErr);
    }, dropTableAndDisconnectErr);
}, dropTableAndDisconnectErr);

