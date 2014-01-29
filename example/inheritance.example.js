var patio = require("../index"),
    comb = require("comb"),
    models = require("./models/inheritance"),
    Employee = models.Employee,
    Staff = models.Staff,
    Manager = models.Manager,
    Executive = models.Executive;

var DB = patio.connect("mysql://test:testpass@localhost:3306/sandbox");
patio.configureLogging();
patio.LOGGER.level = "error";

var createTables = function () {
    return comb.serial([
        function () {
            return DB.forceDropTable(["staff", "executive", "manager", "employee"]);
        },
        function () {
            return DB.createTable("employee", function () {
                this.primaryKey("id");
                this.name(String);
                this.kind(String);
            });
        },
        function () {
            return DB.createTable("manager", function () {
                this.foreignKey("id", "employee", {key: "id"});
                this.numStaff("integer");
            });
        },
        function () {
            return DB.createTable("executive", function () {
                this.foreignKey("id", "manager", {key: "id"});
                this.numManagers("integer");
            });
        },
        function () {
            return DB.createTable("staff", function () {
                this.foreignKey("id", "employee", {key: "id"});
                this.foreignKey("managerId", "manager", {key: "id"});
            });
        }
    ]);
};


var dropTableAndDisconnect = function () {
    return DB.forceDropTable(["staff", "executive", "manager", "employee"])
        .chain(function () {
            return patio.disconnect();
        });
};

var dropTableAndDisconnectErr = function (err) {
    patio.logError(err);
    return dropTableAndDisconnect();
};

createTables().chain(function () {
    patio.syncModels().chain(function () {
        comb.when(
                new Employee({name: "Bob"}).save(),
                new Staff({name: "Greg"}).save(),
                new Manager({name: "Jane"}).save(),
                new Executive({name: "Sue"}).save()
            ).chain(function () {
                Employee.forEach(
                    function (emp) {
                        console.log("Employees %d", emp.id);
                        console.log("\tname - ", emp.name);
                        console.log("\tkind - ", emp.kind);
                        console.log("\tinstanceof Employee? ", emp instanceof Employee);
                        console.log("\tinstanceof Staff? ", emp instanceof Staff);
                        console.log("\tinstanceof Manager? ", emp instanceof Manager);
                        console.log("\tinstanceof Executive? ", emp instanceof Executive);
                    }).chain(dropTableAndDisconnect, dropTableAndDisconnectErr);
            }, dropTableAndDisconnectErr);
    }, dropTableAndDisconnectErr);
}, dropTableAndDisconnectErr);

