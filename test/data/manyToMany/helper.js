var patio = require("index"),
    comb = require("comb");

var DB;
exports.createTables = function (underscore) {
    underscore = underscore === true;
    return patio.connectAndExecute("mysql://test:testpass@localhost:3306/test", function (db) {
            db.forceDropTable(["companiesEmployees", "companies_employees", "employee", "company"]);
            db.createTable("company", function (table) {
                this.primaryKey("id");
                this[underscore ? "company_name" : "companyName"]("string", {size:20, allowNull:false});
            });
            db.createTable("employee", function () {
                this.primaryKey("id");
                this[underscore ? "first_name" : "firstname"]("string", {size:20, allowNull:false});
                this[underscore ? "last_name" : "lastname"]("string", {size:20, allowNull:false});
                this[underscore ? "mid_initial" : "midinitial"]("char", {size:1});
                this.position("integer");
                this.gender("enum", {elements:["M", "F"]});
                this.street("string", {size:50, allowNull:false});
                this.city("string", {size:20, allowNull:false});
            });
            db.createTable(underscore ? "companies_employees" : "companiesEmployees", function () {
                this.foreignKey(underscore ? "company_id" : "companyId", "company", {key:"id", onDelete:"cascade"});
                this.foreignKey(underscore ? "employee_id" : "employeeId", "employee", {key:"id", onDelete:"cascade"});
            });
        }).addCallback(function (db) {
            DB = db;
        });
};


exports.dropTableAndDisconnect = function () {
    return comb.executeInOrder(patio, DB, function (patio, db) {
        db.forceDropTable(["companiesEmployees", "companies_employees", "employee", "company"]);
        patio.disconnect()
    });
};