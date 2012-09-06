var comb = require("comb");

//Up function used to migrate up a version
exports.up = function (db) {
    //create a new table
    return db.renameTable("employees", "employeesOld")
        .chain(function () {
            return db.createTable("employees", function (table) {
                this.primaryKey("id");
                this.firstName(String);
                this.lastName(String);
                this.hireDate(Date);
                this.middleInitial("char", {size:1});
            });
        }).chain(function () {
            return db.from("employeesOld").map(function (employee) {
                return comb.merge(employee, {hireDate:new Date()});
            }).chain(function (employees) {
                    return db.from("employees").multiInsert(employees);
                });
        });
};

//Down function used to migrate down version
exports.down = function (db) {
    return comb.when(
        db.dropTable("employeesOld"),
        db.alterTable("employees", function () {
            this.dropColumn("hireDate");
        })
    );
};