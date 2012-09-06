var comb = require("comb");

//Up function used to migrate up a version
exports.up = function (db) {
    //create a new table
    return comb.when(
        db.createTable("employees", function (table) {
            this.primaryKey("id");
            this.firstName(String);
            this.lastName(String);
            this.middleInitial("char", {size:1});
        }),
        db.from("employees").multiInsert([
            {
                firstName:"Bob",
                lastName:"Yukon",
                middleInitial:"G"
            },
            {
                firstName:"Suzy",
                lastName:"Yukon",
                middleInitial:"G"
            },
            {
                firstName:"Greg",
                lastName:"Yukon",
                middleInitial:"G"
            },
            {
                firstName:"Florence",
                lastName:"Yukon",
                middleInitial:"G"
            },
            {
                firstName:"Nick",
                lastName:"Yukon",
                middleInitial:"G"
            }
        ])
    );
};

//Down function used to migrate down version
exports.down = function (db) {
    return db.dropTable("employees");
};