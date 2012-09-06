var comb = require("comb");
exports.up = function (db) {
    return comb.when(
        db.createTable("test", function () {
            this.column("column", "integer");
        }),
        db.createTable("test2", function () {
            this.column("column", "integer");
        }),
        db.createTable("test3", function () {
            this.column("column", "integer");
        }),
        db.createTable("test4", function () {
            this.column("column", "integer");
        })
    );
};

exports.down = function (db) {
    return db.dropTable("test", "test2", "test3", "test4");
};