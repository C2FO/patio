exports.up = function (db) {
    return db.createTable("test3", function () {
        this.column("column3", "integer");
    });
};

exports.down = function (db) {
    return db.dropTable("test3");
};