exports.up = function (db) {
    return db.alterTable("test3", function () {
        this.renameColumn("column3", "column4");
    });
};

exports.down = function (db) {
    return db.alterTable("test3", function () {
        this.renameColumn("column4", "column3");
    });
};