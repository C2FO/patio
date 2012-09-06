exports.up = function (db) {
    return db.alterTable("test5", function () {
        this.renameColumn("column5", "column6")
    });
};

exports.down = function (db) {
    return db.alterTable("test5", function () {
        this.renameColumn("column6", "column5")
    });
};