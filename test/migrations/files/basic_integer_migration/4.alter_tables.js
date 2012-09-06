var comb = require("comb");
exports.up = function (db, next) {
    comb.when(
        db.alterTable("test1", function () {
            this.renameColumn("column1", "column2");
        }),
        db.alterTable("test2", function () {
            this.renameColumn("column2", "column3");
        }),
        db.alterTable("test3", function () {
            this.renameColumn("column3", "column4");
        }),
        db.alterTable("test4", function () {
            this.renameColumn("column4", "column5");
        })
    ).classic(next);
};

exports.down = function (db) {
    return comb.when(
        db.alterTable("test1", function () {
            this.renameColumn("column2", "column1");
        }),
        db.alterTable("test2", function () {
            this.renameColumn("column3", "column2");
        }),
        db.alterTable("test3", function () {
            this.renameColumn("column4", "column3");
        }),
        db.alterTable("test4", function () {
            this.renameColumn("column5", "column4");
        })
    );
}