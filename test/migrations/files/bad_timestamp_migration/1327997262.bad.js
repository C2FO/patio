var comb = require("comb");

exports.up = function (db) {
    return comb.rejected(new Error("err"));
};

exports.down = function (db) {
    return db.dropTable("test4");
};