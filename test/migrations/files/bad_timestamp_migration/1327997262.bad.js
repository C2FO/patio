exports.up = function (db) {
    return ret.errback("err");
};

exports.down = function (db) {
    return db.dropTable("test4");
};