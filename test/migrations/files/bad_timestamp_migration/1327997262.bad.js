exports.up = function(db){
    return ret.errback("err");
}

exports.down = function(db){
    db.dropTable("test4");
}