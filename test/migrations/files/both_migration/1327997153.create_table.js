exports.up = function(db){
    return db.createTable("test5", function(){
        this.column("column5", "integer");
    });
};

exports.down = function(db){
    return db.dropTable("test5");
};