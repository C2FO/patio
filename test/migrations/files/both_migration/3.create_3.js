exports.up = function(db){
    return db.createTable("test4", function(){
        this.column("column4", "integer");
    });
};

exports.down = function(db){
    return db.dropTable("test4");
};