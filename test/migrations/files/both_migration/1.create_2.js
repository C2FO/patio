exports.up = function(db){
    return db.createTable("test2", function(){
        this.column("column2", "integer");
    });
};

exports.down = function(db){
    return db.dropTable("test2");
};