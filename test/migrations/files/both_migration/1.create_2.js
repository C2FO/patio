exports.up = function(db){
    db.createTable("test2", function(){
        this.column("column2", "integer");
    });
}

exports.down = function(db){
    db.dropTable("test2");
}