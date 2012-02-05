exports.up = function(db){
    db.createTable("test5", function(){
        this.column("column5", "integer");
    });
}

exports.down = function(db){
    db.dropTable("test5");
}