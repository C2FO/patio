exports.up = function(db){
    db.createTable("test4", function(){
        this.column("column4", "integer");
    });
}

exports.down = function(db){
    db.dropTable("test4");
}