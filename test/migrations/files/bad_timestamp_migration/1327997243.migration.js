exports.up = function(db){
    db.createTable("test3", function(){
        this.column("column3", "integer");
    });
}

exports.down = function(db){
    db.dropTable("test3");
}