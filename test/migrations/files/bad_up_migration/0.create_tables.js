exports.up = function(db){
    db.createTable("test", function(){
        this.column("column", "integer");
    });
    db.createTable("test2", function(){
        this.column("column", "integer");
    });
    db.createTable("test3", function(){
        this.column("column", "integer");
    });
    db.createTable("test4", function(){
        this.column("column", "integer");
    });
}

exports.down = function(db){
    db.dropTable("test", "test2", "test3", "test4");
}