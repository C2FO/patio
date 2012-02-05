exports.up = function(db){
    db.alterTable("test2", function(){
        this.renameColumn("column2", "column3");
    });
}

exports.down = function(db){
    db.alterTable("test2", function(){
        this.renameColumn("column3", "column2");
    });
}