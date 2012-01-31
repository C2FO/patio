exports.up = function(db){
    db.alterTable("test4", function(){
        this.renameColumn("column4", "column5");
    });
}

exports.down = function(db){
    db.alterTable("test4", function(){
        this.renameColumn("column5", "column4");
    });
}