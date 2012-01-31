exports.up = function(db){
    db.alterTable("test5", function(){
        this.renameColumn("column5", "column6")
    });
}

exports.down = function(db){
    db.alterTable("test5", function(){
        this.renameColumn("column6", "column5")
    });
}