exports.up = function(db){
    return db.alterTable("test4", function(){
        this.renameColumn("column4", "column5");
    });
};

exports.down = function(db){
    return db.alterTable("test4", function(){
        this.renameColumn("column5", "column4");
    });
};