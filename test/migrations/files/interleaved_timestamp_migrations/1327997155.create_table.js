exports.up = function(db){
    return db.alterTable("test1", function(){
        this.renameColumn("column1", "column2");
    });
};

exports.down = function(db){
    return db.alterTable("test1", function(){
        this.renameColumn("column2", "column1");
    });
};