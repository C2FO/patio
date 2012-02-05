exports.up = function(db){
    db.createTable("test1", function(){
        this.column("column1", "integer");
    });
}

exports.down = function(db){
   return new comb.Promise().errback();
}