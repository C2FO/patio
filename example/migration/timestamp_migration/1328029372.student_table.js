var patio = require("../../../index"),
    sql = patio.sql;

exports.up = function(db){
    return db.createTable("student", function() {
        this.primaryKey("id");
        this.firstName(String);
        this.lastName(String);
        //GPA
        this.gpa(sql.Decimal, {size:[1, 3], "default":0.0});
        //Honors Program?
        this.isHonors(Boolean, {"default":false});
        //freshman, sophmore, junior, or senior
        this.classYear("char");
    });
};

exports.down = function(db){
    return db.dropTable("student");
}