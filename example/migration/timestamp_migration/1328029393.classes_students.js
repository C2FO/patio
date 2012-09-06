exports.up = function(db){
    return db.createTable("classes_students", function() {
        this.foreignKey("studentId", "student", {key:"id"});
        this.foreignKey("classId", "class", {key:"id"});
    });
};

exports.down = function(db){
    return db.dropTable("classes_students");
};

