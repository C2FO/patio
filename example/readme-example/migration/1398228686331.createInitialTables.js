module.exports = {
    //up is called when you migrate your database up
    up: function (db) {
        //create a table called state;
        return db
            .createTable("state", function () {
                this.primaryKey("id");
                this.name(String);
                this.population("integer");
                this.founded(Date);
                this.climate(String);
                this.description("text");
            })
            .chain(function () {
                //create another table called capital
                return db.createTable("capital", function () {
                    this.primaryKey("id");
                    this.population("integer");
                    this.name(String);
                    this.founded(Date);
                    this.foreignKey("stateId", "state", {key: "id", onDelete: "CASCADE"});
                });
            });
    },

    //down is called when you migrate your database down
    down: function (db) {
        //drop the state and capital tables
        return db.dropTable("capital", "state");
    }
};