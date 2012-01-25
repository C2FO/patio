var patio = require("../../lib"),
    comb = require("comb");

var DB;
exports.createTables = function () {
    return patio.connectAndExecute("mysql://test:testpass@localhost:3306/airports",
        function (db) {
            db.forceDropTable(["leg_instance", "flight_leg", "flight", "airplane", "can_land", "airplane_type", "airport"]);
            db.createTable("airport", {engine:"innodb"}, function () {
                this.primaryKey("id");
                this.airport_code(String, {size:4, allowNull:false, unique:true});
                this.name(String, {allowNull:false});
                this.city(String, {allowNull:false});
                this.state(String, {size:2, allowNull:false});
            });
            db.createTable("airplane_type", {engine:"innodb"}, function () {
                this.primaryKey("id");
                this.name(String, {allowNull:false});
                this.max_seats(Number, {size:3, allowNull:false});
                this.company(String, {allowNull:false});
            });
            db.createTable("flight", {engine:"innodb"}, function () {
                this.primaryKey("id");
                this.weekdays("set", {elements:["M", 'T', "W", "TH", "F", "S", "SU"], allowNull:false});
                this.airline(String, {allowNull:false});
            });
            db.createTable("can_land", {engine:"innodb"}, function () {
                this.foreignKey("airplane_type_id", "airplane_type", {key:"id"});
                this.foreignKey("airport_id", "airport", {key:"airport_code", type : String, size : 4});
            });
            db.createTable("airplane", {engine:"innodb"}, function () {
                this.primaryKey("id");
                this.total_no_of_seats(Number, {size:3, allowNull:false});
                this.foreignKey("typeId", "airplane_type", {key:"id"});
            });
            db.createTable("flight_leg", {engine:"innodb"}, function () {
                this.primaryKey("id");
                this.scheduled_departure_time("time");
                this.scheduled_arrival_time("time");
                this.foreignKey("departure_code", "airport", {key:"airport_code", type : String, size : 4});
                this.foreignKey("arrival_code", "airport", {key:"airport_code", type : String, size : 4});
                this.foreignKey("flight_id", "flight", {key:"id"});
            });
            db.createTable("leg_instance", {engine:"innodb"}, function () {
                this.primaryKey("id");
                this.date("date");
                this.arr_time("datetime");
                this.dep_time("datetime");
                this.foreignKey("airplane_id", "airplane", {key:"id"});
                this.foreignKey("flight_leg_id", "flight_leg", {key:"id"});
            });
        }).addCallback(function (db) {
            DB = db;
        });
};


exports.dropTableAndDisconnect = function () {
    return comb.executeInOrder(patio, DB, function (patio, db) {
        db.forceDropTable(["leg_instance", "flight_leg", "flight", "airplane", "can_land", "airplane_type", "airport"]);
        patio.disconnect();
    });
};