var patio = require("../index.js"),
    comb = require("comb"),
    when = comb.when,
    serial = comb.serial;


//set all db name to camelize
patio.camelize = true;
patio.configureLogging();
//connect to the db
var DB = patio.connect("pg://test:testpass@localhost:5432/airports");

var errorHandler = function (error) {
    console.log(error);
    patio.disconnect();
};

var createSchema = function () {
    return DB.transaction(function () {
        return serial([
            function () {
                return DB.forceDropTable(["legInstance", "flightLeg", "flight", "airplane", "canLand", "airplaneType", "airport"]);
            },
            function () {
                //sert up our base tables that have no dependencies
                return when(
                    DB.createTable("airport", function () {
                        this.primaryKey("id");
                        this.airportCode(String, {size: 4, allowNull: false, unique: true});
                        this.name(String, {allowNull: false});
                        this.city(String, {allowNull: false});
                        this.state(String, {size: 2, allowNull: false});
                    }),
                    DB.createTable("airplaneType", function () {
                        this.primaryKey("id");
                        this.name(String, {allowNull: false});
                        this.maxSeats(Number, {size: 3, allowNull: false});
                        this.company(String, {allowNull: false});
                    }),
                    DB.createTable("flight", function () {
                        this.primaryKey("id");
                        this.weekdays(String, {size: 2, allowNull: false});
                        this.airline(String, {allowNull: false});
                    })
                );
            },
            function () {
                //create our join tables
                return when(
                    DB.createTable("canLand", function () {
                        this.foreignKey("airplaneTypeId", "airplaneType", {key: "id"});
                        this.foreignKey("airportId", "airport", {key: "airportCode", type: String, size: 4});
                    }),
                    DB.createTable("airplane", function () {
                        this.primaryKey("id");
                        this.totalNoOfSeats(Number, {size: 3, allowNull: false});
                        this.foreignKey("typeId", "airplaneType", {key: "id"});
                    }),
                    DB.createTable("flightLeg", function () {
                        this.primaryKey("id");
                        this.scheduledDepartureTime("time");
                        this.scheduledArrivalTime("time");
                        this.foreignKey("departureCode", "airport", {key: "airportCode", type: String, size: 4});
                        this.foreignKey("arrivalCode", "airport", {key: "airportCode", type: String, size: 4});
                        this.foreignKey("flightId", "flight", {key: "id"});
                    })
                );
            },
            function () {
                return DB.createTable("legInstance", function () {
                    this.primaryKey("id");
                    this.date("date");
                    this.arrTime("datetime");
                    this.depTime("datetime");
                    this.foreignKey("airplaneId", "airplane", {key: "id"});
                    this.foreignKey("flightLegId", "flightLeg", {key: "id"});
                });
            }
        ]);
    });
};

createSchema()
    .chain(function () {
        var ds = DB.from('airport');
        return ds.multiInsert([
            {airportCode: "OMA", name: "Eppley Airfield", city: "Omaha", state: "NE"},
            {airportCode: "ABR", name: "Aberdeen", city: "Aberdeen", state: "SD"},
            {airportCode: "ASE", name: "Aspen Pitkin County Airport", city: "Aspen", state: "CO"}
        ]);
    })
    .chain(function () {
        return ds.forEach(function (airport) {
            console.log(airport.airportCode);
        });
    })
    .chain(patio.disconnect.bind(patio), errorHandler);