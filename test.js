"use strict";
var patio = require("./index"),
    sql = patio.sql,
    comb = require("comb");

patio.camelize = true;
var createSchema = patio.connectAndExecute("mysql://test:testpass@localhost:3306/airports", function (db) {
    db.forceDropTable(["legInstance", "flightLeg", "flight", "airplane", "canLand", "airplaneType", "airport"]);
    db.createTable("airport", {engine:"innodb"}, function () {
        this.primaryKey("id");
        this.airportCode(String, {size:4, allowNull:false, unique:true});
        this.name(String, {allowNull:false});
        this.city(String, {allowNull:false});
        this.state(String, {size:2, allowNull:false});
    });
    db.createTable("airplaneType", {engine:"innodb"}, function () {
        this.primaryKey("id");
        this.name(String, {allowNull:false});
        this.maxSeats(Number, {size:3, allowNull:false});
        this.company(String, {allowNull:false});
    });
    db.createTable("flight", {engine:"innodb"}, function () {
        this.primaryKey("id");
        this.weekdays("set", {elements:["M", 'T', "W", "TH", "F", "S", "SU"], allowNull:false});
        this.airline(String, {allowNull:false});
    });
    db.createTable("canLand", {engine:"innodb"}, function () {
        this.foreignKey("airplaneTypeId", "airplaneType", {key:"id"});
        this.foreignKey("airportId", "airport", {key:"airportCode", type:String, size:4});
    });
    db.createTable("airplane", {engine:"innodb"}, function () {
        this.primaryKey("id");
        this.totalNoOfSeats(Number, {size:3, allowNull:false});
        this.foreignKey("typeId", "airplaneType", {key:"id"});
    });
    db.createTable("flightLeg", {engine:"innodb"}, function () {
        this.primaryKey("id");
        this.scheduledDepartureTime("time");
        this.scheduledArrivalTime("time");
        this.foreignKey("departureCode", "airport", {key:"airportCode", type:String, size:4});
        this.foreignKey("arrivalCode", "airport", {key:"airportCode", type:String, size:4});
        this.foreignKey("flightId", "flight", {key:"id"});
    });
    db.createTable("leg_instance", {engine:"innodb"}, function () {
        this.primaryKey("id");
        this.date("date");
        this.arrTime("datetime");
        this.depTime("datetime");
        this.foreignKey("airplaneId", "airplane", {key:"id"});
        this.foreignKey("flight_legId", "flightLeg", {key:"id"});
    });
});
createSchema.then(
    function (DB) {
        var ds = DB.from('airport');
        comb.executeInOrder(DB, ds, patio,
            function (DB, ds, patio) {
                DB.createTable("test", function () {
                    this.timestamp(sql.TimeStamp);
                    this.datetime(sql.DateTime);
                    this.time(sql.Time);
                    this.year(sql.Year);
                    this.decimal(sql.Decimal);
                    this.float(sql.Float);
                });
                DB.createTable("biologicalFather", function () {
                    this.primaryKey("id");
                    this.name(String);
                });
                DB.createTable("child", function () {
                    this.primaryKey("id");
                    this.name(String);
                    this.foreignKey("biologicalFatherId", "biologicalFather", {key:"id"});
                });
                //define the BiologicalFather model
                patio.addModel("biologicalFather", {
                    static:{
                        init:function () {
                            this.oneToMany("children");
                        }
                    }
                });

                //define Person  model
                patio.addModel("child", {
                    static:{
                        init:function () {
                            this.manyToOne("biologicalFather");
                        }
                    }
                });

                var BiologicalFather = patio.getModel("biologicalFather");
                BiologicalFather.save([
                    {name:"Fred", children:[
                        {name:"Bobby"},
                        {name:"Alice"},
                        {name:"Susan"}
                    ]},
                    {name:"Ben"},
                    {name:"Bob"},
                    {name:"Scott", children:[
                        {name:"Brad"}
                    ]}
                ]);
                BiologicalFather.forEach(function (father) {
                    father.children.then(function (children) {
                        children.forEach(function(child){
                            console.log(father.name + ":" + child.name);
                        })
                    })
                });
                ds.multiInsert([
                    {airportCode:"OMA", name:"Eppley Airfield", city:"Omaha", state:"NE"},
                    {airportCode:"ABR", name:"Aberdeen", city:"Aberdeen", state:"SD"},
                    {airportCode:"ASE", name:"Aspen Pitkin County Airport", city:"Aspen", state:"CO"}
                ]);
                ds.forEach(function (airport) {
                    console.log(airport.airportCode);
                });
            }).then(function () {

                DB.dropTable("test", "child", "biological_father").both(comb.hitch(patio, "disconnect"));

            }, function (err) {
                console.trace(err);
            });

    });