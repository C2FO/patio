##[Patio](http://pollenware.github.com/patio/index.html)
           
Patio-query is a [Sequel](http://sequel.rubyforge.org/" target="patioapi") inspired query engine and [ORM](http://en.wikipedia.org/wiki/Object-relational_mapping).

## Installation
While it is not mandatory it is recommened that you install comb first, as it will make working with the comb API easier.

    npm install comb patio

To use the patio executable for migrations

    npm install -g patio


##Example

```javascript

 var patio = require("./index");
 var comb = require("comb");

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
        this.foreignKey("airportId", "airport", {key:"airportCode", type : String, size : 4});
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
        this.foreignKey("departureCode", "airport", {key:"airportCode", type : String, size : 4});
        this.foreignKey("arrivalCode", "airport", {key:"airportCode", type : String, size : 4});
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
createSchema.then(function(DB){
    var ds = DB.from('airport');
    comb.executeInOrder(ds, patio, function(ds, patio){
        ds.multiInsert([{airportCode:"OMA", name:"Eppley Airfield", city:"Omaha", state:"NE"},
            {airportCode:"ABR", name:"Aberdeen", city:"Aberdeen", state:"SD"},
            {airportCode:"ASE", name:"Aspen Pitkin County Airport", city:"Aspen", state:"CO"}]);
        ds.forEach(function(airport){
            console.log(airport.airportCode);
        });
        patio.disconnect();
    });
});
```
        
##Features
* [Comprehensive documentation with examples](http://pollenware.github.com/patio/index.html).
* &gt; 80% test coverage
* [Support for connection URIs and objects](http://pollenware.github.com/patio/connecting.html)
* Supported Databases
   * MySQL
   * Postgres - Coming Soon!
* [Models](http://pollenware.github.com/patio/models.html)
* [Associations](http://pollenware.github.com/patio/associtaions.html)
* Simple adapter extensions
* [Migrations](http://pollenware.github.com/patio/migrations.html)
  * Integer and Timestamp based.
* [Powerful query API](http://pollenware.github.com/patio/querying.html)
* [Transactions](http://pollenware.github.com/patio/api/symbols/patio.Database.html#transaction)
   * Savepoints
   * Isolation Levels
   * Two phase commits
* SQL Datatype casting
* [Full database CRUD operations](http://pollenware.github.com/patio/DDL.html)
   * [createTable](http://pollenware.github.com/patio/api/symbols/patio.Database.html#createTable)
   * [alterTable](http://pollenware.github.com/patio/api/symbols/patio.Database.html#alterTable)
   * [dropTable](http://pollenware.github.com/patio/api/symbols/patio.Database.html#dropTable)
   * [insert](http://pollenware.github.com/patio/api/symbols/patio.Dataset.html#insert)
   * [multiInsert](http://pollenware.github.com/patio/api/symbols/patio.Dataset.html#multiInsert)
   * [update](http://pollenware.github.com/patio/api/symbols/patio.Dataset.html#update)
   * [remove](http://pollenware.github.com/patio/api/symbols/patio.Dataset.html#remove)
   * [filter](http://pollenware.github.com/patio/api/symbols/patio.Dataset.html#filter)

##License

MIT <https://github.com/Pollenware/patio/raw/master/LICENSE>


##Meta

* Code: `git clone git://github.com/pollenware/patio.git`
* JsDoc: <http://pollenware.github.com/patio>
* Website:  <http://pollenware.com> - Twitter: <http://twitter.com/pollenware> - 877.465.4045
