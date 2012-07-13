#[Patio](https://pollenware.github.com/patio)

Patio is a <a href="http://sequel.rubyforge.org/" target="patioapi">Sequel</a> inspired query engine.                                                        
                                                                                                                                                             
###Why Use Patio?
                                                                                                                                                             
Patio is different because it allows the developers to choose the level of abtraction they are comfortable with.                                             

If you want to use [ORM](https://pollenware.github.com/patio/models.html) functionality you can. If you dont you can just use the [Database](https://pollenware.github.com/patio/DDL.html] and [Datasets](https://pollenware.github.com/patio/querying.html) as a querying API, and if you need toyou can [write plain SQL](https://pollenware.github.com/patio/patio_Database.html#run)                                                                                                                                                                                                                                                                                           
                                                                                                                                                                                                                                                                                                                         
###Installation
To install patio run                                                                                                                                         
                                                                                                                                                             
`npm install comb patio`
                                                                                                                                                             
If you want to use the patio executable for migrations
                                                                                                                                                             
`npm install -g patio`
                                                                                                                                                             
###Features
                                                                                                                                                                                                                                                                                                          
* Comprehensive documentation with examples.
* &gt; 80% test coverage
* Support for connection URIs
* Supported Databases                                                                                                                                        
  * MySQL
  * Postgres
* [Models](https://pollenware.github.com/patio/models.html)
  * [Associations](https://pollenware.github.com/patio/associations.html)
  * [Inheritance](https://pollenware.github.com/patio/model-inheritance.html)
  * [Plugins](https://pollenware.github.com/patio/plugins.html)
* Simple adapter extensions
* [Migrations](https://pollenware.github.com/patio/migrations.htmls)
  * Integer and Timestamp based.
* Powerful [Querying](https://pollenware.github.com/patio/querying.html)
* [Transactions](https://pollenware.github.com/patio/patio_Database.html#transactions) with                                                                                       
  * Savepoints
  * Isolation Levels
  * Two phase commits
* SQL Datatype casting
* Full database CRUD operations                                                                                                                           
  * [createTable](https://pollenware.github.com/patio/patio_Database.html#createTable)
  * [alterTable](https://pollenware.github.com/patio/patio_Database.html#alterTable)
  * [dropTable](https://pollenware.github.com/patio/patio_Database.html#dropTable) 
  * [insert](https://pollenware.github.com/patio/patio_Dataset.html#insert)
  * [multiInsert](https://pollenware.github.com/patio/patio_Dataset.html#multiInsert)
  * [update](https://pollenware.github.com/patio/patio_Dataset.html#update)
  * [remove](https://pollenware.github.com/patio/patio_Dataset.html#remove)
  * [query](https://pollenware.github.com/patio/patio_Dataset.html#query)
                                                                                       
                                                                                                                                                             
###Example

```javascript                                                                                                                                              
var patio = require("../index.js"),                                                                                                                          
     comb = require("comb"),                                                                                                                                 
     when = comb.when,                                                                                                                                       
     serial = comb.serial;                                                                                                                                   
                                                                                                                                                             
                                                                                                                                                             
 //set all db name to camelize                                                                                                                               
 patio.camelize = true;                                                                                                                                      
 patio.configureLogging();                                                                                                                                   
 //connect to the db                                                                                                                                         
 var DB = patio.connect(&lt;CONNECTION_URI&gt;);                                                                                                             
                                                                                                                                                             
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
             //set up our base tables that have no dependencies                                                                                              
                 return when(                                                                                                                                
                     DB.createTable("airport", function () {                                                                                                 
                         this.primaryKey("id");                                                                                                              
                         this.airportCode(String, {size:4, allowNull:false, unique:true});                                                                   
                         this.name(String, {allowNull:false});                                                                                               
                         this.city(String, {allowNull:false});                                                                                               
                         this.state(String, {size:2, allowNull:false});                                                                                      
                     }),                                                                                                                                     
                     DB.createTable("airplaneType", function () {                                                                                            
                         this.primaryKey("id");                                                                                                              
                         this.name(String, {allowNull:false});                                                                                               
                         this.maxSeats(Number, {size:3, allowNull:false});                                                                                   
                         this.company(String, {allowNull:false});                                                                                            
                     }),                                                                                                                                     
                     DB.createTable("flight", function () {                                                                                                  
                         this.primaryKey("id");                                                                                                              
                         this.weekdays(String, {size:2, allowNull:false});                                                                                   
                         this.airline(String, {allowNull:false});                                                                                            
                     })                                                                                                                                      
                 );                                                                                                                                          
             },                                                                                                                                              
             function () {                                                                                                                                   
                 //create our join tables                                                                                                                    
                 return when(                                                                                                                                
                     DB.createTable("canLand", function () {                                                                                                 
                         this.foreignKey("airplaneTypeId", "airplaneType", {key:"id"});                                                                      
                         this.foreignKey("airportId", "airport", {key:"airportCode", type:String, size:4});                                                  
                     }),                                                                                                                                     
                     DB.createTable("airplane", function () {                                                                                                
                         this.primaryKey("id");                                                                                                              
                         this.totalNoOfSeats(Number, {size:3, allowNull:false});                                                                             
                         this.foreignKey("typeId", "airplaneType", {key:"id"});                                                                              
                     }),                                                                                                                                     
                     DB.createTable("flightLeg", function () {                                                                                               
                         this.primaryKey("id");                                                                                                              
                         this.scheduledDepartureTime("time");                                                                                                
                         this.scheduledArrivalTime("time");                                                                                                  
                         this.foreignKey("departureCode", "airport", {key:"airportCode", type:String, size:4});                                              
                         this.foreignKey("arrivalCode", "airport", {key:"airportCode", type:String, size:4});                                                
                         this.foreignKey("flightId", "flight", {key:"id"});                                                                                  
                     })                                                                                                                                      
                 );                                                                                                                                          
             },                                                                                                                                              
             function () {                                                                                                                                   
                 return DB.createTable("legInstance", function () {                                                                                          
                     this.primaryKey("id");                                                                                                                  
                     this.date("date");                                                                                                                      
                     this.arrTime("datetime");                                                                                                               
                     this.depTime("datetime");                                                                                                               
                     this.foreignKey("airplaneId", "airplane", {key:"id"});                                                                                  
                     this.foreignKey("flightLegId", "flightLeg", {key:"id"});                                                                                
                 });                                                                                                                                         
             }                                                                                                                                               
         ]);                                                                                                                                                 
     });                                                                                                                                                     
 };                                                                                                                                                          
                                                                                                                                                             
 createSchema().then(function () {                                                                                                                           
     var ds = DB.from('airport');                                                                                                                            
                                                                                                                                                             
     ds.multiInsert([                                                                                                                                        
         {airportCode:"OMA", name:"Eppley Airfield", city:"Omaha", state:"NE"},                                                                              
         {airportCode:"ABR", name:"Aberdeen", city:"Aberdeen", state:"SD"},                                                                                  
         {airportCode:"ASE", name:"Aspen Pitkin County Airport", city:"Aspen", state:"CO"}                                                                   
     ]).then(function () {                                                                                                                                   
         ds.forEach(function (airport) {                                                                                                                     
             console.log(airport.airportCode);                                                                                                               
       }).then(patio.disconnect.bind(patio), errorHandler);                                                                                                  
     }, errorHandler);                                                                                                                                       
}, errorHandler);                                                                                                                                            
```                                                                                                                                                           