#Plugins

One of the powerful features of the `patio` [Model](./models.html) api is plugins. Plugins allow developers to easily extend models to do extra functionality.

##Built In Plugins

###[patio.plugins.TimeStampPlugin](./patio_plugins_TimeStampPlugin.html)

This plugin adds timestamp functionality to models. (i.e updated and created).

**Options**


* `updated` : defaults to `updated`. the name of the column to set the updated timestamp on.
* `created` defaults to `created` the name of the column to set the created timestamp on 
* `updateOnCreate` : defaults to `false` Set to true to set the updated column on creation  


Default timestamp functionality                                                                      

```
var MyModel = patio.addModel("testTable", {                                                                       
    plugins : [patio.plugins.TimeStampPlugin],                                                                    
                                                                                                                  
    static : {                                                                                                    
        init : function(){                                                                                        
            this._super("arguments");                                                                             
            this.timestamp();                                                                                     
        }                                                                                                         
    }                                                                                                             
});                                                                                                               
```                                                                                                                  
                                                                                                                                                  
With custom `updated `column   

```                                                                                        
var MyModel = patio.addModel("testTable", {                                                                       
    plugins : [patio.plugins.TimeStampPlugin],                                                                    
                                                                                                                  
    static : {                                                                                                    
        init : function(){                                                                                        
            this._super("arguments");                                                                             
            this.timestamp({updated : "myUpdatedColumn"});                                                        
        }                                                                                                         
    }                                                                                                             
});
```                                                                                                               
                                                                                                                  
Custom `created` column    
                                                                                       
```
var MyModel = patio.addModel("testTable", {                                                                       
    plugins : [patio.plugins.TimeStampPlugin],                                                                    
                                                                                                                  
    static : {                                                                                                    
        init : function(){                                                                                        
            this._super("arguments");                                                                             
            this.timestamp({created : "customCreatedColumn"});                                                    
        }                                                                                                         
    }                                                                                                             
});
```                                                                                                               

Set both custom columns                                                                                                                                                                                                           

```
var MyModel = patio.addModel("testTable", {                                                                       
    plugins : [patio.plugins.TimeStampPlugin],                                                                    
                                                                                                                  
    static : {                                                                                                    
        init : function(){                                                                                        
            this._super("arguments");                                                                             
            this.timestamp({created : "customCreatedColumn", updated : "myUpdatedColumn"});                       
        }                                                                                                         
    }                                                                                                             
});   
```                                                                                                            
                                                                                                                  
Set to update the `updated` column when row is created                                                            

```
var MyModel = patio.addModel("testTable", {                                                                       
    plugins : [patio.plugins.TimeStampPlugin],                                                                    
                                                                                                                  
    static : {                                                                                                    
        init : function(){                                                                                        
            this._super("arguments");                                                                             
            this.timestamp({updateOnCreate : true});                                                              
        }                                                                                                         
    }                                                                                                             
});
```                                                                                                               
                                                                                                                  
Set all three options    
                                                                                       
```
var MyModel = patio.addModel("testTable", {                                                                       
    plugins : [patio.plugins.TimeStampPlugin],                                                                    
                                                                                                                  
    static : {                                                                                                    
        init : function(){                                                                                        
            this._super("arguments");                                                                             
            this.timestamp({created : "customCreatedColumn", updated : "myUpdatedColumn", updateOnCreate : true});
        }                                                                                                         
    }                                                                                                             
});                                                                                                               
```

###[patio.plugins.ClassTableInhertiance](./patio_plugins_ClassTableInhertiance.html)

See [inheritance](./model-inheritance.html) page.

###[patio.plugins.ColumnMapper](./patio_plugins_ColumnMapper.html)

Add a mapped column from another table. This is useful if there columns on                                                               
another table but you do not want to load the association every time.                                                                    
                                                                                                                                         
                                                                                                                                         
For example assume we have an employee and works table. Well we might want the salary from the works table,                              
but do not want to add it to the employee table.                                                                                         
                                                                                                                                         
<b>NOTE:</b> mapped columns are READ ONLY.                                                                                               
                                                                                                                                         
```
patio.addModel("employee")                                                                                                               
   .oneToOne("works")                                                                                                                    
   .mappedColumn("salary", "works", {employeeId : patio.sql.identifier("id")});                                                          
```                                                                                                                                    
                                                                                                                                         
You can also name the local column something different from the remote column by providing the column option. 
                                                                                                                                         
```                                                                                                                                
 patio.addModel("employee")                                                                                                              
   .oneToOne("works")                                                                                                                    
   .mappedColumn("mySalary", "works", {employeeId : patio.sql.identifier("id")}, {                                                       
         column : "salary"                                                                                                               
   });                                                                                                                                   
```        

If you want to prevent the mapped columns from being reloaded after a save or update you can set the `fetchMappedColumnsOnUpdate` or `fetchMappedColumnsOnSave` to false.

```
var Employee = patio.addModel("employee")                                                                                                              
   .oneToOne("works")                                                                                                                    
   .mappedColumn("mySalary", "works", {employeeId : patio.sql.identifier("id")}, {                                                       
         column : "salary"                                                                                                               
   }); 

//prevent the mapped columns from being fetched after a save.
Employee.fetchMappedColumnsOnSave = false;     

//prevent the mapped columns from being re-fetched after an update.
Employee.fetchMappedColumnsOnUpdate = false;    
```             

You can also override prevent the properties from being reloaded by setting the `reload` or `reloadMapped` options when saving or updating.

```
//prevents entire model from being reloaded including mapped columns
employee.save(null, {reload : false});
employee.update(null, {reload : false});

//just prevents the mapped columns from being reloaded
employee.save(null, {reloadMapped : false});
employee.update(null, {reloadMapped : false});
```                                                                                                                                                                                                                                                                                                                                                                                                  
  
**Additional Options**  
                                                                                                                                     
* `joinType` : The join type to use when gathering the properties. Defaults to `left`.
* `column` : The column on the remote table that should be used. This is useful if you want to mapped column named something differently. Defaults to `null`                               
as the local copy.  

###[patio.plugins.ValidatorPlugin](./patio_plugins_ValidatorPlugin.html)

A validation plugin for patio models. This plugin adds a `validate` method to each [Model](./patio_Model.html)
class that adds it as a plugin. This plugin does not include most typecast checks as `patio` already checks
types upon column assignment.                                                                                                                              
                                                                                                                                                           
To do single col validation                                                                                                                                

```
var Model = patio.addModel("validator", {                                                                                                                  
     plugins:[patio.plugins.ValidatorPlugin]
});                                                                                                                                                        
//this ensures column assignment                                                                                                                           
Model.validate("col1").isNotNull().isAlphaNumeric().hasLength(1, 10);                                                                                      
//col2 does not have to be assigned but if it is it must match /hello/ig.                                                                                  
Model.validate("col2").like(/hello/ig);                                                                                                                    
//Ensures that the emailAddress column is a valid email address.                                                                                           
Model.validate("emailAddress").isEmailAddress();
```
                                                                                                                                                           
Or you can do a mass validation through a callback.                                                                                                        

```
var Model = patio.addModel("validator", {                                                                                                                  
     plugins:[patio.plugins.ValidatorPlugin]
});                                                                                                                                                        
Model.validate(function(validate){                                                                                                                         
     //this ensures column assignment                                                                                                                      
     validate("col1").isNotNull().isAlphaNumeric().hasLength(1, 10);                                                                                       
     //col2 does not have to be assigned but if it is it must match /hello/ig.                                                                             
     validate("col2").isLike(/hello/ig);                                                                                                                   
     //Ensures that the emailAddress column is a valid email address.                                                                                      
     validate("emailAddress").isEmail();                                                                                                                   
});                                                                                                                                                        
```
                                                                                                                                                           
To check if a [Model](./patio_Model.html) is valid you can run the `isValid` method.
                                                                                                                                                           
```
var model1 = new Model({col2 : 'grape', emailAddress : "test"}),                                                                                           
    model2 = new Model({col1 : "grape", col2 : "hello", emailAddress : "test@test.com"});                                                                  
                                                                                                                                                           
model1.isValid() //false                                                                                                                                   
model2.isValid() //true                                                                                                                                    
```
                                                                                                                                                           
To get the errors associated with an invalid model you can access the errors property                                                                      
                                                                                                                                                           
```
model1.errors; //{ col1: [ 'col1 must be defined.' ],                                                                                                      
               //  col2: [ 'col2 must be like /hello/gi got grape.' ],                                                                                     
               //  emailAddress: [ 'emailAddress must be a valid Email Address got test' ] }                                                               
```
                                                                                                                                                           
Validation is also run pre save and pre update. To prevent this you can specify the `validate` option
                                                                                                                                                           
```
model1.save(null, {validate : false});                                                                                                                     
model2.save(null, {validate : false});                                                                                                                     
```
                                                                                                                                                           
Or you can specify the class level properties `validateOnSave` and `validateOnUpdate`
to false respectively                                                                                                                                      

```
Model.validateOnSave = false;                                                                                                                              
Model.validateOnUpdate = false;                                                                                                                            
```
                                                                                                                                                           
Available validation methods are.
                                                                                                                                                           
                                                                                                                                                      
 * `isAfter` : check that a date is after a specified date</li>
 * `isBefore` : check that a date is after before a specified date </li>
 * `isDefined` : ensure that a column is defined</li>
 * `isNotDefined` : ensure that a column is not defined</li>
 * `isNotNull` : ensure that a column is defined and not null</li>
 * `isNull` : ensure that a column is not defined or null</li>
 * `isEq` : ensure that a column equals a value <b>this uses deep equal</b></li>
 * `isNeq` : ensure that a column does not equal a value <b>this uses deep equal</b></li>
 * `isLike` : ensure that a column is like a value, can be a regexp or string</li>
 * `isNotLike` : ensure that a column is not like a value, can be a regexp or string</li>
 * `isLt` : ensure that a column is less than a value</li>
 * `isGt` : ensure that a column is greater than a value</li>
 * `isLte` : ensure that a column is less than or equal to a value</li>
 * `isGte` : ensure that a column is greater than or equal to a value</li>
 * `isIn` : ensure that a column is contained in an array of values</li>
 * `isNotIn` : ensure that a column is not contained in an array of values</li>
 * `isMacAddress` : ensure that a column is a valid MAC address</li>
 * `isIPAddress` : ensure that a column is a valid IPv4 or IPv6 address</li>
 * `isIPv4Address` : ensure that a column is a valid IPv4 address</li>
 * `isIPv6Address` : ensure that a column is a valid IPv6 address</li>
 * `isUUID` : ensure that a column is a valid UUID</li>
 * `isEmail` : ensure that a column is a valid email address</li>
 * `isUrl` : ensure than a column is a valid URL</li>
 * `isAlpha` : ensure than a column is all letters</li>
 * `isAlphaNumeric` : ensure than a column is all letters or numbers</li>
 * `hasLength` : ensure than a column is fits within the specified length accepts a min and optional max value</li>
 * `isLowercase` : ensure than a column is lowercase</li>
 * `isUppercase` : ensure than a column is uppercase</li>
 * `isEmpty` : ensure than a column empty (i.e. a blank string)</li>
 * `isNotEmpty` : ensure than a column not empty (i.e. not a blank string)</li>
 * `isCreditCard` : ensure than a is a valid credit card number</li>
 * `check` : accepts a function to perform validation</li>

                                                                                                                                                           
All of the validation methods are chainable, and accept an options argument.                                                                               
                                                                                                                                                           
The options include                                                                                                                                        
<ul>                                                                                                                                                       
    <li>`message` : a message to return if a column fails validation. The message can include `{val}` and `{col}`
    replacements which will insert the invalid value and the column name.                                                                                  
    </li>                                                                                                                                                  
    <li>`[onlyDefined=true]` : set to false to run the method even if the column value is not defined.</li>
    <li>`[onlyNotNull=true]` : set to false to run the method even if the column value is null.</li>
</ul>                                                                                                                                                      
                                                                                                                                                           
                                                                                                                                                                                                                          

##Writing your own plugins

Writing your own custom plugin easy!


###Example Express plugin

The following is a simple express plugin to expose routes for models.

1. Define your plugin

First use [comb](https://github.com/Pollenware/comb) to define a class.


```
var patio = require("patio"), 
    comb = require("comb");

//create an empty class and expose it as the module
module.exports = comb.define(null, {});

```

2. Add Some methods to the plugin

Any method on a plugin will also be added to the model. So lets add a `static` route method that takes an express app as an argument.

```
module.exports = comb.define(null, {
    static:{      
        
        /**
         *Helper method to find a model by id and handle any errors
         */
        findByIdRoute:function (params) {
            return this.findById(params.id).chain(function (model) {
                if(model){
                    return model.toObject();
                }else{
                    throw new Error("Could not find a model with id ");
                }
            });
        },
 
			      
        route:function (app) {
			app.get()"get", "/" + this.tableName + "/:id", this.findByIdRoute.bind(this));
        }
    }
});

```

So we added a get route but thats not very flexible or useful. So lets add the ability to do dynamic routing.


```
var patio = require("patio"), 
    comb = require("comb");

module.exports = comb.define(null, {
    static:{

		/**
         * Adds a "get" route to our routes array.
         */
        addRoute:function (route, cb) {
            this.routes.push(["get", route, cb]);
        },

		/**
         *Helper method to find a model by id and handle any errors
         */
        findByIdRoute:function (params) {
            return this.findById(params.id).chain(function (model) {
                if(model){
                    return model.toObject();
                }else{
                    throw new Error("Could not find a model with id " + id);
                }
            });
        },

		/**
		* Helper method to remove a model by id
		*/
        removeByIdRoute:function (params) {
            return this.removeById(params.id);
        },

		/**
		*Wrapper for a route to handle any async operations
		*/
        __routeProxy:function (cb) {
            return function (req, res) {
                comb.when(cb(req.params)).chain(res.send.bind(res), function (err) {
                    res.send({error:err.message});
                });
            }
        },
		
		/**
		 * Main method to call when routing a model. Call with the express server
		 */
        route:function (app) {
            var routes = this.routes;
            for (var i in routes) {
                var route = routes[i];
                app[route[0]](route[1], this.__routeProxy(route[2]));
            }
        },

        getters:{
            /**
             * Getter for the routes that are on this model
             */
            routes:function () {
                if (comb.isUndefined(this.__routes)) {
                    var routes = this.__routes = [
                        ["get", "/" + this.tableName + "/:id", comb.hitch(this, "findByIdRoute")],
                        ["delete", "/" + this.tableName + "/:id", comb.hitch(this, "removeByIdRoute")]
                    ];
                }
                return this.__routes;
            }

        }
    }
});
```

So with the above definition we can now add more routes from within a model.

3. Add it to a model

Below assume we have a database with a flight and a previously defined `FlightLeg` model.


```
var ExpressPlugin = require("./expressPlugin.js");
module.exports = patio.addModel("flight", {
    plugins:[ExpressPlugin],
    static:{

        init:function () {
            //call the super's init method
            this._super(arguments);
            //add new routes using the addRoute method on the Express Plugin
            this.addRoute("/flights/:airline", comb.hitch(this, function (params) {
                return this.byAirline(params.airline).chain(function (flights) {
                    return flights.map(function (flight) {
                        return flight.toObject();
                    });
                });
            }));
            this.addRoute("/flights/departs/:airportCode", comb.hitch(this, function (params) {
                return this.departsFrom(params.airportCode).chain(function (flights) {
                    return flights.map(function (flight) {
                        return flight.toObject();
                    });
                });
            }));
            this.addRoute("/flights/arrives/:airportCode", comb.hitch(this, function (params) {
                return  this.arrivesAt(params.airportCode).chain(function (flights) {
                    return flights.map(function (flight) {
                        return flight.toObject();
                    });
                });
            }));
        },

        byAirline:function (airline) {
            return this.filter({airline:airline}).all();
        },

        arrivesAt:function (airportCode) {
            return this.join(FlightLeg.select("flightId").filter({arrivalCode:airportCode}).distinct(), {flightId:sql.id}).all();
        },

        departsFrom:function (airportCode) {
            return this.join(FlightLeg.select("flightId").filter({departureCode:airportCode}).distinct(), {flightId:sql.id}).all();
        },

    }
});
```

Now route the model.

```
var app = express.createServer();
Flight.route(app);
```

###Example Logging Plugin

The folowing plugin adds a logger to each model that uses the plugin. And adds convenience methods for logging.

```

var comb = require("comb"), 
	logging = comb.logging, 
	Level = logging.Level, 
	Logger = logging.Logger;

comb.define(null, {

    instance:{

        constructor:function () {
            this.LOGGER = this._static.LOGGER;
            this._super(arguments);
        },

        /**
         * Logs an INFO level message to the "patio" logger.
         */
        logInfo:function(){
            if (this.LOGGER.isInfo) {
                this.LOGGER.info.apply(this.LOGGER, arguments);
            }
        },

        /**
         * Logs a DEBUG level message to the "patio" logger.
         */
        logDebug:function(){
            if (this.LOGGER.isDebug) {
                this.LOGGER.debug.apply(this.LOGGER, arguments);
            }
        },

        /**
         * Logs an ERROR level message to the "patio" logger.
         */
        logError:function(){
            if (this.LOGGER.isError) {
                this.LOGGER.error.apply(this.LOGGER, arguments);
            }
        },

        /**
         * Logs a WARN level message to the "patio" logger.
         */
        logWarn:function(){
            if (this.LOGGER.isWarn) {
                this.LOGGER.warn.apply(this.LOGGER, arguments);
            }
        },

        /**
         * Logs a TRACE level message to the "patio" logger.
         */
        logTrace:function(){
            if (this.LOGGER.isTrace) {
                this.LOGGER.trace.apply(this.LOGGER, arguments);
            }
        },

        /**
         * Logs a FATAL level message to the "patio" logger.
         */
        logFatal:function(){
            if (this.LOGGER.isFatal) {
                this.LOGGER.fatal.apply(this.LOGGER, arguments);
            }
        }


    },

    static:{

        LOGGER:null,

        __level:Level.ERROR,

        init:function () {
            this._super(arguments);
            var logger = Logger.getLogger("model." + this.tableName || this.LOGGER_NAME);
            this.LOGGER = logger;
        },

        /**
         * Logs an INFO level message to the "patio" logger.
         */
        logInfo:function(){
            if (this.LOGGER.isInfo) {
                this.LOGGER.info.apply(this.LOGGER, arguments);
            }
        },

        /**
         * Logs a DEBUG level message to the "patio" logger.
         */
        logDebug:function(){
            if (this.LOGGER.isDebug) {
                this.LOGGER.debug.apply(this.LOGGER, arguments);
            }
        },

        /**
         * Logs an ERROR level message to the "patio" logger.
         */
        logError:function(){
            if (this.LOGGER.isError) {
                this.LOGGER.error.apply(this.LOGGER, arguments);
            }
        },

        /**
         * Logs a WARN level message to the "patio" logger.
         */
        logWarn:function(){
            if (this.LOGGER.isWarn) {
                this.LOGGER.warn.apply(this.LOGGER, arguments);
            }
        },

        /**
         * Logs a TRACE level message to the "patio" logger.
         */
        logTrace:function(){
            if (this.LOGGER.isTrace) {
                this.LOGGER.trace.apply(this.LOGGER, arguments);
            }
        },

        /**
         * Logs a FATAL level message to the "patio" logger.
         */
        logFatal:function(){
            if (this.LOGGER.isFatal) {
                this.LOGGER.fatal.apply(this.LOGGER, arguments);
            }
        },

        setters:{
            level:function (level) {
                this.__level = level;
                this.LOGGER.level = level;
            }
        },

        getters:{
            level:function () {
                return this.__level;
            }
        }
    }

//the .as method is method that is a short cut to module.exports =
//you could also to .as(exports, "LoggingPlugin") which is the same as exports.LoggingPlugin = 
}).as(module);
```

Use it just like you would any other plugin

```
patio.addModel("flight", {
	plugins : [LoggingPlugin],
	
	"static" : {
		findById : function(){
			this.logInfo("Find flight by id!");
			//call our super method
			return this._super(arguments);
		}
	}
		
}).as(module);
```





