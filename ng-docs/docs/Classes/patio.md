A singleton class that acts as the entry point for all actions performed in patio.

*Example*

```
var patio = require("patio");

patio.createConnection(....);

patio.camelize = true;
patio.quoteIdentifiers=false;
patio.parseInt8=false
patio.defaultPrimaryKeyType = "integer" //"bigint"

patio.createModel("my\_table");


 //CHANGING IDENTIFIER INPUT METHOD


 //use whatever is passed in
  patio.identifierInputMethod = null;
 //convert to uppercase
 patio.identifierInputMethod = "toUpperCase";
 //convert to camelCase
 patio.identifierInputMethod = "camelize";
 //convert to underscore
 patio.identifierInputMethod = "underscore";


 //CHANGING IDENTIFIER OUTPUT METHOD

 //use whatever the db returns
  patio.identifierOutputMethod = null;
 //convert to uppercase
 patio.identifierOutputMethod = "toUpperCase";
 //convert to camelCase
 patio.identifierOutputMethod = "camelize";
 //convert to underscore
 patio.identifierOutputMethod = "underscore";

 //TURN QUOTING OFF
  patio.quoteIdentifiers = false
```

*Extends*
* [patio.Time](./patio_Time)

## Properties

*Static Properties*
<table>
    <thead>
        <tr>
            <th>Property</th>
            <th>Type</th>
            <th>Default Value</th>
            <th>Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>patio.Time</td>
            <td></td>
            <td></td>
            <td>Mixin that provides time formatting/conversion functions</td>
        </tr>
    </tbody>
</table>

*Instance Properties*
<table>
    <thead>
        <tr>
            <th>Property</th>
            <th>Type</th>
            <th>Default Value</th>
            <th>Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>DATABASES</td>
            <td>patio.Database[]</td>
            <td><code>[]</code></td>
            <td>An array of databases that are currently connected.</td>
        </tr>
        <tr>
            <td>LOGGER</td>
            <td>Logger</td>
            <td><code>comb.logger("patio")</code></td>
            <td> Returns the root comb logger using this logger you can set the levels add appenders etc.</td>
        </tr>
        <tr>
            <td>camelize</td>
            <td>function</td>
            <td></td>
            <td>
                Sets the whether or not to camelize identifiers coming from the database and to underscore identifiers when sending identifiers to the database. Setting this property to true has the same effect as:
                <pre>
patio.identifierOutputMethod = "camelize";
patio.identifierInputMethod = "underscore";
                </pre>
            </td>
        </tr>
        <tr>
        </tr>
            <td>defaultDatabase</td>
            <td>patio.Database</td>
            <td><code>null</code></td>
            <td>Returns the default database. This is the first database created using patio connect.</td>
        <tr>
            <td>defaultPrimaryKeyType</td>
            <td>String</td>
            <td></td>
            <td>Set the default primary key type when not specified for all databases by default. By default, patio uses "integer".</td>
        </tr>
        <tr>
            <td>identifierInputMethod</td>
            <td>String</td>
            <td></td>
            <td>
                Set the method to call on identifiers going into the database. This affects how identifiers are sent to the database. So if you use camelCased and the db identifiers are all underscored use camelize. The method can include
                <ul>
                    <li>toUpperCase</li>
                    <li>toLowerCase</li>
                    <li>camelize</li>
                    <li>underscore</li>
                    <li>Other String instance method names.</li>
                </ul>
                patio uses toUpperCase identifiers in all SQL strings for most databases.
            </td>
        </tr>
        <tr>
            <td>identifierOutputMethod</td>
            <td>String</td>
            <td></td>
            <td>
                Set the method to call on identifiers coming out of the database. This affects the how identifiers are represented by calling the method on them. The method can include
                <ul>
                    <li>toUpperCase</li>
                    <li>toLowerCase</li>
                    <li>camelize</li>
                    <li>underscore</li>
                    <li>Other String instance method names.</li>
                </ul>
                most database implementations in patio use toLowerCase
            </td>
        </tr>
        <tr>
            <td>parseInt8</td>
            <td>Boolean</td>
            <td></td>
            <td>Sets whether bigint types should be parsed to a number. An error will be thrown if set and the number is set and the number is greater than 2^53 or less than -2^53.</td>
        </tr>
        <tr>
            <td>quoteIdentifiers</td>
            <td>Boolean</td>
            <td></td>
            <td>Set whether to quote identifiers for all databases by default. By default, patio quotes identifiers in all SQL strings.</td>
        </tr>
        <tr>
            <td>underscore</td>
            <td>function</td>
            <td></td>
            <td>
                Sets the whether or not to underscore identifiers coming from the database and to camelize identifiers when sending identifiers to the database. Setting this property to true has the same effect as:
                <pre>
patio.identifierOutputMethod = "underscore";
patio.identifierInputMethod = "camelize";
                </pre>
            </td>
        </tr>
    </tbody>
</table>

## Functions

### Constructor

*Defined index.js*

*Source*
```
function (){
   this.\_super(arguments);
   var constants = SQL.Constants;
   for (var i in constants) {
       this[i] = constants[i];
   }
}
```

### addModel
Public *Defined index.js*

This method is used to create a  [patio.Model](./patio.Model) object.

*Example*
```
var Flight = patio.addModel("flight", {
    instance:{
            toObject:function () {
                var obj = this.\_super(arguments);
                obj.weekdays = this.weekdaysArray;
                obj.legs = this.legs.map(function (l) {
                    return l.toObject();
                });
                return obj;
            },

            \_setWeekdays:function (weekdays) {
                this.weekdaysArray = weekdays.split(",");
                return weekdays;
            }
    },

    static:{
        init:function () {
            this.oneToMany("legs", {
                model:"flightLeg",
                orderBy:"scheduledDepartureTime",
                fetchType:this.fetchType.EAGER
            });
        },

        byAirline:function (airline) {
            return this.filter({airline:airline}).all();
        },

        arrivesAt:function (airportCode) {
            return this.join(this.flightLeg.select("flightId").filter({arrivalCode:airportCode}).distinct(), {flightId:"id"}).all();
        },

        departsFrom:function (airportCode) {
            return this.join(this.flightLeg.select("flightId").filter({departureCode:airportCode}).distinct(), {flightId:"id"}).all();
        },

        getters:{
            flightLeg:function () {
                if (!this.\_\_flightLeg) {
                    this.\_\_flightLeg = this.patio.getModel("flightLeg");
                }
                return this.\_\_flightLeg;
            }
        }
    }
});
```

*Arguments*
* *table* : the table to use as the base for the model.
* *supers* :
* *proto?* : an object to be used as the prototype for the model. See [comb.define](http://c2fo.github.com/comb/symbols/comb.html#.define).
* *Parent* `patio.Model|patio.Model[]` : models of this model. See  [patio.plugins.ClassTableInheritancePlugin](./patio.plugins.ClassTableInheritancePlugin).
* *Object[]?* : [proto.plugins] this can be used to specify additional plugins to use such as
	+ [patio.plugins.TimeStampPlugin](./patio.plugins.TimeStampPlugin)
	+ [patio.plugins.CachePlugin](./patio.plugins.CachePlugin)

*Source*

```
function (table,supers,proto){
    return model.create.apply(model, arguments);
}
```
### configureLogging

Public *Defined index.js*

This can be used to configure logging. If a options hash is passed in then it will passed to the comb.logging.PropertyConfigurator. If the options are omitted then a ConsoleAppender will be added and the level will be set to info.

*Example*
```
var config = {
    "patio" : {
        level : "INFO",
        appenders : [
            {
                type : "RollingFileAppender",
                file : "/var/log/patio.log",
            },
            {
                type : "RollingFileAppender",
                file : "/var/log/patio-error.log",
                name : "errorFileAppender",
                level : "ERROR"
            }
        ]
};
patio.configureLogging(config);
```

*Arguments*
* *opts* :

*Source*
```
function (opts){
   comb.logger.configure(opts);
   if (!opts) {
       LOGGER.level = "info";
   }
}
    
```

### connect

Public *Defined index.js*

*Source*
```
function (){
   return this.createConnection.apply(this, arguments);
}
```

### createConnection

Public *Defined index.js*

Returns a [patio.Database](./patio.Database) object that can be used to for querying.

This method is the entry point for all interactions with a database including getting [patio.Dataset](./patio.Dataset)s for creating queries(see [patio.Database#from](./patio.Database#from)).

The [patio.Database](./patio.Database) returned can also be used to create([patio.Database#createTable](./patio.Database#createTable)), alter([patio.Database#alterTable](patio.Database#alterTable)), rename([patio.Database#renameTable](./patio.Database#renameTable)), and drop([patio.Database#dropTable](./patio.Database#dropTable)) as well as many other [patio.Database](./patio.Database.html) actions.

*Example*

```
//connect using an object
var DB = patio.createConnection({
            host : "127.0.0.1",
            port : 3306,
            type : "mysql",
            maxConnections : 1,
            minConnections : 1,
            user : "test",
            password : "testpass",
            database : 'test'
});
//connect using a connection string
var CONNECT\_STRING = "mysql://test:testpass@localhost:3306/test?maxConnections=1&minConnections=1";
var DB = patio.createConnection(CONNECT\_STRING);

//...do something
DB.createTable("myTable", function(){
    this.name("text");
    this.value("integer");
}).chain(function(){
    //tables created!!!
});
```

*Arguments*
* *options* : the options used to initialize the database connection. This may be a database connetion string or object.
* *[options.maxConnections =  `10`]* `Number` : the number of connections to pool.
* *[options.minConnections =  `3`]* `Number` : the number of connections to pool.
* *[options.type =  `"mysql"`]* `String` : the type of database to communicate with.
* *options.user* `String` : the user to authenticate as.
* *options.password* `String` : the password of the user.
* *options.database* `String` : the name of the database to use, the database specified here is the default database for all connections.

*Source*
```
function (options){
    var ret = Database.connect(options);
    this.emit("connect", ret);
    return ret;
}
```

### disconnect

Public *Defined index.js*

Disconnects all databases in use.

*Arguments*
* *[cb= `null`]* : a callback to call when disconnect has completed
*Returns*
* `comb.Promise` a promise that is resolved once all databases have disconnected.

*Source*
```
function (cb){
    var ret = Database.disconnect(cb), self = this;
    ret.classic(function (err) {
        if (err) {
            self.emit("error", err);
        } else {
            self.emit("disconnect");
        }
    });
    return ret.promise();
}
```

### getModel

Public *Defined index.js*

Returns a model from the name of the table for which the model was created.

```
var TestModel = patio.addModel("test\_model").sync(function(err){
    if(err){
        console.log(err.stack);
    }else{
        var TestModel = patio.getModel("test\_model");
    }
});
```

If you have two tables with the same name in different databases then you can use the db parameter also.

```
var DB1 = patio.createConnection("mysql://test:testpass@localhost:3306/test\_1");
var DB2 = patio.createConnection("mysql://test:testpass@localhost:3306/test\_2");
var Test1 = patio.addModel(DB1.from("test");
var Test2 = patio.addModel(DB2.from("test");

//sync the models
patio.syncModels().chain(function(){
    //now you can use them
    var test1Model = new Test1();
    var test2Model = new Test2();
});
```

*Arguments*
* *name* : the name of the table that the model represents.
* *db?* : optional database in case you have two models with the same table names in
 different databases.

*Source*
```
function (name,db){
    return model.getModel(name, db);
}
```

### logDebug

Public *Defined index.js*

Logs a DEBUG level message to the "patio" logger.

*Source*
```
function (){
    if (LOGGER.isDebug) {
        LOGGER.debug.apply(LOGGER, arguments);
    }
}
```

### logError

Public *Defined index.js*

Logs an ERROR level message to the "patio" logger.

*Source*
```
function (){
    if (LOGGER.isError) {
        LOGGER.error.apply(LOGGER, arguments);
    }
}
```

### logFatal

Public *Defined index.js*

Logs a FATAL level message to the "patio" logger.

*Source*
```
function (){
    if (LOGGER.isFatal) {
        LOGGER.fatal.apply(LOGGER, arguments);
    }
}
```

### logInfo

Public *Defined index.js*

Logs an INFO level message to the "patio" logger.

*Source*
```
function (){
    if (LOGGER.isInfo) {
        LOGGER.info.apply(LOGGER, arguments);
    }
}
```

### logTrace

Public *Defined index.js*

Logs a TRACE level message to the "patio" logger.

*Source*
```
function (){
    if (LOGGER.isTrace) {
        LOGGER.trace.apply(LOGGER, arguments);
    }
}
```
### logWarn

Public *Defined index.js*

Logs a WARN level message to the "patio" logger.

*Source*
```
function (){
    if (LOGGER.isWarn) {
        LOGGER.warn.apply(LOGGER, arguments);
    }
}
```

### migrate

Public *Defined index.js*

Migrates the database using migration files found in the supplied directory.

#### Integer Migrations

Integer migrations are the simpler of the two migrations but are less flexible than timestamp based migrations. In order for patio to determine which versions to use the file names must end in .js where versionNumber is a integer value representing the version number. **NOTE:** With integer migrations missing versions are not allowed.

An example directory structure might look like the following:

```
-migrations
    - createFirstTables.0.js
    - shortDescription.1.js
    - another.2.js
    .
    .
    .
    -lastMigration.n.js
```

In order to easily identify where certain schema alterations have taken place it is a good idea to provide a brief but meaningful migration name.

```
createEmployee.0.js
alterEmployeeNameColumn.1.js
```

#### Timestamp Migrations

Timestamp migrations are the more complex of the two migrations but offer greater flexibility especially with development teams. This is because Timestamp migrations do not require consecutive version numbers, allow for duplicate version numbers(but this should be avoided), keeps track of all currently applied migrations, and it will merge missing migrations. In order for patio to determine the order of the migration files the file names must end in .js where the timestamp can be any form of a time stamp.

```
//yyyyMMdd
20110131
//yyyyMMddHHmmss
20110131123940
//unix epoch timestamp
1328035161
```

as long as it is greater than 20000101 other wise it will be assumed to be part of an integer migration. An example directory structure might look like the following:

```
-migrations
    - createFirstTables.1328035161.js
    - shortDescription.1328035360.js
    - another.1328035376.js
    .
    .
    .
    -lastMigration.n.js
```

In order to easily identify where certain schema alterations have taken place it is a good idea to provide a brief but meaningful migration name.

```
createEmployee.1328035161.js
alterEmployeeNameColumn.1328035360.js
```

**NOTE:** If you start with IntegerBased migrations and decide to transition to Timestamp migrations the patio will attempt the migrate the current schema to the timestamp based migration schema.

In order to run a migraton all one has to do is call patio.migrate(DB, directory, options);

```
var DB = patio.connect("my://connection/string");
patio.migrate(DB, \_\_dirname + "/migrations").chain(function(){
    console.log("migrations finished");
});
```

**Example migration file**
```
//Up function used to migrate up a version
exports.up = function(db) {
    //create a new table
    db.createTable("company", function() {
        this.primaryKey("id");
        this.companyName(String, {size : 20, allowNull : false});
    });
    db.createTable("employee", function(table) {
        this.primaryKey("id");
        this.firstName(String);
        this.lastName(String);
        this.middleInitial("char", {size : 1});
    });
};

//Down function used to migrate down version
exports.down = function(db) {
    db.dropTable("employee", "company");
};
```

*Arguments*
* *db* : the database or connection string to a database to migrate.
* *directory* `String` : directory that the migration files reside in
* *[opts= `{}`]* `Object` : optional parameters.
* *opts.column?* `String` : the column in the table that version information should be stored.
* *opts.table?* `String` : the table that version information should be stored.
* *opts.target?* `Number` : the target migration(i.e the migration to migrate up/down to).
* *opts.current?* `String` : the version that the database is currently at if the current version
is not provided it is retrieved from the database.

*Returns*
* `Promise` a promise that is resolved once the migration is complete.

*Source*
```
function (db){
    db = isString(db) ? this.connect(db) : db;
    var args = argsToArray(arguments);
    args.splice(0, 1);
    return migrate.run.apply(migrate, [db].concat(args));
}
```

### syncModels

Public *Defined index.js*

Helper method to sync all models at once.

*Example*
```
var User = patio.addModel("user");
var Blog = patio.addModel("blog");

//using promise api
patio.syncModels().chain(function(){
    var user = new User();
}, function(error){
    console.log(err);
});

//using a callback

patio.syncModels(function(err){
    if(err){
        console.log(err);
    }else{
        var user = new User();
    }
});
```

*Arguments*
* *cb?* : an optional callback to be invoked when all models have been synced
*Returns*
* `comb.Promise` a promise that will be resolved when the models have been synced.

*Source*
```
function (cb){
    return model.syncModels(cb);
}
```