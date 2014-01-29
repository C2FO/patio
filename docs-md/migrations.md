
        
#Migrations


Migrates the database using migration files found in the supplied directory.

##Integer Migrations

Integer migrations are the simpler of the two migrations but are less flexible than timestamp based migrations. In order for patio to determine which versions the file must have the naming format "{versionnumber}.someFileName.js" where versionNumber is a integer **starting at 0** representing the version number.

**NOTE:** The reason the version number comes first is that most file viewers and IDE's sort files alphabetically and this makes for quicker migration lookup.

**NOTE:** With integer migrations missing versions are not allowed.

An example directory structure might look like the following:

```
-migrations
     - 0.createFirstTables.js
     - 1.shortDescription.js
     - 2.another.js
     .
     .
     .
     -n.lastMigration.js
```

In order to easily identify where certain schema alterations have taken place it is a good idea to provide a brief but meaningful migration name.

```
0.createEmployee.js
1.alterEmployeeNameColumn.js
```


##Timestamp Migrations

Timestamp migrations are the more complex of the two migrations but offer greater flexibility especially with development teams. This is because Timestamp migrations:

* Do not require consecutive version numbers
* Allow for duplicate version numbers(but this should be avoided)
* Keep track of all currently applied migrations
* Will merge missing migrations.

In order for patio to determine the order of the migration files must follow the naming format of {timestamp}.someFileName.js where the timestamp can be any form of a time stamp.

```
 //yyyyMMdd
 20110131
 //yyyyMMddHHmmss
 20110131123940
 //unix epoch timestamp
 1328035161
```

An example directory structure might look like the following:

```
-migrations
     - 1328035161.createFirstTables.js
     - 1328035360.shortDescription.js
     - 1328035376.another.js
     .
     .
     .
     -n.lastMigration.js
```

In order to easily identify where certain schema alterations have taken place it is a good idea to provide a brief but meaningful migration name.

```
 1328035161.createEmployee.js
 1328035360.alterEmployeeNameColumn.js
```



##Mixed Migrations

If you start with IntegerBased migrations and decide to transition to Timestamp migrations the patio will attempt to merge the two migrations. It does this by ordering the files based off of the versionNumber order. So Integerbase migrations will come first. If the version number in any file with a version number lower than or equal to the current verion will be inserted into the schema_migration table and and file with a version greater will be applied up the the supplied target, or greatest version number and will be inserted into the database.

##Migration Files

Migration files are files with an up and down method.
                
###exports.up

The up function is called when applying the migration. The up function typically contains create and alter table statements. See [DDL](./DDL.html) for more examples and operations that are available.


###exports.down

The down function is called when rolling back the migration. The down function typically contains drop and alter table statements. See [DDL](./DDL.html) for more examples and operations that are available.

##Example migration file

```
var comb = require("comb"),
  	when = comb.when;
  	
 //Up function used to migrate up a version
 exports.up = function(db) {
   //return a proiomise to ensure that the migration knows when the actions are done.
   return comb.when(
      //create a company table
        db.createTable("company", function() {
            this.primaryKey("id");
            this.companyName(String, {size : 20, allowNull : false});
        }),
        //create employee table
        db.createTable("employee", function(table) {
            this.primaryKey("id");
            this.firstName(String);
            this.lastName(String);
            this.middleInitial("char", {size : 1});
        })
    );
};

//Down function used to migrate down version
exports.down = function(db) {
    return db.dropTable("employee", "company");
};
```

While it will not break any thing you should always return a promise from the up and down functions. If you perform a single database altering action (i.e. create/alter/drop table) you can just return that action. If you perform multiple then you can wrap it in a `comb.when` call.

```
var comb = require("comb"),
	when = comb.when;
	  
//Up function used to migrate up a version
 exports.up = function(db) {
   //create a new table
   return comb.when(
        db.createTable("company", function() {
            this.primaryKey("id");
            this.companyName(String, {size : 20, allowNull : false});
        }),
        db.createTable("employee", function(table) {
            this.primaryKey("id");
            this.firstName(String);
            this.lastName(String);
            this.middleInitial("char", {size : 1});
        }),
        db.alterTable("works", function(){
            this.addForeignKey("employeeId", "employee", {key : "id"});
        });
    );
};
```

`comb.when` will return a Promise that will resolve once all three database actions have completed.

If you perform more complex actions you may need to should use the Promise api. Consider the following example.

```
 //Up function used to migrate up a version
exports.up = function(db) {
    //create a new table
    return comb.when(
        db.renameTable("employees", "employeesOld"),
        db.createTable("employees", function(table){
            this.primaryKey("id");
            this.firstName(String);
            this.lastName(String);
            this.hireDate(Date);
            this.middleInitial("char", {size:1});
        })
    ).chain(function(){
        return db.from("employeesOld").map(function(employee){
                return comb.merge(employee, {hireDate:new Date()});
        }).chain(function(employees){
                return db.from("employees").multiInsert(employees)
            }, ret);
    });
};

 //Down function used to migrate down version
exports.down = function(db) {
    db.alterTable("employee", function(){
        this.dropColumn("hireDate");
    });
};
```

First we wait for both the rename and createTable action to complete, and then we perform some database actions and finally resolve.

##Running migrations


In order to run a migraton there are two options:

###[patio.migrate](./patio.html#migrate)

**Available options**
                     
* **column** : the column in the table that version information should be stored.
* **table** : the table that version information should be stored.</li>
* **target** : the target migration(i.e the migration to migrate up/down to). **NOTE:** If you are using integer based migrations you must specify your target to `-1` in order to roll all the way back.

**Integer migrator options**

* **current** : the version that the database is currently at if the current version is not provided it is retrieved from the database.

```
  var DB = patio.connect("my://connection/string");
  patio.migrate(DB, __dirname + "/migrations").chain(function(){
      console.log("migrations finished");
  }, errorHandler);
```

###Patio Executable

patio comes with an executable script that can be used to run the migrations

```
patio -u some://connection/string -d ./migrations/
```

**script options**

* **-d, --directory** : Directory of migrations
* **-u, --uri** : connection uri
* **-t, --target** : target migration version
* **-c, --current** : current migration version
* **-tb, --table** : table to store schema information in
* **-C, --column** : column to store schema information in.
* **-v, --verbose** : set logging to debug
* **-q, --quiet** : turn all logging off.
* **-r, --rollback** : roll all migrations back
* **--camelize** : force camel casing
* **-us, --underscore** : force underscore


                