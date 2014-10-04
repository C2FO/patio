#DDL


##[createTable](./patio_Database.html#createTable)


Patio supports the creation to tables through an instance of [patio.Database](./patio_Database.html#createTable). The [createTable](./patio_Database.html#createTable) method is used by passing it a name of the table to create and a function which performs the actions to create `column`s, `indexe`s, `foreignKey`s, `constraint`s, and `primaryKey`s.

```
DB.createTable("airport", function () {
        this.primaryKey("id");
        this.airportCode(String, {size:4, allowNull:false, unique:true});
        this.name(String, {allowNull:false});
        this.city(String, {allowNull:false});
        this.state(String, {size:2, allowNull:false});
    });
```

Patio by default supports the built in JavaScript types

* `String` : varchar(255)
* `Boolean` : boolean or tinyint(1)
* `Number` : numeric
* `Date` : date

```
DB.createTable("test", function(){
    this.name(String); //=> `name` varchar(255)
    this.num(Number); //=>  `num` numeric
    this.boolean(Boolean); //=> `boolean` tinyint(1)
    this.date(Date); //=> `date` date
});
```

Patio also has a few other built in types that can be used.

* [patio.sql.Time](./patio_sql_Time.html) : time
* [patio.sql.TimeStamp](./patio_sql_TimeStamp.html) : timestamp
* [patio.sql.DateTime](./patio_sql_DateTime.html) - datetime
* [patio.sql.Year](./patio_sql_Year.html) - year</li>
* [patio.sql.Float](./patio_sql_Float.html) - double precision
* [patio.sql.Decimal](./patio_sql_Decimal.html) - double precision

```
var sql = patio.sql;
DB.createTable("test", function () {
    this.timestamp(sql.TimeStamp);
    this.datetime(sql.DateTime);
    this.time(sql.Time);
    this.year(sql.Year);
    this.decimal(sql.Decimal);
    this.float(sql.Float);
});
```

When creating a table there are a number of methods that can be invoked to create the table. For a full reference see [patio.SchemaGenerator](./patio_SchemaGenerator.html).

The most commonly used methods are:

###[column](./patio_SchemaGenerator.html#column)

Add a column to the DDL.  

**Options**

* **key** : For foreign key columns, the column in the associated table that this column references. Unnecessary if this column references the primary key of the associated table.
* **allowNull** : Mark the column as allowing NULL values (if true), or not allowing NULL values (if false). If unspecified, will default to whatever the database default is.
* **onDelete** : Specify the behavior of this column when being deleted. Valid options ("restrict", "cascade", "setNull", "setDefault", "noAction").
* **onUpdate** : Specify the behavior of this column when being updated Valid options ("restrict", "cascade", "setNull", "setDefault", "noAction").
* **primaryKey** : Make the column as a single primary key column. This should only be used if you have a single, non-autoincrementing primary key column.
* **size** : The size of the column, generally used with string columns to specify the maximum number of characters the column will hold. An array of two integers can be provided to set the size and the precision, respectively, of decimal columns.
* **unique** : Mark the column as unique, generally has the same effect as creating a unique index on the column.
* **unsigned** : Make the column type unsigned, only useful for integer columns.
* **elements** : Available items used for set and enum columns.    

```
 DB.createTable("test", function(){
     this.column("num", "integer");
         //=> num INTEGER
     this.column("name", String, {allowNull : false, "default" : "a");
         //=> name varchar(255) NOT NULL DEFAULT 'a'
     this.column("ip", "inet");
         //=> ip inet
 });
```

You can also create columns via method missing, so the following are equivalent:

```
DB.createTable("test", function(){
  this.column("number", "integer");
  //Same as
  this.number("integer");
});
```

###[primaryKey](./patio_SchemaGenerator.html#primaryKey)

Adds an auto-incrementing primary key column or a primary key constraint to the DDL

```
db.createTable("airplane_type", function () {
     this.primaryKey("id");
         //=> id integer NOT NULL PRIMARY KEY AUTOINCREMENT
     this.name(String, {allowNull:false});
     this.created(sql.TimeStamp);
});
```

If you want a primary key that is not an auto-incrementing number use [column](./patio_SchemaGenerator.html#column) instead.

```
DB.createTable("test", function(){
    this.pk("integer", {primaryKey : true}); //Non auto incrementing primary key.
});
DB.createTable("test2", function(){
    this.pk(String, {primaryKey : true}); //varchar(255) primary key.
});
```

If you want a composite primary key pass an array of column names. **Note:** when creating a composite primary key it does not create the columns so you must create those also.

```
DB.createTable("test", function(){
    this.firstName(String);
    this.lastName(String);
    this.primaryKey(["firstName", "lastName"]); //composite key
});
```

###[foreignKey](./patio_SchemaGenerator.html#foreignKey)

Add a foreign key constraint to the DDL.

**Options**

* **deferrable** : Makes the foreign key constraint checks deferrable, so they aren't checked until the end of the transaction.
* **key** : For foreign key columns, the column in the associated table that this column references. Unnecessary if this column references the primary key of the associated table, at least on most databases.
* **onDelete** : Specify the behavior of this column when being deleted. Valid options ("restrict", "cascade", "setNull", "setDefault", "noAction").
* **onUpdate ** : Specify the behavior of this column when being updated. Valid options ("restrict", "cascade", "setNull", "setDefault", "noAction").

```
DB.createTable("flight", function () {
     this.primaryKey("id");
     this.airline(String, {allowNull:false});
});
DB.createTable("airport", function () {
     this.primaryKey("id");
     this.airportCode(String, {size:4, allowNull:false, unique:true});
     this.name(String, {allowNull:false});
     this.city(String, {allowNull:false});
     this.state(String, {size:2, allowNull:false});
});
DB.createTable("flight_leg", function () {
     this.primaryKey("id");
     this.scheduled_departure_time("time");
     this.scheduled_arrival_time("time");
     this.foreignKey("departure_code", "airport", {key:"airport_code", type : String, size : 4});
     this.foreignKey("arrival_code", "airport", {key:"airport_code", type : String, size : 4});
     this.foreignKey("flight_id", "flight", {key:"id"});
});
```

###[index](./patio_SchemaGenerator.html#index)

Adds an index to the the DDL. For single columns, calling index is the same as using the `index` option when creating the column:

**Options**

* **name** : The name of the index (generated based on the table and column names if not provided).
* **type** : The type of index to use (only supported by some databases)
* **unique** : Make the index unique, so duplicate values are not allowed.
* **where** : Create a partial index (only supported by some databases).

```
DB.createTable("a", function(){
    this.id("integer", {index : true});
});
// Same as:
DB.createTable("a", function(){
  this.id("integer");
  this.index("id");
});
```

Similar to the [primaryKey](./patio_SchemaGenerator.html#primaryKey) and [foreignKey](./patio_SchemaGenerator.html#foreignKey) methods, calling index with an array of strings will create a multiple column index:

```
DB.createTable("test", function(){
    this.primaryKey("id");
    this.first_name(String);
    this.last_name(String);
    this.index(["first_name", "last_name"]); //multi-column index
});
```

###[unique](./patio_SchemaGenerator.html#unique)
The unique method creates a unique constraint on the table. A unique constraint generally operates identically to a unique index.

```
DB.createTable("a", function(){
    this.id("integer", {unique : true});
});
// Same as:
DB.createTable("a", function(){
  this.id("integer");
  this.index("id", {unique : true});
});

// Same as:
DB.createTable("a", function(){
  this.id("iteger");
  this.unique("id");
});

```

Just like index, unique can set up a multiple column unique constraint, where the combination of the columns must be unique.

```
DB.createTable("test", function(){
    this.primaryKey("id");
    this.first_name(String);
    this.last_name(String);
    this.unique(["first_name", "last_name"]);
});

```

###[constraint](./patio_SchemaGenerator.html#constraint)

creates a named table constraint:

```
DB.createTable("test", function(){
    this.primaryKey("id");
    this.name(String);
    this.constraint("name_min_length", function(){ 
        return this.char_length(this.name).gt(2)
    });
});
```

Instead of using a block, you can use arguments that will be handled similarly to [patio.Dataset#filter](./patio_Dataset.html#filter):

```
var sql = patio.sql;
DB.createTable("test", function(){
    this.primaryKey("id");
    this.name(String);
    this.constraint("name_min_length",  sql.char_length(sql.name).gt(2));
});
```

###[check](./patio_SchemaGenerator.html#check)

Operates just like [constraint](./patio_SchemaGenerator.html#constraint), except that it doesn't take a name and it creates an unnamed constraint.

```
DB.createTable("test", function(){
    this.primaryKey("id");
    this.name(String);
    this.check(function(){
        return this.char_length(this.name).gt(2)
    });
});
```

##[alterTable](./patio_Database.html#alterTable)

[alterTable](./patio_Database.html#alterTable) is used to alter a tables definition. It is used just like [createTable](./patio_Database.html#createTable) where you use a function to alter the table's definition. For a full reference see [patio.AlterTableGenerator](./patio_AlterTableGenerator.html).

```
 DB.alterTable("xyz", function() {
     this.addColumn("aaa", "text", {null : false, unique : true});
     this.dropColumn("bbb");
     this.renameColumn("ccc", "ddd");
     this.setColumnType("eee", "integer");
     this.setColumnDefault("hhh", 'abcd');
     this.addIndex("fff", {unique : true});
     this.dropIndex("ggg");
});
```

###[addColumn](./patio_AlterTableGenerator.html#addColumn)

This method adds a column to the table. This method is similar to `createTable`'s column method where the first parameter is the column and the second parameter is the data type and third parameter an optional options hash

```
 DB.alterTable("test", function(){
     this.addColumn("num", "integer");
     this.addColumn("name", String, {allowNull : false, "default" : "a");
     this.addColumn("ip", "inet");
 });
```

###[dropColumn](./patio_AlterTableGenerator.html#dropColumn)

This method removes a column from the table definition.

```
DB.alterTable("test", function(){
     this.dropColumn("num");
     this.dropColumn("name");
     this.dropColumn("ip");
 });
```

###[renameColumn](./patio_AlterTableGenerator.html#renameColumn)

This method renames a column.

```
DB.alterTable("test", function(){
     this.renameColumn("num", "number");
     this.renameColumn("name", "first_name");
     this.renameColumn("ip", "ip_address");
 });
```

###[addPrimaryKey](./patio_AlterTableGenerator.html#addPrimaryKey)
 
This method is used to add a primaryKey to a table incase you forgot to include a primaryKey when creating the table.

```
DB.alterTable("test", function(){
     this.addPrimaryKey("id");     
 });        
```

Just like `createTable`'s primaryKey method if you provide an array of columns to use it will not create the columns but, add a composite primaryKey.

```
DB.alterTable("test", function(){
    this.addPrimaryKey(["first_name", "last_name"]); //composite key
});
```

If you just want to take an existing single column and make it a primary key, call addPrimaryKey with an array of one element:

```
DB.alterTable("test", function(){
    this.addPrimarykey(["id"]);
});
```

###[addForeignKey](./patio_AlterTableGenerator.html#addForeignKey)

This method is used to add a foreign key to a table. Like when using `addPrimaryKey` if you pass a string as the first argument then a column will be created.


```
DB.alterTable("test", function(){
     this.addForeignKey("test2_id", "test2");
        //=>ADD COLUMN test2_id integer REFERENCES test2
 });
```

Just like `createTable`'s foreignKey method if you provide an array of columns to use it will not create the columns but, add a composite foreignKey.

```
DB.alterTable("test", function(){
    this.addForeignKey(["first_name", "last_name"], "users"); //composite key
});
```

If you just want to take an existing single column and make it a foreign key, call addForeignKey with an array of one element:

```
DB.alterTable("test", function(){
    this.addForeignKey(["test2_id"], "test2");
});
```

###[addIndex](./patio_AlterTableGenerator.html#addIndex)

Just like `createTable`'s index method.

```
DB.alterTable("table", function(){
  this.addIndex("first_name");
});
```

Just like `createTable`'s index method you can create a composite key by passing in an array of column names.

```
DB.alterTable("test", function(){
    this.addIndex(["first_name", "last_name"]);
});
```

###[dropIndex](./patio_AlterTableGenerator.html#dropIndex)

Drops an index from a table.

```
DB.alterTable("test", function(){
    this.dropIndex("first_name");
});
```

To drop an index with a custom name use the name option.

```
DB.alterTable("test", function(){
    this.dropIndex("first_name", {name : "first_name_index"});
});
```

###[addConstraint](./patio_AlterTableGenerator.html#addConstraint)

Adds a named constraint to a table. Just like `createTable`'s constraint method.

```
DB.alterTable("test", function(){
    this.addConstraint("name_min_length", function(){
        return this.char_length(this.name).gt(2);
    });
});
```

**Note:** there is not a method to add an unnamed constraint when altering a table.


###[addUniqueConstraint](./patio_AlterTableGenerator.html#addUniqueConstraint)

Adds a unique constraint to a table. Just like `createTable`'s unique method.

```
DB.alterTable("test", function(){
    this.addUniqueConstraint("name");
});
```

###[dropConstraint](./patio_AlterTableGenerator.html#dropConstraint)

Drops a named constraint from a table.

```
DB.alterTable("albums", function(){
    this.dropConstraint("name_min_length");
});
```

On MySQL you specify the type of constraint you are dropping.

```
DB.alterTable("albums", function(){
  this.dropConstraint("albums_pk", {type : "primaryKey"});
  this.dropConstraint("albums_fk", {type : "foreignKey"});
  this.dropConstraint("albums_uk", {type : "unique"});
});
```

###[setColumnDefault](./patio_AlterTableGenerator.html#setColumnDefault)

Sets a columns default value.

```
DB.alterTable("test", function(){
    this.setColumnDefault("first_name", "John");
    this.setColumnDefault("last_name", "Doe");
});
```

###[setColumnType](./patio_AlterTableGenerator.html#setColumnType)

Sets the columns type.

```
DB.alterTable("test", function(){
    this.setColumnType("first_name", "char(10)");
});
```

###[setAllowNull](./patio_AlterTableGenerator.html#setAllowNull)

Changes the NULL/NOT NULL modifier of a column.

```
DB.alterTable("test", function(){
    this.setAllowNull("first_name", false); //NOT NULL
    this.setAllowNull("last_name", true); //NULL
});
```

##[patio.Database](./patio_Database.html) modification methods

[patio.Database](./patio_Database.html) has methods that act as shortcuts to an [alterTable](./patio_Database.html#alterTable) call these methods include

* [addColumn](./patio_Database.html#addColumn)
* [dropColumn](./patio_Database.html#dropColumn)
* [renameColumn](./patio_Database.html#renameColumn)
* [addIndex](./patio_Database.html#addIndex)
* [dropIndex](./patio_Database.html#dropIndex)
* [setColumnDefault](./patio_Database.html#setColumnDefault)
* [setColumnType](./patio_Database.html#setColumnType)

These methods are useful when your only performing a couple of modifications at a time.

```
DB.alterTable("test", function(){
    this.addColumn("num", "integer");
});

//same as
DB.addColumn("test", "num", "integer");
```

##Tables

###[dropTable](./patio_Database.html#dropTable)

Can drop either a single table or multiple tables at a time.

```
DB.dropTable(["leg_instance", "flight_leg", "flight", "airplane", "can_land", "airplane_type", "airport"]);
//same as
DB.dropTable("leg_instance", "flight_leg", "flight", "airplane", "can_land", "airplane_type", "airport");
//OR one table
DB.dropTable("leg_instance");
```

###[renameTable](./patio_Database.html#renameTable)

Renames an existing table.

```
DB.renameTable("test", "test_old");
```

###[forceCreateTable](./patio_Database.html#forceCreateTable)

Forcibly creates a table, attempting to drop it unconditionally (and catching any errors), then creating it. **Note:** This should not be used within a transaction as it could cause the transaction to fail.

```
DB.forceCreateTable("test", function(){
    this.primaryKey("id);
    this.first_name(String);
    this.last_name(String);
    this.date_of_birth(Date);
});
```

###[createTableUnlessExists](./patio_Database.html#createTableUnlessExists)

Creates the table unless the table already exists.

```
DB.createTableUnlessExists("test", function(){
    this.primaryKey("id);
    this.first_name(String);
    this.last_name(String);
    this.date_of_birth(Date);
});
```

##Views

###[createView](./patio_Database.html#createView)

Creates a view based on a dataset or an SQL string:

```
//CREATE VIEW cheapItems AS SELECT * FROM items WHERE price < 100
DB.createView("cheapItems", "SELECT * FROM items WHERE price < 100");

//CREATE  VIEW miscItems AS SELECT * FROM items WHERE category = 'misc'
DB.createView("miscItems", DB[:items].filter({category : 'misc'}));
```

###[createOrReplaceView](./patio_Database.html#createOrReplaceView)

Same as create view but replaces the view if it already exists.

```
//CREATE VIEW cheapItems AS SELECT * FROM items WHERE price < 100
DB.createOrReplaceView("cheapItems", "SELECT * FROM items WHERE price < 100");

//CREATE  VIEW miscItems AS SELECT * FROM items WHERE category = 'misc'
DB.createOrReplaceView("miscItems", DB[:items].filter({category : 'misc'}));
```

###[dropView](./patio_Database.html#dropView)

Similar to `dropTable` but instead of a table it drops a view.

```
DB.dropView("test_view");
    //=>'DROP VIEW test_view'
DB.dropTable("test_view_1", "test_view_2", "test_view_3");
    //=>'DROP VIEW test_view_1',
    //=>'DROP VIEW test_view_2',
    //=>'DROP VIEW test_view_3'
```