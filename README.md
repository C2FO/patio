[![Build Status](https://travis-ci.org/C2FO/patio.svg?branch=master)](https://travis-ci.org/C2FO/patio)
[![Coverage Status](https://coveralls.io/repos/C2FO/patio/badge.svg?branch=master)](https://coveralls.io/r/C2FO/patio?branch=master)

[![NPM](https://nodei.co/npm/patio.png?downloads=true)](https://nodei.co/npm/patio/)

#[Patio](http://c2fo.github.com/patio)

Patio is a <a href="http://sequel.rubyforge.org/" target="patioapi">Sequel</a> inspired query engine.

###Installation
To install patio run

`npm install comb patio`

If you want to use the patio executable for migrations

`npm install -g patio`

###Running Tests

To run the tests 

```
make test
```

To run just the postgres tests

```
make test-pg
```

To run just the mysql tests

```
make test-mysql
```

###Why Use Patio?
                                                                                                                                                             
Patio is different because it allows the developers to choose the level of abtraction they are comfortable with.                                             

If you want to use the [ORM](http://c2fo.github.com/patio/models.html) functionality you can. If you don't you can just use the [Database](http://c2fo.github.com/patio/DDL.html) and [Datasets](http://c2fo.github.com/patio/querying.html) as a querying API, and if you need to you can [write plain SQL](http://c2fo.github.com/patio/patio_Database.html#run)

###Concepts

1. Model definitions are defined by the tables in the database.

    As you add models the definition is automatically defined from the table definition. This is particularly useful when you want to define your model from a schema designed using another tool (i.e. ActiveRecord, Sequel, etc...)

2. Patio tries to stay out of your way when querying.

    When you define a model you still have the freedom to do any type of query you want.

    Only want certain columns?

    ```javascript
    MyModel.select("id", "name", "created").forEach(function(record){
        //record only has the id, name, and created columns
    });
    ```

    You want to join with another table?

    ```javascript
    MyModel.join("otherTable", {id: patio.sql.identifier("myModelId"}).forEach(function(record){
        //Record has columns from your join table now!
    });
    ```

    You want to run raw SQL?

    ```javascript
    MyModel.db.run("select * from my_model where name = 'Bob'").all().chain(function(records){
        //all records with a name that equals bob.
    });
    ```

    You want to just query the database and not use a model?

    ```javascript
    var DB = patio.connect("pg://test:test@127.0.0.1:5432/test_db");
    DB.from("myTable").filter({id: [1,2,3]}).all().function(records){
       //records with id IN (1,2,3)
    });
    ```

                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
###Getting Started

All the code for this example can be found [here](https://github.com/C2FO/patio/tree/master/example/readme-example)

1. Create a new database

    PostgreSQL
    ```
    psql -c "CREATE DATABASE reademe_example"
    ```

    MySQL

    ```
    mysql -e "CREATE DATABASE readme_example"
    ```

2. Create a migration

    ```
    mkdir migration
    patio migration-file -n createInitialTables ./migration
    ```

    This will add a migration name `createdInitialTables` in your migration directory.

2. Add the following code to your migration

    ```javascript
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
    ```

3. Run your migration

    ```
    patio migrate -v --camelize -u "<DB_CONNECTION_STRING>" -d ./migration
    ```


4. Connect and query!
                                                                                                                      
    ```javascript
    var patio = require("patio");

    //set camelize = true if you want snakecase database columns as camelcase
    patio.camelize = true;
    patio.connect("pg://postgres@127.0.0.1:5432/readme_example");

    //define a State model with a relationship to capital
    var State = patio.addModel("state").oneToOne("capital");

    //define a Capital model with a relationship to State
    var Capital = patio.addModel("capital").manyToOne("state");

    //save a state
    State
        .save({
            name: "Nebraska",
            population: 1796619,
            founded: new Date(1867, 2, 4),
            climate: "continental",
            //notice the capital relationship is inline
            capital: {
                name: "Lincoln",
                founded: new Date(1856, 0, 1),
                population: 258379
            }
        })
        .chain(function () {
            //save a Capital
            return Capital.save({
                name: "Austin",
                founded: new Date(1835, 0, 1),
                population: 790390,
                //define the state inline
                state: {
                    name: "Texas",
                    population: 25674681,
                    founded: new Date(1845, 11, 29)
                }
            });
        })
        .chain(function () {
            //Query all the states by name
            return State.order("name").forEach(function (state) {
                //Get the associated capital
                return state.capital.chain(function (capital) {
                    console.log("%s's capital is %s.", state.name, capital.name);
                });
            });
        })
        .chain(process.exit, function (err) {
            console.log(err)
            process.exit(1);
        });
    ```

###Guides

* [Migrations](http://c2fo.github.com/patio/migrations.html)
* [Models](http://c2fo.github.com/patio/models.html)
    * [Associations](http://c2fo.github.com/patio/associations.html)
    * [Inheritance](http://c2fo.github.com/patio/model-inheritance.html)
    * [Validation](http://c2fo.github.com/patio/validation.html)
    * [Plugins](http://c2fo.github.com/patio/plugins.html)
* [Querying](http://c2fo.github.com/patio/querying.html)
* [DDL](http://c2fo.github.com/patio/DDL.html)
* [Logging](http://c2fo.github.com/patio/logging.html)

###Features

* Comprehensive documentation with examples.
* &gt; 80% test coverage
* Support for connection URIs
* Supported Databases
    * MySQL
    * Postgres
    * Redshift
* [Models](http://c2fo.github.com/patio/models.html)
    * [Associations](http://c2fo.github.com/patio/associations.html)
    * [Inheritance](http://c2fo.github.com/patio/model-inheritance.html)
    * [Validation](http://c2fo.github.com/patio/validation.html)
    * [Plugins](http://c2fo.github.com/patio/plugins.html)
* Simple adapter extensions
* [Migrations](http://c2fo.github.com/patio/migrations.html)
    * Integer and Timestamp based.
* Powerful [Querying](http://c2fo.github.com/patio/querying.html) API
* [Transactions](http://c2fo.github.com/patio/patio_Database.html#transaction) with
    * Savepoints
    * Isolation Levels
    * Two phase commits
* SQL Datatype casting
* Full database CRUD operations
    * [createTable](http://c2fo.github.com/patio/patio_Database.html#createTable)
    * [alterTable](http://c2fo.github.com/patio/patio_Database.html#alterTable)
    * [dropTable](http://c2fo.github.com/patio/patio_Database.html#dropTable)
    * [insert](http://c2fo.github.com/patio/patio_Dataset.html#insert)
    * [multiInsert](http://c2fo.github.com/patio/patio_Dataset.html#multiInsert)
    * [update](http://c2fo.github.com/patio/patio_Dataset.html#update)
    * [remove](http://c2fo.github.com/patio/patio_Dataset.html#remove)
    * [query](http://c2fo.github.com/patio/patio_Dataset.html#filter)



