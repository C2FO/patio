
#Connecting to a database


##[patio.createConnection](./patio.html#createConnection)


When using [patio.createConnection](./patio.html#createConnection) to connect a database there are two types of parameters you can use.

###Connetion URI

This is a well formed URI that will be used to connect to the database.

**Currently supported databases**

* `pg` : example `pg://test:testpass@localhost:5432/test`
* `mysql` : example `mysql://test:testpass@localhost:3306/test`

**Query options**

 * `maxConnections` : maximum number of connections to keep in the pool
 * `minConnections` : minimum number of connections to keep in the pool
 
**Note:** All other options will be passed to the underlying driver see the driver documentation for additional options.

* [mysql](https://github.com/felixge/node-mysql)
* [postgres](https://github.com/brianc/node-postgres)


```
//Create a connection to a mysql database at localhost port 3306, with the username test, password test and
// 1 connection by default and a max of 10
var DB = patio.createConnection("mysql://test:testpass@localhost:3306/test?maxConnections=1&minConnections=10");    
```

###Connection Object

This is an object to used to connect.

**Currently supported databases**

* `pg` : example `pg://test:testpass@localhost:5432/test`
* `mysql` : example `mysql://test:testpass@localhost:3306/test`

**Options**

 * `type` 
   * `pg` : use postgres adapter
   * `mysql` : use mysql adapter
 * `host` : host of the database
 * `port` : port the database is accepting connections on
 * `user` : user to connect to the database as
 * `password` : password
 * `database` : database (`schema`) to connect to
 * `maxConnections` : maximum number of connections to keep in the pool
 * `minConnections` : minimum number of connections to keep in the pool
 
**Note:** All other options will be passed to the underlying driver see the driver documentation for additional options.

* [mysql](https://github.com/felixge/node-mysql)
* [postgres](https://github.com/brianc/node-postgres)

```
//connect using an object
var DB = patio.createConnection({
             host : "localhost",
             port : 3306,
             type : "mysql",
             maxConnections : 10,
             minConnections : 1,
             user : "test",
             password : "testpass",
             database : 'test'
});
    //Create a connection to a mysql database at localhost port 3306, with the username test, password test and
    // 1 connection by default and a max of 10
```

##Disconnecting

To disconnect from a database you can use:

* [patio.disconnect](./patio.html#disconnect) which disconnects all databases currently connected.

```
patio.disconnect().chain(function(){
    //all databases are disconnected all queued queries have finished
});
```

* [patio.Database#disconnect](./patio_Database.html#disconnect) which disconnects only that database.

```
DB.disconnect().chain(function(){
    //database is disconnected and all queued queries have finished
});
```                
