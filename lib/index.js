/**
 * @projectName patio
 * @github https://github.com/C2FO/patio
 * @includeDoc [Connecting] ../docs-md/connecting.md
 * @includeDoc [Models] ../docs-md/models.md
 * @includeDoc [Associations] ../docs-md/associations.md
 * @includeDoc [Model Inheritance] ../docs-md/model-inheritance.md
 * @includeDoc [Model Validation] ../docs-md/validation.md
 * @includeDoc [Model Plugins] ../docs-md/plugins.md
 * @includeDoc [Querying] ../docs-md/querying.md
 * @includeDoc [DDL] ../docs-md/DDL.md
 * @includeDoc [Migrations] ../docs-md/migrations.md
 * @includeDoc [Logging] ../docs-md/logging.md
 * @includeDoc [Change Log] ../History.md
 * @includeDoc [Test Coverage] [../docs-md/coverage.html]
 *
 * @header
 * [![Build Status](https://secure.travis-ci.org/C2FO/patio.png)](http://travis-ci.org/C2FO/patio)
 *
 * Patio is a <a href="http://sequel.rubyforge.org/" target="patioapi">Sequel</a> inspired query engine.
 *
 * <h3>Why Use Patio?</h3>
 *
 * <p>
 * Patio is different because it allows the developers to choose the level of abtraction they are comfortable with.
 * </p>
 * <p>
 * If you want to use <a href="./models.html">ORM</a> functionality you can. If you dont you can just use the
 * <a href="./DDL.html">Database</a> and <a href="./querying.html">Datasets</a> as a querying API, and if you need to
 * you can <a href="./patio_Database.html#run">write plain SQL</a>.
 * </p>
 *
 *
 * ###Getting Started
 *
 * To install patio run
 *
 * `npm install comb patio`
 *
 * If you want to use the patio executable for migrations
 *
 * `npm install -g patio`
 *
 *
 * Create some tables.
 *
 * ```
 * var patio = require("patio"),
 * comb = require("comb"),
 * when = comb.when,
 * serial = comb.serial;
 *
 *
 * //set all db name to camelize
 * patio.camelize = true;
 * patio.configureLogging();
 * //connect to the db
 * var DB = patio.connect(<CONNECTION_URI>);
 *
 * function errorHandler(error) {
 *      console.log(error);
 *      patio.disconnect();
 * };
 *
 * function createTables() {
 *      return comb.serial([
 *           function () {
 *                return DB.forceDropTable(["capital", "state"]);
 *           },
 *           function () {
 *                return DB.createTable("state", function () {
 *                     this.primaryKey("id");
 *                     this.name(String)
 *                     this.population("integer");
 *                     this.founded(Date);
 *                     this.climate(String);
 *                     this.description("text");
 *                });
 *           },
 *           function () {
 *                return DB.createTable("capital", function () {
 *                     this.primaryKey("id");
 *                     this.population("integer");
 *                     this.name(String);
 *                     this.founded(Date);
 *                     this.foreignKey("stateId", "state", {key:"id"});
 *                });
 *           }
 *      ]);
 * };
 *
 * createTables().then(function () {
 *      patio.disconnect();
 * }, errorHandler);
 * ```
 *
 * Next lets create some models for the tables created.
 *
 * ```
 * var State = patio.addModel("state").oneToOne("capital");
 * var Capital = patio.addModel("capital").manyToOne("state");
 * ```
 *
 * Next you'll need to sync your models. **Note**: The sync operation returns a promise.
 *
 * ```
 * patio.syncModels();
 * ```
 *
 * Save some data to query.
 *
 * ```
 * //comb.when waits for the save operation to complete
 * return comb.when(
 *      State.save({
 *           name:"Nebraska",
 *           population:1796619,
 *           founded:new Date(1867, 2, 4),
 *           climate:"continental",
 *           capital:{
 *                name:"Lincoln",
 *                founded:new Date(1856, 0, 1),
 *                population:258379
 *          }
 *      }),
 *      Capital.save({
 *           name:"Austin",
 *           founded:new Date(1835, 0, 1),
 *           population:790390,
 *           state:{
 *                name:"Texas",
 *                population:25674681,
 *                founded:new Date(1845, 11, 29)
 *           }
 *      })
 * );
 * ```
 *
 * Now we can query the states and capitals we created.
 *
 * ```
 * State.order("name").forEach(function (state) {
 *      //if you return a promise here it will prevent the foreach from
 *      //resolving until all inner processing has finished.
 *      return state.capital.then(function (capital) {
 *          console.log("%s's capital is %s.", state.name, capital.name);
 *      });
 * });
 * ```
 *
 * ```
 * Capital.order("name").forEach(function (capital) {
 *      //if you return a promise here it will prevent the foreach from
 *      //resolving until all inner processing has finished.
 *      return capital.state.then(function (state) {
 *          console.log(comb.string.format("%s is the capital of %s.", capital.name, state.name));
 *      });
 * });
 * ```
 */






var Dataset = require("./dataset"),
    Database = require("./database"),
    adapters = require("./adapters"),
    EventEmitter = require("events").EventEmitter,
    PatioError = require("./errors").PatioError,
    migrate = require("./migration"),
    model = require("./model"),
    Model = model.Model,
    plugins = require("./plugins"),
    comb = require("comb-proxy"),
    Time = require("./time"),
    date = comb.date,
    SQL = require("./sql").sql,
    hitch = comb.hitch,
    Promise = comb.Promise,
    PromiseList = comb.PromiseList,
    singleton = comb.singleton,
    isFunction = comb.isFunction,
    executeInOrder = comb.executeInOrder,
    argsToArray = comb.argsToArray,
    isString = comb.isString;

var LOGGER = comb.logger("patio");

var Patio = singleton([EventEmitter, Time], {
    instance:{
        /**
         * @lends patio.prototype
         */

        __camelize:false,

        __underscore:false,

        __inImportOfModels:false,


        /**
         * A singleton class that acts as the entry point for all actions performed in patio.
         *
         * @example
         *
         * var patio = require("patio");
         *
         * patio.createConnection(....);
         *
         * patio.camelize = true;
         * patio.quoteIdentifiers=false;
         *
         * patio.createModel("my_table");
         *
         *
         *  //CHANGING IDENTIFIER INPUT METHOD
         *
         *
         *  //use whatever is passed in
         *   patio.identifierInputMethod = null;
         *  //convert to uppercase
         *  patio.identifierInputMethod = "toUpperCase";
         *  //convert to camelCase
         *  patio.identifierInputMethod = "camelize";
         *  //convert to underscore
         *  patio.identifierInputMethod = "underscore";
         *
         *
         *  //CHANGING IDENTIFIER OUTPUT METHOD
         *
         *  //use whatever the db returns
         *   patio.identifierOutputMethod = null;
         *  //convert to uppercase
         *  patio.identifierOutputMethod = "toUpperCase";
         *  //convert to camelCase
         *  patio.identifierOutputMethod = "camelize";
         *  //convert to underscore
         *  patio.identifierOutputMethod = "underscore";
         *
         *  //TURN QUOTING OFF
         *   patio.quoteIdentifiers = false
         *
         * @constructs
         * @augments patio.Time
         * @param options
         */
        constructor:function () {
            this._super(arguments);
            var constants = SQL.Constants;
            for (var i in constants) {
                this[i] = constants[i];
            }
        },

        /**
         * Returns a {@link patio.Database} object that can be used to for querying.
         *
         * <p>This method is the entry point for all interactions with a database including getting
         * {@link patio.Dataset}s for creating queries(see {@link patio.Database#from}).
         * </p>
         *
         * <p>The {@link patio.Database} returned can also be used to create({@link patio.Database#createTable}),
         * alter(@link patio.Database#alterTable}), rename({@link patio.Database#renameTable}), and
         * drop({@link patio.Database#dropTable}) as well as many other {@link patio.Database} actions.
         * </p>
         *
         * @example
         *
         * //connect using an object
         * var DB = patio.createConnection({
         *              host : "127.0.0.1",
         *              port : 3306,
         *              type : "mysql",
         *              maxConnections : 1,
         *              minConnections : 1,
         *              user : "test",
         *              password : "testpass",
         *              database : 'test'
         * });
         * //connect using a connection string
         * var CONNECT_STRING = "mysql://test:testpass@localhost:3306/test?maxConnections=1&minConnections=1";
         * var DB = patio.createConnection(CONNECT_STRING);
         *
         * //...do something
         * DB.createTable("myTable", function(){
         *    this.name("text");
         *    this.value("integer");
         * }).then(function(){
         *     //tables created!!!
         * });
         *
         * @param {String|Object} options the options used to initialize the database connection.
         *                        This may be a database connetion string or object.
         * @param {Number} [options.maxConnections = 10] the number of connections to pool.
         * @param {Number} [options.minConnections = 3] the number of connections to pool.
         * @param {String} [options.type = "mysql"] the type of database to communicate with.
         * @param {String} options.user the user to authenticate as.
         * @param {String} options.password the password of the user.
         * @param {String} options.database the name of the database to use, the database
         *                                   specified here is the default database for all connections.
         */
        createConnection:function (options) {
            var ret = Database.connect(options);
            this.emit("connect", ret);
            return ret;
        },

        /**
         *  @see patio#createConnection
         */
        connect:function () {
            return this.createConnection.apply(this, arguments);
        },

        /**
         * This method allows one to connect to a database and immediately execute code.
         * For connection options @see patio#createConnection
         *
         * @example
         *
         *
         * var DB;
         * var CONNECT_STRING = "dummyDB://test:testpass@localhost/dummySchema";
         * var connectPromise = patio.connectAndExecute(CONNECT_STRING, function (db) {
         *      db.dropTable("test");
         *      db.createTable("test", function () {
         *          this.primaryKey("id");
         *          this.name(String);
         *          this.age(Number);
         *      });
         * });
         *
         * connectPromise.then(function (db) {
         *      DB = db;
         *      //do more stuff!
         *  });
         *
         * @param {String|Object} options @see patio#createConnection
         * @param {Function} cb the function to callback once connected.
         *
         * @returns {comb.Promise} a promise that is resolved once the database execution has finished.
         */
        connectAndExecute:function (options, cb) {
            if (!isFunction(cb)) {
                throw new PatioError("callback must be a function");
            }
            var db = this.createConnection.apply(this, arguments);
            return executeInOrder(db, patio, function (db, patio) {
                cb(db, patio);
                return db;
            });
        },

        /**
         * Disconnects all databases in use.
         *
         * @param {Function} [cb=null] a callback to call when disconnect has completed
         * @return {comb.Promise} a promise that is resolved once all databases have disconnected.
         */
        disconnect:function (cb) {
            var ret = Database.disconnect(cb);
            ret.classic(hitch(this, function (err) {
                if (err) {
                    this.emit("error", err);
                } else {
                    this.emit("disconnect");
                }
            }));
            return ret.promise();
        },

        /**
         * Allows for the importing of multiple models so you do not have to worry about the promise that is returned from create model,
         * or a directory if an index.js file is used.
         *
         * <p>
         *     To import a group of model files you can do the following:
         * {@code
         * patio.import(__dirname + "/models/Flight.js",
         *              __dirname + "/models/Airport.js",
         *              __dirname + "/models/Airplane.js").then(function(Flight, Airport, Airplane){
         *                  //...
         *              });
         *
         * patio.import(__dirname + "/models/Flight.js",
         *              __dirname + "/models/Airport.js",
         *              __dirname + "/models/Airplane.js").then(function(){
         *                  var Flight = patio.getModel("flight"),
         *                      Airport = patio.getModel("airport"),
         *                      Airplane = patio.getModel("airplane");
         *              });
         *      </pre>
         *     Another approach is to create an index.js file inside of a directory that requires all of the models needed.
         *     then use {@link patio#import} on that file.
         *     <pre class="code">
         * patio.import(__dirname + "/models").then(function(){
         *      var Flight = patio.getModel("flight"),
         *          Airport = patio.getModel("airport"),
         *          Airplane = patio.getModel("airplane");
         * });
         * }
         * </p>
         *
         * @param files a single or list of files to import.
         * @param {Function} [cb] an optional callback to call when importing is complete
         *
         * @return {Promise} returns a promise that will be resolved when all models in the imported files
         * have been loaded.
         */
        "import":function (files, cb) {
            files = argsToArray(arguments);
            if (isFunction(files[files.length])) {
                cb = files.pop();
            } else {
                cb = null;
            }
            files.map(function (file) {
                require(file);
            });
            return model.syncModels(cb).promise();
        },

        /**
         * This method is used to create a {@link patio.Model} object.
         *
         * @example
         * var Flight = patio.addModel("flight", {
         *      instance:{
         *              toObject:function () {
         *                  var obj = this._super(arguments);
         *                  obj.weekdays = this.weekdaysArray;
         *                  obj.legs = this.legs.map(function (l) {
         *                      return l.toObject();
         *                  });
         *                  return obj;
         *              },
         *
         *              _setWeekdays:function (weekdays) {
         *                  this.weekdaysArray = weekdays.split(",");
         *                  return weekdays;
         *              }
         *      },
         *
         *      static:{
         *
         *          init:function () {
         *              this.oneToMany("legs", {
         *                  model:"flightLeg",
         *                  orderBy:"scheduledDepartureTime",
         *                  fetchType:this.fetchType.EAGER
         *              });
         *          },
         *
         *          byAirline:function (airline) {
         *              return this.filter({airline:airline}).all();
         *          },
         *
         *          arrivesAt:function (airportCode) {
         *              return this.join(this.flightLeg.select("flightId").filter({arrivalCode:airportCode}).distinct(), {flightId:"id"}).all();
         *          },
         *
         *          departsFrom:function (airportCode) {
         *              return this.join(this.flightLeg.select("flightId").filter({departureCode:airportCode}).distinct(), {flightId:"id"}).all();
         *          },
         *
         *          getters:{
         *              flightLeg:function () {
         *                  if (!this.__flightLeg) {
         *                      this.__flightLeg = this.patio.getModel("flightLeg");
         *                  }
         *                  return this.__flightLeg;
         *              }
         *          }
         *      }
         *  });
         *
         *
         * @param {String|patio.Dataset} table the table to use as the base for the model.
         * @param {patio.Model|patio.Model[]} Parent models of this model.
         *                  See {@link patio.plugins.ClassTableInheritancePlugin}.
         * @param {Object} [proto] an object to be used as the prototype for the model. See
         * <a href="http://c2fo.github.com/comb/symbols/comb.html#.define">comb.define</a>.
         * @param [Object[]] [proto.plugins] this can be used to specify additional plugins to use such as.
         * <ul>
         *     <li>{@link patio.plugins.TimeStampPlugin</li>
         *     <li>{@link patio.plugins.CachePlugin</li>
         * </ul>
         *
         *
         *
         */
        addModel:function (table, supers, proto) {
            return model.create.apply(model, arguments);
        },

        /**
         * Returns a model from the name of the table for which the model was created.
         *
         * {@code
         * var TestModel = patio.addModel("test_model").sync(function(err){
         *      if(err){
         *         console.log(err.stack);
         *      }else{
         *            var TestModel = patio.getModel("test_model");
         *      }
         * });
         * }
         *
         * If you have two tables with the same name in different databases then you can use the db parameter also.
         *
         * {@code
         *
         * var DB1 = patio.createConnection("mysql://test:testpass@localhost:3306/test_1");
         * var DB2 = patio.createConnection("mysql://test:testpass@localhost:3306/test_2");
         * var Test1 = patio.addModel(DB1.from("test");
         * var Test2 = patio.addModel(DB2.from("test");
         *
         * //sync the models
         * patio.syncModels().then(function(){
         *      //now you can use them
         *      var test1Model = new Test1();
         *      var test2Model = new Test2();
         * });
         * }
         *
         *
         * @param {String} name the name of the table that the model represents.
         * @param {@patio.Database} [db] optional database in case you have two models with the same table names in
         *                               different databases.
         */
        getModel:function (name, db) {
            return model.getModel(name, db);
        },

        /**
         * Helper method to sync all models at once.
         *
         * @example
         *
         * var User = patio.addModel("user");
         * var Blog = patio.addModel("blog");
         *
         * //using promise api
         * patio.syncModels().then(function(){
         *     var user = new User();
         * }, function(error){
         *    console.log(err);
         * });
         *
         * //using a callback
         *
         *  patio.syncModels(function(err){
         *      if(err){
         *          console.log(err);
         *      }else{
         *          var user = new User();
         *      }
         *  });
         *
         * @param {Function} [cb] an optional callback to be invoked when all models have been synced
         * @return {comb.Promise} a promise that will be resolved when the models have been synced.
         */
        syncModels:function (cb) {
            return model.syncModels(cb);
        },

        resetIdentifierMethods:function () {
            this.quoteIdentifiers = true;
            this.identifierOutputMethod = null;
            this.identifierInputMethod = null;
            Model.identifierOutputMethod = null;
            Model.identifierInputMethod = null;
        },


        /**
         * Migrates the database using migration files found in the supplied directory.
         * <br/>
         * <br/>
         * <div>
         *     <h3>Integer Migrations</h3>
         *      Integer migrations are the simpler of the two migrations but are less flexible than timestamp based migrations.
         *      In order for patio to determine which versions to use the file names must end in <versionNumber>.js where
         *      versionNumber is a integer value representing the version number. <b>NOTE:</b>With integer migrations
         *      missing versions are not allowed.
         *      <br/>
         *      <br/>
         *      An example directory structure might look like the following:
         *
         *      <pre class="code">
         * -migrations
         *      - createFirstTables.0.js
         *      - shortDescription.1.js
         *      - another.2.js
         *      .
         *      .
         *      .
         *      -lastMigration.n.js
         *      </pre>
         *      In order to easily identify where certain schema alterations have taken place it is a good idea to provide a brief
         *      but meaningful migration name.
         *      <pre class="code">
         *          createEmployee.0.js
         *          alterEmployeeNameColumn.1.js
         *      </pre>
         *</div>
         *
         * <div>
         *     <h3>Timestamp Migrations</h3>
         *      Timestamp migrations are the more complex of the two migrations but offer greater flexibility especially
         *      with development teams. This is because Timestamp migrations do not require consecutive version numbers,
         *      ,allow for duplicate version numbers(but this should be avoided), keeps track of all currently applied migrations,
         *      and it will merge missing migrations. In order for patio to determine the order of the migration files
         *      the file names must end in <timestamp>.js where the timestamp can be any form of a time stamp.
         *      <pre class="code">
         * //yyyyMMdd
         * 20110131
         * //yyyyMMddHHmmss
         * 20110131123940
         * //unix epoch timestamp
         * 1328035161
         *      </pre>
         *      as long as it is greater than 20000101 other wise it will be assumed to be part of an integer migration.
         *      <br/>
         *      <br/>
         *      An example directory structure might look like the following:
         *
         *      <pre class="code">
         * -migrations
         *      - createFirstTables.1328035161.js
         *      - shortDescription.1328035360.js
         *      - another.1328035376.js
         *      .
         *      .
         *      .
         *      -lastMigration.n.js
         *      </pre>
         *      In order to easily identify where certain schema alterations have taken place it is a good idea to provide a brief
         *      but meaningful migration name.
         *      <pre class="code">
         *          createEmployee.1328035161.js
         *          alterEmployeeNameColumn.1328035360.js
         *      </pre>
         *</div>
         *
         * <b>NOTE:</b>If you start with IntegerBased migrations and decide to transition to Timestamp migrations the
         * patio will attempt the migrate the current schema to the timestamp based migration schema.
         *
         * <div>
         * In order to run a migraton all one has to do is call patio.migrate(DB, directory, options);
         *
         * <pre class="code">
         *  var DB = patio.connect("my://connection/string");
         *  patio.migrate(DB, __dirname + "/migrations").then(function(){
         *      console.log("migrations finished");
         *  });
         * </pre>
         *
         * <b>Example migration file</b>
         * <pre class="code">
         *
         * //Up function used to migrate up a version
         * exports.up = function(db) {
         *   //create a new table
         *   db.createTable("company", function() {
         *       this.primaryKey("id");
         *       this.companyName(String, {size : 20, allowNull : false});
         *   });
         *   db.createTable("employee", function(table) {
         *       this.primaryKey("id");
         *       this.firstName(String);
         *       this.lastName(String);
         *       this.middleInitial("char", {size : 1});
         *   });
         *};
         *
         * //Down function used to migrate down version
         *exports.down = function(db) {
         *    db.dropTable("employee", "company");
         *};
         * </pre>
         *
         *</div>
         *
         * @param {String|patio.Database} db the database or connection string to a database to migrate.
         * @param {String} directory directory that the migration files reside in
         * @param {Object} [opts={}] optional parameters.
         * @param {String} [opts.column] the column in the table that version information should be stored.
         * @param {String} [opts.table] the table that version information should be stored.
         * @param {Number} [opts.target] the target migration(i.e the migration to migrate up/down to).
         * @param {String} [opts.current] the version that the database is currently at if the current version
         * is not provided it is retrieved from the database.
         *
         * @return {Promise} a promise that is resolved once the migration is complete.
         */
        migrate:function (db) {
            db = isString(db) ? this.connect(db) : db;
            var args = argsToArray(arguments);
            args.splice(0, 1);
            return migrate.run.apply(migrate, [db].concat(args));
        },

        /**
         * This can be used to configure logging. If a options
         * hash is passed in then it will passed to the comb.logging.PropertyConfigurator.
         * If the options are omitted then a ConsoleAppender will be added and the level will
         * be set to info.
         *
         * @example
         * var config = {
         *     "patio" : {
         *         level : "INFO",
         *         appenders : [
         *             {
         *                 type : "RollingFileAppender",
         *                 file : "/var/log/patio.log",
         *             },
         *             {
         *                 type : "RollingFileAppender",
         *                 file : "/var/log/patio-error.log",
         *                 name : "errorFileAppender",
         *                 level : "ERROR"
         *             }
         *         ]
         * };
         *
         * patio.configureLogging(config);
         *
         * @param opts
         */
        configureLogging:function (opts) {
            comb.logger.configure(opts);
            if (!opts) {
                LOGGER.level = "info";
            }
        },

        /**
         * Logs an INFO level message to the "patio" logger.
         */
        logInfo:function () {
            if (LOGGER.isInfo) {
                LOGGER.info.apply(LOGGER, arguments);
            }
        },

        /**
         * Logs a DEBUG level message to the "patio" logger.
         */
        logDebug:function () {
            if (LOGGER.isDebug) {
                LOGGER.debug.apply(LOGGER, arguments);
            }
        },

        /**
         * Logs an ERROR level message to the "patio" logger.
         */
        logError:function () {
            if (LOGGER.isError) {
                LOGGER.error.apply(LOGGER, arguments);
            }
        },

        /**
         * Logs a WARN level message to the "patio" logger.
         */
        logWarn:function () {
            if (LOGGER.isWarn) {
                LOGGER.warn.apply(LOGGER, arguments);
            }
        },

        /**
         * Logs a TRACE level message to the "patio" logger.
         */
        logTrace:function () {
            if (LOGGER.isTrace) {
                LOGGER.trace.apply(LOGGER, arguments);
            }
        },

        /**
         * Logs a FATAL level message to the "patio" logger.
         */
        logFatal:function () {
            if (LOGGER.isFatal) {
                LOGGER.fatal.apply(LOGGER, arguments);
            }
        },


        /**@ignore*/
        getters:{
            /**@lends patio.prototype*/
            /**
             * An array of databases that are currently connected.
             * @field
             * @type patio.Database[]
             * @default []
             */
            DATABASES:function () {
                return Database.DATABASES;
            },
            /**
             * Returns the default database. This is the first database created using {@link patio#connect}.
             * @field
             * @type patio.Database
             * @default null
             */
            defaultDatabase:function () {
                return this.DATABASES.length ? this.DATABASES[0] : null;
            },
            /**@ignore*/
            Database:function () {
                return Database;
            },

            /**@ignore*/
            Dataset:function () {
                return Dataset;
            },

            /**@ignore*/
            SQL:function () {
                return SQL;
            },

            /**@ignore*/
            sql:function () {
                return SQL;
            },

            /**@ignore*/
            plugins:function () {
                return plugins;
            },

            /**@ignore*/
            migrations:function () {
                return migrate;
            },

            /**
             * Returns the root comb logger using this logger you
             * can set the levels add appenders etc.
             *
             * @type Logger
             * @field
             * @default comb.logger("patio")
             */
            LOGGER:function () {
                return LOGGER;
            },

            /**
             * Returns the default method used to transform identifiers sent to the database.
             * See (@link patio.Database.identifierInputMethod}
             * @ignore
             * @field
             * @type String
             * @default Database.identifierInputMethod
             */
            identifierInputMethod:function () {
                return Database.identifierInputMethod;
            },

            /**
             * Returns the default method used to transform identifiers returned from the database.
             * See (@link patio.Database.identifierOutputMethod}
             * @ignore
             * @field
             * @type String
             *
             */
            identifierOutputMethod:function () {
                return Database.identifierOutputMethod;
            },

            /**
             * @ignore
             * @type Boolean
             * Returns whether or not identifiers are quoted before being sent to the database.
             */
            quoteIdentifiers:function (value) {
                return Database.quoteIdentifiers;
            },

            /**@ignore*/
            camelize:function () {
                return this.__camelize;
            },

            /**@ignore*/
            underscore:function () {
                return this.__underscore;
            }

        },


        /**@ignore*/
        setters:{
            /**@lends patio.prototype*/
            /**
             * Set the method to call on identifiers going into the database.  This affects
             * how identifiers are sent to the database. So if you use camelCased and the db identifiers are all underscored
             * use camelize. The method can include
             * <ul>
             *     <li>toUpperCase</li>
             *     <li>toLowerCase</li>
             *     <li>camelize</li>
             *     <li>underscore</li>
             *     <li>Other String instance method names.</li>
             * </ul>
             *
             * patio uses toUpperCase identifiers in all SQL strings for most databases.
             *
             * @field
             * @type String
             * @ignoreCode
             * @example
             *  //use whatever is passed in
             *   patio.identifierInputMethod = null;
             *  //convert to uppercase
             *  patio.identifierInputMethod = "toUpperCase";
             *  //convert to camelCase
             *  patio.identifierInputMethod = "camelize";
             *  //convert to underscore
             *  patio.identifierInputMethod = "underscore";
             *
             * */
            identifierInputMethod:function (value) {
                Database.identifierInputMethod = value;
            },

            /**
             * Set the method to call on identifiers coming out of the database.  This affects
             * the how identifiers are represented by calling the method on them.
             * The method can include
             * <ul>
             *     <li>toUpperCase</li>
             *     <li>toLowerCase</li>
             *     <li>camelize</li>
             *     <li>underscore</li>
             *     <li>Other String instance method names.</li>
             * </ul>
             * most database implementations in patio use toLowerCase
             * @ignoreCode
             * @field
             * @type String
             * @example
             *  //use whatever the db returns
             *   patio.identifierOutputMethod = null;
             *  //convert to uppercase
             *  patio.identifierOutputMethod = "toUpperCase";
             *  //convert to camelCase
             *  patio.identifierOutputMethod = "camelize";
             *  //convert to underscore
             *  patio.identifierOutputMethod = "underscore";
             *
             * */
            identifierOutputMethod:function (value) {
                Database.identifierOutputMethod = value;
            },

            /**
             * Set whether to quote identifiers for all databases by default. By default,
             * patio quotes identifiers in all SQL strings.
             *
             * @ignoreCode
             * @field
             * @type Boolean
             *
             * @example
             *   //Turn quoting off
             *   patio.quoteIdentifiers = false
             * */
            quoteIdentifiers:function (value) {
                Database.quoteIdentifiers = value;
            },

            /**
             * Sets the whether or not to camelize identifiers coming from the database and to underscore
             * identifiers when sending identifiers to the database. Setting this property to true has the same effect
             * as:
             * <pre class="code">
             *     patio.identifierOutputMethod = "camelize";
             *     patio.identifierInputMethod = "underscore";
             * </pre>

             * @field
             * @ignoreCode
             * @example
             * patio.camelize = true;
             * patio.connectAndExecute("mysql://test:testpass@localhost:3306/airports", function (db) {
             *      db.createTable("airport", function () {
             *          this.primaryKey("id");
             *          this.airportCode(String, {size:4, allowNull:false, unique:true});
             *          this.name(String, {allowNull:false});
             *          this.city(String, {allowNull:false});
             *          this.state(String, {size:2, allowNull:false});
             *      });
             *      //=> CREATE TABLE `airport`(
             *      //    id integer PRIMARY KEY AUTO_INCREMENT,
             *      //    airport_code varchar(4) UNIQUE NOT NULL,
             *      //    name varchar(255) NOT NULL,
             *      //    city varchar(255) NOT NULL,
             *      //    state varchar(2) NOT NULL
             *      //);
             *  }):
             *
             * @param {Boolean} camelize set to true to camelize all identifiers coming from the database and to
             *                  underscore all identifiers sent to the database.
             */
            camelize:function (camelize) {
                camelize = camelize === true;
                Model.camelize = camelize;
                this.identifierOutputMethod = camelize ? "camelize" : "underscore";
                this.identifierInputMethod = camelize ? "underscore" : "camelize";
                this.__underscore = !camelize;
                this.__camelize = camelize;
            },

            /**
             * Sets the whether or not to underscore identifiers coming from the database and to camelize
             * identifiers when sending identifiers to the database. Setting this property to true has the same effect
             * as:
             * <pre class="code">
             *     patio.identifierOutputMethod = "underscore";
             *     patio.identifierInputMethod = "camelize";
             * </pre>
             *
             *
             * @field
             * @ignoreCode
             * @example
             * patio.camelize = true;
             * patio.connectAndExecute("mysql://test:testpass@localhost:3306/airports", function (db) {
             *      db.createTable("airport", function () {
             *          this.primaryKey("id");
             *          this.airport_code(String, {size:4, allowNull:false, unique:true});
             *          this.name(String, {allowNull:false});
             *          this.city(String, {allowNull:false});
             *          this.state(String, {size:2, allowNull:false});
             *      });
             *      //=> CREATE TABLE `airport`(
             *      //    id integer PRIMARY KEY AUTO_INCREMENT,
             *      //    airportCode varchar(4) UNIQUE NOT NULL,
             *      //    name varchar(255) NOT NULL,
             *      //    city varchar(255) NOT NULL,
             *      //    state varchar(2) NOT NULL
             *      //);
             *  }):
             *
             * @param {Boolean} camelize set to true to underscore all identifiers coming from the database and to
             *                  camelize all identifiers sent to the database.
             */
            underscore:function (underscore) {
                underscore = underscore === true;
                Model.underscore = underscore;
                this.identifierOutputMethod = underscore ? "underscore" : "camelize";
                this.identifierInputMethod = underscore ? "camelize" : "underscore";
                this.__camelize = !underscore;
                this.__underscore = underscore;
            }
        }
    }
});


var patio = exports;
module.exports = patio = new Patio();
patio.__Patio = Patio;
var adapters = Database.ADAPTERS;
for (var i in adapters) {
    patio[i] = adapters[i];
}