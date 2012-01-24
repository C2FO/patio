var Dataset = require("./dataset"),
    Database = require("./database"),
    adapters = require("./adapters"),
    PatioError = require("./errors").PatioError,
    model = require("./model"),
    Model = model.Model,
    plugins = require("./plugins");
comb = require("comb"),
    Time = require("./time"),
    date = comb.date,
    SQL = require("./sql").sql,
    hitch = comb.hitch,
    Promise = comb.Promise,
    PromiseList = comb.PromiseList;

var LOGGER = comb.logging.Logger.getLogger("patio");
new comb.logging.BasicConfigurator().configure();
LOGGER.level = comb.logging.Level.INFO;


var Patio = comb.singleton(Time, {
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
         *
         * @property {patio.Database} defaultDatabase the default database to use, this property can only be used after the conneciton has
         *                             initialized.
         *
         * @property {patio.Database[]} DATABASES An array of databases that are currently connected.
         *
         *
         * @property {String} identifierInputMethod  Set the method to call on identifiers going into the database.  This affects
         *  how identifiers are sent to the database. So if you use camelCased and the db identifiers are all underscored
         *  use camelize. The method can include
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
         *
         * @property {String} identifierOutputMethod Set the method to call on identifiers coming out of the database.  This affects
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
         *
         * @property {Boolean} quoteIdentifiers Set whether to quote identifiers for all databases by default. By default,
         * patio quotes identifiers in all SQL strings.
         *
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
         * @params {Number} [options.maxConnections = 10] the number of connections to pool.
         * @params {Number} [options.minConnections = 3] the number of connections to pool.
         * @param {String} [options.type = "mysql"] the type of database to communicate with.
         * @params {String} options.user the user to authenticate as.
         * @params {String} options.password the password of the user.
         * @params {String} options.database the name of the database to use, the database
         *                                   specified here is the default database for all connections.
         */
        createConnection:function (options) {
            return Database.connect(options);
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
            if (!comb.isFunction(cb)) {
                throw new PatioError("callback must be a function");
            }
            var db = this.createConnection.apply(this, arguments);
            return comb.executeInOrder(db, patio, function (db, patio) {
                cb(db, patio);
                return db;
            });
        },

        /**
         * Disconnects all databases in use.
         *
         * @return {comb.Promise} a promise that is resolved once all databases have disconnected.
         */
        disconnect:function () {
            return Database.disconnect();
        },

        /**
         * Allows for the importing of multiple models so you do not have to worry about the promise that is returned from create model,
         * or a directory if an index.js file is used.
         *
         * <p>
         *     To import a group of model files you can do the following:
         *      <pre class="code">
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
         *     </pre>
         * </p>
         *
         * @param {...} files a single or list of files to import.
         *
         * @return {Promise} returns a promise that will be resolved when all models in the imported files
         * have been loaded.
         */
        import:function (files) {
            files = comb.argsToArray(arguments);
            return new PromiseList(files.map(hitch(this, "__addModelProxy")), true);
        },

        /**
         * This method is used to create a {@link patio.Model} object. This method returns a Promise which will
         * be resolved once the model has been loaded.
         *
         * @example
         * patio.addModel("flight", {
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
         * @param {Object} proto an object to be used as the prototype for the model. See
         * <a href="http://pollenware.github.com/comb/symbols/comb.html#.define">comb.define</a>.
         * @param [Object[]] [proto.plugins] this can be used to specify additional plugins to use such as.
         * <ul>
         *     <li>{@link patio.plugins.TimeStampPlugin</li>
         *     <li>{@link patio.plugins.CachePlugin</li>
         * </ul>
         *
         *
         *
         */
        addModel:function (table, proto) {
            var ret = new Promise();
            model.create(table, proto).then(hitch(this, function (model) {
                ret.callback(model);
            }), hitch(ret, "errback"));
            return ret;
        },

        /**
         * Returns a model from the name of the table for which the model was created.
         *
         * <pre class="code">
         * patio.addModel("test_model").then(function(){
         *      var TestModel = patio.getModel("test_model");
         * });
         * </pre>
         *
         * If you have two tables with the same name in different databases then you can use the db parameter also.
         *
         * <pre class="code">
         * var DB1 = patio.createConnection("mysql://test:testpass@localhost:3306/test_1");
         * var DB2 = patio.createConnection("mysql://test:testpass@localhost:3306/test_2");
         * comb.executeInOrder(patio, function(patio){
         *     patio.addModel(DB1.from("test");
         *     patio.addModel(DB2.from("test");
         * }).then(function(){
         *      var DB1Test = patio.getModel("test", DB1);
         *      var DB2Test = patio.getModel("test", DB2);
         * });
         *
         * </pre>
         *
         * @param {String} name the name of the table that the model represents.
         * @param {@patio.Database} [db] optional database in case you have two models with the same table names in
         *                               different databases.
         */
        getModel:function (name, db) {
            return model.getModel(name, db);
        },

        /**
         * @private
         *
         * Proxies all {@link patio#addModel} calls to allow for the easy import of a group of models that
         * are in a directory.
         * @param file
         */
        __addModelProxy:function (file) {
            var promises = [];
            var repl = [];

            var orig = this["addModel"];
            repl.push({name:"addModel", orig:orig});
            this["addModel"] = function (arg1, arg2) {
                try {
                    var ret;
                    ret = orig.apply(this, arguments);
                    promises.push(comb.when(ret));
                } catch (e) {
                    promises.push(new Promise().errback(e));
                }
                return ret;
            };
            require(file);
            if (promises.length == 0) {
                promises.push(new Promise().callback());
            }
            return new PromiseList(promises, true).both(hitch(this, function () {
                repl.forEach(function (o) {
                    this[o.name] = o.orig;
                }, this)
            }));
        },

        resetIdentifierMethods : function(){
            this.identifierOutputMethod = null;
            this.identifierInputMethod = null;
            Model.identifierOutputMethod = null;
            Model.identifierInputMethod = null;
        },


        /**@ignore*/
        getters:{

            /**
             * @ignore
             * @type patio.Database[]
             * @memberOf patio.prototype
             * An array of databases that are currently connected.
             */
            DATABASES:function () {
                return Database.DATABASES;
            },
            /**
             * @ignore
             * @type patio.Database
             * Returns the default database. This is the first database created using
             * @see patio#connect
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
                return Dataset
            },

            /**@ignore*/
            SQL:function () {
                return SQL
            },

            /**@ignore*/
            sql:function () {
                return SQL
            },

            /**@ignore*/
            plugins:function () {
                return plugins;
            },

            /**
             * @ignore
             * @type String
             * Returns the default method used to transform identifiers sent to the database.
             */
            identifierInputMethod:function () {
                return Database.identifierInputMethod;
            },

            /**
             * @ignore
             * @type String
             * Returns the default method used to transform identifiers returned from the database.
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

            camelize:function () {
                return this.__camelize;
            },

            underscore:function () {
                return this.__underscore;
            }

        },


        /**@ignore*/
        setters:{
            /**
             *
             * @ignore
             * @type String
             *  Set the method to call on identifiers going into the database.  This affects
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
                Database.identifierInputMethod = value
            },

            /**
             * @ignore
             * @type String
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
             * @ignore
             * @type Boolean
             * Set whether to quote identifiers for all databases by default. By default,
             * patio quotes identifiers in all SQL strings.
             *
             * @example
             *   //Turn quoting off
             *   patio.quoteIdentifiers = false
             * */
            quoteIdentifiers:function (value) {
                Database.quoteIdentifiers = value;
            },

            /**
             * @ignore
             * Sets the whether or not to camelize identifiers coming from the database and to underscore
             * identifiers when sending identifiers to the database. Setting this property to true has the same effect
             * as:
             * <pre class="code">
             *     patio.identifierOutputMethod = "camelize";
             *     patio.identifierInputMethod = "underscore";
             * </pre>
             *
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
             * @ignore
             * Sets the whether or not to underscore identifiers coming from the database and to camelize
             * identifiers when sending identifiers to the database. Setting this property to true has the same effect
             * as:
             * <pre class="code">
             *     patio.identifierOutputMethod = "underscore";
             *     patio.identifierInputMethod = "camelize";
             * </pre>
             *
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