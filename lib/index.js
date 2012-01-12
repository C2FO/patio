var Dataset = require("./dataset"),
    Database = require("./database"),
    adapters = require("./adapters"),
    MooseError = require("./errors").MooseError,
    comb = require("comb"),
    Time = require("./time"),
    date = comb.date,
    SQL = require("./sql").sql,
    hitch = comb.hitch,
    Promise = comb.Promise,
    PromiseList = comb.PromiseList;

var LOGGER = comb.logging.Logger.getLogger("moose");
new comb.logging.BasicConfigurator().configure();
LOGGER.level = comb.logging.Level.INFO;



var Moose = comb.singleton(Time, {
    instance:{
        /**
         * @lends moose.prototype
         */

        /**
         * A singleton class that acts as the entry point for all actions performed in moose.
         *
         * @example
         *
         * var moose = require("moose");
         *
         * moose.createConnection(....);
         *
         * moose.camelize = true;
         * moose.quoteIdentifiers=false;
         *
         * moose.createModel("my_table");
         *
         *
         *  //CHANGING IDENTIFIER INPUT METHOD
         *
         *
         *  //use whatever is passed in
         *   moose.identifierInputMethod = null;
         *  //convert to uppercase
         *  moose.identifierInputMethod = "toUpperCase";
         *  //convert to camelCase
         *  moose.identifierInputMethod = "camelize";
         *  //convert to underscore
         *  moose.identifierInputMethod = "underscore";
         *
         *
         *  //CHANGING IDENTIFIER OUTPUT METHOD
         *
         *  //use whatever the db returns
         *   moose.identifierOutputMethod = null;
         *  //convert to uppercase
         *  moose.identifierOutputMethod = "toUpperCase";
         *  //convert to camelCase
         *  moose.identifierOutputMethod = "camelize";
         *  //convert to underscore
         *  moose.identifierOutputMethod = "underscore";
         *
         *  //TURN QUOTING OFF
         *   moose.quoteIdentifiers = false
         *
         * @constructs
         * @augments moose.Time
         * @param options
         *
         * @property {moose.Database} defaultDatabase the default database to use, this property can only be used after the conneciton has
         *                             initialized.
         * @property {Object[]} adapters that are available to use.
         *
         *
         * @property {moose.Database[]} DATABASES An array of databases that are currently connected.
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
         * moose uses toUpperCase identifiers in all SQL strings for most databases.
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
         * most database implementations in moose use toLowerCase
         *
         * @property {Boolean} quoteIdentifiers Set whether to quote identifiers for all databases by default. By default,
         * moose quotes identifiers in all SQL strings.
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
         * Initialize the connection information, and prepare moose to communicate with the DB.
         * All models, and schemas, and models that are created before this method has been called will be deferred.
         *
         * @example
         *
         * //connect using an object
         * moose.createConnection({
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
         * moose.createConnection(CONNECT_STRING);
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
         *  @see moose#createConnection
         */
        connect:function () {
            return this.createConnection.apply(this, arguments);
        },

        /**
         * This method allows one to connect to a database and immediately execute code.
         * For connection options @see moose#createConnection
         *
         * @example
         *
         *
         * var DB;
         * var CONNECT_STRING = "dummyDB://test:testpass@localhost/dummySchema";
         * var connectPromise = moose.connectAndExecute(CONNECT_STRING, function (db) {
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
         * @param {String|Object} options @see moose#createConnection
         * @param {Function} cb the function to callback once connected.
         *
         * @returns {comb.Promise} a promise that is resolved once the database execution has finished.
         */
        connectAndExecute:function (options, cb) {
            if (!comb.isFunction(cb)) throw new MooseError("callback must be a function");
            var db = this.createConnection.apply(this, arguments);
            return comb.executeInOrder(db, moose, function (db, moose) {
                cb(db, moose);
                return db;
            });
        },

        /**
         * Disconnects all databases in use.
         *
         * @return {comb.Promise} a promise that is resolved once all databases have disconnected.
         */
        disconnect:function () {
            return new PromiseList(this.DATABASES.map(function (d) {
                return d.disconnect()
            }), true);
        },

        /**@ignore*/
        getters:{

            /**
             * @ignore
             * @type moose.Database[]
             * @memberOf moose.prototype
             * An array of databases that are currently connected.
             */
            DATABASES:function () {
                return Database.DATABASES;
            },
            /**
             * @ignore
             * @type moose.Database
             * Returns the default database. This is the first database created using
             * @see moose#connect
             */
            defaultDatabase:function () {
                return this.DATABASES[0];
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
             * moose uses toUpperCase identifiers in all SQL strings for most databases.
             *
             *  * @example
             *  //use whatever is passed in
             *   moose.identifierInputMethod = null;
             *  //convert to uppercase
             *  moose.identifierInputMethod = "toUpperCase";
             *  //convert to camelCase
             *  moose.identifierInputMethod = "camelize";
             *  //convert to underscore
             *  moose.identifierInputMethod = "underscore";
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
             * most database implementations in moose use toLowerCase
             * @example
             *  //use whatever the db returns
             *   moose.identifierOutputMethod = null;
             *  //convert to uppercase
             *  moose.identifierOutputMethod = "toUpperCase";
             *  //convert to camelCase
             *  moose.identifierOutputMethod = "camelize";
             *  //convert to underscore
             *  moose.identifierOutputMethod = "underscore";
             *
             * */
            identifierOutputMethod:function (value) {
                Database.identifierOutputMethod = value;
            },

            /**
             * @ignore
             * @type Boolean
             * Set whether to quote identifiers for all databases by default. By default,
             * moose quotes identifiers in all SQL strings.
             *
             * @example
             *   //Turn quoting off
             *   moose.quoteIdentifiers = false
             * */
            quoteIdentifiers:function (value) {
                Database.quoteIdentifiers = value;
            }
        }
    }
});


var moose = exports;
module.exports = moose = new Moose();
moose.__Moose = Moose;
var adapters = Database.ADAPTERS;
for (var i in adapters) {
    moose[i] = adapters[i];
}