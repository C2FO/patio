var Client = require('mysql').Client,
    Dataset = require("./dataset"),
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

var connectionReady = false;

var LOGGER = comb.logging.Logger.getLogger("moose");
new comb.logging.BasicConfigurator().configure();
LOGGER.level = comb.logging.Level.INFO;

/**
 * @class A singleton class that acts as the entry point for all actions performed in moose.
 *
 * @constructs
 * @name moose
 * @augments Migrations
 * @param options
 *
 * @property {String} database the default database to use, this property can only be used after the conneciton has
 *                             initialized.
 * @property {moose.adapters} adapter the adapter moose is using. <b>READ ONLY</b>
 *
 */
var Moose = comb.singleton(Time, {
    instance:{

        /**
         * @lends moose.prototype
         */

        /**
         *The type of database moose will be connecting to. Currently only mysql is supported.
         */
        type:"mysql",

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
         * moose.createConnection({
         *              host : "127.0.0.1",
         *              port : 3306,
         *              type : "mysql",
         *              maxConnections : 1,
         *              minConnections : 1,
         *              user : "test",
         *              password : "testpass",
         *              database : 'test'
         *});
         *
         * @param {Object} options the options used to initialize the database connection.
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

        connect:function () {
            return this.createConnection.apply(this, arguments);
        },

        connectAndExecute:function (options, cb) {
            if (!comb.isFunction(cb)) throw new MooseError("callback must be a function");
            var db = this.createConnection.apply(this, arguments);
            return comb.executeInOrder(db, moose, function (db, moose) {
                cb(db, moose);
                return db;
            });
        },

        disconnect:function () {
            return new PromiseList(this.DATABASES.map(function (d) {
                return d.disconnect()
            }));
        },

        /**@ignore*/
        getters:{
            DATABASES:function () {
                return Database.DATABASES;
            },

            defaultDatabase:function () {
                return this.DATABASES[0];
            },

            Database:function () {
                return Database;
            },
            Dataset:function () {
                return Dataset
            },
            SQL:function () {
                return SQL
            }

        },


        /**@ignore*/
        setters:{
            /* Set the method to call on identifiers going into the database.  This affects
             * the literalization of identifiers by calling this method on them before they are input.
             * Sequel upcases identifiers in all SQL strings for most databases, so to turn that off:
             *
             *   Sequel.identifier_input_method = nil
             *
             * to downcase instead:
             *
             *   Sequel.identifier_input_method = :downcase
             *
             * Other String instance methods work as well.
             * */
            identifierInputMethod:function (value) {
                Database.identifierInputMethod = value
            },

            /*
             * Set the method to call on identifiers coming out of the database.  This affects
             * the literalization of identifiers by calling this method on them when they are
             * retrieved from the database.  Sequel downcases identifiers retrieved for most
             * databases, so to turn that off:
             *
             *   Sequel.identifier_output_method = nil
             *
             * to upcase instead:
             *
             *   Sequel.identifier_output_method = :upcase
             *
             * Other String instance methods work as well.
             * */
            identifierOutputMethod:function (value) {
                Database.identifierOutputMethod = value;
            },

            /*
             * Set whether to quote identifiers for all databases by default. By default,
             * Sequel quotes identifiers in all SQL strings, so to turn that off:
             *
             *   Sequel.quote_identifiers = false
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

//moose.Table = Table;
/**
 * @namespace
 */
//moose.adapters = adapters;
/**
 * @namespace
 */
//moose.plugins = plugins;