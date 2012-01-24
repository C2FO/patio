var comb = require("comb"),
    errors = require("../errors"),
    NotImplemented = errors.NotImplemented,
    URL = require("url"),
    DatabaseError = errors.DatabaseError;


var ADAPTERS = {};
var DB = comb.define(null, {
    instance : {
        /**@lends patio.Database.prototype*/
        disconnect : function() {
            return this.pool.endAll().then(comb.hitch(this, function(){
                this.onDisconnect(this);
            }));
        },

        onDisconnect : function(){

        },

        /**
         * This is an abstract method that should be implemented by adapters to create
         * a connection to the database.
         * @param {Object} options options that are adapter specific.
         */
        createConnection : function(options) {
            throw new NotImplemented("Create connect must be implemented by the adapter");
        },

        /**
         * This is an abstract method that should be implemented by adapters to close
         * a connection to the database.
         * @param conn the database connection to close.
         */
        closeConnection : function(conn) {
            throw new NotImplemented("Close connection must be implemented by the adapter");
        },
        /**
         * Validates a connection before it is returned to the {@link patio.ConnectionPool}. This
         * method should be implemented by the adapter.
         * @param conn
         */
        validate : function(conn) {
            throw new NotImplemented("Validate must be implemented by the adapter");
        },

        /**
         * @ignore
         */
        getters : {
            uri : function() {
                /**
                 * @ignore
                 */
                if (!this.opts.uri) {
                    var opts = {
                        protocol : this.type,
                        hostname : this.opts.host,
                        auth :  comb.string.format("{user}:{password}", this.opts),
                        port : this.opts.port,
                        pathname : "/" + this.opts.database
                    };
                    return URL.format(opts);
                } else {
                }
                return this.opts.uri;
            },

            url : function() {
                return this.uri;
            }
        }
    },

    static : {
        /**@lends patio.Database*/

        /**
         * Creates a connection to a Database see {@link patio#createConnection}.
         */
        connect : function(connectionString, opts) {
            opts = opts || {};
            if (comb.isString(connectionString)) {
                var url = URL.parse(connectionString, true);
                if (url.auth) {
                    var parts = url.auth.split(":");
                    !opts.user && (opts.user = parts[0]);
                    !opts.password && (opts.password = parts[1]);
                }
                opts.type = url.protocol.replace(":", "");
                opts.host = url.hostname;
                if (url.pathname) {
                    var path = url.pathname;
                    var pathParts = path.split("/").slice(1);
                    if (pathParts.length >= 1) {
                        opts.database = pathParts[0];
                    }
                }
                opts = comb.merge(opts, url.query, {uri : connectionString});
            } else {
                opts = comb.merge({}, connectionString, opts);
            }
            if (opts && comb.isHash(opts) && (opts.adapter || opts.type)) {
                var type = (opts.type = opts.adapter || opts.type);
                var Adapter = ADAPTERS[type];
                if (Adapter) {
                    var adapter = new Adapter(opts);
                    this.DATABASES.push(adapter);
                    return adapter;
                } else {
                    throw DatabaseError(type + " adapter was not found")
                }
            } else {
                throw new DatabaseError("Options required when connecting.");
            }
        },

        setAdapterType : function(type) {
            type = type.toLowerCase();
            this.type = type;
            ADAPTERS[type] = this;
        },

        disconnect : function(){
            var dbs = this.DATABASES;
            var ret= new comb.PromiseList(dbs.map(function (d) {
                return d.disconnect();
            }), true);
            dbs.length = 0;
            return ret;
        },

        ADAPTERS : ADAPTERS
    }
}).as(module);

DB.setAdapterType("default");

