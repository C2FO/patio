var comb = require("comb"),
    errors = require("../errors"),
    NotImplemented = errors.NotImplemented,
    URL = require("url"),
    DatabaseError = errors.DatabaseError;


var ADAPTERS = {};
var DB = comb.define(null, {


    instance : {

        disconnect : function() {
            return this.pool.endAll();
        },


        createConnection : function(options) {
            throw new NotImplemented("Create connect must be implemented by the adapter");
        },

        closeConnection : function(conn) {
            throw new NotImplemented("Close connection must be implemented by the adapter");
        },

        validate : function(conn) {
            throw new NotImplemented("Validate must be implemented by the adapter");
        },

        getters : {
            uri : function() {
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
                    return new Adapter(opts);
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

        ADAPTERS : ADAPTERS
    }
}).as(module);

DB.setAdapterType("default");

