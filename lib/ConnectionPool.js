var comb = require("comb"),
    Promise = comb.Promise,
    PromiseList = comb.PromiseList,
    isFunction = comb.isFunction,
    Queue = comb.collections.Queue,
    merge = comb.merge,
    define = comb.define,
    Pool = comb.collections.Pool;

define(Pool, {

    instance: {
        /**@lends patio.ConnectionPool.prototype*/

        /**
         * ConnectionPool object used internall by the {@link patio.Database} class;
         * @constructs;
         * @param options
         */
        constructor: function (options) {
            options = options || {};
            if (!options.createConnection || !isFunction(options.createConnection)) {
                throw new Error("patio.adapters.clients.ConnectionPool : create connection CB required.");
            }
            if (!options.closeConnection || !isFunction(options.closeConnection)) {
                throw new Error("patio.adapters.clients.ConnectionPool : close connection CB required.");
            }
            options.minObjects = parseInt(options.minConnections || 0, 10);
            options.maxObjects = parseInt(options.maxConnections || 10, 10);
            this.__deferredQueue = new Queue();
            this._options = options;
            this.__createConnectionCB = options.createConnection;
            this.__closeConnectionCB = options.closeConnection;
            this.__validateConnectionCB = options.validateConnection;
            this._super(arguments);
        },

        /**
         * Checks all deferred connection requests.
         */
        __checkQueries: function () {
            var fc = this.freeCount, def, defQueue = this.__deferredQueue;
            while (fc-- >= 0 && defQueue.count) {
                def = defQueue.dequeue();
                var conn = this.getObject();
                if (conn) {
                    def.callback(conn);
                } else {
                    throw new Error("UNEXPECTED ERROR");
                }
                fc--;
            }
        },

        /**
         * Performs a query on one of the connection in this Pool.
         *
         * @return {comb.Promise} A promise to called back with a connection.
         */
        getConnection: function () {
            var ret = new Promise(), conn;
            if (this.count > this.__maxObjects) {
                this.__deferredQueue.enqueue(ret);
            } else {
                //todo override getObject to make async so creating a connetion can execute setup sql
                conn = this.getObject();
                if (!conn) {
                    //we need to deffer it
                    this.__deferredQueue.enqueue(ret);
                } else {
                    ret.callback(conn);
                }
            }
            if (this.count > this.__maxObjects && !conn) {
                ret.errback(new Error("Unexpected ConnectionPool error"));
            }
            return ret.promise();
        },

        /**
         * Override comb.collections.Pool to allow async validation to allow
         * pools to do any calls to reset a connection if it needs to be done.
         *
         * @param {*} connection the connection to return.
         *
         */
        returnObject: function (obj) {
            var self = this;
            this.validate(obj).chain(function (valid) {
                var index;
                if (self.count <= self.__maxObjects && valid && (index = self.__inUseObjects.indexOf(obj)) > -1) {
                    self.__inUseObjects.splice(index, 1);
                    self.__freeObjects.enqueue(obj);
                    self.__checkQueries();
                } else {
                    self.removeObject(obj);
                }
            });
        },

        /**
         * Removes a connection from the pool.
         * @param conn
         */
        removeConnection: function (conn) {
            this.closeConnection(conn);
            return this.removeObject(conn);
        },

        /**
         * Return a connection to the pool.
         *
         * @param {*} connection the connection to return.
         *
         * @return {*} an adapter specific connection.
         */
        returnConnection: function (connection) {
            this.returnObject(connection);
        },

        createObject: function () {
            return this.createConnection();
        },

        /**
         * Override to implement the closing of all connections.
         *
         * @return {comb.Promise} called when all connections are closed.
         */
        endAll: function () {
            this.__ending = true;
            var conn, fQueue = this.__freeObjects, count = this.count, ps = [];
            while ((conn = this.__freeObjects.dequeue()) !== undefined) {
                ps.push(this.closeConnection(conn));
            }
            var inUse = this.__inUseObjects;
            for (var i = inUse.length - 1; i >= 0; i--) {
                ps.push(this.closeConnection(inUse[i]));
            }
            this.__inUseObjects.length = 0;
            return new PromiseList(ps).promise();
        },


        /**
         * Override to provide any additional validation. By default the promise is called back with true.
         *
         * @param {*} connection the conneciton to validate.
         *
         * @return {comb.Promise} called back with a valid or invalid state.
         */
        validate: function (conn) {
            if (!this.__validateConnectionCB) {
                var ret = new Promise();
                ret.callback(true);
                return ret;
            } else {
                return this.__validateConnectionCB(conn);
            }
        },

        /**
         * Override to create connections to insert into this ConnectionPool.
         */
        createConnection: function () {
            return this.__createConnectionCB(this._options);
        },

        /**
         * Override to implement close connection functionality;
         * @param {*} conn the connection to close;
         *
         * @return {comb.Promise} called back when the connection is closed.
         */
        closeConnection: function (conn) {
            return this.__closeConnectionCB(conn);
        }
    },

    "static": {
        /**@lends patio.ConnectionPool*/

        getPool: function (opts, createConnection, closeConnection, validateConnection) {
            var Self = this;
            return new Self(merge(opts, {
                createConnection: createConnection,
                closeConnection: closeConnection,
                validateConnection: validateConnection
            }));
        }
    }
}).as(module);