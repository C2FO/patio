var comb = require("comb"),
    errors = require("../errors"),
    asyncArray = comb.async.array,
    NotImplemented = errors.NotImplemented,
    QueryError = errors.QueryError,
    sql = require("../sql").sql,
    Identifier = sql.Identifier,
    isUndefinedOrNull = comb.isUndefinedOrNull,
    argsToArray = comb.argsToArray,
    isFunction = comb.isFunction,
    isNumber = comb.isNumber,
    QualifiedIdentifier = sql.QualifiedIdentifier,
    AliasedExpression = sql.AliasedExpression,
    define = comb.define,
    isInstanceOf = comb.isInstanceOf,
    merge = comb.merge,
    isBoolean = comb.isBoolean,
    isString = comb.isString,
    flatten = comb.array.flatten,
    when = comb.when,
    logging = comb.logging,
    Logger = logging.Logger,
    Promise = comb.Promise,
    PromiseList = comb.PromiseList,
    TransformStream = require("stream").Transform,
    pipeAll = require("../utils").pipeAll;

var Dataset;

var LOGGER = Logger.getLogger("patio.Dataset");

function partition(arr, sliceSize) {
    var output = [], j = 0;
    for (var i = 0, l = arr.length; i < l; i += sliceSize) {
        output[j++] = arr.slice(i, i + sliceSize);
    }
    return output;
}

define({
    instance: {
        /**@lends patio.Dataset.prototype*/

        /**@ignore*/
        constructor: function () {
            if (!Dataset) {
                Dataset = require("../index").Dataset;
            }
            this._super(arguments);
        },


        /**
         * Returns a [Stream](http://nodejs.org/api/stream.html) for streaming data from the database.
         *
         *```
         * User
         *   .stream()
         *   .on("data", function(record){
         *       console.log(record);
         *    })
         *   .on("error", errorHandler)
         *   .on("end", function(){
         *      console.log("all done")
         *   });
         *
         * //postgres options
         * User
         *   .stream({batchSize: 100, highWaterMark: 1000})
         *   .on("data", function(record){
         *       console.log(record);
         *    })
         *   .on("error", errorHandler)
         *   .on("end", function(){
         *      console.log("all done")
         *   });
         *```
         * @param {Object} opts an object to pass to the adapters connection stream implementation
         * @return {Stream}
         */
        stream: function (opts) {
            var queryStream = this.fetchRows(this.selectSql, merge(opts || {}, {stream: true})), rowCb, ret;
            if ((rowCb = this.rowCb)) {
                ret = new TransformStream({objectMode: true});
                ret._transform = function (data, encoding, done) {
                    when(rowCb(data)).chain(function (data) {
                        ret.push(data);
                        done();
                    }, done);
                };
                pipeAll(queryStream, ret);
            } else {
                ret = queryStream;
            }
            return ret;
        },


        /**
         * Returns a Promise that is resolved with an array with all records in the dataset.
         * If a block is given, the array is iterated over after all items have been loaded.
         *
         * @example
         *
         * // SELECT * FROM table
         * DB.from("table").all().chain(function(res){
         *      //res === [{id : 1, ...}, {id : 2, ...}, ...];
         * });
         * // Iterate over all rows in the table
         * var myArr = [];
         * var rowPromise = DB.from("table").all(function(row){ myArr.push(row);});
         * rowPromise.chain(function(rows){
         *    //=> rows == myArr;
         * });
         *
         * @param {Function} block a block to be called with each item. The return value of the block is ignored.
         * @param {Function} [cb] a block to invoke when the action is done
         *
         * @return {comb.Promise} a promise that is resolved with an array of rows.
         */
        all: function (block, cb) {
            var self = this;
            var ret = asyncArray(this.forEach().chain(function (records) {
                return self.postLoad(records);
            }));
            if (block) {
                ret = ret.forEach(block);
            }
            return ret.classic(cb).promise();
        },


        /**
         * Returns a promise that is resolved with the average value for the given column.
         *
         * @example
         *
         * // SELECT avg(number) FROM table LIMIT 1
         * DB.from("table").avg("number").chain(function(avg){
         *      // avg === 3
         * });
         *
         * @param {String|patio.sql.Identifier} column the column to average
         * @param {Function} [cb] the callback to invoke when the action is done.
         *
         * @return {comb.Promise} a promise that is resolved with the average value of the column.
         */
        avg: function (column, cb) {
            return this.__aggregateDataset().get(sql.avg(this.stringToIdentifier(column)), cb);
        },

        /**
         * Returns a promise that is resolved with the number of records in the dataset.
         *
         * @example
         *
         * // SELECT COUNT(*) AS count FROM table LIMIT 1
         * DB.from("table").count().chain(function(count){
         *     //count === 3;
         * });
         *
         * @param {Function} [cb] the callback to invoke when the action is done.
         *
         * @return {comb.Promise} a promise that is resolved with the the number of records in the dataset.
         */
        count: function (cb) {
            return this.__aggregateDataset().get(sql.COUNT(sql.literal("*")).as("count")).chain(function (res) {
                return parseInt(res, 10);
            }).classic(cb);
        },

        /**@ignore*/
        "delete": function () {
            return this.remove();
        },


        /**
         * Deletes the records in the dataset.  The returned Promise should be resolved with the
         * number of records deleted, but that is adapter dependent.
         *
         * @example
         *
         * // DELETE * FROM table
         * DB.from("table").remove().chain(function(numDeleted){
         *     //numDeleted === 3
         * });
         *
         * @param {Function} [cb] the callback to invoke when the action is done.
         *
         * @return {comb.Promise} a promise resolved with the
         * number of records deleted, but that is adapter dependent.
         */
        remove: function (cb) {
            return this.executeDui(this.deleteSql).classic(cb).promise();
        },

        /**
         * Iterates over the records in the dataset as they are returned from the
         * database adapter.
         *
         * @example
         *
         * // SELECT * FROM table
         * DB.from("table").forEach(function(row){
         *      //....do something
         * });
         *
         * @param {Function} [block] the block to invoke for each row.
         * @param {Function} [cb] the callback to invoke when the action is done.
         *
         * @return {comb.Promise} a promise that is resolved when the action has completed.
         */
        forEach: function (block, cb) {
            var rowCb, ret;
            if (this.__opts.graph) {
                ret = this.graphEach(block);
            } else {
                ret = this.fetchRows(this.selectSql);
                if ((rowCb = this.rowCb)) {
                    ret = ret.map(function (r) {
                        return rowCb(r);
                    });
                }
                if (block) {
                    ret = ret.forEach(block);
                }
            }
            return ret.classic(cb);
        },

        /**
         * Returns a promise that is resolved with true if no records exist in the dataset,
         * false otherwise.
         * @example
         *
         * // SELECT 1 FROM table LIMIT 1
         * DB.from("table").isEmpty().chain(function(isEmpty){
         *   // isEmpty === false
         * });
         *
         * @param {Function} [cb] a function to callback when action is done
         *
         * @return {comb.Promise} a promise that is resolved with a boolean indicating if the table is empty.
         */
        isEmpty: function (cb) {
            return this.get(1).chain(function (res) {
                return isUndefinedOrNull(res) || res.length === 0;
            }.bind(this)).classic(cb);
        },

        __processFields: function (fields) {
            throw new Error("Not Implemented");
        },

        __processRow: function (row, cols) {
            var h = {}, i = -1, l = cols.length, col;
            while (++i < l) {
                col = cols[i];
                h[col[0]] = col[1](row[col[2]]);
            }
            return h;
        },

        __processRows: function (rows, cols) {
            //dp this so the callbacks are called in appropriate order also.
            var ret = [], i = -1, l = rows.length, processRow = this.__processRow;
            while (++i < l) {
                ret[i] = processRow.call(this, rows[i], cols);
            }
            cols = null;
            rows.length = 0;
            return ret;
        },

        fetchStreamedRows: function (sql, opts) {
            var ret = new TransformStream({objectMode: true}), cols, self = this;
            ret._transform = function (row, encoding, callback) {
                ret.push(self.__processRow(row, cols));
                callback();
            };
            var queryStream = this.execute(sql, opts);
            queryStream.on("fields", function (fields) {
                cols = self.__processFields(fields);
            });
            pipeAll(queryStream, ret);
            return ret;
        },

        fetchPromisedRows: function (sql, opts) {
            var self = this;
            return asyncArray(this.execute(sql, opts).chain(function (rows, fields) {
                return self.__processRows(rows, self.__processFields(fields));
            }));
        },

        /**
         * @private
         * Executes a select query and fetches records, passing each record to the
         * supplied cb. This method should not be called by user code, use {@link patio.Dataset#forEach}
         * instead.
         */
        fetchRows: function (sql, opts) {
            opts = opts || {};
            var ret;
            if (opts.stream) {
                ret = this.fetchStreamedRows(sql, opts);
            } else {
                ret = this.fetchPromisedRows(sql, opts);
            }
            return ret;
        },

        /**
         * If a integer argument is given, it is interpreted as a limit, and then returns all
         * matching records up to that limit.
         *
         * If no arguments are passed, it returns the first matching record.
         *
         * If a function taking no arguments is passed in as the last parameter then it
         * is assumed to be a filter block. If the a funciton is passed in that takes arguments
         * then it is assumed to be a callback. You may also pass in both the second to last argument
         * being a filter function, and the last being a callback.
         *
         * If any other type of argument(s) is passed, it is given to {@link patio.Dataset#filter} and the
         * first matching record is returned.  Examples:
         *
         * @example
         *
         * comb.executeInOrder(DB.from("table"), function(ds){
         *   // SELECT * FROM table LIMIT 1
         *   ds.first(); // => {id : 7}
         *
         *   // SELECT * FROM table LIMIT 2
         *   ds.first(2); // => [{id : 6}, {id : 4}]
         *
         *   // SELECT * FROM table WHERE (id = 2) LIMIT 1
         *   ds.first({id : 2}) // => {id : 2}
         *
         *
         *  // SELECT * FROM table WHERE (id = 3) LIMIT 1
         *   ds.first("id = 3"); // => {id : 3}
         *
         *   // SELECT * FROM table WHERE (id = 4) LIMIT 1
         *   ds.first("id = ?", 4); // => {id : 4}
         *
         *   // SELECT * FROM table WHERE (id > 2) LIMIT 1
         *   ds.first(function(){return this.id.gt(2);}); // => {id : 5}
         *
         *
         *   // SELECT * FROM table WHERE ((id > 4) AND (id < 6)) LIMIT 1
         *   ds.first("id > ?", 4, function(){
         *          return this.id.lt(6);
         *   }); // => {id : 5}
         *
         *    // SELECT * FROM table WHERE (id < 2) LIMIT 2
         *   ds.first(2, function(){
         *          return this.id.lt(2)
         *   }); // => [{id:1}]
         * });
         *
         * @param {*} args varargs to be used to limit/filter the result set.
         *
         * @return {comb.Promise} a promise that is resolved with the either the first matching record.
         *                        Or an array of items if a limit was provided as the first argument.
         */
        first: function (args) {
            args = comb(arguments).toArray();
            var cb,
                block = isFunction(args[args.length - 1]) ? args.pop() : null;
            if (block && block.length > 0) {
                cb = block;
                block = isFunction(args[args.length - 1]) ? args.pop() : null;
            }

            var ds = block ? this.filter(block) : this;
            if (!args.length) {
                return ds.singleRecord(cb);
            } else {
                args = (args.length === 1) ? args[0] : args;
                if (isNumber(args)) {
                    return ds.limit(args).all(null, cb);
                } else {
                    return ds.filter(args).singleRecord(cb);
                }
            }
        },

        /**
         * Return the column value for the first matching record in the dataset.
         *
         * @example
         *  // SELECT id FROM table LIMIT 1
         *  DB.from("table").get("id").chain(function(val){
         *   // val === 3
         *  });
         *
         *
         * // SELECT sum(id) FROM table LIMIT 1
         * ds.get(sql.sum("id")).chain(function(val){
         *      // val === 6;
         * });
         *
         * // SELECT sum(id) FROM table LIMIT 1
         * ds.get(function(){
         *      return this.sum("id");
         * }).chain(function(val){
         *      // val === 6;
         * });
         *
         * @param {*} column the column to filter on can be anything that
         *            {@link patio.Dataset#select} accepts.
         * @param {Function} [cb] the callback to invoke when the action is done.
         *
         * @return {comb.Promise} a promise that will be resolved will the value requested.
         */
        get: function (column, cb) {
            return this.select(column).singleValue(cb);
        },

        /**
         * Inserts multiple records into the associated table. This method can be
         * used to efficiently insert a large number of records into a table in a
         * single query if the database supports it. Inserts
         * are automatically wrapped in a transaction.
         *
         * This method is called with a columns array and an array of value arrays:
         * <pre class="code">
         *   // INSERT INTO table (x, y) VALUES (1, 2)
         *   // INSERT INTO table (x, y) VALUES (3, 4)
         *   DB.from("table").import(["x", "y"], [[1, 2], [3, 4]]).
         * </pre>
         *
         * This method also accepts a dataset instead of an array of value arrays:
         *
         * <pre class="code">
         *  // INSERT INTO table (x, y) SELECT a, b FROM table2
         *  DB.from("table").import(["x", "y"], DB.from("table2").select("a", "b"));
         * </pre>
         *
         * The method also accepts a commitEvery option that specifies
         * the number of records to insert per transaction. This is useful especially
         * when inserting a large number of records, e.g.:
         *
         * <pre class="code">
         *   // this will commit every 50 records
         *  DB.from("table").import(["x", "y"], [[1, 2], [3, 4], ...], {commitEvery : 50});
         * </pre>
         *
         * @param {Array} columns The columns to insert values for.
         *                  This array will be used as the base for each values item in the values array.
         * @param {Array[Array]} values Array of arrays of values to insert into the columns.
         * @param {Object} [opts] options
         * @param {Number} [opts.commitEvery] the number of records to insert per transaction.
         * @param {Function} [cb] the callback to invoke when the action is done.
         *
         * @return {comb.Promise} a promise that is resolved once all records have been inserted.
         */
        "import": function (columns, values, opts, cb) {
            if (isFunction(opts)) {
                cb = opts;
                opts = null;
            }
            opts = opts || {};
            var ret, self = this;
            if (isInstanceOf(values, Dataset)) {
                ret = this.db.transaction(function () {
                    return self.insert(columns, values);
                });
            } else {

                if (!values.length) {
                    ret = new Promise().callback();
                } else if (!columns.length) {
                    throw new QueryError("Invalid columns in import");
                }

                var sliceSize = opts.commitEvery || opts.slice, result = [];
                if (sliceSize) {
                    ret = asyncArray(partition(values, sliceSize)).forEach(function (entries, offset) {
                        offset = (offset * sliceSize);
                        return self.db.transaction(opts, function () {
                            return when(self.multiInsertSql(columns, entries).map(function (st, index) {
                                return self.executeDui(st).chain(function (res) {
                                    result[offset + index] = res;
                                });
                            }));
                        });
                    }, 1);
                } else {
                    var statements = this.multiInsertSql(columns, values);
                    ret = this.db.transaction(function () {
                        return when(statements.map(function (st, index) {
                            return self.executeDui(st).chain(function (res) {
                                result[index] = res;
                            });
                        }));
                    });
                }
            }
            return ret.chain(function () {
                return flatten(result);
            }).classic(cb).promise();
        },

        /**
         * This is the recommended function to do the insert of multiple items into the
         * database. This acts as a proxy to the {@link patio.Dataset#import} method so
         * one can use an array of hashes rather than an array of columns and an array of values.
         * See {@link patio.Dataset#import} for more information regarding the method of inserting.
         * <p>
         *     <b>NOTE:</b>All hashes should have the same keys other wise some values could be missed</b>
         * </p>
         *
         * @example
         *
         * // INSERT INTO table (x) VALUES (1)
         * // INSERT INTO table (x) VALUES (2)
         * DB.from("table").multiInsert([{x : 1}, {x : 2}]).chain(function(){
         *     //...do something
         * })
         *
         * //commit every 50 inserts
         * DB.from("table").multiInsert([{x : 1}, {x : 2},....], {commitEvery : 50}).chain(function(){
         *     //...do something
         * });
         *
         * @param {Object[]} hashes an array of objects to insert into the database. The keys of
         * the first item in the array will be used to look up columns in all subsequent objects. If the
         * array is empty then the promise is resolved immediatly.
         *
         * @param {Object} opts See {@link patio.Dataset#import}.
         * @param {Function} [cb] the callback to invoke when the action is done.
         *
         * @return {comb.Promise} See {@link patio.Dataset#import} for return functionality.
         */
        multiInsert: function (hashes, opts, cb) {
            if (isFunction(opts)) {
                cb = opts;
                opts = null;
            }
            opts = opts || {};
            hashes = hashes || [];
            var ret = new Promise();
            if (!hashes.length) {
                ret.callback();
            } else {
                var columns = Object.keys(hashes[0]);
                ret = this["import"](columns, hashes.map(function (h) {
                    return columns.map(function (c) {
                        return h[c];
                    });
                }), opts, cb);
            }
            return ret.classic(cb).promise();
        },

        /**
         * Inserts values into the associated table. The returned value is generally
         * the value of the primary key for the inserted row, but that is adapter dependent.
         *
         * @example
         *
         * // INSERT INTO items DEFAULT VALUES
         * DB.from("items").insert()
         *
         * // INSERT INTO items DEFAULT VALUES
         * DB.from("items").insert({});
         *
         * // INSERT INTO items VALUES (1, 2, 3)
         * DB.from("items").insert([1,2,3]);
         *
         * // INSERT INTO items (a, b) VALUES (1, 2)
         * DB.from("items").insert(["a", "b"], [1,2]);
         *
         * // INSERT INTO items (a, b) VALUES (1, 2)
         * DB.from("items").insert({a : 1, b : 2});
         *
         * // INSERT INTO items SELECT * FROM old_items
         * DB.from("items").insert(DB.from("old_items"));
         *
         * // INSERT INTO items (a, b) SELECT * FROM old_items
         * DB.from("items").insert(["a", "b"], DB.from("old_items"));
         *
         *
         *
         * @param {patio.Dataset|patio.sql.LiteralString|Array|Object|patio.sql.BooleanExpression|...} values  values to
         *      insert into the database. The INSERT statement generated depends on the type.
         *      <ul>
         *          <li>Empty object| Or no arugments: then DEFAULT VALUES is used.</li>
         *          <li>Object: the keys will be used as the columns, and values will be the values inserted.</li>
         *          <li>Single {@link patio.Dataset} : an insert with subselect will be performed.</li>
         *          <li>Array with {@link patio.Dataset} : The array will be used for columns and a subselect will performed with the dataset for the values.</li>
         *          <li>{@link patio.sql.LiteralString} : the literal value will be used.</li>
         *          <li>Single Array : the values in the array will be used as the VALUES clause.</li>
         *          <li>Two Arrays: the first array is the columns the second array is the values.</li>
         *          <li>{@link patio.sql.BooleanExpression} : the expression will be used as the values.
         *          <li>An arbitrary number of arguments : the {@link patio.Dataset#literal} version of the values will be used</li>
         *      </ul>
         * @param {Function} [cb] the callback to invoke when the action is done.
         *
         * @return {comb.Promise} a promise that is typically resolved with the ID of the inserted row.
         */
        insert: function () {
            var args = argsToArray(arguments);
            var cb = isFunction(args[args.length - 1]) ? args.pop() : null;
            return this.executeInsert(this.insertSql.apply(this, args)).classic(cb);
        },

        /**
         * @see patio.Dataset#insert
         */
        save: function () {
            return this.insert.apply(this, arguments);
        },

        /**
         * Inserts multiple values. If a block is given it is invoked for each
         * item in the given array before inserting it.  See {@link patio.Dataset#multiInsert} as
         * a possible faster version that inserts multiple records in one SQL statement.
         *
         * <b> Params see @link patio.Dataset#insert</b>
         *
         * @example
         *
         * DB.from("table").insertMultiple([{x : 1}, {x : 2}]);
         *      //=> INSERT INTO table (x) VALUES (1)
         *      //=> INSERT INTO table (x) VALUES (2)
         *
         * DB.from("table").insertMultiple([{x : 1}, {x : 2}], function(row){
         *      row.y = row.x * 2;
         * });
         *      //=> INSERT INTO table (x, y) VALUES (1, 2)
         *      //=> INSERT INTO table (x, y) VALUES (2, 4)
         *
         * @param array See {@link patio.Dataset#insert} for possible values.
         * @param {Function} [block] a function to be called before each item is inserted.
         * @param {Function} [cb] a function to be called when the aciton is complete
         *
         * @return {comb.PromiseList} a promiseList that should be resolved with the id of each item inserted
         *          in the order that was in the array.
         */
        insertMultiple: function (array, block, cb) {
            var promises, ret;
            if (block) {
                ret = when(array.map(function (i) {
                    return this.insert(block(i));
                }, this));
            } else {
                ret = when(array.map(function (i) {
                    return this.insert(i);
                }, this));
            }
            return ret.classic(cb).promise();
        },

        /**
         * @see patio.Dataset#insertMultiple
         */
        saveMultiple: function () {
            return this.insertMultiple.apply(this, arguments);
        },

        /**
         * Returns a promise that is resolved with the interval between minimum and maximum values
         * for the given column.
         *
         * @example
         *  // SELECT (max(id) - min(id)) FROM table LIMIT 1
         *   DB.from("table").interval("id").chain(function(interval){
         *      //(e.g) interval === 6
         *   });
         *
         * @param {String|patio.sql.Identifier} column to find the interval of.
         * @param {Function} [cb] a function to be called when the aciton is complete
         *
         * @return {comb.Promise} a promise that will be resolved with the interval between the min and max values
         * of the column.
         */
        interval: function (column, cb) {
            return this.__aggregateDataset().get(sql.max(column).minus(sql.min(column)), cb);
        },

        /**
         * Reverses the order and then runs first.  Note that this
         * will not necessarily give you the last record in the dataset,
         * unless you have an unambiguous order.
         *
         * @example
         *
         *  // SELECT * FROM table ORDER BY id DESC LIMIT 1
         *  DB.from("table").order("id").last().chain(function(lastItem){
         *      //...(e.g lastItem === {id : 10})
         *  });
         *
         *  // SELECT * FROM table ORDER BY id ASC LIMIT 2
         *   DB.from("table").order(sql.id.desc()).last(2).chain(function(lastItems){
         *      //...(e.g lastItems === [{id : 1}, {id : 2});
         *  });
         *
         * @throws {patio.error.QueryError} If there is not currently an order for this dataset.
         *
         * @param {*} args See {@link patio.Dataset#first} for argument types.
         *
         * @return {comb.Promise} a promise that will be resolved with a single object or array depending on the
         * arguments provided.
         */
        last: function (args) {
            if (!this.__opts.order) {
                throw new QueryError("No order specified");
            }
            var ds = this.reverse();
            return ds.first.apply(ds, arguments);
        },

        /**
         * Maps column values for each record in the dataset (if a column name is
         * given).
         *
         * @example
         *
         * // SELECT * FROM table
         * DB.from("table").map("id").chain(function(ids){
         *   // e.g. ids === [1, 2, 3, ...]
         * });
         *
         *  // SELECT * FROM table
         * DB.from("table").map(function(r){
         *      return r.id * 2;
         * }).chain(function(ids){
         *     // e.g. ids === [2, 4, 6, ...]
         * });
         *
         * @param {Function|String} column if a string is provided then then it is assumed
         * to be the name of a column in that table and the value of the column for each row
         * will be returned. If column is a function then the return value of the function will
         * be used.
         * @param {Function} [cb] a function to be called when the aciton is complete
         *
         * @return {comb.Promise} a promise resolved with the array of mapped values.
         */
        map: function (column, cb) {
            var ret = this.forEach();
            column && (ret = ret[isFunction(column) ? "map" : "pluck"](column));
            return ret.classic(cb).promise();
        },

        /**
         * Returns a promise resolved with  the maximum value for the given column.
         *
         * @example
         *
         * // SELECT max(id) FROM table LIMIT 1
         * DB.from("table").max("id").chain(function(max){
         *   // e.g. max === 10.
         * });
         *
         *
         * @param {String|patio.sql.Identifier} column the column to find the maximum value for.
         * @param {Function} [cb] callback to invoke when action is done
         *
         * @return {*} the maximum value for the column.
         */
        max: function (column, cb) {
            return this.__aggregateDataset().get(sql.max(this.stringToIdentifier(column)), cb);
        },

        /**
         * Returns a promise resolved with  the minimum value for the given column.
         *
         * @example
         *
         * // SELECT min(id) FROM table LIMIT 1
         * DB.from("table").min("id").chain(function(min){
         *   // e.g. max === 0.
         * });
         *
         *
         * @param {String|patio.sql.Identifier} column the column to find the minimum value for.
         * @param {Function} [cb] callback to invoke when action is done
         *
         * @return {*} the minimum value for the column.
         */
        min: function (column, cb) {
            return this.__aggregateDataset().get(sql.min(this.stringToIdentifier(column)), cb);
        },

        /**
         * Returns a promise resolved with  a range from the minimum and maximum values for the
         * given column.
         *
         * @example
         *  // SELECT max(id) AS v1, min(id) AS v2 FROM table LIMIT 1
         *  DB.from("table").range("id").chain(function(min, max){
         *      //e.g min === 1 AND max === 10
         *  });
         *
         * @param {String|patio.sql.Identifier} column the column to find the min and max value for.
         * @param {Function} [cb] the callback to invoke when the action is done.
         *
         * @return {comb.Promise} a promise that is resolved with the min and max value, as the first
         * and second args respectively.
         */
        range: function (column, cb) {
            var ret = new Promise();
            this.__aggregateDataset()
                .select(sql.min(this.stringToIdentifier(column)).as("v1"), sql.max(this.stringToIdentifier(column)).as("v2"))
                .first()
                .chain(function (r) {
                    ret.callback(r.v1, r.v2);
                }, ret.errback);
            return ret.classic(cb).promise();
        },


        /**
         * Selects the column given (either as an argument or as a callback), and
         * returns an array of all values of that column in the dataset.  If you
         * give a block argument that returns an array with multiple entries,
         * the contents of the resulting array are undefined.
         *
         *
         * @example
         *  // SELECT id FROM table
         *  DB.from("table").selectMap("id").chain(function(selectMap){
         *   // e,g. selectMap === [3, 5, 8, 1, ...]
         * });
         *
         * // SELECT abs(id) FROM table
         * DB.from("table").selectMap(function(){
         *      return this.abs("id");
         * }).chain(function(selectMap){
         *   //e.g selectMap === [3, 5, 8, 1, ...]
         * });
         *
         * @param {*} column  The column to return the values for.
         *      See {@link patio.Dataset#select} for valid column values.
         * @param {Function} [cb] a function to be called when the aciton is complete
         *
         * @return {comb.Promise} a promise resolved with the array of mapped values.
         */
        selectMap: function (column, cb) {
            var ds = this.naked().ungraphed().select(column), col;
            return ds.map(function (r) {
                return r[col || (col = Object.keys(r)[0])];
            }, cb);
        },

        /**
         * The same as {@link patio.Dataset#selectMap}, but in addition orders the array by the column.
         *
         * @example
         * // SELECT id FROM table ORDER BY id
         * DB.from("table").selectOrderMap("id").chain(function(mappedIds){
         *   //e.g. [1, 2, 3, 4, ...]
         * });
         *
         *  // SELECT abs(id) FROM table ORDER BY abs(id)
         *  DB.from("table").selectOrderMap(function(){
         *          return this.abs("id");
         *  }).chain(function(mappedIds){
         *      //e.g. [1, 2, 3, 4, ...]
         *  });
         *
         * @param {*} column  The column to return the values for.
         *      See {@link patio.Dataset#select} for valid column values.
         *
         * @return {comb.Promise} a promise resolved with the array of mapped values.
         */
        selectOrderMap: function (column, cb) {
            var col, ds = this.naked()
                .ungraphed()
                .select(column)
                .order(isFunction(column) ? column : this._unaliasedIdentifier(column));
            return ds.map(function (r) {
                return r[col || (col = Object.keys(r)[0])];
            }, cb);
        },

        /**
         * Same as {@link patio.Dataset#singleRecord} but accepts arguments
         * to filter the dataset. See {@link patio.Dataset#filter} for argument types.
         *
         * <b>NOTE</b> If the last argument is a function that accepts arguments it is not assumed to
         * be a filter function but instead a callback.
         *
         * @return {comb.Promise} a promise resolved with a single row from the database that matched the filter.
         */
        one: function () {
            var args = comb(arguments).toArray(), cb;
            var last = args[args.length - 1];
            if (isFunction(last) && last.length > 0) {
                cb = args.pop();
            }
            var ret = this;
            if (args.length) {
                ret = ret.filter.apply(ret, args);
            }
            return ret.singleRecord(cb);
        },

        /**
         * Returns a promise resolved with  the first record in the dataset, or null if the dataset
         * has no records. Users should probably use {@link patio.Dataset#first} instead of
         * this method.
         *
         * @example
         *
         * //'SELECT * FROM test LIMIT 1'
         * DB.from("test").singleRecord().chain(function(r) {
         *     //e.g r === {id : 1, name : "firstName"}
         * });
         *
         * @param {Function} [cb] a function to be called when the aciton is complete
         *
         * @return {comb.Promise} a promise resolved with the first record returned from the query.
         */
        singleRecord: function (cb) {
            return this.mergeOptions({limit: 1}).all().chain(function (r) {
                return r && r.length ? r[0] : null;
            }).classic(cb).promise();
        },

        /**
         * Returns a promise resolved with the first value of the first record in the dataset.
         * Returns null if dataset is empty.  Users should generally use
         * {@link patio.Dataset#get} instead of this method.
         *
         * @example
         *
         * //'SELECT * FROM test LIMIT 1'
         * DB.from("test").singleValue().chain(function(r) {
         *     //e.g r === 1
         * });
         *
         * @param {Function} [cb] the callback to invoke when the action is done.
         *
         * @return {comb.Promise} a promise that will be resolved with the first value of the first row returned
         * from the dataset.
         *
         */
        singleValue: function (cb) {
            return this.naked().ungraphed().singleRecord().chain(function (r) {
                return r ? r[Object.keys(r)[0]] : null;
            }).classic(cb).promise();
        },

        /**
         * Returns a promise resolved the sum for the given column.
         *
         * @example
         *
         *  // SELECT sum(id) FROM table LIMIT 1
         * DB.from("table").sum("id").chain(function(sum){
         *   // e.g sum === 55
         * });
         *
         * @param {String|patio.sql.Identifier\patio.sql.QualifiedIdentifier|patio.sql.AliasedExpression} column
         * the column to find the sum of.
         * @param {Function} [cb] the callback to invoke when the action is done.
         *
         * @return {comb.Promise} a promise resolved with the sum of the column.
         */
        sum: function (column, cb) {
            return this.__aggregateDataset().get(sql.sum(this.stringToIdentifier(column)), cb);
        },


        /**
         * Returns a promise resolved with a string in CSV format containing the dataset records. By
         * default the CSV representation includes the column titles in the
         * first line. You can turn that off by passing false as the
         * includeColumnTitles argument.
         *
         * <p>
         *    <b>NOTE:</b> This does not use a CSV library or handle quoting of values in
         *          any way.  If any values in any of the rows could include commas or line
         *          endings, you shouldn't use this.
         * </p>
         *
         * @example
         *  // SELECT * FROM table
         *  DB.from("table").toCsv().chain(function(csv){
         *      console.log(csv);
         *      //outputs
         *        id,name
         *        1,Jim
         *        2,Bob
         *  });
         *
         *    // SELECT * FROM table
         *  DB.from("table").toCsv(false).chain(function(csv){
         *      console.log(csv);
         *      //outputs
         *        1,Jim
         *        2,Bob
         *  });
         *
         *  @param {Boolean} [includeColumnTitles=true] Set to false to prevent the printing of the column
         *  titles as the first line.
         *
         *  @param {Function} [cb] the callback to invoke when the action is done.
         *
         *  @return {comb.Promise} a promise that will be resolved with the CSV string of the results of the
         *  query.
         */
        toCsv: function (includeColumnTitles, cb) {
            var n = this.naked();
            if (isFunction(includeColumnTitles)) {
                cb = includeColumnTitles;
                includeColumnTitles = true;
            }
            includeColumnTitles = isBoolean(includeColumnTitles) ? includeColumnTitles : true;
            return n.columns.chain(function (cols) {
                var vals = [];
                if (includeColumnTitles) {
                    vals.push(cols.join(", "));
                }
                return n.forEach(function (r) {
                    vals.push(cols.map(function (c) {
                        return r[c] || "";
                    }).join(", "));
                }).chain(function () {
                    return vals.join("\r\n") + "\r\n";
                });
            }.bind(this)).classic(cb).promise();
        },

        /**
         * Returns a promise resolved with a hash with keyColumn values as keys and valueColumn values as
         * values.  Similar to {@link patio.Dataset#toHash}, but only selects the two columns.
         *
         * @example
         *  // SELECT id, name FROM table
         *  DB.from("table").selectHash("id", "name").chain(function(hash){
         *   // e.g {1 : 'a', 2 : 'b', ...}
         *  });
         *
         * @param {String|patio.sql.Identifier\patio.sql.QualifiedIdentifier|patio.sql.AliasedExpression} keyColumn the column
         * to use as the key in the hash.
         *
         * @param {String|patio.sql.Identifier\patio.sql.QualifiedIdentifier|patio.sql.AliasedExpression} valueColumn the column
         * to use as the value in the hash.
         *
         * @param {Function} [cb] the callback to invoke when the action is done.
         *
         * @return {comb.Promise} a promise that is resolved with an array of hashes, that have the keyColumn
         * as the key and the valueColumn as the value.
         */
        selectHash: function (keyColumn, valueColumn, cb) {
            var map = {}, args = comb.argsToArray(arguments);
            cb = isFunction(args[args.length - 1]) ? args.pop() : null;
            var k = this.__hashIdentifierToName(keyColumn),
                v = this.__hashIdentifierToName(valueColumn);
            return this.select.apply(this, args).map(function (r) {
                map[r[k]] = v ? r[v] : r;
            }).chain(function () {
                return map;
            }).classic(cb).promise();
        },

        /**
         * Returns a promise resolved with a hash with one column used as key and another used as value.
         * If rows have duplicate values for the key column, the latter row(s)
         * will overwrite the value of the previous row(s). If the valueColumn
         * is not given or null, uses the entire hash as the value.
         *
         * @example
         *  // SELECT * FROM table
         *  DB.from("table").toHash("id", "name").chain(function(hash){
         *    // {1 : 'Jim', 2 : 'Bob', ...}
         *  });
         *
         * // SELECT * FROM table
         * DB.from("table").toHash("id").chain(function(hash){
         *   // {1 : {id : 1, name : 'Jim'}, 2 : {id : 2, name : 'Bob'}, ...}
         * });
         *
         *  @param {String|patio.sql.Identifier\patio.sql.QualifiedIdentifier|patio.sql.AliasedExpression} keyColumn the column
         *  to use as the key in the returned hash.
         *
         *  @param {String|patio.sql.Identifier\patio.sql.QualifiedIdentifier|patio.sql.AliasedExpression} [keyValue=null] the
         *  key of the column to use as the value in the hash
         *
         *  @param {Function} [cb] the callback to invoke when the action is done.
         *
         *  @return {comb.Promise} a promise that will be resolved with the resulting hash.
         */
        toHash: function (keyColumn, valueColumn, cb) {
            var ret = new Promise(), map = {};
            if (isFunction(valueColumn)) {
                cb = valueColumn;
                valueColumn = null;
            }
            var k = this.__hashIdentifierToName(keyColumn), v = this.__hashIdentifierToName(valueColumn);
            return this.map(function (r) {
                map[r[k]] = v ? r[v] : r;
            }).chain(function () {
                return map;
            }).classic(cb).promise();
        },

        /**
         * Truncates the dataset.  Returns a promise that is resolved once truncation is complete.
         *
         * @example
         *
         * // TRUNCATE table
         * DB.from("table").truncate().chain(function(){
         *     //...do something
         * });
         *
         * @param {Function} [cb] the callback to invoke when the action is done.
         *
         * @return {comb.Promise} a promise that is resolved once truncation is complete.
         */
        truncate: function (cb) {
            return this.executeDdl(this.truncateSql).classic(cb);
        },

        /**
         * Updates values for the dataset.  The returned promise is resolved with a value that is generally the
         * number of rows updated, but that is adapter dependent.
         *
         * @example
         * // UPDATE table SET x = NULL
         *  DB.from("table").update({x : null}).chain(function(numRowsUpdated){
         *      //e.g. numRowsUpdated === 10
         *  });
         *
         * // UPDATE table SET x = (x + 1), y = 0
         * DB.from("table").update({ x : sql.x.plus(1), y : 0}).chain(function(numRowsUpdated){
         *   // e.g. numRowsUpdated === 10
         * });
         *
         * @param {Object} values See {@link patio.Dataset#updateSql} for parameter types.
         * @param {Function} [cb] the callback to invoke when the action is done.
         *
         * @return {comb.Promise}  a promise that is generally resolved with the
         * number of rows updated, but that is adapter dependent.
         */
        update: function (values, cb) {
            return this.executeDui(this.updateSql(values)).classic(cb);
        },

        /**
         * @see patio.Dataset#set
         */
        set: function () {
            this.update.apply(this, arguments);
        },

        /**
         * @private
         * Execute the given select SQL on the database using execute. Use the
         * readOnly server unless a specific server is set.
         */
        execute: function (sql, opts) {
            return this.db.execute(sql, merge({server: this.__opts.server || "readOnly"}, opts || {}));
        },

        /**
         * @private
         * Execute the given SQL on the database using {@link patio.Database#executeDdl}.
         */
        executeDdl: function (sql, opts) {
            return this.db.executeDdl(sql, this.__defaultServerOpts(opts || {}));
        },

        /**
         * @private
         * Execute the given SQL on the database using {@link patio.Database#executeDui}.
         */
        executeDui: function (sql, opts) {
            return this.db.executeDui(sql, this.__defaultServerOpts(opts || {}));
        },

        /**
         * @private
         * Execute the given SQL on the database using {@link patio.Database#executeInsert}.
         */
        executeInsert: function (sql, opts) {
            return this.db.executeInsert(sql, this.__defaultServerOpts(opts || {}));
        },

        /**
         * This is run inside {@link patio.Dataset#all}, after all of the records have been loaded
         * via {@link patio.Dataset#forEach}, but before any block passed to all is called.  It is called with
         * a single argument, an array of all returned records.  Does nothing by
         * default.
         */
        postLoad: function (allRecords) {
            return allRecords;
        },

        /**
         * @private
         *
         * Clone of this dataset usable in aggregate operations.  Does
         * a {@link patio.Dataset#fromSelf} if dataset contains any parameters that would
         * affect normal aggregation, or just removes an existing
         * order if not.
         */
        __aggregateDataset: function () {
            return this._optionsOverlap(this._static.COUNT_FROM_SELF_OPTS) ? this.fromSelf() : this.unordered();
        },

        /**
         * @private
         * Set the server to use to "default" unless it is already set in the passed opts
         */
        __defaultServerOpts: function (opts) {
            return merge({server: this.__opts.server || "default"}, opts || {});
        },

        /**
         * @private
         *
         * Returns the string version of the identifier.
         *
         * @param {String|patio.sql.Identifier\patio.sql.QualifiedIdentifier|patio.sql.AliasedExpression} identifier
         *      identifier to resolve to a string.
         * @return {String} the string version of the identifier.
         */
        __hashIdentifierToName: function (identifier) {
            return isString(identifier) ? this.__hashIdentifierToName(this.stringToIdentifier(identifier)) :
                isInstanceOf(identifier, Identifier) ? identifier.value :
                    isInstanceOf(identifier, QualifiedIdentifier) ? identifier.column :
                        isInstanceOf(identifier, AliasedExpression) ? identifier.alias : identifier;
        },

        /**@ignore*/
        getters: {
            /**@lends patio.Dataset.prototype*/
            /**
             * @field
             * @type {comb.Promise}
             *
             *  Returns a promise that is resolved with the columns in the result set in order as an array of strings.
             * If the columns are currently cached, then the promise is immediately resolved with the cached value. Otherwise,
             * a SELECT query is performed to retrieve a single row in order to get the columns.
             *
             * If you are looking for all columns for a single table and maybe some information about
             * each column (e.g. database type), see {@link patio.Database#schema}.
             *
             * <pre class="code">
             *  DB.from("table").columns.chain(function(columns){
             *        // => ["id", "name"]
             *   });
             * </pre>
             **/
            columns: function () {
                if (this.__columns) {
                    return asyncArray(this.__columns);
                } else {
                    var ds = this.unfiltered().unordered().mergeOptions({distinct: null, limit: 1});
                    return asyncArray(ds.forEach().chain(function () {
                        var columns = this.__columns = ds.__columns || [];
                        return columns;
                    }.bind(this)));
                }
            }
        }
    },

    "static": {

        /**@lends patio.Dataset*/

        /**
         * List of action methods avaiable on the dataset.
         *
         * @type String[]
         * @default ['all', 'one', 'avg', 'count', 'columns', 'remove', 'forEach', 'isEmpty', 'first',
         *          'get', 'import', 'insert', 'save', 'insertMultiple', 'saveMultiple', 'interval', 'last',
         *          'map', 'max', 'min', 'multiInsert', 'range', 'selectHash', 'selectMap', 'selectOrderMap', 'set',
         *          'singleRecord', 'singleValue', 'sum', 'toCsv', 'toHash', 'truncate', 'update', 'stream']
         */
        ACTION_METHODS: ['all', 'one', 'avg', 'count', 'columns', 'remove', 'forEach', 'isEmpty', 'first',
            'get', 'import', 'insert', 'save', 'insertMultiple', 'saveMultiple', 'interval', 'last',
            'map', 'max', 'min', 'multiInsert', 'range', 'selectHash', 'selectMap', 'selectOrderMap', 'set',
            'singleRecord', 'singleValue', 'sum', 'toCsv', 'toHash', 'truncate', 'update', 'stream'],

        /**
         * List of options that can interfere with the aggregation of a {@link patio.Dataset}
         * @type String[]
         * @default ["distinct", "group", "sql", "limit", "compounds"]
         */
        COUNT_FROM_SELF_OPTS: ["distinct", "group", "sql", "limit", "compounds"]
    }
}).as(module);