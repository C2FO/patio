var comb = require("comb"),
    hitch = comb.hitch,
    errors = require("../errors"),
    NotImplemented = errors.NotImplemented,
    QueryError = errors.QueryError,
    sql = require("../sql").sql,
    Identifier = sql.Identifier,
    QualifiedIdentifier = sql.QualifiedIdentifier,
    AliasedExpression = sql.AliasedExpression,
    logging = comb.logging,
    Logger = logging.Logger,
    Promise = comb.Promise,
    PromiseList = comb.PromiseList;

var Dataset;

var LOGGER = Logger.getLogger("moose.Dataset");

comb.define(null, {
    instance:{
        /**@lends moose.Dataset.prototype*/

        /**@ignore*/
        constructor:function () {
            !Dataset && (Dataset = require("../index").Dataset);
            this._super(arguments);
        },


        /**
         * Returns a Promise that is resolved with an array with all records in the dataset.
         * If a block is given, the array is iterated over after all items have been loaded.
         *
         * @example
         *
         * // SELECT * FROM table
         * DB.from("table").all().then(function(res){
         *      //res === [{id : 1, ...}, {id : 2, ...}, ...];
         * });
         * // Iterate over all rows in the table
         * var myArr = [];
         * var rowPromise = DB.from("table").all(function(row){ myArr.push(row);});
         * rowPromise.then(function(rows){
         *    //=> rows == myArr;
         * });
         *
         * @param {Function} block a block to be called with each item. The return value of the block is ignored.
         *
         * @return {comb.Promise} a promise that is resolved with an array of rows.
         */
        all:function (block) {
            var a = [];
            var ret = new Promise();
            this.forEach(hitch(this, function (r) {
                a.push(r);
            })).then(hitch(this, function () {
                this.postLoad(a);
                block && a.forEach(block, this);
                ret.callback(a);
            }), hitch(ret, "errback"));
            return ret;
        },

        /**
         * Returns a promise that is resolved with the average value for the given column.
         *
         * @example
         *
         * // SELECT avg(number) FROM table LIMIT 1
         * DB.from("table").avg("number").then(function(avg){
         *      // avg === 3
         * });
         *
         * @return {comb.Promise} a promise that is resolved with the average value of the column.
         */
        avg:function (column) {
            return this.__aggregateDataset().get(sql.avg(this.stringToIdentifier(column)));
        },

        /**
         * Returns a promise that is resolved with the number of records in the dataset.
         *
         * @example
         *
         * // SELECT COUNT(*) AS count FROM table LIMIT 1
         * DB.from("table").count().then(function(count){
         *     //count === 3;
         * });
         *
         * @return {comb.Promise} a promise that is resolved with the the number of records in the dataset.
         */
        count:function () {
            var ret = new Promise();
            this.__aggregateDataset().get(sql.COUNT(sql.literal("*")).as("count")).then(hitch(this, function (res) {
                ret.callback(parseInt(res, 10));
            }), hitch(ret, "errback"));
            return ret;
        },

        /**@ignore*/
        "delete":function () {
            this.remove();
        },


        /**
         * Deletes the records in the dataset.  The returned Promise should be resolved with the
         * number of records deleted, but that is adapter dependent.
         *
         * @example
         *
         * // DELETE * FROM table
         * DB.from("table").remove().then(function(numDeleted){
         *     //numDeleted === 3
         * });
         *
         * @return {comb.Promise} a promise resolved with the
         * number of records deleted, but that is adapter dependent.
         */
        remove:function () {
            return this.executeDui(this.deleteSql);
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
         * @return {comb.Promise} a promise that is resolved when the action has completed.
         */
        forEach:function (cb) {
            var rowCb;
            if (this.__opts.graph) {
                return this.graphEach(cb);
            } else if ((rowCb = this.rowCb)) {
                return this.fetchRows(this.selectSql, function (r) {
                    var ret = new comb.Promise();
                    comb.when(rowCb(r), function (r) {
                        comb.when(cb(r), hitch(ret, "callback"), hitch(ret, "errback"));
                    });
                    return ret;
                });
            } else {
                return this.fetchRows(this.selectSql, cb);
            }
        },

        /**
         * Returns a promise that is resolved with true if no records exist in the dataset,
         * false otherwise.
         * @example
         *
         * // SELECT 1 FROM table LIMIT 1
         * DB.from("table").isEmpty().then(function(isEmpty){
         *   // isEmpty === false
         * });
         *
         * @return {comb.Promise} a promise that is resolved with a boolean indicating if the table is empty.
         */
        isEmpty:function () {
            var ret = new Promise();
            this.get(1).then(hitch(this, function (res) {
                ret.callback(comb.isUndefinedOrNull(res) || res.length == 0);
            }), hitch(ret, "errback"));
            return ret;
        },

        /**
         * @private
         * Executes a select query and fetches records, passing each record to the
         * supplied cb. This method should not be called by user code, use {@link moose.Dataset#forEach}
         * instead.
         */
        fetchRows:function (sql, cb) {
            throw new NotImplemented("fetchRows must be implemented by the adapter");
        },

        /**
         * If a integer argument is given, it is interpreted as a limit, and then returns all
         * matching records up to that limit.  If no argument is passed,
         * it returns the first matching record.  If any other type of
         * argument(s) is passed, it is given to {@link moose.Dataset#filter} and the
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
        first:function (args) {
            args = comb.argsToArray(arguments);
            var block = comb.isFunction(args[args.length - 1]) ? args.pop() : null;
            var ds = block ? this.filter(block) : this;
            if (!args.length) {
                return ds.singleRecord();
            } else {
                args = (args.length == 1) ? args.shift() : args;
                if (comb.isNumber(args)) {
                    return ds.limit(args).all();
                } else {
                    return ds.filter(args).singleRecord();
                }
            }
        },

        /**
         * Return the column value for the first matching record in the dataset.
         *
         * @example
         *  // SELECT id FROM table LIMIT 1
         *  DB.from("table").get("id").then(function(val){
         *   // val === 3
         *  });
         *
         *
         * // SELECT sum(id) FROM table LIMIT 1
         * ds.get(sql.sum("id")).then(function(val){
         *      // val === 6;
         * });
         *
         * // SELECT sum(id) FROM table LIMIT 1
         * ds.get(function(){
         *      return this.sum("id");
         * }).then(function(val){
         *      // val === 6;
         * });
         *
         * @param {*} column the column to filter on can be anything that
         *            {@link moose.Dataset#select} accepts.
         *
         * @return {comb.Promise} a promise that will be resolved will the value requested.
         */
        get:function (column, cb) {
            return this.select(column).singleValue();
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
         *
         * @return {comb.Promise} a promise that is resolved once all records have been inserted.
         */
        import:function (columns, values, opts) {
            opts = opts || {};
            if (comb.isInstanceOf(values, Dataset)) {
                return this.db.transaction(hitch(this, function () {
                    return this.insert(columns, values)
                }));
            }
            var ret = new comb.Promise();
            if (!values.length) {
                ret.callback();
            }
            if (!columns.length) {
                throw new QueryError("Invalid columns in import");
            }

            var sliceSize = opts.commitEvery || opts.slice;
            if (sliceSize) {
                var offset = 0;
                var ret = new Promise();
                var execute = hitch(this, function () {
                    if (offset >= values.length) {
                        ret.callback();
                        return;
                    }
                    this.db.transaction(opts, hitch(this, function () {
                        this.multiInsertSql(columns, values.slice(offset, sliceSize + offset)).forEach(hitch(this, function (st) {
                            return this.executeDui(st);
                        }));
                    })).then(hitch(this, function () {
                        offset += sliceSize;
                        execute();
                    }), hitch(ret, "errback"));
                });
                execute();
                return ret;
            } else {
                var statements = this.multiInsertSql(columns, values);
                return this.db.transaction(hitch(this, function () {
                    statements.forEach(function (st) {
                        this.executeDui(st);
                    }, this);
                }));
            }
        },

        /**
         * This is the recommended function to do the insert of multiple items into the
         * database. This acts as a proxy to the {@link moose.Dataset#import} method so
         * one can use an array of hashes rather than an array of columns and an array of values.
         * See {@link moose.Dataset#import} for more information regarding the method of inserting.
         * <p>
         *     <b>NOTE:</b>All hashes should have the same keys other wise some values could be missed</b>
         * </p>
         *
         * @example
         *
         * // INSERT INTO table (x) VALUES (1)
         * // INSERT INTO table (x) VALUES (2)
         * DB.from("table").multiInsert([{x : 1}, {x : 2}]).then(function(){
         *     //...do something
         * })
         *
         * //commit every 50 inserts
         * DB.from("table").multiInsert([{x : 1}, {x : 2},....], {commitEvery : 50}).then(function(){
         *     //...do something
         * });
         *
         * @param {Object[]} hashes an array of objects to insert into the database. The keys of
         * the first item in the array will be used to look up columns in all subsequent objects. If the
         * array is empty then the promise is resolved immediatly.
         *
         * @param {Object} opts See {@link moose.Dataset#import}.
         *
         * @return {comb.Promise} See {@link moose.Dataset#import} for return functionality.
         */
        multiInsert:function (hashes, opts) {
            opts = opts || {};
            hashes = hashes || [];
            var ret = new Promise();
            if (!hashes.length) {
                ret.callback();
            } else {
                var columns = Object.keys(hashes[0]);
                return this.import(columns, hashes.map(function (h) {
                    return columns.map(function (c) {
                        return h[c]
                    })
                }), opts);
            }
            return ret;
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
         * @param {moose.Dataset|moose.sql.LiteralString|Array|Object|moose.sql.BooleanExpression|...} values  values to
         *      insert into the database. The INSERT statement generated depends on the type.
         *      <ul>
         *          <li>Empty object| Or no arugments: then DEFAULT VALUES is used.</li>
         *          <li>Object: the keys will be used as the columns, and values will be the values inserted.</li>
         *          <li>Single {@link moose.Dataset} : an insert with subselect will be performed.</li>
         *          <li>Array with {@link moose.Dataset} : The array will be used for columns and a subselect will performed with the dataset for the values.</li>
         *          <li>{@link moose.sql.LiteralString} : the literal value will be used.</li>
         *          <li>Single Array : the values in the array will be used as the VALUES clause.</li>
         *          <li>Two Arrays: the first array is the columns the second array is the values.</li>
         *          <li>{@link moose.sql.BooleanExpression} : the expression will be used as the values.
         *          <li>An arbitrary number of arguments : the {@link moose.Dataset#literal} version of the values will be used</li>
         *      </ul>
         *
         * @return {comb.Promise} a promise that is typically resolved with the ID of the inserted row.
         */
        insert:function () {
            return this.executeInsert(this.insertSql.apply(this, arguments));
        },

        /**
         * @see moose.Dataset#insert
         */
        save:function () {
            return this.insert.apply(this, arguments);
        },

        /**
         * Inserts multiple values. If a block is given it is invoked for each
         * item in the given array before inserting it.  See {@link moose.Dataset#multiInsert} as
         * a possible faster version that inserts multiple records in one SQL statement.
         *
         * <b> Params see @link moose.Dataset#insert</b>
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
         * @param array See {@link moose.Dataset#insert} for possible values.
         * @param {Function} [block] a function to be called before each item is inserted.
         *
         * @return {comb.PromiseList} a promiseList that should be resolved with the id of each item inserted
         *          in the order that was in the array.
         */
        insertMultiple:function (array, block) {
            var promises;
            if (block) {
                promises = array.map(function (i) {
                    return this.insert(block(i));
                }, this);
            } else {
                promises = array.map(function (i) {
                    return this.insert(i);
                }, this);
            }
            return new PromiseList(promises, true);
        },

        /**
         * @see moose.Dataset#insertMultiple
         */
        saveMultiple:function () {
            return this.insertMultiple.apply(this, arguments);
        },

        /**
         * Returns a promise that is resolved with the interval between minimum and maximum values
         * for the given column.
         *
         * @example
         *  // SELECT (max(id) - min(id)) FROM table LIMIT 1
         *   DB.from("table").interval("id").then(function(interval){
         *      //(e.g) interval === 6
         *   });
         *
         * @param {String|moose.sql.Identifier} column to find the interval of.
         *
         * @return {comb.Promise} a promise that will be resolved with the interval between the min and max values
         * of the column.
         */
        interval:function (column) {
            return this.__aggregateDataset().get(sql.max(column).minus(sql.min(column)));
        },

        /**
         * Reverses the order and then runs first.  Note that this
         * will not necessarily give you the last record in the dataset,
         * unless you have an unambiguous order.
         *
         * @example
         *
         *  // SELECT * FROM table ORDER BY id DESC LIMIT 1
         *  DB.from("table").order("id").last().then(function(lastItem){
         *      //...(e.g lastItem === {id : 10})
         *  });
         *
         *  // SELECT * FROM table ORDER BY id ASC LIMIT 2
         *   DB.from("table").order(sql.id.desc()).last(2).then(function(lastItems){
         *      //...(e.g lastItems === [{id : 1}, {id : 2});
         *  });
         *
         * @throws {moose.error.QueryError} If there is not currently an order for this dataset.
         *
         * @param {*} args See {@link moose.Dataset#first} for argument types.
         *
         * @return {comb.Promise} a promise that will be resolved with a single object or array depending on the
         * arguments provided.
         */
        last:function (args) {
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
         * DB.from("table").map("id").then(function(ids){
         *   // e.g. ids === [1, 2, 3, ...]
         * });
         *
         *  // SELECT * FROM table
         * DB.from("table").map(function(r){
         *      return r.id * 2;
         * }).then(function(ids){
         *     // e.g. ids === [2, 4, 6, ...]
         * });
         *
         * @param {Function|String} column if a string is provided then then it is assumed
         * to be the name of a column in that table and the value of the column for each row
         * will be returned. If column is a function then the return value of the function will
         * be used.
         *
         * @return {comb.Promise} a promise resolved with the array of mapped values.
         */
        map:function (column) {
            var ret = new Promise(), block;
            if (comb.isFunction(column)) {
                block = column;
                column = null;
            }
            var a = [], forEachP;
            forEachP = this.forEach(function (r) {
                a.push(column ? r[column] : (block ? block(r) : r));
            });
            forEachP.then(hitch(this, function () {
                ret.callback(a);
            }), hitch(ret, "errback"));
            return ret;
        },

        /**
         * Returns a promise resolved with  the maximum value for the given column.
         *
         * @example
         *
         * // SELECT max(id) FROM table LIMIT 1
         * DB.from("table").max("id").then(function(max){
         *   // e.g. max === 10.
         * });
         *
         *
         * @param {String|moose.sql.Identifier} column the column to find the maximum value for.
         *
         * @return {*} the maximum value for the column.
         */
        max:function (column) {
            return this.__aggregateDataset().get(sql.max(this.stringToIdentifier(column)));
        },

        /**
         * Returns a promise resolved with  the minimum value for the given column.
         *
         * @example
         *
         * // SELECT min(id) FROM table LIMIT 1
         * DB.from("table").min("id").then(function(min){
         *   // e.g. max === 0.
         * });
         *
         *
         * @param {String|moose.sql.Identifier} column the column to find the minimum value for.
         *
         * @return {*} the minimum value for the column.
         */
        min:function (column) {
            return this.__aggregateDataset().get(sql.min(this.stringToIdentifier(column)));
        },

        /**
         * Returns a promise resolved with  a range from the minimum and maximum values for the
         * given column.
         *
         * @example
         *  // SELECT max(id) AS v1, min(id) AS v2 FROM table LIMIT 1
         *  DB.from("table").range("id").then(function(min, max){
         *      //e.g min === 1 AND max === 10
         *  });
         *
         * @param {String|moose.sql.Identifier} column the column to find the min and max value for.
         *
         * @return {comb.Promise} a promise that is resolved with the min and max value, as the first
         * and second args respectively.
         */
        range:function (column) {
            var ret = new Promise();
            this.__aggregateDataset().select(sql.min(this.stringToIdentifier(column)).as("v1"), sql.max(this.stringToIdentifier(column)).as("v2")).first().then(hitch(this, function (r) {
                ret.callback(r.v1, r.v2);
            }), hitch(ret, "errback"));

            return ret;
        },

        /**
         * Returns a promise resolved with a hash with keyColumn values as keys and valueColumn values as
         * values.  Similar to {@link moose.Dataset#toHash}, but only selects the two columns.
         *
         * @example
         *  // SELECT id, name FROM table
         *  DB.from("table").selectHash("id", "name").then(function(hash){
         *   // e.g {1 : 'a', 2 : 'b', ...}
         *  });
         *
         * @param {String|moose.sql.Identifier\moose.sql.QualifiedIdentifier|moose.sql.AliasedExpression} keyColumn the column
         * to use as the key in the hash.
         *
         * @param {String|moose.sql.Identifier\moose.sql.QualifiedIdentifier|moose.sql.AliasedExpression} valueColumn the column
         * to use as the value in the hash.
         *
         * @return {comb.Promise} a promise that is resolved with an array of hashes, that have the keyColumn
         * as the key and the valueColumn as the value.
         */
        selectHash:function (keyColumn, valueColumn) {
            return this.toHash(keyColumn, valueColumn);
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
         *  DB.from("table").selectMap("id").then(function(selectMap){
         *   // e,g. selectMap === [3, 5, 8, 1, ...]
         * });
         *
         * // SELECT abs(id) FROM table
         * DB.from("table").selectMap(function(){
         *      return this.abs("id");
         * }).then(function(selectMap){
         *   //e.g selectMap === [3, 5, 8, 1, ...]
         * });
         *
         * @param {*} column  The column to return the values for.
         *      See {@link moose.Dataset#select} for valid column values.
         *
         * @return {comb.Promise} a promise resolved with the array of mapped values.
         */
        selectMap:function (column) {
            var ds = this.naked().ungraphed().select(column);
            return ds.map(function (r) {
                return r[Object.keys(r)[0]];
            });
        },

        /**
         * The same as {@link moose.Dataset#selectMap}, but in addition orders the array by the column.
         *
         * @example
         * // SELECT id FROM table ORDER BY id
         * DB.from("table").selectOrderMap("id").then(function(mappedIds){
         *   //e.g. [1, 2, 3, 4, ...]
         * });
         *
         *  // SELECT abs(id) FROM table ORDER BY abs(id)
         *  DB.from("table").selectOrderMap(function(){
         *          return this.abs("id");
         *  }).then(function(mappedIds){
         *      //e.g. [1, 2, 3, 4, ...]
         *  });
         *
         * @param {*} column  The column to return the values for.
         *      See {@link moose.Dataset#select} for valid column values.
         *
         * @return {comb.Promise} a promise resolved with the array of mapped values.
         */
        selectOrderMap:function (column) {
            var ds = this.naked()
                .ungraphed()
                .select(column)
                .order(comb.isFunction(column) ? column : this._unaliasedIdentifier(column));
            return ds.map(function (r) {
                return r[Object.keys(r)[0]];
            });
        },

        /**
         * Same as {@link moose.Dataset#singleRecord} but accepts arguments
         * to filter the dataset. See {@link moose.Dataset#filter} for argument types.
         *
         * @return {comb.Promise} a promise resolved with a single row from the database that matched the filter.
         */
        one:function () {
            var args = comb.argsToArray(arguments);
            if (args.length) {
                return this.filter.apply(this, arguments).singleRecord();
            } else {
                return this.singleRecord();
            }
        },

        /**
         * Returns a promise resolved with  the first record in the dataset, or null if the dataset
         * has no records. Users should probably use {@link moose.Dataset#first} instead of
         * this method.
         *
         * @example
         *
         * //'SELECT * FROM test LIMIT 1'
         * DB.from("test").singleRecord().then(function(r) {
         *     //e.g r === {id : 1, name : "firstName"}
         * });
         *
         * @return {comb.Promise} a promise resolved with the first record returned from the query.
         */
        singleRecord:function () {
            var ret = new Promise();
            this.mergeOptions({limit:1}).all().then(function (r) {
                ret.callback(r && r.length ? r[0] : null);
            }, hitch(ret, "errback"));
            return ret;
        },

        /**
         * Returns a promise resolved with the first value of the first record in the dataset.
         * Returns null if dataset is empty.  Users should generally use
         * {@link moose.Dataset#get} instead of this method.
         *
         * @example
         *
         * //'SELECT * FROM test LIMIT 1'
         * DB.from("test").singleValue().then(function(r) {
         *     //e.g r === 1
         * });
         *
         * @return {comb.Promise} a promise that will be resolved with the first value of the first row returned
         * from the dataset.
         *
         */
        singleValue:function () {
            var ret = new Promise();
            this.naked().ungraphed().singleRecord().then(function (r) {
                ret.callback(r ? r[Object.keys(r)[0]] : null);
            }, hitch(ret, "errback"));
            return ret;
        },

        /**
         * Returns a promise resolved the sum for the given column.
         *
         * @example
         *
         *  // SELECT sum(id) FROM table LIMIT 1
         * DB.from("table").sum("id").then(function(sum){
         *   // e.g sum === 55
         * });
         *
         * @param {String|moose.sql.Identifier\moose.sql.QualifiedIdentifier|moose.sql.AliasedExpression} column
         * the column to find the sum of.
         *
         * @return {comb.Promise} a promise resolved with the sum of the column.
         */
        sum:function (column) {
            return this.__aggregateDataset().get(sql.sum(this.stringToIdentifier(column)));
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
         *  DB.from("table").toCsv().then(function(csv){
         *      console.log(csv);
         *      //outputs
         *        id,name
         *        1,Jim
         *        2,Bob
         *  });
         *
         *    // SELECT * FROM table
         *  DB.from("table").toCsv(false).then(function(csv){
         *      console.log(csv);
         *      //outputs
         *        1,Jim
         *        2,Bob
         *  });
         *
         *  @param {Boolean} [includeColumnTitles=true] Set to false to prevent the printing of the column
         *  titles as the first line.
         *
         *  @return {comb.Promise} a promise that will be resolved with the CSV string of the results of the
         *  query.
         */
        toCsv:function (includeColumnTitles) {
            var n = this.naked();
            includeColumnTitles = comb.isBoolean(includeColumnTitles) ? includeColumnTitles : true;
            var csv = null, cols = [];
            var ret = new comb.Promise();
            n.columns.then(hitch(this, function (cols) {
                var vals = [];
                includeColumnTitles && vals.push(cols.join(", "));
                n.forEach(
                    function (r) {
                        vals.push(cols.map(
                            function (c) {
                                return r[c] || ""
                            }).join(", "));
                    }).then(function () {
                        ret.callback(vals.join("\r\n") + "\r\n");
                    }, hitch(ret, "errback"));
            }), hitch(ret, "errback"));
            return ret;
        },

        /**
         * Returns a promise resolved with a hash with one column used as key and another used as value.
         * If rows have duplicate values for the key column, the latter row(s)
         * will overwrite the value of the previous row(s). If the valueColumn
         * is not given or null, uses the entire hash as the value.
         *
         * @example
         *  // SELECT * FROM table
         *  DB.from("table").toHash("id", "name").then(function(hash){
         *    // {1 : 'Jim', 2 : 'Bob', ...}
         *  });
         *
         * // SELECT * FROM table
         * DB.from("table").toHash("id").then(function(hash){
         *   // {1 : {id : 1, name : 'Jim'}, 2 : {id : 2, name : 'Bob'}, ...}
         * });
         *
         *  @param {String|moose.sql.Identifier\moose.sql.QualifiedIdentifier|moose.sql.AliasedExpression} keyColumn the column
         *  to use as the key in the returned hash.
         *
         *  @param {String|moose.sql.Identifier\moose.sql.QualifiedIdentifier|moose.sql.AliasedExpression} [keyValue=null] the
         *  key of the column to use as the value in the hash.
         *
         *  @return {comb.Promise} a promise that will be resolved with the resulting hash.
         */
        toHash:function (keyColumn, valueColumn) {
            var ret = new Promise(), map = {};
            var k = this.__hashIdentifierToName(keyColumn), v = this.__hashIdentifierToName(valueColumn)
            var mapPromise = this.select.apply(this, arguments).map(function (r) {
                map[r[k]] = v ? r[v] : r;
            });
            mapPromise.then(function () {
                ret.callback(map);
            }, hitch(ret, "errback"));
            return ret;
        },

        /**
         * Truncates the dataset.  Returns a promise that is resolved once truncation is complete.
         *
         * @example
         *
         * // TRUNCATE table
         * DB.from("table").truncate().then(function(){
         *     //...do something
         * });
         *
         * @return {comb.Promise} a promise that is resolved once truncation is complete.
         */
        truncate:function () {
            return this.executeDdl(this.truncateSql);
        },

        /**
         * Updates values for the dataset.  The returned promise is resolved with a value that is generally the
         * number of rows updated, but that is adapter dependent.
         *
         * @example
         * // UPDATE table SET x = NULL
         *  DB.from("table").update({x : null}).then(function(numRowsUpdated){
         *      //e.g. numRowsUpdated === 10
         *  });
         *
         * // UPDATE table SET x = (x + 1), y = 0
         * DB.from("table").update({ x : sql.x.plus(1), y : 0}).then(function(numRowsUpdated){
         *   // e.g. numRowsUpdated === 10
         * });
         *
         * @param {Object} values See {@link moose.Dataset#updateSql} for parameter types.
         *
         * @return {comb.Promise}  a promise that is generally resolved with the
         * number of rows updated, but that is adapter dependent.
         */
        update:function (values) {
            return this.executeDui(this.updateSql(values));
        },

        /**
         * @see moose.Dataset#set
         */
        set:function () {
            this.update.apply(this, arguments);
        },

        /**
         * @private
         * Execute the given select SQL on the database using execute. Use the
         * readOnly server unless a specific server is set.
         */
        execute:function (sql, opts, block) {
            opts = opts || {};
            return this.db.execute(sql, comb.merge({server:this.__opts.server || "readOnly"}, opts), block);
        },

        /**
         * @private
         * Execute the given SQL on the database using {@link moose.Database#executeDdl}.
         */
        executeDdl:function (sql, opts, block) {
            opts = opts || {};
            return this.db.executeDdl(sql, this.__defaultServerOpts(opts), block);
        },

        /**
         * @private
         * Execute the given SQL on the database using {@link moose.Database#executeDui}.
         */
        executeDui:function (sql, opts, block) {
            opts = opts || {};
            return this.db.executeDui(sql, this.__defaultServerOpts(opts), block);
        },

        /**
         * @private
         * Execute the given SQL on the database using {@link moose.Database#executeInsert}.
         */
        executeInsert:function (sql, opts, block) {
            opts = opts || {};
            return this.db.executeInsert(sql, this.__defaultServerOpts(opts), block);
        },

        /**
         * This is run inside {@link moose.Dataset#all}, after all of the records have been loaded
         * via {@link moose.Dataset#forEach}, but before any block passed to all is called.  It is called with
         * a single argument, an array of all returned records.  Does nothing by
         * default.
         */
        postLoad:function (allRecords) {
            return allRecords;
        },

        /**
         * @private
         *
         * Clone of this dataset usable in aggregate operations.  Does
         * a {@link moose.Dataset#fromSelf} if dataset contains any parameters that would
         * affect normal aggregation, or just removes an existing
         * order if not.
         */
        __aggregateDataset:function () {
            return this._optionsOverlap(this._static.COUNT_FROM_SELF_OPTS) ? this.fromSelf() : this.unordered();
        },

        /**
         * @private
         * Set the server to use to "default" unless it is already set in the passed opts
         */
        __defaultServerOpts:function (opts) {
            return comb.merge({server:this.__opts.server || "default"}, opts);
        },

        /**
         * @private
         *
         * Returns the string version of the identifier.
         *
         * @param {String|moose.sql.Identifier\moose.sql.QualifiedIdentifier|moose.sql.AliasedExpression} identifier
         *      identifier to resolve to a string.
         * @return {String} the string version of the identifier.
         */
        __hashIdentifierToName:function (identifier) {
            return comb.isString(identifier) ? this.__hashIdentifierToName(this.stringToIdentifier(identifier)) :
                comb.isInstanceOf(identifier, Identifier) ? identifier.value :
                    comb.isInstanceOf(identifier, QualifiedIdentifier) ? identifier.column :
                        comb.isInstanceOf(identifier, AliasedExpression) ? identifier.alias : identifier;
        },

        /**@ignore*/
        getters:{

            /**
             * @ignore
             * @property columns
             * @type {comb.Promise}
             *
             *  Returns a promise that is resolved with the columns in the result set in order as an array of strings.
             * If the columns are currently cached, then the promise is immediately resolved with the cached value. Otherwise,
             * a SELECT query is performed to retrieve a single row in order to get the columns.
             *
             * If you are looking for all columns for a single table and maybe some information about
             * each column (e.g. database type), see {#link moose.Database#schema}.
             *
             * <pre class="code">
             *  DB.from("table").columns.then(function(columns){
             *        // => ["id", "name"]
             *   });
             * </pre>
             **/
            columns:function () {
                var ret = new Promise();
                if (this.__columns) {
                    ret.callback(this.__columns)
                } else {
                    var ds = this.unfiltered().unordered().mergeOptions({distinct:null, limit:1});
                    ds.forEach().then(hitch(this, function () {
                        this.__columns = ds.__columns || [];
                        ret.callback(this.__columns);
                    }), hitch(ret, "errback"));
                }
                return ret;
            }
        }
    },

    static:{

        /**@lends moose.Dataset*/

        /**
         * List of action methods avaiable on the dataset.
         *
         * @type String[]
         * @default ['all', 'one', 'avg', 'count', 'columns', 'remove', 'forEach', 'empty', 'fetchRows', 'first',
         *          'get', 'import', 'insert', 'save', 'insertMultiple', 'saveMultiple', 'interval', 'last',
         *          'map', 'max', 'min', 'multiInsert', 'range', 'selectHash', 'selectMap', 'selectOrderMap', 'set',
         *          'singleRecord', 'singleValue', 'sum', 'toCsv', 'toHash', 'truncate', 'update']
         */
        ACTION_METHODS:['all', 'one', 'avg', 'count', 'columns', 'remove', 'forEach', 'empty', 'fetchRows', 'first',
            'get', 'import', 'insert', 'save', 'insertMultiple', 'saveMultiple', 'interval', 'last',
            'map', 'max', 'min', 'multiInsert', 'range', 'selectHash', 'selectMap', 'selectOrderMap', 'set',
            'singleRecord', 'singleValue', 'sum', 'toCsv', 'toHash', 'truncate', 'update'],

        /**
         * List of options that can interfere with the aggregation of a {@link moose.Dataset}
         * @type String[]
         * @default ["distinct", "group", "sql", "limit", "compounds"]
         */
        COUNT_FROM_SELF_OPTS:["distinct", "group", "sql", "limit", "compounds"]
    }
}).
    as(module);

