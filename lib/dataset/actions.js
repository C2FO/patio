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
         *   ds.first(); // => {:id=>7}
         *
         *   // SELECT * FROM table LIMIT 2
         *   ds.first(2); // => [{:id=>6}, {:id=>4}]
         *
         *   // SELECT * FROM table WHERE (id = 2) LIMIT 1
         *   ds.first({id : 2}) // => {:id=>2}
         *
         *
         *  // SELECT * FROM table WHERE (id = 3) LIMIT 1
         *   ds.first("id = 3"); // => {:id=>3}
         *
         *   // SELECT * FROM table WHERE (id = 4) LIMIT 1
         *   ds.first("id = ?", 4); // => {:id=>4}
         *
         *   // SELECT * FROM table WHERE (id > 2) LIMIT 1
         *   ds.first(function(){return this.id.gt(2);}); // => {:id=>5}
         *
         *
         *   // SELECT * FROM table WHERE ((id > 4) AND (id < 6)) LIMIT 1
         *   ds.first("id > ?", 4, function(){
         *          return this.id.lt(6);
         *   }); // => {:id=>5}
         *
         *    // SELECT * FROM table WHERE (id < 2) LIMIT 2
         *   ds.first(2, function(){
         *          return this.id.lt(2)
         *   }); // => [{:id=>1}]
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
         * Raises an error if both an argument and block is given.
         *
         * @example
         *
         *   DB.from("table").get(:id) // SELECT id FROM table LIMIT 1
         *   // => 3
         *
         *   ds.get{sum(id)} // SELECT sum(id) FROM table LIMIT 1
         *   // => 6
         */
        get:function (column, cb) {
            if (column) {
                if (cb) throw new QueryError("Block and Column cannot both be specified");
                return this.select(column).singleValue();
            } else {
                return this.select(cb).singleValue();
            }
        },

        // Inserts multiple records into the associated table. This method can be
        // used to efficiently insert a large number of records into a table in a
        // single query if the database supports it. Inserts
        // are automatically wrapped in a transaction.
        //
        // This method is called with a columns array and an array of value arrays:
        //
        //   DB.from("table").import([:x, :y], [[1, 2], [3, 4]])
        //   // INSERT INTO table (x, y) VALUES (1, 2)
        //   // INSERT INTO table (x, y) VALUES (3, 4)
        //
        // This method also accepts a dataset instead of an array of value arrays:
        //
        //   DB.from("table").import([:x, :y], DB[:table2].select(:a, :b))
        //   // INSERT INTO table (x, y) SELECT a, b FROM table2
        //
        // The method also accepts a :slice or :commit_every option that specifies
        // the number of records to insert per transaction. This is useful especially
        // when inserting a large number of records, e.g.:
        //
        //   // this will commit every 50 records
        //   dataset.import([:x, :y], [[1, 2], [3, 4], ...], :slice => 50)
        import:function (columns, values, opts) {
            opts = opts || {};
            if (comb.isInstanceOf(values, Dataset)) {
                return this.db.transaction(hitch(this, function () {
                    return this.insert(columns, values)
                }));
            }
            var ret = new comb.Promise();
            if (!values.length) ret.callback();
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

        // Inserts values into the associated table.  The returned value is generally
        // the value of the primary key for the inserted row, but that is adapter dependent.
        //
        // +insert+ handles a number of different argument formats:
        // * No arguments, single empty hash - Uses DEFAULT VALUES
        // * Single hash - Most common format, treats keys as columns an values as values
        // * Single array - Treats entries as values, with no columns
        // * Two arrays - Treats first array as columns, second array as values
        // * Single Dataset - Treats as an insert based on a selection from the dataset given,
        //   with no columns
        // * Array and dataset - Treats as an insert based on a selection from the dataset
        //   given, with the columns given by the array.
        //
        //   DB[:items].insert
        //   // INSERT INTO items DEFAULT VALUES
        //
        //   DB[:items].insert({})
        //   // INSERT INTO items DEFAULT VALUES
        //
        //   DB[:items].insert([1,2,3])
        //   // INSERT INTO items VALUES (1, 2, 3)
        //
        //   DB[:items].insert([:a, :b], [1,2])
        //   // INSERT INTO items (a, b) VALUES (1, 2)
        //
        //   DB[:items].insert(:a => 1, :b => 2)
        //   // INSERT INTO items (a, b) VALUES (1, 2)
        //
        //   DB[:items].insert(DB[:old_items])
        //   // INSERT INTO items SELECT * FROM old_items
        //
        //   DB[:items].insert([:a, :b], DB[:old_items])
        //   // INSERT INTO items (a, b) SELECT * FROM old_items
        insert:function () {
            return this.executeInsert(this.insertSql.apply(this, arguments));
        },

        save:function () {
            return this.insert.apply(this, arguments);
        },

        // Inserts multiple values. If a block is given it is invoked for each
        // item in the given array before inserting it.  See +multi_insert+ as
        // a possible faster version that inserts multiple records in one
        // SQL statement.
        //
        //   DB.from("table").insert_multiple([{:x=>1}, {:x=>2}])
        //   // INSERT INTO table (x) VALUES (1)
        //   // INSERT INTO table (x) VALUES (2)
        //
        //   DB.from("table").insert_multiple([{:x=>1}, {:x=>2}]){|row| row[:y] = row[:x] * 2}
        //   // INSERT INTO table (x, y) VALUES (1, 2)
        //   // INSERT INTO table (x, y) VALUES (2, 4)
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
            return new PromiseList(promises);
        },

        saveMultiple:function () {
            return this.insertMultiple.apply(this, arguments);
        },

        // Returns the interval between minimum and maximum values for the given
        // column.
        //
        //   DB.from("table").interval(:id) // SELECT (max(id) - min(id)) FROM table LIMIT 1
        //   // => 6
        interval:function (column) {
            return this.__aggregateDataset().get(sql.max(column).minus(sql.min(column)));
        },

        // Reverses the order and then runs first.  Note that this
        // will not necessarily give you the last record in the dataset,
        // unless you have an unambiguous order.  If there is not
        // currently an order for this dataset, raises an +Error+.
        //
        //   DB.from("table").order(:id).last // SELECT * FROM table ORDER BY id DESC LIMIT 1
        //   // => {:id=>10}
        //
        //   DB.from("table").order(:id.desc).last(2) // SELECT * FROM table ORDER BY id ASC LIMIT 2
        //   // => [{:id=>1}, {:id=>2}]
        last:function () {
            if (!this.__opts.order) throw new QueryError("No order specified");
            var ds = this.reverse();
            return ds.first.apply(ds, arguments);
        },

        // Maps column values for each record in the dataset (if a column name is
        // given), or performs the stock mapping functionality of +Enumerable+ otherwise.
        // Raises an +Error+ if both an argument and block are given.
        //
        //   DB.from("table").map(:id) // SELECT * FROM table
        //   // => [1, 2, 3, ...]
        //
        //   DB.from("table").map{|r| r[:id] * 2} // SELECT * FROM table
        //   // => [2, 4, 6, ...]
        map:function (column) {
            var ret = new Promise(), block;
            if (comb.isFunction(column)) {
                block = column;
                column = null;
            }
            var a = [];
            if (column) {
                this.forEach(
                    function (r) {
                        a.push(r[column]);
                    }).then(hitch(this, function () {
                    ret.callback(a);
                }), hitch(ret, "errback"));
            } else {
                this.forEach(
                    function (r) {
                        a.push(block ? block(r) : r);
                    }).then(hitch(this, function () {
                    ret.callback(a);
                }), hitch(ret, "errback"));
            }
            return ret;
        },

        // Returns the maximum value for the given column.
        //
        //   DB.from("table").max(:id) // SELECT max(id) FROM table LIMIT 1
        //   // => 10
        max:function (column) {
            return this.__aggregateDataset().get(sql.max(this.stringToIdentifier(column)));
        },

        // Returns the minimum value for the given column.
        //
        //   DB.from("table").min(:id) // SELECT min(id) FROM table LIMIT 1
        //   // => 1
        min:function (column) {
            return this.__aggregateDataset().get(sql.min(this.stringToIdentifier(column)));
        },

        // This is a front end for import that allows you to submit an array of
        // hashes instead of arrays of columns and values:
        //
        //   DB.from("table").multi_insert([{:x => 1}, {:x => 2}])
        //   // INSERT INTO table (x) VALUES (1)
        //   // INSERT INTO table (x) VALUES (2)
        //
        // Be aware that all hashes should have the same keys if you use this calling method,
        // otherwise some columns could be missed or set to null instead of to default
        // values.
        //
        // You can also use the :slice or :commit_every option that import accepts.
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

        // Returns a +Range+ instance made from the minimum and maximum values for the
        // given column.
        //
        //   DB.from("table").range(:id) // SELECT max(id) AS v1, min(id) AS v2 FROM table LIMIT 1
        //   // => 1..10
        range:function (column) {
            var ret = new Promise();
            this.__aggregateDataset().select(sql.min(this.stringToIdentifier(column)).as("v1"), sql.max(this.stringToIdentifier(column)).as("v2")).first().then(hitch(this, function (r) {
                ret.callback(r.v1, r.v2);
            }), hitch(ret, "errback"));

            return ret;
        },

        __hashIdentifierToName:function (identifier) {
            return comb.isString(identifier) ? this.__hashIdentifierToName(this.stringToIdentifier(identifier)) :
                comb.isInstanceOf(identifier, Identifier) ? identifier.value :
                    comb.isInstanceOf(identifier, QualifiedIdentifier) ? identifier.column :
                        comb.isInstanceOf(identifier, AliasedExpression) ? identifier.alias : identifier;
        },

        // Returns a hash with key_column values as keys and value_column values as
        // values.  Similar to to_hash, but only selects the two columns.
        //
        //   DB.from("table").select_hash(:id, :name) // SELECT id, name FROM table
        //   // => {1=>'a', 2=>'b', ...}
        selectHash:function (keyColumn, valueColumn) {
            return this.toHash(keyColumn, valueColumn);
        },

        // Selects the column given (either as an argument or as a block), and
        // returns an array of all values of that column in the dataset.  If you
        // give a block argument that returns an array with multiple entries,
        // the contents of the resulting array are undefined.
        //
        //   DB.from("table").select_map(:id) // SELECT id FROM table
        //   // => [3, 5, 8, 1, ...]
        //
        //   DB.from("table").select_map{abs(id)} // SELECT abs(id) FROM table
        //   // => [3, 5, 8, 1, ...]
        selectMap:function (column, block) {
            var ds = this.naked().ungraphed();
            if (column) {
                if (block) throw new QueryError("Column and block cannot both be specified");
                ds = ds.select(column);
            } else {
                ds = ds.select(block);
            }
            return ds.map(function (r) {
                return r[Object.keys(r)[0]];
            });
        },

        // The same as select_map, but in addition orders the array by the column.
        //
        //   DB.from("table").select_order_map(:id) // SELECT id FROM table ORDER BY id
        //   // => [1, 2, 3, 4, ...]
        //
        //   DB.from("table").select_order_map{abs(id)} // SELECT abs(id) FROM table ORDER BY abs(id)
        //   // => [1, 2, 3, 4, ...]
        selectOrderMap:function (column, block) {
            var ds = this.naked().ungraphed();
            if (column) {
                if (block) throw new QueryError("Column and block cannot both be specified");
                ds = ds.select(column).order(ds._unaliasedIdentifier(column));
            } else {
                ds = ds.select(block).order(block);
            }
            return ds.map(function (r) {
                return r[Object.keys(r)[0]];
            });
        },

        // Alias for update, but not aliased directly so subclasses
        // don't have to override both methods.
        set:function () {
            this.update.apply(this, arguments);
        },

        one:function () {
            var args = comb.argsToArray(arguments);
            if (args.length) {
                return this.filter.apply(this, arguments).singleRecord();
            } else {
                return this.singleRecord();
            }
        },

        // Returns the first record in the dataset, or nil if the dataset
        // has no records. Users should probably use +first+ instead of
        // this method.
        singleRecord:function () {
            var ret = new Promise();
            this.mergeOptions({limit:1}).all().then(function (r) {
                ret.callback(r[0] || null);
            }, hitch(ret, "errback"));
            return ret;
        },

        // Returns the first value of the first record in the dataset.
        // Returns nil if dataset is empty.  Users should generally use
        // +get+ instead of this method.
        singleValue:function () {
            var ret = new Promise();
            this.naked().ungraphed().singleRecord().then(function (r) {
                ret.callback(r ? r[Object.keys(r)[0]] : null);
            }, hitch(ret, "errback"));
            return ret;
        },

        // Returns the sum for the given column.
        //
        //   DB.from("table").sum(:id) // SELECT sum(id) FROM table LIMIT 1
        //   // => 55
        sum:function (column) {
            return this.__aggregateDataset().get(sql.sum(this.stringToIdentifier(column)));
        },

        // Returns a string in CSV format containing the dataset records. By
        // default the CSV representation includes the column titles in the
        // first line. You can turn that off by passing false as the
        // include_column_titles argument.
        //
        // This does not use a CSV library or handle quoting of values in
        // any way.  If any values in any of the rows could include commas or line
        // endings, you shouldn't use this.
        //
        //   puts DB.from("table").to_csv // SELECT * FROM table
        //   // id,name
        //   // 1,Jim
        //   // 2,Bob
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

        // Returns a hash with one column used as key and another used as value.
        // If rows have duplicate values for the key column, the latter row(s)
        // will overwrite the value of the previous row(s). If the value_column
        // is not given or nil, uses the entire hash as the value.
        //
        //   DB.from("table").to_hash(:id, :name) // SELECT * FROM table
        //   // {1=>'Jim', 2=>'Bob', ...}
        //
        //   DB.from("table").to_hash(:id) // SELECT * FROM table
        //   // {1=>{:id=>1, :name=>'Jim'}, 2=>{:id=>2, :name=>'Bob'}, ...}
        toHash:function (keyColumn, valueColumn) {
            var ret = new Promise(), map = {};
            var k = this.__hashIdentifierToName(keyColumn), v = this.__hashIdentifierToName(valueColumn)
            this.select.apply(this, arguments).map(
                function (r) {
                    map[r[k]] = v ? r[v] : r;
                }).then(function () {
                    ret.callback(map);
                }, hitch(ret, "errback"));
            return ret;
        },

        // Truncates the dataset.  Returns nil.
        //
        //   DB.from("table").truncate // TRUNCATE table
        //   // => nil
        truncate:function () {
            return this.executeDdl(this.truncateSql);
        },

        // Updates values for the dataset.  The returned value is generally the
        // number of rows updated, but that is adapter dependent. +values+ should
        // a hash where the keys are columns to set and values are the values to
        // which to set the columns.
        //
        //   DB.from("table").update(:x=>nil) // UPDATE table SET x = NULL
        //   // => 10
        //
        //   DB.from("table").update(:x=>:x+1, :y=>0) // UPDATE table SET x = (x + 1), :y = 0
        //   // => 10
        update:function (values) {
            return this.executeDui(this.updateSql(values));
        },


        //Clone of this dataset usable in aggregate operations.  Does
        //a from_self if dataset contains any parameters that would
        //affect normal aggregation, or just removes an existing
        //order if not.
        __aggregateDataset:function () {
            return this._optionsOverlap(this._static.COUNT_FROM_SELF_OPTS) ? this.fromSelf() : this.unordered();
        },


        // Set the server to use to :default unless it is already set in the passed opts
        __defaultServerOpts:function (opts) {
            return comb.merge({server:this.__opts.server || "default"}, opts);
        },

        // Execute the given select SQL on the database using execute. Use the
        // :read_only server unless a specific server is set.
        execute:function (sql, opts, block) {
            opts = opts || {};
            return this.db.execute(sql, comb.merge({server:this.__opts.server || "readOnly"}, opts), block);
        },

        // Execute the given SQL on the database using execute_ddl.
        executeDdl:function (sql, opts, block) {
            opts = opts || {};
            return this.db.executeDdl(sql, this.__defaultServerOpts(opts), block);
        },

        // Execute the given SQL on the database using execute_dui.
        executeDui:function (sql, opts, block) {
            opts = opts || {};
            return this.db.executeDui(sql, this.__defaultServerOpts(opts), block);
        },

        // Execute the given SQL on the database using execute_insert.
        executeInsert:function (sql, opts, block) {
            opts = opts || {};
            return this.db.executeInsert(sql, this.__defaultServerOpts(opts), block);
        },


        // This is run inside .all, after all of the records have been loaded
        // via .each, but before any block passed to all is called.  It is called with
        // a single argument, an array of all returned records.  Does nothing by
        // default, added to make the model eager loading code simpler.
        postLoad:function (allRecords) {
            return allRecords;
        },

        getters:{
            /** Returns the columns in the result set in order as an array of symbols.
             * If the columns are currently cached, returns the cached value. Otherwise,
             * a SELECT query is performed to retrieve a single row in order to get the columns.
             *
             * If you are looking for all columns for a single table and maybe some information about
             * each column (e.g. database type), see <tt>Database#schema</tt>.
             *
             *   DB.from("table").columns
             *   # => [:id, :name]
             *   */
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
        ACTION_METHODS:('all one avg count columns remove forEach empty fetchRows first get import insert save insertMultiple ' +
            'saveMultiple interval last map max min multiInsert range selectHash selectMap selectOrderMap set singleRecord ' +
            'singleValue sum toCsv toHash truncate update').split(" "),

        COUNT_FROM_SELF_OPTS:["distinct", "group", "sql", "limit", "compounds"]
    }
}).
    as(module);

