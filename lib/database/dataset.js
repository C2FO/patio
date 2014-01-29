var comb = require("comb"),
    define = comb.define,
    argsToArray = comb.argsToArray,
    isFunction = comb.isFunction,
    errors = require("../errors"),
    NotImplemented = errors.NotImplemented,
    Promise = comb.Promise,
    Dataset = require("../dataset");

var Database = define(null, {
    instance: {
        /**@lends patio.Database.prototype*/
        /**
         *Fetches records for an arbitrary SQL statement. If a block is given,
         * it is used to iterate over the records:
         * <pre class="code">
         *   DB.fetch('SELECT * FROM items', function(r){
         *      //do something with row
         *   });
         *</pre>
         * If a block is not given then {@link patio.Database#fetch} method returns a {@link patio.Dataset} instance:
         * <pre class="code">
         *   DB.fetch('SELECT * FROM items').all().chain(function(records){
         *      //do something with the records.
         *   });
         * </pre>
         *
         * {@link patio.Database#fetch} can also perform parameterized queries for protection against SQL
         * injection:
         * <pre class="code">
         *   DB.fetch('SELECT * FROM items WHERE name = ?', myName).all().chain(function(records){
         *      //do something with the records.
         *   });
         * </pre>
         *
         * @param {String...} args variable number of args where the first argument is a String. If more than
         * one argument is given then the SQL will be treated as a place holder string. See {@link patio.Dataset#withSql}.
         * @param {Function} [block=null] if the last argument given is a function then {@link patio.Dataset#forEach} will be
         * invoked on the dataset with the block called for each row.
         *
         * @return {Promise|patio.Dataset} if no block is given then a {@link patio.Dataset} will be returned. If a block
         * is given then the return value will be a Promise that will be invoked after all records have been returned
         * and processed through the block.
         * */
        fetch: function (args, block) {
            var ret;
            args = argsToArray(arguments);
            block = isFunction(args[args.length - 1]) ? args.pop() : null;
            var ds = this.dataset.withSql.apply(this.dataset, args);
            if (block) {
                ret = ds.forEach(block).chain(function () {
                    return ds;
                }).promise();
            } else {
                ret = ds;
            }
            return ret;
        },

        /**
         * Returns a new {@link patio.Dataset} with the [@link patio.Dataset#from} method invoked. If a block is given,
         * it is used as a filter(see {@link patio.Dataset#filter} on the dataset.
         * @example
         *   DB.from("items").sql //=> SELECT * FROM items
         *   DB.from("items", function(){
         *          return this.id.gt(2)
         *   }).sql; //=> SELECT * FROM items WHERE (id > 2)
         * @param {String...} args table/s to pass to {@link patio.Dataset#from} with.
         * @param {function} [block] an option block to pass to {@link patio.Dataset#filter} with.
         *
         * @return {patio.Dataset} a dataset to use for querying the {@link patio.Database} with.
         * */
        from: function (args, block) {
            args = argsToArray(arguments);
            block = isFunction(args[args.length - 1]) ? args.pop() : null;
            var ds = this.dataset;
            ds = ds.from.apply(ds, args);
            return block ? ds.filter(block) : ds;
        },

        /**
         * Returns a new {@link patio.Dataset} with the {@link patio.Dataset#select} method invoked.
         * @example
         *   DB.select(1) //=> SELECT 1
         *   DB.select(function(){
         *      return this.server_version();
         *   }).sql; //=> SELECT server_version()
         *   DB.select("id").from("items").sql; //=> SELECT id FROM items
         * @link {patio.Dataset} a dataset to query the {@link patio.Database} with.
         **/
        select: function () {
            var ds = this.dataset;
            return ds.select.apply(ds, arguments);
        },

        /**@ignore*/
        getters: {
            dataset: function () {
                return new Dataset(this);
            }
        }

    }
}).as(module);


