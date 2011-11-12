var comb = require("comb"), hitch = comb.hitch, errors = require("../errors"), NotImplemented = errors.NotImplemented, Dataset = require("../dataset");

var Database = comb.define(null, {
    instance : {

        /*
         Fetches records for an arbitrary SQL statement. If a block is given,

         * it is used to iterate over the records:
         *
         *   DB.fetch('SELECT * FROM items'){|r| p r}
         *
         * The +fetch+ method returns a dataset instance:
         *
         *   DB.fetch('SELECT * FROM items').all
         *
         * +fetch+ can also perform parameterized queries for protection against SQL
         * injection:
         *
         *   DB.fetch('SELECT * FROM items WHERE name = ?', my_name).all
         *   */
        fetch : function() {
            var ret = new comb.Promise();
            var args = comb.argsToArray(arguments);
            var block = comb.isFunction(args[args.length - 1]) ? args.pop() : null;
            var ds = this.dataset.withSql.apply(this.dataset, args);
            if (block) {
                ds.forEach(block).then(function() {
                    ret.callback(ds);
                }, comb.hitch(ret, "errback"));
                return ret;
            } else {
                return ds;
            }
        },

        /*
         * Returns a new dataset with the +from+ method invoked. If a block is given,
         * it is used as a filter on the dataset.
         *
         *   DB.from(:items) # SELECT * FROM items
         *   DB.from(:items){id > 2} # SELECT * FROM items WHERE (id > 2)
         * */
        from : function() {
            var args = comb.argsToArray(arguments);
            var block = comb.isFunction(args[args.length - 1]) ? args.pop() : null;
            var ds = this.dataset.from.apply(this.dataset, args);
            return block ? ds.filter(block) : ds;
        },

        /*
         * Returns a new dataset with the select method invoked.
         *
         *   DB.select(1) # SELECT 1
         *   DB.select{server_version{}} # SELECT server_version()
         *   DB.select(:id).from(:items) # SELECT id FROM items
         *   */
        select : function() {
            return this.dataset.select.apply(this.dataset, arguments);
        },

        getters : {
            dataset : function() {
                return new Dataset(this);
            }
        }

    }}).export(module);


