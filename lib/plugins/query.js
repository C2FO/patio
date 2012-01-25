var comb = require("comb"),
    Dataset = require("../dataset"),
    hitch = comb.hitch,
    Promise = comb.Promise,
    PromiseList = comb.PromiseList;

/**
 *@class Adds query support to a model. The QueryPlugin exposes methods to save, update, create, and delete.
 * The plugin also exposes static functions to query Models. The functions exposed on class are.
 * <ul>
 *     <li>filter</li>
 *     <li>findById</li>
 *     <li>count</li>
 *     <li>join</li>
 *     <li>where</li>
 *     <li>select</li>
 *     <li>all</li>
 *     <li>forEach</li>
 *     <li>first</li>
 *     <li>one</li>
 *     <li>last</li>
 * </ul>
 *
 * All queries require an action to be called on them before the results are fetched. The action methods are :
 *
 * <ul>
 *     <li>all</li>
 *     <li>forEach</li>
 *     <li>first</li>
 *     <li>one</li>
 *     <li>last</li>
 *
 * </ul>
 *
 * The action items accept a callback that will be called with the results. They also return a promise that will be
 * called with the results.
 *
 * <p>Assume we have an Employee model.</p>
 *
 * <p>
 *      <b>first</b></br>
 *      Get the record in a dataset, this query does not require an action method
 *      <pre class="code">
 *          Employee.first() => select * from employee limit 1
 *      </pre>
 * </p>
 *
 * <p>
 *      <b>filter</b></br>
 *      Sets the where clause on a query. See {@link Dataset}
 *      <pre class="code">
 *          //Equality Checks
 *          Employee.filter({eid : 1})
 *                  => select * from employee where eid = 1
 *          Employee.filter({eid : {gt : 1}})
 *                  => select * from employee where eid > 1
 *          Employee.filter({eid : {gte : 1}})
 *                  => select * from employee where eid >= 1
 *          Employee.filter({eid : {lt : 1}})
 *                  => select * from employee where eid < 1
 *          Employee.filter({eid : {lte : 1}})
 *                  => select * from employee where eid <= 1
 *          //Nested query in filter
 *          Employee.filter({eid : {gt : 1}, lastname : "bob"})
 *                  => select * from employee where eid > 1 and lastname = 'bob';
 *          Employee.filter({eid : [1,2,3], lastname : "bob"})
 *                  => select * from employee where eid in (1,2,3) and lastname = 'bob'
 *      </pre>
 * </p>
 * <p>
 *      <b>findById</b></br>
 *      Find a record in a dataset by id, this query does not require an action method
 *      <pre class="code">
 *          Employee.findById(1) => select * from employee where eid = 1
 *      </pre>
 * </p>
 * <p>
 *      <b>count</b></br>
 *      Find the number of records in a dataset, this query does not require an action method
 *      <pre class="code">
 *          Employee.count() => select count(*) as count from employee
 *          Employee.filter({eid : {gte : 1}}).count()
 *                  => select count(*) as count from employee  where eid > 1
 *      </pre>
 * </p>
 * <p>
 *      <b>join</b></br>
 *      Get Join two models together, this will not create model instances for the result.
 *      <pre class="code">
 *          Employee.join("words", {eid : "eid"}).where({"employee.eid" : 1})
 *                  => select * from employee inner join works on employee.id=works.id where employee.eid = 1
 *      </pre>
 * </p>
 *
 * <p>
 *      <b>Where</b></br>
 *      Sets the where clause on a query. See {@link Dataset}
 *      <pre class="code">
 *          //Equality Checks
 *          Employee.where({eid : 1})
 *                  => select * from employee where eid = 1
 *          Employee.where({eid : {gt : 1}})
 *                  => select * from employee where eid > 1
 *          Employee.where({eid : {gte : 1}})
 *                  => select * from employee where eid >= 1
 *          Employee.where({eid : {lt : 1}})
 *                  => select * from employee where eid < 1
 *          Employee.where({eid : {lte : 1}})
 *                  => select * from employee where eid <= 1
 *          //Nested query in filter
 *          Employee.where({eid : {gt : 1}, lastname : "bob"})
 *                  => select * from employee where eid > 1 and lastname = 'bob';
 *          Employee.where({eid : [1,2,3], lastname : "bob"})
 *                  => select * from employee where eid in (1,2,3) and lastname = 'bob'
 *      </pre>
 * </p>
 *
 * <p>
 *      <b>select</b></br>
 *      Selects only certain columns to return, this will not create model instances for the result.
 *      <pre class="code">
 *          Employee.select(eid).where({firstname : { gt : "bob"}})
 *                  => select eid from employee where firstname > "bob"
 *      </pre>
 * </p>
 *
 *
 * <p>
 *      <b>all, foreach, first, one, last</b></br>
 *      These methods all act as action methods and fetch the results immediately. Each method accepts a query, callback, and errback.
 *      The methods return a promise that can be used to listen for results also.
 *      <pre class="code">
 *          Employee.all()
 *                  => select * from employee
 *          Employee.forEach(function(){})
 *                  => select * from employee
 *          Employee.forEach({eid : [1,2,3]}, function(){}))
 *                  => select * from employee where eid in (1,2,3)
 *          Employee.one()
 *                  => select * from employee limit 1
 *      </pre>
 * </p>
 *
 * @name QueryPlugin
 * @memberOf patio.plugins
 *
 * @borrows Dataset#all as all
 * @borrows Dataset#forEach as forEach
 * @borrows Dataset#first as first
 * @borrows Dataset#one as one
 * @borrows Dataset#last as last
 * @borrows SQL#join as join
 * @borrows SQL#where as where
 * @borrows SQL#select as select
 *
 */
var QueryPlugin = comb.define(null, {
    instance:{
        /**@lends patio.plugins.QueryPlugin.prototype*/

        _getPrimaryKeyQuery:function () {
            var q = {};
            this.primaryKey.forEach(function (k) {
                q[k] = this[k];
            }, this);
            return q;
        },

        /**
         * Force the reload of the data for a particular model instance.
         *
         * @example
         *
         * myModel.reload().then(function(myModel){
         *    //work with this instance
         * });
         *
         * @return {comb.Promise} called back with the reloaded model instance.
         */
        reload:function () {
            var ret = new Promise();
            if (!this.__isNew) {
                ret = this.dataset.filter(this._getPrimaryKeyQuery()).one();
            } else {
                ret.callback(this);
            }
            return ret;
        },

        /**
         * Remove this model.
         *
         * @param {Function} errback called in the deletion fails.
         *
         * @return {comb.Promise} called back after the deletion is successful
         */
        remove:function (options) {
            if (!this.__isNew) {
                return this._checkTransaction(options, comb.hitch(this, function () {
                    return comb.executeInOrder(this, this.dataset, function (m, ds) {
                        m._hook("pre", "remove");
                        var ret = ds.filter(m._getPrimaryKeyQuery()).remove();
                        m._hook("post", "remove");
                        return ret;
                    });
                }));
            } else {
                return new comb.Promise().callback(0);
            }
        },

        /**
         * Update a model with new values.
         *
         * @example
         *
         * someModel.update({
         *      myVal1 : "newValue1",
         *      myVal2 : "newValue2",
         *      myVal3 : "newValue3"
         *      }).then(..do something);
         *
         * //or
         *
         * someModel.myVal1 = "newValue1";
         * someModel.myVal2 = "newValue2";
         * someModel.myVal3 = "newValue3";
         *
         * someModel.update().then(..so something);
         *
         * @param {Object} [options] values to update this model with
         * @param {Function} [errback] function to call if the update fails, the promise will errback also if it fails.
         *
         * @return {comb.Promise} called on completion or error of update.
         */
        update:function (vals, options) {
            if (!this.__isNew && this.__isChanged) {
                return this._checkTransaction(options, comb.hitch(this, function () {
                    comb.isHash(vals) && this.__set(vals);
                    var saveChanged = !comb.isEmpty(this.__changed);
                    return comb.executeInOrder(this, this.dataset.filter(this._getPrimaryKeyQuery()), function (m, ds) {
                        m._hook("pre", "update");
                        saveChanged && ds.update(m.__changed);
                        m._hook("post", "update");
                        return m;
                    });
                }));
            } else if (this.__isNew && this.__isChanged) {
                return this.save(vals, options);
            }
        },

        /**
         * Save a model with new values.
         *
         * @example
         *
         * someModel.save({
         *      myVal1 : "newValue1",
         *      myVal2 : "newValue2",
         *      myVal3 : "newValue3"
         *      }).then(..do something);
         *
         * //or
         *
         * someModel.myVal1 = "newValue1";
         * someModel.myVal2 = "newValue2";
         * someModel.myVal3 = "newValue3";
         *
         * someModel.save().then(..so something);
         *
         * @param {Object} [options] values to save this model with
         * @param {Function} [errback] function to call if the save fails, the promise will errback also if it fails.
         *
         * @return {comb.Promise} called on completion or error of save.
         */
        save:function (vals, options) {
            if (this.__isNew) {
                return this._checkTransaction(options, comb.hitch(this, function () {
                    comb.isHash(vals) && this.__set(vals);
                    var pk = this._static.primaryKey[0];
                    return comb.executeInOrder(this, this.dataset, function (m, ds) {
                        m._hook("pre", "save");
                        var id = ds.insert(m._toObject());
                        m.__ignore = true;
                        m[pk] = id;
                        m.__ignore = false;
                        m.__isNew = false;
                        m.__isChanged = false;
                        m._hook("post", "save");
                        return m;
                    });
                }));
            } else {
                return this.update(vals, options);
            }
        }

    },

    static:{

        /**@lends patio.plugins.QueryPlugin*/

        /**
         * Filter a model to return a subset of results. {@link SQL#find}
         *
         * <p><b>This function requires all, forEach, one, last,
         *       or count to be called inorder for the results to be fetched</b></p>
         * @param {Object} [options] query to filter the dataset by.
         * @param {Boolean} [hydrate=true] if true model instances will be the result of the query,
         *                                  otherwise just the results will be returned.
         *
         *@return {Dataset} A dataset to query, and or fetch results.
         */
        filter:function (options, hydrate) {
            var ds = this.dataset;
            return ds.filter.apply(ds, arguments);
        },

        /**
         * Retrieves a record by the primarykey of a table.
         * @param {*} id the primary key record to find.
         *
         * @return {comb.Promise} called back with the record or null if one is not found.
         */
        findById:function (id) {
            var pk = this.primaryKey;
            pk = pk.length == 1 ? pk[0] : pk;
            var q = {};
            if (comb.isArray(pk)) {
                pk.forEach(function (k, i) {
                    q[k] = id[i]
                });
            } else {
                q[pk] = id;
            }
            return this.filter(q).one();
        },


        find:function (id) {
            var args = comb.argsToArray(arguments);
            var query = args.every(function (h) {
                return comb.isHash(h)
            });
            if (query) {
                return this.filter.apply(this, args).one();
            } else {
                return this.findById.apply(this, args).one();
            }
        },

        /**
         * Update multiple rows with a set of values.
         *
         * @param {Object} vals the values to set on each row.
         * @param {Object} [options] query to limit the rows that are updated
         * @param {Function} [callback] function to call after the update is complete.
         * @param {Function} [errback] function to call if the update errors.
         *
         * @return {comb.Promise|Dataset} if just values were passed in then a dataset is returned and exec has to be
         *                                  called in order to complete the update.
         *                                If options, callback, or errback are provided then the update is executed
         *                                and a promise is returned that will be called back when the update completes.
         */
        update:function (vals, /*?object*/options) {
            return this._checkTransaction(options, comb.hitch(this, function () {
                var args = comb.argsToArray(arguments);
                var dataset = this.dataset;
                if (args.length > 1) {
                    dataset = dataset.filter(options);
                }
                return dataset.update(vals);
            }));
        },

        /**
         * Remove rows from the Model.
         *
         * @param {Object} [q] query to filter the rows to remove
         * @param {Function} [errback] function to call if the removal fails.
         *
         * @return {comb.Promise} called back when the removal completes.
         */
        remove:function (q, options) {
            //first find all records so we call alert all associations and all other crap that needs to be
            //done in middle ware
            return this._checkTransaction(options, comb.hitch(this, function () {
                var p = new Promise();
                var ds = this.dataset;
                this.dataset.filter(ds, arguments).all(function (items) {
                    //todo this sucks find a better way!
                    var pl = items.map(function (r) {
                        return r.remove();
                    });
                    new PromiseList(pl).then(hitch(p, "callback"), hitch(p, "errback"));
                }, hitch(p, "errback"));
                return p;
            }));
        },

        removeById:function (q, options) {
            //first find all records so we call alert all associations and all other crap that needs to be
            //done in middle ware
            return this._checkTransaction(options, comb.hitch(this, function () {
                var p = new Promise();
                this.findById(1).then(function (model) {
                    if (model) {
                        model.remove().then(hitch(p, "callback"), hitch(p, "errback"));
                    } else {
                        p.callback(0);
                    }
                }, hitch(p, "errback"));
                return p;
            }));
        },

        /**
         * Save either a new model or list of models to the database.
         *
         * @example
         *
         * //Save a group of records
         * MyModel.save([m1,m2, m3]);
         *
         * Save a single record
         * MyModel.save(m1);
         *
         * @param {Array|Object} record the record/s to save to the database
         * @param {Function} [errback] function to execute if the save fails
         *
         * @return {comb.Promise} called back with the saved record/s.
         */
        save:function (items, options) {
            return this._checkTransaction(options, hitch(this, function () {
                var ps;
                if (comb.isArray(items)) {
                    ps = items.map(function (o) {
                        return this.save(o);
                    }, this);
                    var ret = new comb.Promise();
                    new PromiseList(ps, true).then(hitch(ret, "callback"), hitch(ret, "errback"));
                    return ret;
                } else {
                    var ret = new comb.Promise();
                    try{
                        ret = new this(items).save();
                    }catch(e){
                        ret.errback(e);
                    }
                    return ret;
                }
            }));
        }
    }
}).as(exports, "QueryPlugin");

Dataset.ACTION_METHODS.concat(Dataset.QUERY_METHODS).forEach(function (m) {
    if (!QueryPlugin[m]) {
        QueryPlugin[m] = function () {
            var ds = this.dataset;
            return ds[m].apply(ds, arguments);
        }
    }
});

