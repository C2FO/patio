var comb = require("comb"),
    Dataset = require("../dataset"),
    ModelError = require("../errors"),
    hitch = comb.hitch,
    Promise = comb.Promise,
    PromiseList = comb.PromiseList;


var QueryPlugin = comb.define(null, {
    instance:{
        /**@lends patio.Model.prototype*/

        _getPrimaryKeyQuery:function(){
            var q = {};
            this.primaryKey.forEach(function(k){
                q[k] = this[k];
            }, this);
            return q;
        },

        _clearPrimaryKeys:function(){
            var q = {};
            this.__ignore = true;
            this.primaryKey.forEach(function(k){
                this.__values[k] = null;
            }, this);
            this.__ignore = false;
        },

        /**
         * Forces the reload of the data for a particular model instance.  The Promise returned is resolved with the
         * model.
         *
         * @example
         *
         * myModel.reload().then(function(model){
         *    //model === myModel
         * });
         *
         * @return {comb.Promise} resolved with the model instance.
         */
        reload:function(){
            var ret = new Promise();
            if (!this.__isNew) {
                this.dataset.naked().filter(this._getPrimaryKeyQuery()).one().then(hitch(this, function(values){
                    this.__set(values, true);
                    ret.callback(this);
                }), hitch(ret, "errback"));
            } else {
                ret.callback(this);
            }
            return ret;
        },

        /**
         * This method removes the instance of the model. If the model {@link patio.Model#isNew} then the promise is
         * resolved with a 0 indicating no rows were affected. Otherwise the model is removed, primary keys are cleared
         * and the model's isNew flag is set to true.
         *
         * @example
         * myModel.remove().then(function(){
         *     //model is deleted
         *     assert.isTrue(myModel.isNew);
         * });
         *
         * //dont use a transaction to remove this model
         * myModel.remove({transaction : false}).then(function(){
         *     //model is deleted
         *     assert.isTrue(myModel.isNew);
         * });
         *
         * @param {Object} [options] additional options.
         * @param {Boolean} [options.transaction] boolean indicating if a transaction should be used when
         * removing the model.
         *
         * @return {comb.Promise} called back after the deletion is successful.
         */
        remove:function(options){
            if (!this.__isNew) {
                return this._checkTransaction(options, comb.hitch(this, function(){
                    return comb.executeInOrder(this, this.dataset, function(m, ds){
                        m._hook("pre", "remove");
                        var ret = ds.filter(m._getPrimaryKeyQuery()).remove();
                        m._hook("post", "remove");
                        m._clearPrimaryKeys();
                        m.__isNew = true;
                        return ret;
                    });
                }));
            } else {
                return new comb.Promise().callback(0);
            }
        },

        /**
         * @private
         * Called after a save action to reload the model properties,
         * abstracted out so this can be overidden by sub classes
         */
        _saveReload:function(){
            return this._static.reloadOnSave ? this.reload() : new comb.Promise().callback(this);
        },

        /**
         * @private
         * Called after an update action to reload the model properties,
         * abstracted out so this can be overidden by sub classes
         */
        _updateReload:function(){
            return this._static.reloadOnUpdate ? this.reload() : new comb.Promise().callback(this);
        },

        /**
         * Updates a model. This action checks if the model is not new and values have changed.
         * If the model is new then the {@link patio.Model#save} action is called.
         *
         * @example
         *
         * //set values before saving
         * someModel.update({
         *      myVal1 : "newValue1",
         *      myVal2 : "newValue2",
         *      myVal3 : "newValue3"
         *      }).then(function(){
         *     //do something
         * });
         *
         * //set vals before saving and don't use a transaction
         * someModel.update({
         *      myVal1 : "newValue1",
         *      myVal2 : "newValue2",
         *      myVal3 : "newValue3"
         *      }, {transaction : false}).then(function(){
         *     //do something
         * });
         *
         * //or
         *
         * someModel.myVal1 = "newValue1";
         * someModel.myVal2 = "newValue2";
         * someModel.myVal3 = "newValue3";
         *
         * //update model with current values
         * someModel.update().then(function(){
         *     //do something
         * });
         *
         *
         * //don't use a transaction
         * someModel.update(null, {transaction : false});
         *
         * @param {Object} [vals] optional values hash to set on the model before saving.
         * @param {Object} [options] additional options.
         * @param {Boolean} [options.transaction] boolean indicating if a transaction should be used when
         * updating the model.
         *
         * @return {comb.Promise} resolved when the update action has completed.
         */
        update:function(vals, options){
            if (!this.__isNew && this.__isChanged) {
                return this._checkTransaction(options, comb.hitch(this, function(){
                    comb.isHash(vals) && this.__set(vals);
                    var saveChanged = !comb.isEmpty(this.__changed);
                    return comb.executeInOrder(this, this.dataset.filter(this._getPrimaryKeyQuery()), function(m, ds){
                        m._hook("pre", "update");
                        saveChanged && ds.update(m.__changed);
                        m._updateReload();
                        m._hook("post", "update");
                        return m;
                    });
                }));
            } else if (this.__isNew && this.__isChanged) {
                return this.save(vals, options);
            } else {
                return new comb.Promise().callback(this);
            }
        },

        /**
         * Updates a model. This action checks if the model is new and values have changed.
         * If the model is not new then the {@link patio.Model#update} action is called.
         *
         * @example
         *
         * //set new values and save the model
         * someModel.save({
         *      myVal1 : "newValue1",
         *      myVal2 : "newValue2",
         *      myVal3 : "newValue3"
         *      }).then(function(){
         *          //do something
         *      });
         *
         * //or
         *
         * //set new values and save the model and DO NOT use a transaction
         * someModel.save({
         *      myVal1 : "newValue1",
         *      myVal2 : "newValue2",
         *      myVal3 : "newValue3"
         *      }, {transaction : false}).then(function(){
         *          //do something
         *      });
         *
         * //or
         * someModel.myVal1 = "newValue1";
         * someModel.myVal2 = "newValue2";
         * someModel.myVal3 = "newValue3";
         *
         * someModel.save().then(function(){
         *     //do something
         * });
         *
         * //or
         *
         * //dont use a transaction
         * someModel.save(null, {transaction : false}).then(function(){
         *     //do something
         * });
         *
         * @param {Object} [vals] optional values hash to set on the model before saving.
         * @param {Object} [options] additional options.
         * @param {Boolean} [options.transaction] boolean indicating if a transaction should be used when
         * saving the model.
         *
         * @return {comb.Promise} resolved when the save action has completed.
         */
        save:function(vals, options){
            if (this.__isNew) {
                return this._checkTransaction(options, comb.hitch(this, function(){
                    comb.isHash(vals) && this.__set(vals);
                    var pk = this._static.primaryKey[0];
                    return comb.executeInOrder(this, this.dataset, function(m, ds){
                        m._hook("pre", "save");
                        var id = ds.insert(m._toObject());
                        m.__ignore = true;
                        m[pk] = id;
                        m.__ignore = false;
                        m.__isNew = false;
                        m.__isChanged = false;
                        m._saveReload();
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

        /**@lends patio.Model*/

        /**
         * Retrieves a record by the primaryKey/s of a table.
         *
         * @example
         *
         * var User = patio.getModel("user");
         *
         * User.findById(1).then(function(userOne){
         *
         * });
         *
         * //assume the primary key is a compostie of first_name and last_name
         * User.findById(["greg", "yukon"]).then(function(userOne){
         *
         * });
         *
         *
         * @param {*} id the primary key of the record to find.
         *
         * @return {comb.Promise} called back with the record or null if one is not found.
         */
        findById:function(id){
            var pk = this.primaryKey;
            pk = pk.length == 1 ? pk[0] : pk;
            var q = {};
            if (comb.isArray(id) && comb.isArray(pk)) {
                if (id.length === pk.length) {
                    pk.forEach(function(k, i){
                        q[k] = id[i]
                    });
                } else {
                    throw new ModelError("findById : ids length does not equal the primaryKeys length.");
                }
            } else {
                q[pk] = id;
            }
            return this.filter(q).one();
        },

        /**
         * Finds a single model according to the supplied filter.
         * See {@link patio.Dataset#filter} for filter options.
         *
         *
         *
         * @param id
         */
        find:function(id){
            return this.filter.apply(this, args).first();
        },

        /**
         * Update multiple rows with a set of values.
         *
         * @example
         * var User = patio.getModel("user");
         *
         * //BEGIN
         * //UPDATE `user` SET `password` = NULL WHERE (`last_accessed` <= '2011-01-27')
         * //COMMIT
         * User.update({password : null}, function(){
         *  return this.lastAccessed.lte(comb.date.add(new Date(), "year", -1));
         * });
         * //same as
         * User.update({password : null}, {lastAccess : {lte : comb.date.add(new Date(), "year", -1)}});
         *
         * //UPDATE `user` SET `password` = NULL WHERE (`last_accessed` <= '2011-01-27')
         * User.update({password : null}, function(){
         *  return this.lastAccessed.lte(comb.date.add(new Date(), "year", -1));
         * }, {transaction : false});
         *
         * @param {Object} vals a hash of values to update. See {@link patio.Dataset#update}.
         * @param query a filter to apply to the UPDATE.  See {@link patio.Dataset#filter}.
         *
         * @param {Object} [options] additional options.
         * @param {Boolean} [options.transaction] boolean indicating if a transaction should be used when
         * updating the models.
         *
         * @return {Promise} a promise that is resolved once the update statement has completed.
         */
        update:function(vals, /*?object*/query, options){
            options = options || {};
            var args = comb.argsToArray(arguments);
            return this._checkTransaction(options, comb.hitch(this, function(){
                var dataset = this.dataset;
                if (!comb.isUndefined(query)) {
                    dataset = dataset.filter(query);
                }
                return dataset.update(vals);
            }));
        },

        /**
         * Remove(delete) models. This can be used to do a mass delete of models.
         *
         * @example
         * var User = patio.getModel("user");
         *
         * //remove all users
         * User.remove();
         *
         * //remove all users who's names start with a.
         * User.remove({name : /A%/i});
         *
         * //remove all users who's names start with a, without a transaction.
         * User.remove({name : /A%/i}, {transaction : false});
         *
         * @param {Object} [q] query to filter the rows to remove. See {@link patio.Dataset#filter}.
         * @param {Object} [options] additional options.
         * @param {Boolean} [options.transaction] boolean indicating if a transaction should be used when
         * removing the models.
         * @param {Boolean} [options.load=true] boolean set to prevent the loading of each model. This is more efficient
         * but the pre/post remove hooks not be notified of the deletion.
         *
         * @return {comb.Promise} called back when the removal completes.
         */
        remove:function(q, options){
            options = options || {};
            var loadEach = comb.isBoolean(options.load) ? options.load : true;
            //first find all records so we call alert the middleware for each model
            return this._checkTransaction(options, comb.hitch(this, function(){
                var ds = this.dataset;
                ds = ds.filter.call(ds, q);
                if (loadEach) {
                    return ds.map(function(r){
                        //todo this sucks find a better way!
                        return r.remove(options);
                    });
                } else {
                    return ds.remove();
                }
            }));
        },

        /**
         * Similar to remove but takes an id or an array for a composite key.
         *
         * @example
         *
         * User.removeById(1);
         *
         * @param id id or an array for a composite key, to find the model by
         * @param {Object} [options] additional options.
         * @param {Boolean} [options.transaction] boolean indicating if a transaction should be used when
         * removing the model.
         *
         * @return {comb.Promise} called back when the removal completes.
         */
        removeById:function(id, options){
            return this._checkTransaction(options, comb.hitch(this, function(){
                var p = new Promise();
                this.findById(id).then(function(model){
                    if (model) {
                        model.remove(options).then(hitch(p, "callback"), hitch(p, "errback"));
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
         * var Student = patio.getModel("student");
         * Student.save([
         *      {
         *          firstName:"Bob",
         *          lastName:"Yukon",
         *          gpa:3.689,
         *          classYear:"Senior"
         *      },
         *      {
         *          firstName:"Greg",
         *          lastName:"Horn",
         *          gpa:3.689,
         *          classYear:"Sohpmore"
         *      },
         *      {
         *          firstName:"Sara",
         *          lastName:"Malloc",
         *          gpa:4.0,
         *          classYear:"Junior"
         *      },
         *      {
         *          firstName:"John",
         *          lastName:"Favre",
         *          gpa:2.867,
         *          classYear:"Junior"
         *      },
         *      {
         *          firstName:"Kim",
         *          lastName:"Bim",
         *          gpa:2.24,
         *          classYear:"Senior"
         *      },
         *      {
         *          firstName:"Alex",
         *          lastName:"Young",
         *          gpa:1.9,
         *          classYear:"Freshman"
         *      }
         * ]).then(function(users){
         *     //work with the users
         * });
         *
         * Save a single record
         * MyModel.save(m1);
         *
         * @param {patio.Model|Object|patio.Model[]|Object[]} record the record/s to save.
         * @param {Object} [options] additional options.
         * @param {Boolean} [options.transaction] boolean indicating if a transaction should be used when
         * saving the models.
         *
         * @return {comb.Promise} called back with the saved record/s.
         */
        save:function(items, options){
            options = options || {};
            return this._checkTransaction(options, hitch(this, function(){
                var ps;
                if (comb.isArray(items)) {
                    ps = items.map(function(o){
                        if (!comb.isInstanceOf(o, this)) {
                            o = new this(o);
                        }
                        return o.save(null, options);
                    }, this);
                    var ret = new comb.Promise();
                    return new PromiseList(ps, true).then(hitch(ret, "callback"), hitch(ret, "errback"));
                } else {
                    var ret = new comb.Promise();
                    try {
                        if (!comb.isInstanceOf(items, this)) {
                            items = new this(items);
                        }
                        ret = items.save(null, options);
                    } catch (e) {
                        ret.errback(e);
                    }
                    return ret;
                }
            }));
        }
    }
}).as(exports, "QueryPlugin");

Dataset.ACTION_METHODS.concat(Dataset.QUERY_METHODS).forEach(function(m){
    if (!QueryPlugin[m]) {
        QueryPlugin[m] = function(){
            var ds = this.dataset;
            return ds[m].apply(ds, arguments);
        }
    }
});

