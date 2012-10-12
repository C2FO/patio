var comb = require("comb"),
    asyncArray = comb.async.array,
    when = comb.when,
    isBoolean = comb.isBoolean,
    isArray = comb.isArray,
    isHash = comb.isHash,
    isUndefined = comb.isUndefined,
    isInstanceOf = comb.isInstanceOf,
    isEmpty = comb.isEmpty,
    serial = comb.serial,
    Dataset = require("../dataset"),
    ModelError = require("../errors").ModelError,
    hitch = comb.hitch,
    hitchIgnore = comb.hitchIgnore,
    Promise = comb.Promise,
    PromiseList = comb.PromiseList;


var QueryPlugin = comb.define(null, {
    instance:{
        /**@lends patio.Model.prototype*/

        _getPrimaryKeyQuery:function () {
            var q = {}, pk = this.primaryKey;
            for (var i = 0, l = pk.length; i < l; i++) {
                var k = pk[i];
                q[k] = this[k];
            }
            return q;
        },

        _clearPrimaryKeys:function () {
            var pk = this.primaryKey;
            for (var i = 0, l = pk.length; i < l; i++) {
                this.__values[pk[i]] = null;
            }
        },

        reload:function () {
            if (this.synced) {
                return serial([
                    this._hook.bind(this, "pre", "load"),
                    this.__reload.bind(this),
                    this._hook.bind(this, "post", "load")
                ]).chain(this);
            } else {
                throw new ModelError("Model " + this.tableName + " has not been synced");
            }
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
        __reload:function () {
            if (!this.__isNew) {
                return this.dataset.naked().filter(this._getPrimaryKeyQuery()).one().chain(function (values) {
                    this.__setFromDb(values, true);
                    return this;
                }.bind(this));
            } else {
                return when(this);
            }
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
        remove:function (options) {
            if (this.synced) {
                if (!this.__isNew) {
                    return this._checkTransaction(options, hitch(this, function () {
                        return serial([
                            this._hook.bind(this, "pre", "remove", [options]),
                            this._remove.bind(this, options),
                            this._hook.bind(this, "post", "remove", [options]),
                            function () {
                                this._clearPrimaryKeys();
                                this.__isNew = true;
                                if (this._static.emitOnRemove) {
                                    this.emit("remove", this);
                                    this._static.emit("remove", this);
                                }
                            }.bind(this)
                        ]).chain(this);
                    }));
                } else {
                    return when(0);
                }
            } else {
                throw new ModelError("Model " + this.tableName + " has not been synced");
            }
        },

        _remove:function () {
            return this.dataset.filter(this._getPrimaryKeyQuery()).remove();
        },

        /**
         * @private
         * Called after a save action to reload the model properties,
         * abstracted out so this can be overidden by sub classes
         */
        _saveReload:function (options) {
            options || (options = {});
            var reload = isBoolean(options.reload) ? options.reload : this._static.reloadOnSave;
            return reload ? this.__reload() : when(this);
        },

        /**
         * @private
         * Called after an update action to reload the model properties,
         * abstracted out so this can be overidden by sub classes
         */
        _updateReload:function (options) {
            options = options || {};
            options || (options = {});
            var reload = isBoolean(options.reload) ? options.reload : this._static.reloadOnUpdate;
            return reload ? this.__reload() : when(this);
        },

        /**
         * Updates a model. This action checks if the model is not new and values have changed.
         * If the model is new then the {@link patio.Model#save} action is called.
         *
         * When updating a model you can pass values you want set as the first argument.
         *
         * {@code
         *
         * someModel.update({
         *      myVal1 : "newValue1",
         *      myVal2 : "newValue2",
         *      myVal3 : "newValue3"
         * }).then(function(){
         *      //do something
         * }, errorHandler);
         *
         * }
         *
         * Or you can set values on the model directly
         *
         * {@code
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
         * }
         *
         * Update also accepts an options object as the second argument allowing the overriding of default behavior.
         *
         * To override <code>useTransactions</code> you can set the <code>transaction</code> option.
         *
         * {@code
         * someModel.update(null, {transaction : false});
         * }
         *
         * You can also override the <code>reloadOnUpdate</code> property by setting the <code>reload</code> option.
         * {@code
         * someModel.update(null, {reload : false});
         * }
         *
         * @param {Object} [vals] optional values hash to set on the model before saving.
         * @param {Object} [options] additional options.
         * @param {Boolean} [options.transaction] boolean indicating if a transaction should be used when
         * updating the model.
         * @param {Boolean} [options.reload] boolean indicating if the model should be reloaded after the update. This will take
         * precedence over {@link patio.Model.reloadOnUpdate}
         *
         * @return {comb.Promise} resolved when the update action has completed.
         */
        update:function (vals, options) {
            if (this.synced) {
                if (!this.__isNew) {
                    return this._checkTransaction(options, hitch(this, function () {
                        if (isHash(vals)) {
                            this.__set(vals);
                        }
                        var saveChange = !isEmpty(this.__changed);
                        return serial([
                            this._hook.bind(this, "pre", "update", [options]),
                            function () {
                                return saveChange ? this._update(options) : null;
                            }.bind(this),
                            this._hook.bind(this, "post", "update", [options]),
                            this._updateReload.bind(this, options),
                            function () {
                                if (this._static.emitOnUpdate) {
                                    this.emit("update", this);
                                    this._static.emit("update", this);
                                }
                            }.bind(this)
                        ]).chain(this);

                    }));
                } else if (this.__isNew && this.__isChanged) {
                    return this.save(vals, options);
                } else {
                    return when(this);
                }
            } else {
                throw new ModelError("Model " + this.tableName + " has not been synced");
            }
        },

        _update:function (options) {
            var ret = this.dataset.filter(this._getPrimaryKeyQuery()).update(this.__changed);
            this.__changed = {};
            return ret;
        },

        /**
         * Saves a model. This action checks if the model is new and values have changed.
         * If the model is not new then the {@link patio.Model#update} action is called.
         *
         * When saving a model you can pass values you want set as the first argument.
         *
         * {@code
         *
         * someModel.save({
         *      myVal1 : "newValue1",
         *      myVal2 : "newValue2",
         *      myVal3 : "newValue3"
         * }).then(function(){
         *      //do something
         * }, errorHandler);
         *
         * }
         *
         * Or you can set values on the model directly
         *
         * {@code
         *
         * someModel.myVal1 = "newValue1";
         * someModel.myVal2 = "newValue2";
         * someModel.myVal3 = "newValue3";
         *
         * //update model with current values
         * someModel.save().then(function(){
         *     //do something
         * });
         *
         * }
         *
         * Save also accepts an options object as the second argument allowing the overriding of default behavior.
         *
         * To override <code>useTransactions</code> you can set the <code>transaction</code> option.
         *
         * {@code
         * someModel.save(null, {transaction : false});
         * }
         *
         * You can also override the <code>reloadOnSave</code> property by setting the <code>reload</code> option.
         * {@code
         * someModel.save(null, {reload : false});
         * }
         *
         *
         * @param {Object} [vals] optional values hash to set on the model before saving.
         * @param {Object} [options] additional options.
         * @param {Boolean} [options.transaction] boolean indicating if a transaction should be used when
         * saving the model.
         * @param {Boolean} [options.reload] boolean indicating if the model should be reloaded after the save. This will take
         * precedence over {@link patio.Model.reloadOnSave}
         *
         * @return {comb.Promise} resolved when the save action has completed.
         */
        save:function (vals, options) {
            if (this.synced) {
                if (this.__isNew) {
                    return this._checkTransaction(options, hitch(this, function () {
                        if (isHash(vals)) {
                            this.__set(vals);
                        }
                        return serial([
                            this._hook.bind(this, "pre", "save", [options]),
                            this._save.bind(this, options),
                            this._hook.bind(this, "post", "save", [options]),
                            this._saveReload.bind(this, options),
                            function () {
                                if (this._static.emitOnSave) {
                                    this.emit("save", this);
                                    this._static.emit("save", this);
                                }
                            }.bind(this)
                        ]).chain(this);
                    }));
                } else {
                    return this.update(vals, options);
                }
            } else {
                throw new ModelError("Model " + this.tableName + " has not been synced");
            }
        },

        _save:function (options) {
            var pk = this._static.primaryKey[0];
            return this.dataset.insert(this._toObject()).chain(function (id) {
                this.__ignore = true;
                if (id) {
                    this[pk] = id;
                }
                this.__ignore = false;
                this.__isNew = false;
                this.__isChanged = false;
                return this;
            }.bind(this));
        },

        getUpdateSql:function () {
            return this.updateDataset.filter(this._getPrimaryKeyQuery()).updateSql(this.__changed);
        },


        getInsertSql:function () {
            return this.insertDataset.insertSql(this._toObject());
        },

        getRemoveSql:function () {
            return this.removeDataset.filter(this._getPrimaryKeyQuery()).deleteSql;
        },


        getters:{
            updateSql:function () {
                return this.getUpdateSql();
            },


            insertSql:function () {
                return this.getInsertSql();
            },

            removeSql:function () {
                return this.getRemoveSql();
            },

            deleteSql:function () {
                return this.removeSql;
            }
        }

    },

    static:{

        /**@lends patio.Model*/

        /**
         * Set to false to prevent the emitting on an event when a model is saved.
         * @default true
         */
        emitOnSave:true,

        /**
         * Set to false to prevent the emitting on an event when a model is updated.
         * @default true
         */
        emitOnUpdate:true,

        /**
         * Set to false to prevent the emitting on an event when a model is removed.
         * @default true
         */
        emitOnRemove:true,

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
        findById:function (id) {
            var pk = this.primaryKey;
            pk = pk.length == 1 ? pk[0] : pk;
            var q = {};
            if (isArray(id) && isArray(pk)) {
                if (id.length === pk.length) {
                    pk.forEach(function (k, i) {
                        q[k] = id[i];
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
        find:function (id) {
            return this.filter.apply(this, arguments).first();
        },

        /**
         * Finds a single model according to the supplied filter.
         * See {@link patio.Dataset#filter} for filter options. If the model
         * does not exist then a new one is created as passed back.
         * @param q
         */
        findOrCreate:function (q) {
            return this.find(q).chain(hitch(this, function (res) {
                return res || this.create(q);
            }));
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
        update:function (vals, /*?object*/query, options) {
            options = options || {};
            var args = comb(arguments).toArray();
            return this._checkTransaction(options, hitch(this, function () {
                var dataset = this.dataset;
                if (!isUndefined(query)) {
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
        remove:function (q, options) {
            options = options || {};
            var loadEach = isBoolean(options.load) ? options.load : true;
            //first find all records so we call alert the middleware for each model
            return this._checkTransaction(options, hitch(this, function () {
                var ds = this.dataset;
                ds = ds.filter.call(ds, q);
                if (loadEach) {
                    return ds.map(function (r) {
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
        removeById:function (id, options) {
            return this._checkTransaction(options, hitch(this, function () {
                return this.findById(id).chain(function (model) {
                    if (model) {
                        return model.remove(options);
                    }
                });
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
        save:function (items, options) {
            options = options || {};
            var isArr = isArray(items);
            return this._checkTransaction(options, hitch(this, function () {
                return asyncArray(items).map(function (o) {
                    if (!isInstanceOf(o, this)) {
                        o = new this(o);
                    }
                    return o.save(null, options);
                }, this).chain(function (res) {
                        return isArr ? res : res[0];
                    });
            }));
        }
    }
}).as(exports, "QueryPlugin");

Dataset.ACTION_METHODS.concat(Dataset.QUERY_METHODS).forEach(function (m) {
    if (!QueryPlugin[m]) {
        QueryPlugin[m] = function () {
            if (this.synced) {
                var ds = this.dataset;
                return ds[m].apply(ds, arguments);
            } else {
                throw new ModelError("Model " + this.tableName + " has not been synced");
            }
        }
    }
});

