var comb = require("comb-proxy"),
    define = comb.define,
    isUndefined = comb.isUndefined,
    isUndefinedOrNull = comb.isUndefinedOrNull,
    isBoolean = comb.isBoolean,
    isString = comb.isString,
    isHash = comb.isHash,
    when = comb.when,
    isFunction = comb.isFunction,
    isInstanceOf = comb.isInstanceOf,
    Promise = comb.Promise,
    PromiseList = comb.PromiseList,
    array = comb.array,
    toArray = array.toArray,
    isArray = comb.isArray,
    Middleware = comb.plugins.Middleware,
    PatioError = require("../errors").PatioError;

var fetch = {
    LAZY: "lazy",
    EAGER: "eager"
};


/**
 * @class
 * Base class for all associations.
 *
 * </br>
 * <b>NOT to be instantiated directly</b>
 * Its just documented for reference.
 *
 * @constructs
 * @param {Object} options
 * @param {String} options.model a string to look up the model that we are associated with
 * @param {Function} options.filter  a callback to find association if a filter is defined then
 *                                    the association is read only
 * @param {Object} options.key object with left key and right key
 * @param {String|Object} options.orderBy<String|Object> - how to order our association @see Dataset.order
 * @param {fetch.EAGER|fetch.LAZY} options.fetchType the fetch type of the model if fetch.Eager is supplied then
 *                                    the associations are automatically filled, if fetch.Lazy is supplied
 *                                    then a promise is returned and is called back with the loaded models
 * @property {Model} model the model associatied with this association.
 * @name Association
 * @memberOf patio.associations
 * */
define(Middleware, {
    instance: {
        /**@lends patio.associations.Association.prototype*/

        type: "",

        //Our associated model
        _model: null,

        /**
         * Fetch type
         */
        fetchType: fetch.LAZY,

        /**how to order our association*/
        orderBy: null,

        /**Our filter method*/
        filter: null,

        __hooks: null,

        isOwner: true,

        createSetter: true,

        isCascading: false,

        supportsStringKey: true,

        supportsHashKey: true,

        supportsCompositeKey: true,

        supportsLeftAndRightKey: true,

        /**
         *
         *Method to call to look up association,
         *called after the model has been filtered
         **/
        _fetchMethod: "all",


        constructor: function (options, patio, filter) {
            options = options || {};
            if (!options.model) {
                throw new Error("Model is required for " + this.type + " association");
            }
            this._model = options.model;
            this.patio = patio;
            this.__opts = options;
            !isUndefined(options.isCascading) && (this.isCascading = options.isCascading);
            this.filter = filter;
            this.readOnly = isBoolean(options.readOnly) ? options.readOnly : false;
            this.__hooks =
            {before: {add: null, remove: null, "set": null, load: null}, after: {add: null, remove: null, "set": null, load: null}};
            var hooks = ["Add", "Remove", "Set", "Load"];
            ["before", "after"].forEach(function (h) {
                hooks.forEach(function (a) {
                    var hookName = h + a, hook;
                    if (isFunction((hook = options[hookName]))) {
                        this.__hooks[h][a.toLowerCase()] = hook;
                    }
                }, this);
            }, this);
            this.fetchType = options.fetchType || fetch.LAZY;
        },

        _callHook: function (hook, action, args) {
            var func = this.__hooks[hook][action], ret;
            if (isFunction(func)) {
                ret = func.apply(this, args);
            }
            return ret;
        },

        _clearAssociations: function (model) {
            if (!this.readOnly) {
                delete model.__associations[this.name];
            }
        },

        _forceReloadAssociations: function (model) {
            if (!this.readOnly) {
                delete model.__associations[this.name];
                return model[this.name];
            }
        },

        /**
         * @return {Boolean} true if the association is eager.
         */
        isEager: function () {
            return this.fetchType === fetch.EAGER;
        },

        _checkAssociationKey: function (parent) {
            var q = {};
            this._setAssociationKeys(parent, q);
            return Object.keys(q).every(function (k) {
                return q[k] !== null;
            });
        },

        _getAssociationKey: function () {
            var options = this.__opts, key, ret = [], lk, rk;
            if (!isUndefinedOrNull((key = options.key))) {
                if (this.supportsStringKey && isString(key)) {
                    //normalize the key first!
                    ret = [
                        [this.isOwner ? this.defaultLeftKey : key],
                        [this.isOwner ? key : this.defaultRightKey]
                    ];
                } else if (this.supportsHashKey && isHash(key)) {
                    var leftKey = Object.keys(key)[0];
                    var rightKey = key[leftKey];
                    ret = [
                        [leftKey],
                        [rightKey]
                    ];
                } else if (this.supportsCompositeKey && isArray(key)) {
                    ret = [
                        [key],
                        null
                    ];
                }
            } else if (this.supportsLeftAndRightKey && (!isUndefinedOrNull((lk = options.leftKey)) && !isUndefinedOrNull((rk = options.rightKey)))) {
                ret = [
                    toArray(lk), toArray(rk)
                ];
            } else {
                //todo handle composite primary keys
                ret = [
                    [this.defaultLeftKey],
                    [this.defaultRightKey]
                ];
            }
            return ret;
        },


        _setAssociationKeys: function (parent, model, val) {
            var keys = this._getAssociationKey(parent), leftKey = keys[0], rightKey = keys[1], i = leftKey.length - 1;
            if (leftKey && rightKey) {
                for (; i >= 0; i--) {
                    model[rightKey[i]] = !isUndefined(val) ? val : parent[leftKey[i]] ? parent[leftKey[i]] : null;
                }
            } else {
                for (; i >= 0; i--) {
                    var k = leftKey[i];
                    model[k] = !isUndefined(val) ? val : parent[k] ? parent[k] : null;

                }
            }
        },

        _setDatasetOptions: function (ds) {
            var options = this.__opts || {};
            var order, limit, distinct, select, query;
            //allow for multi key ordering
            if (!isUndefined((select = this.select))) {
                ds = ds.select.apply(ds, toArray(select));
            }
            if (!isUndefined((query = options.query)) || !isUndefined((query = options.conditions))) {
                ds = ds.filter(query);
            }
            if (isFunction(this.filter)) {
                var ret = this.filter.apply(this, [ds]);
                if (isInstanceOf(ret, ds._static)) {
                    ds = ret;
                }
            }
            if (!isUndefined((distinct = options.distinct))) {
                ds = ds.limit.apply(ds, toArray(distinct));
            }
            if (!isUndefined((order = options.orderBy)) || !isUndefined((order = options.order))) {
                ds = ds.order.apply(ds, toArray(order));
            }
            if (!isUndefined((limit = options.limit))) {
                ds = ds.limit.apply(ds, toArray(limit));
            }
            return ds;

        },

        /**
         *Filters our associated dataset to load our association.
         *
         *@return {Dataset} the dataset with all filters applied.
         **/
        _filter: function (parent) {
            var options = this.__opts || {};
            var ds, self = this;
            if (!isUndefined((ds = options.dataset)) && isFunction(ds)) {
                ds = ds.apply(parent, [parent]);
            }
            if (!ds) {
                var q = {};
                this._setAssociationKeys(parent, q);
                ds = this.model.dataset.naked().filter(q);
                var recip = this.model._findAssociation(this);
                recip && (recip = recip[1]);
                ds.rowCb = function (item) {
                    var model = self._toModel(item, true);
                    recip && recip.__setValue(model, parent);
                    //call hook to finish other model associations
                    return model._hook("post", "load").chain(function () {
                        return model;
                    });
                };
            } else if (!ds.rowCb && this.model) {
                ds.rowCb = function (item) {
                    var model = self._toModel(item, true);
                    //call hook to finish other model associations
                    return model._hook("post", "load").chain(function () {
                        return model;
                    });
                };
            }

            return this._setDatasetOptions(ds);
        },

        __setValue: function (parent, model) {
            var associations;
            if (this._fetchMethod === "all") {
                associations = !isArray(model) ? [model] : model;
            } else {
                associations = isArray(model) ? model[0] : model;
            }
            parent.__associations[this.name] = associations;
            return parent.__associations[this.name];
        },

        fetch: function (parent) {
            var ret = new Promise();
            if (this._checkAssociationKey(parent)) {
                var self = this;
                return this._filter(parent)[this._fetchMethod]().chain(function (result) {
                    self.__setValue(parent, result);
                    parent = null;
                    return result;
                });
            } else {
                this.__setValue(parent, null);
                ret.callback(null);
            }
            return ret;
        },

        /**
         * Middleware called before a model is removed.
         * </br>
         * <b> This is called in the scope of the model</b>
         * @param {Function} next function to pass control up the middleware stack.
         * @param {_Association} self reference to the Association that is being acted up.
         */
        _preRemove: function (next, model) {
            if (this.isOwner && !this.isCascading) {
                var q = {};
                this._setAssociationKeys(model, q, null);
                model[this.associatedDatasetName].update(q).classic(next);
            } else {
                next();
            }
        },

        /**
         * Middleware called aft era model is removed.
         * </br>
         * <b> This is called in the scope of the model</b>
         * @param {Function} next function to pass control up the middleware stack.
         * @param {_Association} self reference to the Association that is being called.
         */
        _postRemove: function (next, model) {
            next();
        },

        /**
         * Middleware called before a model is saved.
         * </br>
         * <b> This is called in the scope of the model</b>
         * @param {Function} next function to pass control up the middleware stack.
         * @param {_Association} self reference to the Association that is being called.
         */
        _preSave: function (next, model) {
            next();
        },

        /**
         * Middleware called after a model is saved.
         * </br>
         * <b> This is called in the scope of the model</b>
         * @param {Function} next function to pass control up the middleware stack.
         * @param {_Association} self reference to the Association that is being called.
         */
        _postSave: function (next, model) {
            next();
        },

        /**
         * Middleware called before a model is updated.
         * </br>
         * <b> This is called in the scope of the model</b>
         * @param {Function} next function to pass control up the middleware stack.
         * @param {_Association} self reference to the Association that is being called.
         */
        _preUpdate: function (next, model) {
            next();
        },

        /**
         * Middleware called before a model is updated.
         * </br>
         * <b> This is called in the scope of the model</b>
         * @param {Function} next function to pass control up the middleware stack.
         * @param {_Association} self reference to the Association that is being called.
         */
        _postUpdate: function (next, model) {
            next();
        },

        /**
         * Middleware called before a model is loaded.
         * </br>
         * <b> This is called in the scope of the model</b>
         * @param {Function} next function to pass control up the middleware stack.
         * @param {_Association} self reference to the Association that is being called.
         */
        _preLoad: function (next, model) {
            next();
        },

        /**
         * Middleware called after a model is loaded.
         * </br>
         * <b> This is called in the scope of the model</b>
         * @param {Function} next function to pass control up the middleware stack.
         * @param {_Association} self reference to the Association that is being called.
         */
        _postLoad: function (next, model) {
            next();
        },

        /**
         * Alias used to explicitly set an association on a model.
         * @param {*} val the value to set the association to
         * @param {_Association} self reference to the Association that is being called.
         */
        _setter: function (val, model) {
            model.__associations[this.name] = val;
        },

        associationLoaded: function (model) {
            return model.__associations.hasOwnProperty(this.name);
        },

        getAssociation: function (model) {
            return model.__associations[this.name];
        },

        /**
         * Alias used to explicitly get an association on a model.
         * @param {_Association} self reference to the Association that is being called.
         */
        _getter: function (model) {
            //if we have them return them;
            if (this.associationLoaded(model)) {
                var assoc = this.getAssociation(model);
                return this.isEager() ? assoc : when(assoc);
            } else if (model.isNew) {
                return null;
            } else {
                return this.fetch(model);
            }
        },

        _toModel: function (val, fromDb) {
            var Model = this.model;
            if (!isUndefinedOrNull(Model)) {
                if (!isInstanceOf(val, Model)) {
                    val = new this.model(val, fromDb);
                }
            } else {
                throw new PatioError("Invalid model " + this.name);
            }
            return val;
        },

        /**
         * Method to inject functionality into a model. This method alters the model
         * to prepare it for associations, and initializes all required middleware calls
         * to fulfill requirements needed to loaded the associations.
         *
         * @param {Model} parent the model that is having an associtaion set on it.
         * @param {String} name the name of the association.
         */
        inject: function (parent, name) {
            this.name = name;
            var self = this;
            this.parent = parent;
            var parentProto = parent.prototype;
            parentProto["__defineGetter__"](name, function () {
                return self._getter(this);
            });
            parentProto["__defineGetter__"](this.associatedDatasetName, function () {
                return self._filter(this);
            });

            if (!this.readOnly && this.createSetter) {
                //define a setter because we arent read only
                parentProto["__defineSetter__"](name, function (vals) {
                    self._setter(vals, this);
                });
            }

            //set up all callbacks
            ["pre", "post"].forEach(function (op) {
                ["save", "update", "remove", "load"].forEach(function (type) {
                    parent[op](type, function (next) {
                        return self["_" + op + type.charAt(0).toUpperCase() + type.slice(1)](next, this);
                    });
                }, this);
            }, this);
        },

        getters: {

            select: function () {
                return this.__opts.select;
            },

            defaultLeftKey: function () {
                var ret = "";
                if (this.isOwner) {
                    ret = this.__opts.primaryKey || this.parent.primaryKey[0];
                } else {
                    ret = this.model.tableName + "Id";
                }
                return ret;
            },

            defaultRightKey: function () {
                return this.associatedModelKey;
            },

            //Returns our model
            model: function () {
                return this["__model__"] || (this["__model__"] = this.patio.getModel(this._model, this.parent.db));
            },

            associatedModelKey: function () {
                var ret = "";
                if (this.isOwner) {
                    ret = this.__opts.primaryKey || this.parent.tableName + "Id";
                } else {
                    ret = this.model.primaryKey[0];
                }
                return ret;
            },

            associatedDatasetName: function () {
                return this.name + "Dataset";
            },

            removeAssociationFlagName: function () {
                return "__remove" + this.name + "association";
            }

        }
    },

    static: {
        /**@lends patio.associations.Association*/

        fetch: {

            LAZY: "lazy",

            EAGER: "eager"
        }
    }
}).as(module);
