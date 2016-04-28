var comb = require("comb"),
    asyncArray = comb.async.array,
    isFunction = comb.isFunction,
    isUndefined = comb.isUndefined,
    isDefined = comb.isDefined,
    isBoolean = comb.isBoolean,
    isString = comb.isString,
    argsToArray = comb.argsToArray,
    isInstanceOf = comb.isInstanceOf,
    isHash = comb.isHash,
    when = comb.when,
    merge = comb.merge,
    toArray = comb.array.toArray,
    ModelError = require("./errors").ModelError,
    plugins = require("./plugins"),
    isUndefinedOrNull = comb.isUndefinedOrNull,
    AssociationPlugin = plugins.AssociationPlugin,
    QueryPlugin = plugins.QueryPlugin,
    Promise = comb.Promise,
    PromiseList = comb.PromiseList,
    HashTable = comb.collections.HashTable,
    hitch = comb.hitch,
    Middleware = comb.plugins.Middleware,
    EventEmitter = require("events").EventEmitter,
    util = require("util"),
    define = comb.define,
    patio;


var MODELS = new HashTable();

var applyColumnTransformMethod = function (val, meth) {
    return !isUndefinedOrNull(meth) ? isFunction(val[meth]) ? val[meth] : isFunction(comb[meth]) ? comb[meth](val) : val : val;
};

var Model = define([QueryPlugin, Middleware], {
    instance: {
        /**
         * @lends patio.Model.prototype
         */

        __ignore: false,

        __changed: null,

        __values: null,

        /**
         * patio  - read only
         *
         * @type patio
         */
        patio: null,

        /**
         * The database type such as mysql
         *
         * @type String
         *
         * */
        type: null,

        /**
         * Whether or not this model is new
         * */
        __isNew: true,

        /**
         * Signifies if the model has changed
         * */
        __isChanged: false,

        /**
         * Base class for all models.
         * <p>This is used through {@link patio.addModel}, <b>NOT directly.</b></p>
         *
         * @constructs
         * @augments comb.plugins.Middleware
         *
         * @param {Object} columnValues values of each column to be used by this Model.
         *
         * @property {patio.Dataset} dataset a dataset to use to retrieve models from the database. The dataset
         *                          has the {@link patio.Dataset#rowCb} set to create instances of this model.
         * @property {String[]} columns a list of columns this models table contains.
         * @property {Object} schema the schema of this models table.
         * @property {String} tableName the table name of this models table.
         * @property {*} primaryKeyValue the value of this models primaryKey
         * @property {Boolean} isNew true if this model is new and does not exist in the database.
         * @property {Boolean} isChanged true if the model has been changed and not saved.
         *
         * @borrows patio.Dataset#all as all
         * @borrows patio.Dataset#one as one
         * @borrows patio.Dataset#avg as avg
         * @borrows patio.Dataset#count as count
         * @borrows patio.Dataset#columns as columns
         * @borrows patio.Dataset#forEach as forEach
         * @borrows patio.Dataset#isEmpty as empty
         * @borrows patio.Dataset#first as first
         * @borrows patio.Dataset#get as get
         * @borrows patio.Dataset#import as import
         * @borrows patio.Dataset#insert as insert
         * @borrows patio.Dataset#insertMultiple as insertMultiple
         * @borrows patio.Dataset#saveMultiple as saveMultiple
         * @borrows patio.Dataset#interval as interval
         * @borrows patio.Dataset#last as last
         * @borrows patio.Dataset#map as map
         * @borrows patio.Dataset#max as max
         * @borrows patio.Dataset#min as min
         * @borrows patio.Dataset#multiInsert as multiInsert
         * @borrows patio.Dataset#range as range
         * @borrows patio.Dataset#selectHash as selectHash
         * @borrows patio.Dataset#selectMap as selectMap
         * @borrows patio.Dataset#selectOrderMap as selectOrderMap
         * @borrows patio.Dataset#set as set
         * @borrows patio.Dataset#singleRecord as singleRecord
         * @borrows patio.Dataset#singleValue as singleValue
         * @borrows patio.Dataset#sum as sum
         * @borrows patio.Dataset#toCsv as toCsv
         * @borrows patio.Dataset#toHash as toHash
         * @borrows patio.Dataset#truncate as truncate
         * @borrows patio.Dataset#addGraphAliases as addGraphAliases
         * @borrows patio.Dataset#and as and
         * @borrows patio.Dataset#distinct as distinct
         * @borrows patio.Dataset#except as except
         * @borrows patio.Dataset#exclude as exclude
         * @borrows patio.Dataset#is as is
         * @borrows patio.Dataset#isNot as isNot
         * @borrows patio.Dataset#eq as eq
         * @borrows patio.Dataset#neq as neq
         * @borrows patio.Dataset#lt as lt
         * @borrows patio.Dataset#lte as lte
         * @borrows patio.Dataset#gt as gt
         * @borrows patio.Dataset#gte as gte
         * @borrows patio.Dataset#forUpdate as forUpdate
         * @borrows patio.Dataset#from as from
         * @borrows patio.Dataset#fromSelf as fromSelf
         * @borrows patio.Dataset#graph as graph
         * @borrows patio.Dataset#grep as grep
         * @borrows patio.Dataset#group as group
         * @borrows patio.Dataset#groupAndCount as groupAndCount
         * @borrows patio.Dataset#groupBy as groupBy
         * @borrows patio.Dataset#having as having
         * @borrows patio.Dataset#intersect as intersect
         * @borrows patio.Dataset#invert as invert
         * @borrows patio.Dataset#limit as limit
         * @borrows patio.Dataset#lockStyle as lockStyle
         * @borrows patio.Dataset#naked as naked
         * @borrows patio.Dataset#or as or
         * @borrows patio.Dataset#order as order
         * @borrows patio.Dataset#orderAppend as orderAppend
         * @borrows patio.Dataset#orderBy as orderBy
         * @borrows patio.Dataset#orderMore as orderMore
         * @borrows patio.Dataset#orderPrepend as orderPrepend
         * @borrows patio.Dataset#qualify as qualify
         * @borrows patio.Dataset#reverse as reverse
         * @borrows patio.Dataset#reverseOrder as reverseOrder
         * @borrows patio.Dataset#select as select
         * @borrows patio.Dataset#selectAll as selectAll
         * @borrows patio.Dataset#selectAppend as selectAppend
         * @borrows patio.Dataset#selectMore as selectMore
         * @borrows patio.Dataset#setDefaults as setDefaults
         * @borrows patio.Dataset#setGraphAliases as setGraphAliases
         * @borrows patio.Dataset#setOverrides as setOverrides
         * @borrows patio.Dataset#unfiltered as unfiltered
         * @borrows patio.Dataset#ungraphed as ungraphed
         * @borrows patio.Dataset#ungrouped as ungrouped
         * @borrows patio.Dataset#union as union
         * @borrows patio.Dataset#unlimited as unlimited
         * @borrows patio.Dataset#unordered as unordered
         * @borrows patio.Dataset#where as where
         * @borrows patio.Dataset#with as with
         * @borrows patio.Dataset#withRecursive as withRecursive
         * @borrows patio.Dataset#withSql as withSql
         * @borrows patio.Dataset#naturalJoin as naturalJoin
         * @borrows patio.Dataset#naturalLeftJoin as naturalLeftJoin
         * @borrows patio.Dataset#naturalRightJoin as naturalRightJoin
         * @borrows patio.Dataset#naturalFullJoin as naturalFullJoin
         * @borrows patio.Dataset#crossJoin as crossJoin
         * @borrows patio.Dataset#innerJoin as innerJoin
         * @borrows patio.Dataset#fullOuterJoin as fullOuterJoin
         * @borrows patio.Dataset#rightOuterJoin as rightOuterJoin
         * @borrows patio.Dataset#leftOuterJoin as leftOuterJoin
         * @borrows patio.Dataset#fullJoin as fullJoin
         * @borrows patio.Dataset#rightJoin as rightJoin
         * @borrows patio.Dataset#leftJoin as leftJoin
         * */
        constructor: function (options, fromDb) {
            if (this.synced) {
                this.__emitter = new EventEmitter();
                this._super(arguments);
                this.patio = patio || require("./index");
                fromDb = isBoolean(fromDb) ? fromDb : false;
                this.__changed = {};
                this.__values = {};
                if (fromDb) {
                    this._hook("pre", "load");
                    this.__isNew = false;
                    this.__setFromDb(options, true);
                    if (this._static.emitOnLoad) {
                        this.emit("load", this);
                        this._static.emit("load", this);
                    }
                } else {
                    this.__isNew = true;
                    this.__set(options);
                }
            } else {
                throw new ModelError("Model " + this.tableName + " has not been synced");
            }
        },

        __set: function (values, ignore) {
            values = values || {};
            this.__ignore = ignore === true;
            Object.keys(values).forEach(function (attribute) {
                var value = values[attribute];
                //check if the column is a constrained value and is allowed to be set
                !ignore && this._checkIfColumnIsConstrained(attribute);
                this[attribute] = value;
            }, this);
            this.__ignore = false;
        },

        __setFromDb: function (values, ignore) {
            values = values || {};
            this.__ignore = ignore === true;
            var schema = this.schema;
            Object.keys(values).forEach(function (column) {
                var value = values[column];
                // Typecast value retrieved from db
                if (schema.hasOwnProperty(column)) {
                    this.__values[column] = this._typeCastValue(column, value, ignore);
                } else {
                    this[column] = value;
                }
            }, this);
            this.__ignore = false;

        },

        /**
         * Set multiple values at once. Useful if you have a hash of properties that you want to set.
         *
         * <b>NOTE:</b> This method will use the static restrictedColumns property of the model.
         *
         * @example
         *
         * myModel.setValues({firstName : "Bob", lastName : "yukon"});
         *
         * //this will throw an error by default, assuming id is a pk.
         * myModel.setValues({id : 1, firstName : "Bob", lastName : "yukon"});
         *
         * @param {Object} values value to set on the model.
         *
         * @return {patio.Model} return this for chaining.
         */
        setValues: function (values) {
            this.__set(values, false);
            return this;
        },

        _toObject: function () {
            if (this.synced) {
                var columns = this._static.columns, ret = {};
                for (var i in columns) {
                    var col = columns[i];
                    var val = this.__values[col];
                    if (!isUndefined(val)) {
                        ret[col] = val;
                    }
                }
                return ret;
            } else {
                throw new ModelError("Model " + this.tableName + " has not been synced");
            }
        },

        _addColumnToIsChanged: function (name, val) {
            if (!this.isNew && !this.__ignore) {
                this.__isChanged = true;
                this.__changed[name] = val;
            }
        },

        _checkIfColumnIsConstrained: function (name) {
            if (this.synced && !this.__ignore) {
                var col = this.schema[name], restrictedCols = this._static.restrictedColumns || [];
                if (!isUndefined(col) && (col.primaryKey && this._static.isRestrictedPrimaryKey) || restrictedCols.indexOf(name) !== -1) {
                    throw new ModelError("Cannot set primary key of model " + this._static.tableName);
                }
            }
        },

        _getColumnValue: function (name) {
            var val = this.__values[name];
            var getterFunc = this["_get" + name.charAt(0).toUpperCase() + name.substr(1)];
            var columnValue = isFunction(getterFunc) ? getterFunc.call(this, val) : val;

            return columnValue;
        },

        _setColumnValue: function (name, val) {
            var ignore = this.__ignore;
            val = this._typeCastValue(name, val, ignore);
            var setterFunc = this["_set" + name.charAt(0).toUpperCase() + name.substr(1)];
            var columnValue = isFunction(setterFunc) ? setterFunc.call(this, val, ignore) : val;
            this._addColumnToIsChanged(name, columnValue);
            this.__values[name] = columnValue;
            if (this._static.emitOnColumnSet) {
                this.emit("column", name, columnValue, ignore);
            }
        },


        //Typecast the value to the column's type if typecasting.  Calls the database's
        //typecast_value method, so database adapters can override/augment the handling
        //for database specific column types.
        _typeCastValue: function (column, value, fromDatabase) {
            var colSchema, clazz = this._static;
            if (((fromDatabase && clazz.typecastOnLoad) || (!fromDatabase && clazz.typecastOnAssignment)) && !isUndefinedOrNull(this.schema) && !isUndefinedOrNull((colSchema = this.schema[column]))) {
                var type = colSchema.type;
                if (value === "" && clazz.typecastEmptyStringToNull === true && !isUndefinedOrNull(type) && ["string", "blob", "text"].indexOf(type) === -1) {
                    value = null;
                }
                var raiseOnError = clazz.raiseOnTypecastError;
                if (raiseOnError === true && isUndefinedOrNull(value) && colSchema.allowNull === false) {
                    throw new ModelError("null is not allowed for the " + column + " column on model " + clazz.tableName);
                }
                try {
                    value = clazz.db.typecastValue(type, value);
                } catch (e) {
                    if (raiseOnError === true) {
                        throw e;
                    }
                }
            }
            return value;
        },

        /**
         * Convert this model to an object, containing column, value pairs.
         *
         * @return {Object} the object version of this model.
         **/
        toObject: function () {
            return this._toObject(false);
        },

        /**
         * Convert this model to JSON, containing column, value pairs.
         *
         * @return {JSON} the JSON version of this model.
         **/
        toJSON: function () {
            return this.toObject();
        },

        /**
         * Convert this model to a string, containing column, value pairs.
         *
         * @return {String} the string version of this model.
         **/
        toString: function () {
            return JSON.stringify(this.toObject(), null, 4);
        },

        /**
         * Convert this model to a string, containing column, value pairs.
         *
         * @return {String} the string version of this model.
         **/
        valueOf: function () {
            return this.toObject();
        },

        _checkTransaction: function (options, cb) {
            return this._static._checkTransaction(options, cb);
        },

        addListener: function () {
            var emitter = this.__emitter;
            return emitter.addListener.apply(emitter, arguments);
        },
        on: function () {
            var emitter = this.__emitter;
            return emitter.on.apply(emitter, arguments);
        },
        once: function () {
            var emitter = this.__emitter;
            return emitter.once.apply(emitter, arguments);
        },
        removeListener: function () {
            var emitter = this.__emitter;
            return emitter.removeListener.apply(emitter, arguments);
        },
        removeAllListeners: function () {
            var emitter = this.__emitter;
            return emitter.removeAllListeners.apply(emitter, arguments);
        },
        setMaxListeners: function () {
            var emitter = this.__emitter;
            return emitter.setMaxListeners.apply(emitter, arguments);
        },
        listeners: function () {
            var emitter = this.__emitter;
            return emitter.listeners.apply(emitter, arguments);
        },
        emit: function () {
            var emitter = this.__emitter;
            return emitter.emit.apply(emitter, arguments);
        },


        getters: {
            /**@lends patio.Model.prototype*/

            /*Returns my actual primary key value*/
            primaryKeyValue: function () {
                return this[this.primaryKey];
            },

            /*Return if Im a new object*/
            isNew: function () {
                return this.__isNew;
            },

            /*Return if Im changed*/
            isChanged: function () {
                return this.__isChanged;
            },

            /**@lends patio.Model.prototype*/

            primaryKey: function () {
                return this._static.primaryKey;
            },

            tableName: function () {
                return this._static.tableName;
            },

            dataset: function () {
                return this.__dataset || (this.__dataset = this._static.dataset);
            },


            removeDataset: function () {
                return this.__removeDataset || (this.__removeDataset = this._static.removeDataset);
            },

            queryDataset: function () {
                return this.__queryDataset || (this.__queryDataset = this._static.queryDataset);
            },

            updateDataset: function () {
                return this.__updateDataset || (this.__updateDataset = this._static.updateDataset);
            },

            insertDataset: function () {
                return this.__insertDataset || (this.__insertDataset = this._static.insertDataset);
            },

            db: function () {
                return this._static.db;
            },

            schema: function () {
                return this._static.schema;
            },

            columns: function () {
                return this._static.columns;
            },

            synced: function () {
                return this._static.synced;
            }

        }
    },

    static: {
        /**
         * @lends patio.Model
         */

        synced: false,

        /**
         * Set to false to prevent the emitting of an event on load
         * @default true
         */
        emitOnLoad: true,

        /**
         * Set to false to prevent the emitting of an event on the setting of a column value
         * @default true
         */
        emitOnColumnSet: true,


        /**
         * Set to false to prevent empty strings from being type casted to null
         * @default true
         */
        typecastEmptyStringToNull: true,

        /**
         * Set to false to prevent properties from being type casted when loaded from the database.
         * See {@link patio.Database#typecastValue}
         * @default true
         */
        typecastOnLoad: true,

        /**
         * Set to false to prevent properties from being type casted when manually set.
         * See {@link patio.Database#typecastValue}
         * @default true
         */
        typecastOnAssignment: true,

        /**
         * Set to false to prevent errors thrown while type casting a value from being propogated.
         * @default true
         */
        raiseOnTypecastError: true,

        /**
         * Set to false to allow the setting of primary keys.
         * @default false
         */
        isRestrictedPrimaryKey: true,

        /**
         * Set to false to prevent models from using transactions when saving, deleting, or updating.
         * This applies to the model associations also.
         */
        useTransactions: true,

        /**
         * See {@link patio.Dataset#identifierOutputMethod}
         * @default null
         */
        identifierOutputMethod: null,

        /**
         * See {@link patio.Dataset#identifierInputMethod}
         * @default null
         */
        identifierInputMethod: null,

        /**
         * Set to false to prevent the reload of a model after saving.
         * @default true
         */
        reloadOnSave: true,

        /**
         * Columns that should be restriced when setting values through the {@link patio.Model#set} method.
         *
         */
        restrictedColumns: null,

        /**
         * Set to false to prevent the reload of a model after updating.
         * @default true
         */
        reloadOnUpdate: true,


        __camelize: false,

        __underscore: false,

        __columns: null,

        __schema: null,

        __primaryKey: null,

        __dataset: null,

        __db: null,

        __tableName: null,


        /**
         * The table that this Model represents.
         * <b>READ ONLY</b>
         */
        table: null,

        init: function () {
            var emitter = new EventEmitter();
            ["addListener", "on", "once", "removeListener",
                "removeAllListeners", "setMaxListeners", "listeners", "emit"].forEach(function (name) {
                    this[name] = hitch(emitter, emitter[name]);
                }, this);
            if (this.__tableName) {
                this._setTableName(this.__tableName);
            }
            if (this.__db) {
                this._setDb(this.__db);
            }
        },

        sync: function (cb) {
            var ret = new Promise();
            if (!this.synced) {
                var db = this.db, tableName = this.tableName, supers = this.__supers, self = this;
                ret = db.schema(tableName).chain(function (schema) {
                    if (!self.synced && schema) {
                        self._setSchema(schema);
                        if (supers && supers.length) {
                            return when(supers.map(function (sup) {
                                return sup.sync();
                            })).chain(function () {
                                self.synced = true;
                                supers.forEach(self.inherits, self);
                                return self;
                            });
                        } else {
                            self.synced = true;
                            return self;
                        }
                    } else {
                        var error = new ModelError("Unable to find schema for " + tableName);
                        self.emit("error", error);
                        throw error;

                    }
                });
            } else {
                ret.callback(this);
            }
            if (isFunction(cb)) {
                ret.classic(cb);
            }
            return ret.promise();
        },

        /**
         * Stub for plugins to notified of model inheritance
         *
         * @param {patio.Model} model a model class to inherit from
         */
        inherits: function (model) {
        },


        /**
         * Create a new model initialized with the specified values.
         *
         * @param {Object} values  the values to initialize the model with.
         *
         * @returns {Model} instantiated model initialized with the values passed in.
         */
        create: function (values) {
            //load an object from an object
            var Self = this;
            return new Self(values, false);
        },

        load: function (vals) {
            var Self = this, ret;
            if (!this.synced) {
                //sync our model
                ret = this.sync().chain(function () {
                    var m = new Self(vals, true);
                    //call the hooks!
                    return m._hook("post", "load").chain(function () {
                        return m;
                    });
                });
            } else {
                var m = new Self(vals, true);
                //call the hooks!
                ret = m._hook("post", "load").chain(function () {
                    return m;
                });
            }
            return ret;
        },

        _checkTransaction: function (opts, cb) {
            if (isFunction(opts)) {
                cb = opts;
                opts = {};
            } else {
                opts = opts || {};
            }
            var retVal = null, errored = false, self = this;
            return this.sync().chain(function () {
                if (self.useTransaction(opts)) {
                    return self.db.transaction(opts, function () {
                        return when(cb()).chain(function (val) {
                            retVal = val;
                        }, function (err) {
                            retVal = err;
                            errored = true;
                        });
                    }).chain(function () {
                        if (errored) {
                            throw retVal;
                        } else {
                            return retVal;
                        }
                    }, function (err) {
                        if (errored) {
                            throw retVal;
                        } else {
                            throw err;
                        }
                    });
                } else {
                    return when(cb());
                }
            });
        },

        /**
         * @private
         * Returns a boolean indicating whether or not to use a transaction.
         * @param {Object} [opts] set a transaction property to override the {@link patio.Model#useTransaction}.
         */
        useTransaction: function (opts) {
            opts = opts || {};
            return isBoolean(opts.transaction) ? opts.transaction === true : this.useTransactions === true;
        },

        _setDataset: function (ds) {
            this.__dataset = ds;
            if (ds.db) {
                this._setDb(ds.db);
            }
        },

        _setDb: function (db) {
            this.__db = db;
        },

        _setTableName: function (name) {
            this.__tableName = name;
        },

        _setColumns: function (cols) {
            var proto = this.prototype;
            if (this.__columns) {
                this.__columns.forEach(function (name) {
                    delete proto[name];
                });
            }
            this.__columns = cols;
            cols.forEach(function (name) {
                this._defineColumnSetter(name);
                this._defineColumnGetter(name);
            }, this);
        },

        _setPrimaryKey: function (pks) {
            this.__primaryKey = pks || [];
        },

        _setSchema: function (schema) {
            var columns = [];
            var pks = [];
            for (var i in schema) {
                var col = schema[i];
                var name = applyColumnTransformMethod(i, this.identifierOutputMethod);
                schema[name] = col;
                columns.push(name);
                col.primaryKey && pks.push(name);
            }
            this.__schema = schema;
            this._setPrimaryKey(pks);
            this._setColumns(columns);
        },

        _defineColumnSetter: function (name) {
            /*Adds a setter to an object*/
            this.prototype["__defineSetter__"](name, function (val) {
                this._setColumnValue(name, val);
            });
        },

        _defineColumnGetter: function (name) {
            this.prototype["__defineGetter__"](name, function () {
                return this._getColumnValue(name);
            });
        },

        _getDataset: function () {
            var ds = this.__dataset, self = this;
            if (!ds) {
                ds = this.db.from(this.tableName);
                ds.rowCb = function (vals) {
                    return self.load(vals);
                };
                this.identifierInputMethod && (ds.identifierInputMethod = this.identifierInputMethod);
                this.identifierOutputMethod && (ds.identifierOutputMethod = this.identifierOutputMethod);
                this.__dataset = ds;
            } else if (!ds.rowCb) {
                ds.rowCb = function rowCb(vals) {
                    return self.load(vals);
                };
            }
            return ds;
        },

        _getQueryDataset: function () {
            return this._getDataset();
        },

        _getUpdateDataset: function () {
            return this._getDataset();
        },

        _getRemoveDataset: function () {
            return this._getDataset();
        },

        _getInsertDataset: function () {
            return this._getDataset();
        },


        /**
         * @ignore
         */
        getters: {
            /**@lends patio.Model*/

            /**
             * Set to true if this models column names should be use the "underscore" method when sending
             * keys to the database and to "camelize" method on columns returned from the database. If set to false see
             * {@link patio.Model#underscore}.
             * @field
             * @default false
             * @type {Boolean}
             */
            camelize: function (camelize) {
                return this.__camelize;

            },

            /**
             * Set to true if this models column names should be use the "camelize" method when sending
             * keys to the database and to "underscore" method on columns returned from the database. If set to false see
             * {@link patio.Model#underscore}.
             * @field
             * @default false
             * @type {Boolean}
             */
            underscore: function (underscore) {
                return this.__underscore;

            },

            /**@lends patio.Model*/

            /**
             * The name of the table all instances of the this {@link patio.Model} use.
             * @field
             * @ignoreCode
             * @type String
             */
            tableName: function () {
                return this.__tableName;
            },

            /**
             * The database all instances of this {@link patio.Model} use.
             * @field
             *  @ignoreCode
             * @type patio.Database
             */
            db: function () {
                var db = this.__db;
                if (!db) {
                    db = this.__db = patio.defaultDatabase;
                }
                if (!db) {
                    throw new ModelError("patio has not been connected to a database");
                }
                return db;
            },

            /**
             * A dataset to use to retrieve instances of this {@link patio.Model{ from the database. The dataset
             * has the {@link patio.Dataset#rowCb} set to create instances of this model.
             * @field
             *  @ignoreCode
             * @type patio.Dataset
             */
            dataset: function () {
                return this._getDataset();
            },

            removeDataset: function () {
                return this._getRemoveDataset();
            },

            queryDataset: function () {
                return this._getQueryDataset();
            },

            updateDataset: function () {
                return this._getUpdateDataset();
            },

            insertDataset: function () {
                return this._getInsertDataset();
            },

            /**
             * A list of columns this models table contains.
             * @field
             *  @ignoreCode
             * @type String[]

             */
            columns: function () {
                return this.__columns;
            },

            /**
             * The schema of this {@link patio.Model}'s table. See {@link patio.Database#schema} for details
             * on the schema object.
             * @field
             *  @ignoreCode
             * @type Object
             */
            schema: function () {
                if (this.synced) {
                    return this.__schema;
                } else {
                    throw new ModelError("Model has not been synced yet");
                }
            },

            /**
             * The primaryKey column/s of this {@link patio.Model}
             * @field
             *  @ignoreCode
             */
            primaryKey: function () {
                if (this.synced) {
                    return this.__primaryKey.slice(0);
                } else {
                    throw new ModelError("Model has not been synced yet");
                }
            },

            /**
             * A reference to the global {@link patio}.
             * @field
             *  @ignoreCode
             */
            patio: function () {
                return patio || require("./index");
            }
        },

        /**@ignore*/
        setters: {
            /**@lends patio.Model*/
            /**@ignore*/
            camelize: function (camelize) {
                camelize = camelize === true;
                if (camelize) {
                    this.identifierOutputMethod = "camelize";
                    this.identifierInputMethod = "underscore";
                }
                this.__camelize = camelize;
                this.__underscore = !camelize;

            },
            /**@ignore*/
            underscore: function (underscore) {
                underscore = underscore === true;
                if (underscore) {
                    this.identifierOutputMethod = "underscore";
                    this.identifierInputMethod = "camelize";
                }
                this.__underscore = underscore;
                this.__camelize = !underscore;

            }
        }
    }

}).as(exports, "Model");


function checkAndAddDBToTable(db, table) {
    if (!table.contains(db)) {
        table.set(db, new HashTable());
    }
}

var allModels = [];
/**@ignore*/
exports.create = function (name, supers, modelOptions) {
    if (!patio) {
        (patio = require("./index"));
        patio.on("disconnect", function () {
            allModels.length = 0;
            MODELS.clear();
        });
    }
    var db, ds, tableName, modelKey;
    if (isString(name)) {
        tableName = name;
        db = patio.defaultDatabase || "default";
    } else if (isInstanceOf(name, patio.Dataset)) {
        ds = name;
        tableName = ds.firstSourceAlias.toString();
        db = ds.db;
    }
    var hasSuper = false;
    if (isHash(supers) || isUndefinedOrNull(supers)) {
        modelOptions = supers;
        supers = [Model];
    } else {
        supers = toArray(supers);
        supers = supers.map(function (sup) {
            return exports.getModel(sup, db);
        });
        hasSuper = true;
    }

    var model;
    checkAndAddDBToTable(db, MODELS);

    var DEFAULT_PROTO = {instance: {}, "static": {}};
    modelOptions = merge(DEFAULT_PROTO, modelOptions || {});
    modelOptions.instance._hooks = ["save", "update", "remove", "load"];
    modelOptions.instance.__hooks = {pre: {}, post: {}};
    //Mixin the column setter/getters
    modelOptions["static"].synced = false;
    modelOptions["static"].__tableName = tableName;
    modelOptions["static"].__db = (db === "default" ? null : db);
    modelOptions["static"].__supers = hasSuper ? supers : [];
    modelOptions["static"].__dataset = ds;
    model = define(supers.concat(modelOptions.plugins || []).concat([AssociationPlugin]), modelOptions);
    ["pre", "post"].forEach(function (op) {
        var optionsOp = modelOptions[op];
        if (optionsOp) {
            for (var i in optionsOp) {
                model[op](i, optionsOp[i]);
            }
        }
    });
    allModels.push(model);
    if (!(MODELS.get(db).contains(tableName))) {
        MODELS.get(db).set(tableName, model);
    }
    return model;
};

exports.syncModels = function (cb) {
    // is this syncing correctly?
    return asyncArray(allModels).forEach(function (model) {
        return model.sync();
    }, 1).classic(cb).promise();
};

var checkAndGetModel = function (db, name) {
    var ret;
    if (MODELS.contains(db)) {
        ret = MODELS.get(db).get(name);
    }
    return ret;
};


exports.getModel = function (name, db) {
    var ret = null;
    if (isDefined(name)) {
        !patio && (patio = require("./index"));
        if (isFunction(name)) {
            ret = name;
        } else {
            if (!db && isInstanceOf(name, patio.Dataset)) {
                db = name.db;
                name = name.firstSourceAlias.toString();
            }
            var defaultDb = patio.defaultDatabase;
            if (db) {
                ret = checkAndGetModel(db, name);
                if (!ret && db === defaultDb) {
                    ret = checkAndGetModel("default", name);
                }
            } else {
                db = patio.defaultDatabase;
                ret = checkAndGetModel(db, name);
                if (!ret) {
                    ret = checkAndGetModel("default", name);
                }
            }
        }
    } else {
        ret = name;
    }
    if (isUndefinedOrNull(ret)) {
        throw new ModelError("Model " + name + " has not been registered with patio");
    }
    return ret;
};
