var comb = require("comb-proxy"),
    sql = require("../sql").sql,
    hitch = comb.hitch,
    Promise = comb.Promise,
    PromiseList = comb.PromiseList,
    OneToMany = require("./oneToMany"),
    AssociationError = require("../errors").AssociationError;

var LOGGER = comb.logging.Logger.getLogger("comb.associations.ManyToMany");
/**
 * @class Class to define a manyToMany association.
 *
 * </br>
 * <b>NOT to be instantiated directly</b>
 * Its just documented for reference.
 *
 * @name ManyToMany
 * @augments patio.associations.OneToMany
 * @memberOf patio.associations
 *
 * @param {String} options.joinTable the joinTable of the association.
 *
 *
 * @property {String} joinTable the join table used in the relation.
 * */
module.exports = exports = comb.define(OneToMany, {
    instance:{
        /**@lends patio.associations.ManyToMany.prototype*/
        type:"manyToMany",

        _fetchMethod:"all",

        supportsStringKey:false,

        supportsCompositeKey:false,


        _filter:function (parent) {
            var keys = this._getAssociationKey(parent);
            var options = this.__opts || {};
            var ds;
            if (!comb.isUndefined((ds = options.dataset)) && comb.isFunction(ds)) {
                ds = ds.apply(parent, [parent]);
            }
            if (!ds) {
                ds = this.model.dataset.naked().innerJoin(this.joinTableName, comb.array.zip(keys[1], this.modelPrimaryKey.map(function (k) {
                    return sql.stringToIdentifier(k);
                })).concat(comb.array.zip(keys[0], this.parentPrimaryKey.map(function (k) {
                    return parent[k]
                }))));
                var recip = this.model._findAssociation(this);
                recip && (recip = recip[1]);
                ds.rowCb = comb.hitch(this, function (item) {
                    var ret = new comb.Promise();
                    var model = this._toModel(item, true);
                    recip && recip.__setValue(model, parent);
                    //call hook to finish other model associations
                    model._hook("post", "load").then(hitch(this, function () {
                        ret.callback(model);
                    }), hitch(ret, "errback"));
                    return ret;
                });

            }

            return this._setDatasetOptions(ds);
        },

        _setAssociationKeys:function (parent, model, val) {
            var keys = this._getAssociationKey(parent),
                leftKey = keys[0],
                parentPk = this.parentPrimaryKey;
            if (!(leftKey && leftKey.length == parentPk.length)) {
                throw new AssociationError("Invalid leftKey for " + this.name + " : " + leftKey);
            }
            for (var i = 0; i < leftKey.length; i++) {
                model[leftKey[i]] = !comb.isUndefined(val) ? val : parent[parentPk[i]];
            }
        },


        __createJoinTableInsertRemoveQuery:function (model, item) {
            var q = {};
            var keys = this._getAssociationKey(model),
                leftKey = keys[0],
                rightKey = keys[1],
                parentPk = this.parentPrimaryKey,
                modelPk = this.modelPrimaryKey;
            if (!(leftKey && leftKey.length == parentPk.length)) {
                throw new AssociationError("Invalid leftKey for " + this.name + " : " + leftKey);
            }
            if (!(rightKey && rightKey.length == modelPk.length)) {
                throw new AssociationError("Invalid rightKey for " + this.name + " : " + rightKey);
            }
            for (var i = 0; i < leftKey.length; i++) {
                q[leftKey[i]] = model[parentPk[i]];
            }
            for (var i = 0; i < rightKey.length; i++) {
                q[rightKey[i]] = item[modelPk[i]];
            }
            return q;
        },

        _preRemove:function (next, model) {
            if (this.isOwner && !this.isCascading) {
                var q = {};
                this._setAssociationKeys(model, q);
                this.joinTable.where(q).remove().then(next, function (e) {
                    throw e;
                });
            } else {
                next();
            }
        },


        addAssociation:function (item, model, reload) {
            reload = comb.isBoolean(reload) ? reload : false;
            var ret = new Promise().callback(model);
            if (!comb.isUndefinedOrNull(item)) {
                if (!model.isNew) {
                    item = this._toModel(item);
                    var loaded = this.associationLoaded(model);
                    var recip = this.model._findAssociation(this), save = item.isNew;
                    ret = model._checkTransaction(comb.hitch(this, function () {
                        return comb.executeInOrder(item, model, this, this.joinTable, this.parent, function (item, model, self, joinTable, parent) {
                            save && item.save();
                            joinTable.insert(self.__createJoinTableInsertRemoveQuery(model, item));
                            if (recip) {
                                recip[1].__setValue(item, [model]);
                            }
                            if (loaded && reload) {
                                return parent._reloadAssociationsForType(self.type, self.model, model);
                            } else {
                                return model;
                            }
                        });
                    }));
                } else {
                    item = this._toModel(item);
                    var items = this.getAssociation(model);
                    if (comb.isUndefinedOrNull(items)) {
                        this.__setValue(model, [item]);
                    } else {
                        items.push(item);
                    }
                }
            }
            return ret;
        },

        removeItem:function (item, model, remove, reload) {
            reload = comb.isBoolean(reload) ? reload : false;
            remove = comb.isBoolean(remove) ? remove : false;
            var ret = new Promise().callback(model);
            if (!comb.isUndefinedOrNull(item)) {
                if (!model.isNew) {
                    if (comb.isInstanceOf(item, this.model) && !item.isNew) {
                        var loaded = this.associationLoaded(model), remove = remove && !item.isNew;
                        var ret = new comb.Promise();
                        model._checkTransaction(comb.hitch(this, function () {
                            var p = remove ? item.remove() :  this.joinTable.where(this.__createJoinTableInsertRemoveQuery(model, item)).remove();
                        })).then(comb.hitch(this, function () {
                            if (loaded && reload) {
                                return this.parent._reloadAssociationsForType(this.type, this.model, model).then(comb.hitch(ret, "callback"), comb.hitch(ret, "errback"));
                            } else {
                                ret.callback(model);
                            }
                        }), ret);
                        return ret;
                    }
                } else {
                    item = this._toModel(item);
                    var items = this.getAssociation(model), index;
                    if (!comb.isUndefinedOrNull(items) && (index = items.indexOf(item)) != -1) {
                        items.splice(index, 1);
                    }
                }
            }
            return ret;
        },

        removeAllItems:function (model, remove) {
            remove = comb.isBoolean(remove) ? remove : false;
            var ret = new comb.Promise();
            if (!model.isNew) {
                var q = {}, removeQ = {};
                this._setAssociationKeys(model, q);
                this._setAssociationKeys(model, removeQ, null);
                var loaded = this.associationLoaded(model);
                return model._checkTransaction(comb.hitch(this, function () {
                    var ds = this.model.dataset, ret = new comb.Promise();
                    comb.when(remove ? this._filter(model).forEach(function(m){
                        return m.remove();
                    }) : this.joinTable.filter(q).update(removeQ)).then(function () {
                        if (loaded) {
                            this.parent._reloadAssociationsForType(this.type, this.model, model)
                                .then(comb.hitchIgnore(ret, "callback", model), ret);
                        } else {
                            ret.callback(model);
                        }
                    }.bind(this), ret);
                    return ret;
                }));
            } else {
                //todo we may want to check if any of the items were previously saved items;
                this._clearAssociations(model);
                ret.callback(model);
            }
            return ret;
        },



        getters:{

            select:function () {
                return this.__select;
            },

            defaultLeftKey:function () {
                return this.__opts.leftKey || this.parent.tableName + "Id";
            },

            defaultRightKey:function () {
                return this.__opts.rightKey || this.model.tableName + "Id";
            },

            parentPrimaryKey:function () {
                return this.__opts.leftPrimaryKey || this.parent.primaryKey;
            },

            modelPrimaryKey:function () {
                return this.__opts.rightPrimaryKey || this.model.primaryKey;
            },

            joinTableName:function () {
                if (!this._joinTable) {
                    var options = this.__opts;
                    var joinTable = options.joinTable;
                    if (comb.isUndefined(joinTable)) {
                        var defaultJoinTable = this.defaultJoinTable;
                        if (comb.isUndefined(defaultJoinTable)) {
                            throw new Error("Unable to determine jointable for " + this.name)
                        } else {
                            this._joinTable = defaultJoinTable;
                        }
                    } else {
                        this._joinTable = joinTable;
                    }
                }
                return this._joinTable;
            },

            //returns our join table model
            joinTable:function () {
                if (!this.__joinTableDataset) {
                    var ds = this.__joinTableDataset = this.model.dataset.db.from(this.joinTableName), model = this.model, options = this.__opts;
                    var identifierInputMethod = comb.isUndefined(options.identifierInputMethod) ? model.identifierInputMethod : options.identifierInputMethod,
                        identifierOutputMethod = comb.isUndefined(options.identifierOutputMethod) ? model.identifierOutputMethod : options.identifierOutputMethod;
                    identifierInputMethod && (ds.identifierInputMethod = identifierInputMethod);
                    identifierOutputMethod && (ds.identifierOutputMethod = identifierOutputMethod);
                }
                return this.__joinTableDataset;
            },

            defaultJoinTable:function () {
                var ret;
                var recip = this.model._findAssociation(this);
                if (recip && recip.length) {
                    var names = [comb.pluralize(this._model), comb.pluralize(recip[1]._model)].sort();
                    names[1] = names[1].charAt(0).toUpperCase() + names[1].substr(1);
                    ret = names.join("");
                }
                return ret;
            }
        }
    }
});

