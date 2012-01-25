var comb = require("comb"),
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
 * @augments OneToMany
 *
 * @param {String} options.joinTable the joinTable of the association.
 *
 *
 * @property {String} joinTable the join table used in the relation.
 * */
module.exports = exports = comb.define(OneToMany, {
    instance:{

        type:"manyToMany",

        _fetchMethod:"all",

        supportsStringKey:false,

        supportsCompositeKey:false,


        _filter:function (parent) {
            var keys = this._getAssociationKey(parent);
            return this._setDatasetOptions(this.model.dataset.naked().innerJoin(this.joinTableName, comb.array.zip(keys[1], this.modelPrimaryKey).concat(comb.array.zip(keys[0], this.parentPrimaryKey.map(function (k) {
                return parent[k]
            })))));
        },

        _setAssociationKeys:function (parent, model, val) {
            var keys = this._getAssociationKey(parent),
                leftKey = keys[0],
                parentPk = this.parentPrimaryKey;
            if (!(leftKey && leftKey.length == parentPk.length)) throw new AssociationError("Invalid leftKey for " + this.name + " : " + leftKey);
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
            if (!(leftKey && leftKey.length == parentPk.length)) throw new AssociationError("Invalid leftKey for " + this.name + " : " + leftKey);
            if (!(rightKey && rightKey.length == modelPk.length)) throw new AssociationError("Invalid rightKey for " + this.name + " : " + rightKey);
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
                    ret = comb.executeInOrder(item, model, this, this.joinTable, this.parent, function (item, model, self, joinTable, parent) {
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
                        ret = comb.executeInOrder(item, this.joinTable, model, this, this.parent, function (item, joinTable, model, self, parent) {
                            if (remove) {
                                item.remove();
                            } else {
                                joinTable.where(self.__createJoinTableInsertRemoveQuery(model, item))["remove"]();
                            }
                            if (loaded && reload) {
                                return parent._reloadAssociationsForType(self.type, self.model, model);
                            }
                            return model;
                        });
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


        getters:{

            select:function () {
                if (!this.__select) {
                    this.__select = this.__opts.select || new this.patio.SQL.ColumnAll(this.model.tableName);
                }
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

