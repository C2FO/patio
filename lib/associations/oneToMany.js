var comb = require("comb-proxy"),
    isArray = comb.isArray,
    isUndefinedOrNull = comb.isUndefinedOrNull,
    isBoolean = comb.isBoolean,
    define = comb.define,
    Promise = comb.Promise,
    isNull = comb.isNull,
    when = comb.when,
    isInstanceOf = comb.isInstanceOf,
    PromiseList = comb.PromiseList,
    isUndefined = comb.isUndefined,
    singularize = comb.singularize,
    _Association = require("./_Association");

/**
 * @class Class to define a one to many association.
 *
 * </br>
 * <b>NOT to be instantiated directly</b>
 * Its just documented for reference.
 *
 * Adds the following methods to each model.
 * <ul>
 *  <li>add{ModelName} - add an association</li>
 *  <li>add{comb.pluralize(ModelName)} - add multiple associations</li>
 *  <li>remove{ModelName} - remove an association</li>
 *  <li>remove{comb.pluralize(ModelName)} - remove multiple association</li>
 *  <li>removeAll - removes all associations of this type</li>
 *  </ul>
 *
 * @name OneToMany
 * @augments patio.associations.Association
 * @memberOf patio.associations
 *
 * */
module.exports = define(_Association, {
    instance: {
        /**@lends patio.associations.OneToMany.prototype*/

        type: "oneToMany",

        createSetter: true,

        _postSave: function (next, model) {
            var loaded = this.associationLoaded(model), vals;
            if (loaded && (vals = this.getAssociation(model))) {
                if (isArray(vals) && vals.length) {
                    this._clearAssociations(model);
                    var pl = this.addAssociations(vals, model);
                    if (this.isEager()) {
                        var self = this;
                        pl.chain(function () {
                            return self.fetch(model);
                        }).classic(next);
                    } else {
                        pl.classic(next);
                    }
                } else {
                    next();
                }
            } else if (this.isEager() && !loaded) {
                this.fetch(model).classic(next);
            } else {
                next();
            }
        },


        _postUpdate: function (next, model) {
            var removeAssociationFlagName = this.removeAssociationFlagName;
            if (model[removeAssociationFlagName]) {
                var oldVals = this._getCachedOldVals(model);
                this._clearCachedOldVals(model);
                var pl = oldVals.length ? this.removeItems(oldVals, model, false) : null, self = this;
                when(pl).chain(function () {
                    return self.addAssociations(self.getAssociation(model), model);
                }).classic(next);
                model[removeAssociationFlagName] = false;
            } else {
                next();
            }
        },
        _postLoad: function (next, model) {
            if (this.isEager() && !this.associationLoaded(model)) {
                this.fetch(model).classic(next);
            } else {
                next();
            }
        },

        /**
         * Middleware called before a model is removed.
         * </br>
         * <b> This is called in the scope of the model</b>
         * @param {Function} next function to pass control up the middleware stack.
         * @param {_Association} self reference to the Association that is being acted up.
         */
        _preRemove: function (next, model) {
            this.removeAllItems(model).classic(next);
        },

        _getCachedOldVals: function (model) {
            return model[this.oldAssocationCacheName] || [];
        },

        _clearCachedOldVals: function (model) {
            model[this.oldAssocationCacheName] = [];
        },

        _cacheOldVals: function (model) {
            var oldVals = model[this.oldAssocationCacheName] || [];
            oldVals = oldVals.concat(this.getAssociation(model));
            model[this.oldAssocationCacheName] = oldVals;
        },

        _setter: function (vals, model) {
            if (!isUndefined(vals)) {
                if (model.isNew) {
                    if (!isNull(vals)) {
                        this.addAssociations(vals, model);
                        //this.__setValue(model, vals);
                    } else {
                        this.__setValue(model, []);
                    }
                } else {
                    model.__isChanged = true;
                    model[this.removeAssociationFlagName] = true;
                    this._cacheOldVals(model);
                    if (!isNull(vals)) {
                        //ensure its an array!
                        vals = (isArray(vals) ? vals : [vals]).map(function (m) {
                            return this._toModel(m);
                        }, this);
                    } else {
                        vals = [];
                    }
                    this.__setValue(model, vals);
                }
            }

        },

        addAssociation: function (item, model, reload) {
            reload = isBoolean(reload) ? reload : false;
            var ret;
            if (!isUndefinedOrNull(item)) {
                if (!model.isNew) {
                    item = this._toModel(item);
                    var loaded = this.associationLoaded(model), self = this;
                    this._setAssociationKeys(model, item);
                    var recip = this.model._findAssociation(this);
                    if (recip) {
                        recip[1].__setValue(item, model);
                    }
                    ret = model._checkTransaction(function () {
                        return item.save()
                            .chain(function () {
                                if (loaded && reload) {
                                    return self.parent._reloadAssociationsForType(self.type, self.model, model);
                                }
                            }).chain(function () {
                                return model;
                            });
                    });
                } else {
                    ret = new Promise().callback(model);
                    item = this._toModel(item);
                    var items = this.getAssociation(model);
                    if (isUndefinedOrNull(items)) {
                        this.__setValue(model, [item]);
                    } else {
                        items.push(item);
                    }
                }
            } else {
                new Promise().callback(model);
            }
            return ret.promise();
        },

        addAssociations: function (items, model) {
            var ret, self = this;
            items = (isArray(items) ? items : [items]);
            if (model.isNew) {
                items.map(function (item) {
                    return self.addAssociation(item, model, false);
                }, this);
                ret = when(model);
            } else {
                ret = model
                    ._checkTransaction(function () {
                        return new PromiseList(items.map(function (item) {
                            return self.addAssociation(item, model, false);
                        }));
                    })
                    .chain(function () {
                        if (!model.isNew && self.associationLoaded(model)) {
                            return self.parent._reloadAssociationsForType(self.type, self.model, model);
                        }
                    }).chain(function () {
                        return model;
                    });
            }
            return ret.promise();
        },

        removeItem: function (item, model, remove, reload) {
            reload = isBoolean(reload) ? reload : false;
            remove = isBoolean(remove) ? remove : false;
            var ret;
            if (!isUndefinedOrNull(item)) {
                if (!model.isNew) {
                    if (isInstanceOf(item, this.model) && !item.isNew) {
                        if (!remove) {
                            this._setAssociationKeys(model, item, null);
                        }
                        var loaded = this.associationLoaded(model), self = this;
                        ret = model._checkTransaction(function () {
                            return item[remove ? "remove" : "save"]().chain(function () {
                                if (loaded && reload) {
                                    return self.parent._reloadAssociationsForType(self.type, self.model, model);
                                }
                            });
                        })
                            .chain(function () {
                                return model;
                            });
                    } else {
                        ret = when(model);
                    }
                } else {
                    item = this._toModel(item);
                    var items = this.getAssociation(model), index;
                    if (!isUndefinedOrNull(items) && (index = items.indexOf(item)) !== -1) {
                        items.splice(index, 1);
                    }
                    ret = when(model);
                }
            } else {
                ret = when(model);
            }
            return ret.promise();
        },

        removeItems: function (items, model, remove) {
            //todo make this more efficient!!!!
            var ret, self = this;
            items = isArray(items) ? items : [items];
            if (model.isNew) {
                items.map(function (item) {
                    return self.removeItem(item, model, remove, false);
                });
                ret = when(model);
            } else {
                ret = model._checkTransaction(
                    function () {
                        return new PromiseList(items.map(function (item) {
                                return self.removeItem(item, model, remove, false);
                            })
                        );
                    })
                    .chain(function () {
                        if (self.associationLoaded(model)) {
                            return self.parent._reloadAssociationsForType(self.type, self.model, model);
                        }
                    })
                    .chain(function () {
                        return model;
                    });
            }
            return ret.promise();
        },

        removeAllItems: function (model, remove) {
            remove = isBoolean(remove) ? remove : false;
            var ret;
            if (!model.isNew) {
                var q = {}, removeQ = {};
                this._setAssociationKeys(model, q);
                this._setAssociationKeys(model, removeQ, null);
                var loaded = this.associationLoaded(model), self = this;
                return model._checkTransaction(
                    function () {
                        return self._filter(model)
                            .forEach(function (m) {
                                return remove ? m.remove() : m.update(removeQ);
                            })
                            .chain(function () {
                                if (loaded) {
                                    return self.parent._reloadAssociationsForType(self.type, self.model, model);
                                }
                            })
                            .chain(function () {
                                return model;
                            });
                    });
            } else {
                //todo we may want to check if any of the items were previously saved items;
                this._clearAssociations(model);
                ret = when(model);
            }
            return ret.promise();
        },


        inject: function (parent, name) {
            this._super(arguments);
            var singular = singularize(name);
            if (this._model === name) {
                this._model = singular;
            }
            singular = singular.charAt(0).toUpperCase() + singular.slice(1);
            if (!this.readOnly) {
                this.removedKey = "__removed" + name + "";
                this.addedKey = "__added_" + name + "";
                parent.prototype[this.removedKey] = [];
                parent.prototype[this.addedKey] = [];
                var self = this;

                name = name.charAt(0).toUpperCase() + name.slice(1);
                var addName = "add" + singular;
                var addNames = "add" + name;
                var removeName = "remove" + singular;
                var removeNames = "remove" + name;
                var removeAllName = "removeAll" + name;
                parent.prototype[addName] = function (item) {
                    return isArray(item) ? self.addAssociations(item, this) : self.addAssociation(item, this, true);
                };

                parent.prototype[addNames] = function (items) {
                    return isArray(items) ? self.addAssociations(items, this) : self.addAssociation(items, this);
                };

                parent.prototype[removeName] = function (item, remove) {
                    return isArray(item) ? self.removeItems(item, this, remove) : self.removeItem(item, this, remove, true);
                };

                parent.prototype[removeNames] = function (item, remove) {
                    return isArray(item) ? self.removeItems(item, this, remove) : self.removeItem(item, this, remove);
                };

                parent.prototype[removeAllName] = function (remove) {
                    return self.removeAllItems(this, remove);
                };

            }
        },

        getters: {
            oldAssocationCacheName: function () {
                return "_" + this.name + "OldValues";
            },

            //Returns our model
            model: function () {
                var model;
                try {
                    model = this["__model__"] || (this["__model__"] = this.patio.getModel(this._model, this.parent.db));
                } catch (e) {
                    model = this["__model__"] = this.patio.getModel(this.name, this.parent.db);
                }
                return model;
            }
        }
    }
});