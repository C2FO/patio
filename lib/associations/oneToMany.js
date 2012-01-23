var comb = require("comb"),
    hitch = comb.hitch,
    Promise = comb.Promise,
    PromiseList = comb.PromiseList,
    _Association = require("./_Association");

/**
 * @class Class to define a one to many association.
 *
 * </br>
 * <b>NOT to be instantiated directly</b>
 * Its just documented for reference.
 *
 * @name OneToMany
 * @augments _Association
 *
 * */
module.exports = exports = comb.define(_Association, {
    instance:{

        type:"oneToMany",

        createSetter:true,

        /**@lends OneToMany.prototype*/

        _postSave:function (next, model) {
            var loaded = this.associationLoaded(model), vals;
            if (loaded && (vals = this.getAssociation(model))) {
                if (comb.isArray(vals) && vals.length) {
                    this._clearAssociations(model);
                    var pl = this.addAssociations(vals, model);
                    if (this.isEager()) {
                        pl = pl.chain(hitch(this, "fetch", model), next)
                    }
                    pl.both(next);
                } else {
                    next();
                }
            } else if (this.isEager() && !loaded) {
                this.fetch(model).both(next);
            } else {
                next();
            }
        },


        _postUpdate:function (next, model) {
            var removeAssociationFlagName = this.removeAssociationFlagName;
            if (model[removeAssociationFlagName]) {
                var oldVals = this._getCachedOldVals(model);
                this._clearCachedOldVals(model);
                var pl = oldVals.length ? this.removeItems(oldVals, model, false) : new Promise().callback();
                pl.then(hitch(this, function () {
                    this.addAssociations(this.getAssociation(model), model).then(next, function (e) {
                        throw e;
                    });
                }), function (e) {
                    throw e;
                });
                model[removeAssociationFlagName] = false;
            } else {
                next();
            }
        },
        _postLoad:function (next, model) {
            if (this.isEager() && !this.associationLoaded(model)) {
                this.fetch(model).then(next, function (e) {
                    throw e;
                });
            } else {
                next();
            }
        },

        _getCachedOldVals:function (model) {
            return model[this.oldAssocationCacheName] || [];
        },

        _clearCachedOldVals:function (model) {
            model[this.oldAssocationCacheName] = [];
        },

        _cacheOldVals:function (model) {
            var oldVals = model[this.oldAssocationCacheName] || [];
            oldVals = oldVals.concat(this.getAssociation(model));
            model[this.oldAssocationCacheName] = oldVals;
        },

        _setter:function (vals, model) {
            if (!comb.isUndefined(vals)) {
                if (model.isNew) {
                    if (!comb.isNull(vals)) {
                        this.addAssociations(vals, model);
                        //this.__setValue(model, vals);
                    } else {
                        this.__setValue(model, []);
                    }
                } else {
                    model.__isChanged = true;
                    model[this.removeAssociationFlagName] = true;
                    this._cacheOldVals(model);
                    if (!comb.isNull(vals)) {
                        //ensure its an array!
                        vals = (comb.isArray(vals) ? vals : [vals]).map(function (m) {
                            return this._toModel(m);
                        }, this);
                    } else {
                        vals = [];
                    }
                    this.__setValue(model, vals);
                }
            }

        },

        addAssociation:function (item, model, reload) {
            reload = comb.isBoolean(reload) ? reload : false;
            var ret = new comb.Promise().callback(model);
            if (!comb.isUndefinedOrNull(item)) {
                if (!model.isNew) {
                    item = this._toModel(item);
                    var loaded = this.associationLoaded(model);
                    this._setAssociationKeys(model, item);
                    var recip = this.model._findAssociation(this);
                    ret = comb.executeInOrder(item, model, this, this.parent, function (item, model, self, parent) {
                        item.save();
                        if (recip) {
                            recip[1].__setValue(item, model);
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

        addAssociations:function (items, model) {
            var ret = new comb.Promise();
            var pl = new PromiseList((comb.isArray(items) ? items : [items]).map(function (item) {
                return this.addAssociation(item, model, false);
            }, this));
            pl.then(hitch(this, function () {
                if (!model.isNew && this.associationLoaded(model)) {
                    this.parent._reloadAssociationsForType(this.type, this.model, model).then(hitch(ret, "callback", model), hitch(ret, "errback"));
                } else {
                    ret.callback(model);
                }
            }), hitch(ret, "errback"));
            return ret;
        },

        removeItem:function (item, model, remove, reload) {
            reload = comb.isBoolean(reload) ? reload : false;
            remove = comb.isBoolean(remove) ? remove : false;
            var ret = new comb.Promise().callback(model);
            if (!comb.isUndefinedOrNull(item)) {
                if (!model.isNew) {
                    if (comb.isInstanceOf(item, this.model) && !item.isNew) {
                        !remove && this._setAssociationKeys(model, item, null);
                        var loaded = this.associationLoaded(model);
                        var ret = comb.executeInOrder(item, model, this, this.parent, function (item, model, self, parent) {
                            item[remove ? "remove" : "save"]();
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

        removeItems:function (items, model, remove) {
            //todo make this more efficient!!!!
            var ret = new comb.Promise();
            var pl = new PromiseList((comb.isArray(items) ? items : [items]).map(function (item) {
                return this.removeItem(item, model, remove, false);
            }, this));
            pl.then(hitch(this, function () {
                if (this.associationLoaded(model)) {
                    this.parent._reloadAssociationsForType(this.type, this.model, model).then(hitch(ret, "callback", model), hitch(ret, "errback"));
                } else {
                    ret.callback(model);
                }
            }), hitch(ret, "errback"));
            return ret;
        },

        removeAllItems:function (model, remove) {
            remove = comb.isBoolean(remove) ? remove : false;
            var ret = new comb.Promise();
            if (!model.isNew) {
                var q = {};
                this._setAssociationKeys(model, q, null);
                var loaded = this.associationLoaded(model);
                var ret = comb.executeInOrder(model[this.associatedDatasetName], model, this, this.parent, function (ds, model, self, parent) {
                    remove ? ds.filter(q).remove() : ds.update(q);
                    if (loaded) {
                        return parent._reloadAssociationsForType(self.type, self.model, model);
                    }
                    return model;
                });

            } else {
                //todo we may want to check if any of the items were previously saved items;
                this._clearAssociations(model);
                ret.callback(model);
            }
            return ret;
        },

        /**
         *SEE {@link _Association#inject}.
         * </br>
         *Adds the following methods to each model.
         * <ul>
         *  <li>add<ModelName> - add an association</li>
         *  <li>add<ModelsName>s - add multiple associations</li>
         *  <li>remove<ModelName> - remove an association</li>
         *  <li>splice<ModelName>s - splice a number of associations</li>
         *  </ul>
         **/
        inject:function (parent, name) {
            this._super(arguments);
            var singular = comb.singularize(name);
            this._model == name && (this._model = singular);
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
                    return comb.isArray(item) ? self.addAssociations(item, this) : self.addAssociation(item, this, true);
                };

                parent.prototype[addNames] = function (items) {
                    return comb.isArray(items) ? self.addAssociations(items, this) : self.addAssociation(items, this);
                };

                parent.prototype[removeName] = function (item, remove) {
                    return comb.isArray(item) ? self.removeItems(item, this, remove) : self.removeItem(item, this, remove, true);
                };

                parent.prototype[removeNames] = function (item, remove) {
                    return comb.isArray(item) ? self.removeItems(item, this, remove) : self.removeItem(item, this, remove);
                };

                parent.prototype[removeAllName] = function (item, remove) {
                    return self.removeAllItems(this, remove);
                };

            }
        },

        getters:{
            oldAssocationCacheName:function () {
                return "_" + this.name + "OldValues";
            }
        }
    }
});