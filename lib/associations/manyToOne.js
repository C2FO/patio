var comb = require("comb"),
    hitch = comb.hitch,
    Promise = comb.Promise,
    PromiseList = comb.PromiseList,
    _Association = require("./_Association");
/**
 * @class Class to define a many to one association.
 *
 * </br>
 * <b>NOT to be instantiated directly</b>
 * Its just documented for reference.
 *
 * @name ManyToOne
 * @augments patio.associations.Association
 * @memberOf patio.associations
 *
 * */
module.exports = exports = comb.define(_Association, {
    instance:{
        /**@lends patio.associations.ManyToOne.prototype*/

        _fetchMethod:"one",

        type:"manyToOne",

        isOwner:false,

        _preSave:function (next, model) {
            var assoc;
            if (this.associationLoaded(model) && (assoc = this.getAssociation(model)) != null && assoc.isNew) {
                assoc.save().both(hitch(this, function (assoc) {
                    var recip = this.model._findAssociation(this);
                    if (recip) {
                        //set up our association
                        recip[1]._setAssociationKeys(assoc, model);
                    }
                    next();
                }));
            } else {
                this.__setValue(model, null);
                next();
            }
        },


        //override
        //@see _Association
        _postLoad:function (next, model) {
            if (this.isEager() && !this.associationLoaded(model)) {
                this.fetch(model).then(next, function (e) {
                    throw e;
                });
            } else {
                next();
            }
        },

        //override
        //@see _Association
        _setter:function (val, model) {
            if (!comb.isUndefinedOrNull(val)) {
                val = this._toModel(val);
                this.__setValue(model, val);
            } else if (!model.isNew && comb.isNull(val)) {
                var keys = this._getAssociationKey(model)[0].forEach(function (k) {
                    model[k] = null;
                });
            }
        }
    }
});