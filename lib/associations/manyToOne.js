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
 * @augments _Association
 *
 * */
module.exports = exports = comb.define(_Association, {
    instance : {
        _fetchMethod : "one",

        type : "manyToOne",

        isOwner : false,


        //override
        //@see _Association
        _postLoad : function(next, model) {
            if (this.isEager() && !this.associationLoaded(model)) {
                this.fetch(model).then(next, function(e) {
                    throw e;
                });
            } else {
                next();
            }
        },

        //override
        //@see _Association
        _setter : function(val, model) {
            if (!comb.isUndefinedOrNull(val)) {
                val = this._toModel(val);
                if (!model.isNew) {
                    this._setAssociationKeys(val, model);
                }
                this.__setValue(model, val);
            } else if (!model.isNew && comb.isNull(val)) {
                var keys = this._getAssociationKey(model)[0].forEach(function(k) {
                    model[k] = null;
                });
            }
        }
    }
});