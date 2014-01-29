var comb = require("comb"),
    Promise = comb.Promise,
    isUndefinedOrNull = comb.isUndefinedOrNull,
    isNull = comb.isNull,
    PromiseList = comb.PromiseList,
    ManyToOne = require("./manyToOne"),
    define = comb.define;

/**
 * @class Class to define a manyToOne association.
 *
 * </br>
 * <b>NOT to be instantiated directly</b>
 * Its just documented for reference.
 *
 * @name ManyToOne
 * @augments patio.associations.ManyToOne
 * @memberOf patio.associations
 *
 **/
define(ManyToOne, {
    instance:{
        /**@lends patio.associations.OneToOne.prototype*/

        type:"oneToOne",

        __remove:false,

        //override
        //@see _Association
        _fetchMethod:"one",

        isOwner:true,

        _preSave:function (next, model) {
            //handle case where no association was initially set
            if (!this.associationLoaded(model) || !this.getAssociation(model)) {
                this.__setValue(model, null);
            }
            //
            next();
        },

        //override
        //@see _Association
        _postSave:function (next, model) {
            var loaded = this.associationLoaded(model), val;
            if (loaded && (val = this.getAssociation(model))) {
                this._setAssociationKeys(model, val);
                val.save().classic(next);
            } else {
                next();
            }
        },

        _preUpdate : function(next){
            next();
        },

        _postUpdate:function (next, model) {
            var removeAssociationFlagName = this.removeAssociationFlagName;
            if (model[removeAssociationFlagName]) {
                var q = {};
                this._setAssociationKeys(model, q, null);
                model[this.associatedDatasetName].update(q).classic(next);
                this.__setValue(model, null);
                model[removeAssociationFlagName] = false;
            } else {
                next();
            }
        },

        //override
        //@see _Association
        _setter:function (val, model) {
            var name = this.name;
            if (!isUndefinedOrNull(val)) {
                this.__setValue(model, this._toModel(val));
            } else if (!model.isNew && isNull(val)) {
                model.__isChanged = true;
                this.__setValue(model, this._toModel(val));
                model[this.removeAssociationFlagName] = true;
            }
        }
    }
}).as(module);