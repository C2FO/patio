var associations = require("../associations"),
    oneToMany = associations.oneToMany,
    manyToOne = associations.manyToOne,
    oneToOne = associations.oneToOne,
    manyToMany = associations.manyToMany,
    fetch = associations.fetch,
    comb = require("comb"),
    asyncArray = comb.async.array,
    Promise = comb.Promise,
    PromiseList = comb.PromiseList;

var RECIPROCAL_ASSOC = {
    "oneToOne": ["manyToOne"],
    "manyToOne": ["oneToMany", "oneToOne"],
    "oneToMany": ["manyToOne"],
    "manyToMany": ["manyToMany"]
};


exports.AssociationPlugin = comb.define(null, {

    instance: {
        /**@lends patio.Model.prototype*/

        /**
         * @ignore
         * <p>Plugin to expose association capability.</p>
         *
         * The associations exposed include
         *
         * <ul>
         *     <li><b>oneToMany</b> - Foreign key in associated model's table points to this
         *         model's primary key.   Each current model object can be associated with
         *         more than one associated model objects.  Each associated model object
         *         can be associated with only one current model object.</li>
         *     <li><b>manyToOne</b> - Foreign key in current model's table points to
         *         associated model's primary key.  Each associated model object can
         *         be associated with more than one current model objects.  Each current
         *         model object can be associated with only one associated model object.</li>
         *     <li><b>oneToOne</b> - Similar to one_to_many in terms of foreign keys, but
         *         only one object is associated to the current object through the
         *         association.  The methods created are similar to many_to_one, except
         *         that the one_to_one setter method saves the passed object./li>
         *     <li><b>manyToMany</b> - A join table is used that has a foreign key that points
         *         to this model's primary key and a foreign key that points to the
         *         associated model's primary key.  Each current model object can be
         *         associated with many associated model objects, and each associated
         *         model object can be associated with many current model objects./li>
         * </ul>
         *
         * @constructs
         *
         */
        constructor: function () {
            if (comb.isUndefinedOrNull(this.__associations)) {
                this.__associations = {};
            }
            this._super(arguments);
        },

        reload: function () {
            this.__associations = {};
            return this._super(arguments);
        },

        /**@ignore*/
        getters: {
            /**@lends patio.Model.prototype*/

            /**
             * List of associations on the {@link patio.Model}
             * @field
             * @ignoreCode
             */
            associations: function () {
                return this._static.associations;
            },

            /**
             * Returns true if this {@link patio.Model} has associations.
             * @field
             * @ignoreCode
             */
            hasAssociations: function () {
                return this._static.hasAssociations;
            }
        }
    },

    static: {
        /**@lends patio.Model*/

        __associations: null,

        /**
         * Set to false to prevent an event from being emitted when an association is added to the model
         * @default true
         */
        emitOnAssociationAdd: true,

        /**
         * @borrows _Association.fetch as fetch
         */
        fetchType: fetch,

        /**
         * String for to signify an association as one to one.
         * @const
         */
        ONE_TO_ONE: "oneToOne",
        /**
         * String for to signify an association as one to many.
         * @const
         */
        ONE_TO_MANY: "oneToMany",
        /**
         * String for to signify an association as many to one.
         * @const
         */
        MANY_TO_ONE: "manyToOne",
        /**
         * String for to signify an association as many to many.
         * @const
         */
        MANY_TO_MANY: "manyToMany",

        /**
         *Creates a ONE_TO_MANY association.
         * See {@link patio.plugins.AssociationPlugin.associate} for options.
         *
         * @example
         *
         *
         *  //define the BiologicalFather model
         *  patio.addModel("biologicalFather", {
         *      static:{
         *          init:function () {
         *              this._super("arguments");
         *              this.oneToMany("children");
         *          }
         *      }
         * });
         *
         *
         * //define Child  model
         * patio.addModel("child", {
         *      static:{
         *          init:function () {
         *              this._super("arguments");
         *              this.manyToOne("biologicalFather");
         *          }
         *     }
         * });
         *
         */
        oneToMany: function (name, options, filter) {
            return this.associate(this.ONE_TO_MANY, name, options, filter);
        },

        /*  Allows eager loading of an association. This does an extra SQL query for the association.
         *  It will load any association singular or plural.
         *
         *  @example
         *
         *  Person.eager('company').one()
         *  { id: 1,
         *    name: 'Obi-Wan',
         *    company: {
         *      id: 1,
         *      name: 'Jedi council'
         *    }
         *  }
         *
         *  Person.eager(['emails', 'phones', 'company']).limit(2).all()
         *  [{ id: 1,
         *    name: 'Obi-Wan',
         *    emails: ['obi@gmail.com', 'obi@jedi.com'],
         *    phones: ['911', '888-991-0991'],
         *    company: {
         *      id: 1,
         *      name: 'Jedi council'
         *    }
         *  },
         *  { id: 2,
         *    name: 'Luke',
         *    emails: ['luke@gmail.com', 'luke@jedi.com'],
         *    phones: ['911', '888-991-0992'],
         *    company: {
         *      id: 1,
         *      name: 'Jedi council'
         *    }
         *  }]
         *
         */
        eager: function(associations) {
            var model = new this(),
                includes = [],
                associationsObj = {};

            if (Array.isArray(associations)) {
                includes = includes.concat(associations);
            } else if(associations) {
                includes.push(associations);
            }

            includes.forEach(function(association) {
                associationsObj[association] = function(parent) {
                    if (!parent[association]) {
                        throw new Error("Association of " + association + " not found");
                    }
                    return parent[association];
                };
            });

            return model.dataset.eager(associationsObj);
        },

        /**
         * Creates a MANY_TO_ONE association.
         * See {@link patio.plugins.AssociationPlugin.oneToMany}.
         * See {@link patio.plugins.AssociationPlugin.associate}
         */
        manyToOne: function (name, options, filter) {
            return this.associate(this.MANY_TO_ONE, name, options, filter);
        },

        /**
         * Creates a ONE_TO_ONE relationship between models.
         * See {@link patio.plugins.AssociationPlugin.associate} for options.
         *
         * @example
         *
         * patio.addModel("state", {
         *      static:{
         *          init:function () {
         *              this._super("arguments");
         *              this.oneToOne("capital");
         *          }
         *     }
         * });
         *
         * patio.addModel("capital", {
         *      static:{
         *        init:function () {
         *          this._super("arguments");
         *          this.manyToOne("state");
         *        }
         *      }
         * });
         */
        oneToOne: function (name, options, filter) {
            return this.associate(this.ONE_TO_ONE, name, options, filter);
        },

        /**
         * Creates a MANY_TO_MANY relationship between models.
         * See {@link patio.plugins.AssociationPlugin.associate} for options.
         *
         * @example
         *
         *  patio.addModel("class", {
         *      static:{
         *          init:function(){
         *              this._super("arguments");
         *              this.manyToMany("students", {fetchType:this.fetchType.EAGER, order : [sql.firstName.desc(), sql.lastName.desc()]});
         *          }
         *      }
         * });
         * patio.addModel("student", {
         *      instance:{
         *          enroll:function(clas){
         *              if (comb.isArray(clas)) {
         *                  return this.addClasses(clas);
         *              } else {
         *                  return this.addClass(clas);
         *              }
         *          }
         *      },
         *      static:{
         *          init:function(){
         *              this._super("arguments");
         *              this.manyToMany("classes", {fetchType:this.fetchType.EAGER, order : sql.name.desc()});
         *          }
         *      }
         });
         *
         */
        manyToMany: function (name, options, filter) {
            return this.associate(this.MANY_TO_MANY, name, options, filter);
        },

        /**
         * Associates a related model with the current model. The following types are
         * supported:
         *
         * <ul>
         *     <li><b>oneToMany</b> - Foreign key in associated model's table points to this
         *         model's primary key.   Each current model object can be associated with
         *         more than one associated model objects.  Each associated model object
         *         can be associated with only one current model object.</li>
         *     <li><b>manyToOne</b> - Foreign key in current model's table points to
         *         associated model's primary key.  Each associated model object can
         *         be associated with more than one current model objects.  Each current
         *         model object can be associated with only one associated model object.</li>
         *     <li><b>oneToOne</b> - Similar to one_to_many in terms of foreign keys, but
         *         only one object is associated to the current object through the
         *         association.  The methods created are similar to many_to_one, except
         *         that the one_to_one setter method saves the passed object./li>
         *     <li><b>manyToMany</b> - A join table is used that has a foreign key that points
         *         to this model's primary key and a foreign key that points to the
         *         associated model's primary key.  Each current model object can be
         *         associated with many associated model objects, and each associated
         *         model object can be associated with many current model objects.</li>
         * </ul>
         *
         * @param {patio.Model.ONE_TO_ONE|patio.Model.ONE_TO_MANY|patio.Model.MANY_TO_ONE|patio.Model.MANY_TO_MANY} type the
         *          type of association that is to be created.
         * @param {String} name the name of the association, the name specified will be exposed as a property on instances
         * of the model.
         @param {Object} [options] additional options.
         * The following options can be supplied:
         * <ul>
         *   <li><b>model</b> - The associated class or its name. If not  given, uses the association's name,
         *    which is singularized unless the type is MANY_TO_ONE or ONE_TO_ONE</li>
         *   <li><b>query</b> - The conditions to use to filter the association, can be any argument passed
         *   to {@link patio.Dataset#filter}.</li>
         *   <li><b>dataset</b> - A function that is called in the scope of the model and called with the model as the
         *     first argument. The function must return a dataset that can be used as the base for all dataset
         *     operations.<b>NOTE:</b>The dataset returned will have all options applied to it.</li>
         *   <li><b>distinct</b> Use the DISTINCT clause when selecting associated objects.
         *   See {@link patio.Dataset#distinct}.</li>
         *   <li><b>limit</b> : Limit the number of records to the provided value.  Use
         *     an array with two elements for the value to specify a limit (first element) and an
         *     offset (second element). See {@link patio.Dataset#limit}.</li>
         *   <li><b>order</b> : the column/s order the association dataset by.  Can be a
         *     one or more columns.
         *     See {@link patio.Dataset#order}.</li>
         *   <li><b>readOnly</b> : Do not add a setter method (for MANY_TO_ONE or ONE_TO_ONE associations),
         *     or add/remove/removeAll methods (for ONE_TO_MANY and MANY_TO_MANY associations).</li>
         *   <li><b>select</b> : the columns to select.  Defaults to the associated class's
         *     tableName.* in a MANY_TO_MANY association, which means it doesn't include the attributes from the
         *     join table.  If you want to include the join table attributes, you can
         *     use this option, but beware that the join table attributes can clash with
         *     attributes from the model table, so you should alias any attributes that have
         *     the same name in both the join table and the associated table.</li>
         *   </ul>
         *   ManyToOne additional options:
         *   <ul>
         *      <li><b>key</b> : foreignKey in current model's table that references
         *          associated model's primary key.  Defaults to : "{tableName}Id".  Can use an
         *          array of strings for a composite key association.</li>
         *       <li><b>primaryKey</b> : column in the associated table that the <b>key</b> option references.
         *              Defaults to the primary key of the associated table. Can use an
         *              array of strings for a composite key association.</li>
         *   </ul>
         * OneToMany and OneToOne additional options:
         * <ul>
         *      <li><b>key</b> : foreign key in associated model's table that references
         *             current model's primary key, as a string.  Defaults to
         *             "{thisTableName}Id".  Can use an array of columns for a composite key association.</li>
         *      <li><b>primaryKey</b> : column in the current table that <b>key</b> option references.
         *             Defaults to primary key of the current table. Can use an array of strings for a
         *             composite key association.</li>
         * </ul>
         *
         * ManyToMany additional options:
         *   <ul>
         *      <li><b>joinTable</b> : name of table that includes the foreign keys to both
         *          the current model and the associated model.  Defaults to the name
         *          of current model and name of associated model, pluralized,
         *          sorted, and joined with '' and camelized.
         *      <li><b>leftKey</b> : foreign key in join table that points to current model's
         *          primary key. Defaults to :"{tableName}Id".
         *          Can use an array of strings for a composite key association.
         *      <li><b>leftPrimaryKey</b> - column in current table that <b>leftKey</b> points to.
         *          Defaults to primary key of current table.  Can use an array of strings for a
         *          composite key association.
         *      <li><b>rightKey</b> : foreign key in join table that points to associated
         *          model's primary key.  Defaults to Defaults to :"{associatedTableName}Id".
         *          Can use an array of strings for a composite key association.
         *      <li><b>rightPrimaryKey</b> : column in associated table that <b>rightKey</b> points to.
         *          Defaults to primary key of the associated table.  Can use an
         *          array of strings for a composite key association.
         *  </ul>
         * @param {Function} [filter] optional function to filter the dataset after all other options have been applied.
         *
         */
        associate: function (type, name, options, filter) {
            if (comb.isFunction(options)) {
                filter = options;
                options = {};
            }
            this.__associations = this.__associations || {manyToMany: {}, oneToMany: {}, manyToOne: {}, oneToOne: {}};
            var assoc = new associations[type](comb.merge({model: name}, options), this.patio, filter);
            assoc.inject(this, name);
            this.__associations[type][name] = assoc;
            this.emit("associate", type, this);
            return this;
        },

        sync: function (cb) {
            if (!this.synced && this.hasAssociations) {
                var self = this;
                return this._super().chain(function () {
                    var associations = self.__associations;
                    return asyncArray(Object.keys(associations)).map(function (type) {
                        var types = associations[type];
                        return asyncArray(Object.keys(types)).map(function (name) {
                            return types[name].model.sync();
                        }, 1);
                    }, 1);

                });
            } else {
                return this._super(arguments);
            }
        },

        __isReciprocalAssociation: function (assoc, pAssoc) {
            var keys = assoc._getAssociationKey(), leftKey = keys[0], rightKey = keys[1];
            var pKeys = pAssoc._getAssociationKey(), pLeftKey = pKeys[0], pRightKey = pKeys[1];
            return leftKey.every(function (k, i) {
                return pRightKey[i] === k;
            }) && rightKey.every(function (k, i) {
                return pLeftKey[i] === k;
            }) && assoc.parent === pAssoc.model;
        },

        _findAssociation: function (assoc) {
            var ret = null;
            if (!comb.isEmpty(this.__associations)) {
                var type = assoc.type, recipTypes = RECIPROCAL_ASSOC[type];
                for (var i in recipTypes) {
                    var recipType = recipTypes[i];
                    var potentialAssociations = this.__associations[recipType];
                    var found = false;
                    for (var j in potentialAssociations) {
                        var pAssoc = potentialAssociations[j];
                        if (this.__isReciprocalAssociation(assoc, pAssoc)) {
                            ret = [i, pAssoc], found = true;
                            break;
                        }
                    }
                    if (found) {
                        break;
                    }

                }
            }
            return ret;
        },

        _clearAssociationsForType: function (type, clazz, model) {
            this._findAssociationsForType(type, clazz).forEach(function (assoc) {
                assoc._clearAssociations(model);
            });
        },

        _reloadAssociationsForType: function (type, clazz, model) {
            var pl = this._findAssociationsForType(type, clazz).map(function (assoc) {
                return assoc._forceReloadAssociations(model);
            });
            return (pl.length ? new PromiseList(pl) : new Promise().callback()).promise();
        },

        _findAssociationsForType: function (type, clazz) {
            var associations = this.__associations[type], ret = [];
            for (var i in associations) {
                var assoc = associations[i];
                if (assoc.model === clazz) {
                    ret.push(assoc);
                }
            }
            return ret;

        },

        /**@ignore*/
        getters: {
            /**@lends patio.plugins.AssociationPlugin*/

            /**
             * A list of associated model names.
             * @field
             * @type String[]
             */
            associations: function () {
                var ret = [], assoc = this.__associations;
                if (assoc != null) {
                    Object.keys(assoc).forEach(function (k) {
                        ret = ret.concat(Object.keys(assoc[k]));
                    });
                }
                return ret;
            },

            /**
             * Returns true if this model has associations.
             * @field
             * @type Boolean
             */
            hasAssociations: function () {
                return this.associations.length > 0;
            }
        }
    }});