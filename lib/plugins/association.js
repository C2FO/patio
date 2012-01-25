var associations = require("../associations"),
    oneToMany = associations.oneToMany,
    manyToOne = associations.manyToOne,
    oneToOne = associations.oneToOne,
    manyToMany = associations.manyToMany,
    fetch = associations.fetch,
    comb = require("comb");

var RECIPROCAL_ASSOC = {
    "oneToOne":["manyToOne"],
    "manyToOne":["oneToMany", "oneToOne"],
    "oneToMany":["manyToOne"],
    "manyToMany":["manyToMany"]
};


exports.AssociationPlugin = comb.define(null, {

    instance:{
        /**@lends patio.plugins.AssociationPlugin.prototype*/

        /**
         *  <p>Plugin to expose association capability.</p>
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
        constructor:function () {
            this._super(arguments);
            if (comb.isUndefinedOrNull(this.__associations)) {
                this.__associations = {};
            }
        },

        /**@ignore*/
        getters:{
            /**@lends patio.plugins.AssociationPlugin.prototype*/

            /**
             * List of associations on the {@link patio.Model}
             */
            associations:function () {
                return this._static.associations;
            },

            /**
             * Returns true if this {@link patio.Model} has associations.
             */
            hasAssociations:function () {
                return this._static.hasAssociations;
            }
        }
    },

    static:{
        /**@lends patio.plugins.AssociationPlugin*/

        __associations:null,

        /**
         *Creates a manyToOne association.
         * See {@link patio.plugins.AssociationPlugin.associate} for options.
         *
         * @example
         * var patio = require("./index"),
         *     sql = patio.sql,
         *     comb = require("comb");
         *
         * //set up camelization so that properties can be camelcase but will be inserted
         * //snake case (i.e. 'biologicalFather' becomes 'biological_father').
         * patio.camelize = true;
         * patio.connectAndExecute("mysql://test:testpass@localhost:3306/sandbox", function (db, patio) {
         *         db.forceCreateTable("biologicalFather", function () {
         *             this.primaryKey("id");
         *             this.name(String);
         *         });
         *         db.forceCreateTable("child", function () {
         *             this.primaryKey("id");
         *             this.name(String);
         *             this.foreignKey("biologicalFatherId", "biologicalFather", {key:"id"});
         *         });
         *
         *
         *         //define the BiologicalFather model
         *         patio.addModel("biologicalFather", {
         *             static:{
         *                 init:function () {
         *                     this.oneToMany("children");
         *                 }
         *             }
         *         });
         *
         *
         *         //define Child  model
         *         patio.addModel("child", {
         *             static:{
         *                 init:function () {
         *                     this.manyToOne("biologicalFather");
         *                 }
         *             }
         *         });
         *
         *         var BiologicalFather = patio.getModel("biologicalFather");
         *         var Child = patio.getModel("child");
         *         BiologicalFather.save([
         *             {name:"Fred", children:[
         *                 {name:"Bobby"},
         *                 {name:"Alice"},
         *                 {name:"Susan"}
         *             ]},
         *             {name:"Ben"},
         *             {name:"Bob"},
         *             {name:"Scott", children:[
         *                 {name:"Brad"}
         *             ]}
         *         ]);
         *         var father = BiologicalFather.findById(1);
         *         patio.logInfo(Child.findById(1).biologicalFather.name);
         *         patio.logInfo(father.name);
         *         patio.logInfo(father.children.map(function(child){return child.name}));
         *     }).both(comb.hitch(patio, "disconnect"));
         */
        oneToMany:function (name, options, filter) {
            return this.associate(this.ONE_TO_MANY, name, options, filter);
        },

        /**
         * Creates a manyToOne association.
         * See {@link patio.plugins.AssociationPlugin.oneToMany}.
         * See {@link patio.plugins.AssociationPlugin.associate}
         */
        manyToOne:function (name, options, filter) {
            return this.associate(this.MANY_TO_ONE, name, options, filter);
        },

        /**
         * Creates a oneToOne relationship between models.
         * See {@link patio.plugins.AssociationPlugin.associate} for options.
         *
         * @example
         * var patio = require("./index"),
         *     sql = patio.sql,
         *     comb = require("comb");
         *
         * patio.camelize = true;
         * var createSchema = patio.connectAndExecute("mysql://test:testpass@localhost:3306/sandbox", function (db, patio) {
         *     db.forceDropTable(["capital", "state"]);
         *     db.createTable("state", function () {
         *         this.primaryKey("id");
         *         this.name(String)
         *         this.population("integer");
         *         this.founded(Date);
         *         this.climate(String);
         *         this.description("text");
         *     });
         *     db.createTable("capital", function () {
         *         this.primaryKey("id");
         *         this.population("integer");
         *         this.name(String);
         *         this.founded(Date);
         *         this.foreignKey("stateId", "state", {key:"id"});
         *     });
         *     patio.addModel("state", {
         *         static:{
         *             init:function () {
         *                 this.oneToOne("capital");
         *             }
         *         }
         *     });
         *     patio.addModel("capital", {
         *         static:{
         *             init:function () {
         *                 this.manyToOne("state");
         *             }
         *         }
         *     });
         *     var State = patio.getModel("state");
         *     State.save([
         *         {
         *             name : "Nebraska",
         *             population : 1796619,
         *             founded : new Date(1867, 2,4),
         *             climate : "continental",
         *             capital : {
         *                 name : "Lincoln",
         *                 founded : new Date(1856, 0,1),
         *                 population : 258379
         *
         *             }
         *         },
         *         {
         *             name : "Texas",
         *             population : 25674681,
         *             founded : new Date(1845, 11,29),
         *             capital : {
         *                 name : "Austin",
         *                 founded : new Date(1835, 0,1),
         *                 population : 790390
         *
         *             }
         *         }
         *     ]);
         *     State.forEach(function(state){
         *         return state.capital.then(function(capital){
         *             console.log(state.name + "'s capital is " + capital.name);
         *             console.log(capital.name + " was founded in " + capital.founded);
         *         });
         *     });
         *
         * }).both(comb.hitch(patio, "disconnect"));
         */
        oneToOne:function (name, options, filter) {
            return this.associate(this.ONE_TO_ONE, name, options, filter);
        },

        /**
         * The manyToMany association allows a model to be associated to many other rows in another model.
         * and the associated model can be associated with many rows in this model. This is done by
         * using a join table to associate the two models.
         * <p>For example consider phone numbers. Each person can have multiple phone numbers.
         *
         * @example
         * phone          person_phone                   person
         * ------         ----------------------         -----
         * |id  |         |person_id | phone_id|         |id |
         * ------         ----------------------         -----
         * | 1  |         |        1 |       1 |         | 1 |
         * | .  | <------ |        1 |       2 | ------> | 2 |
         * | .  |         |        2 |       2 |         | 3 |
         * | n  |         |        2 |       1 |         | 4 |
         * ------         ----------------------         -----
         *
         * @example
         *
         * //define the PhoneNumber model
         * var PhoneNumber = patio.addModel("phone");
         *
         * //define Person model
         * var Person = patio.addModel("person");
         *
         * //Create manyToMany relationship from person to PhoneNumber
         * Person.manyToMany("phoneNumbers", {
         *                      model : PhoneNumber.tableName,
         *                      joinTable : "person_phone",
         *                      key : {person_id : "phone_id"}
         *});
         *
         *
         * PhoneNumber.manyToMany("owners", {
         *                      model : Person.tableName,
         *                      joinTable : "person_phone",
         *                      key : {phone_id : "person_id"}
         *});
         *
         * Person.findById(1).then(function(person){
         *    person.phoneNumbers.then(function(numbers){
         *       numbers.length => 2
         *    });
         * });
         *
         * PhoneNumber.findById(1).then(function(number){
         *    number.owners.then(function(owners){
         *        owners.length => 2;
         *    });
         * });
         * </p>
         * @param {String} name the alias of the association. The key you provide here is how the association
         *                      will be looked up on instances of this model.
         * @param {Object} options object that describes the association.
         * @param {String} options.model the table name of the model that this Model is associated with.
         * @param {String} options.joinTable the name of the joining table.
         * @param {Function} options.filter Custom filter to define a custom association.
         *                  The filter is called in the scope of model that the association is added to.
         *                  Say we have the same models as defined above.
         * <pre class="code">
         * //Define the join table model so we can query it.
         * PersonPhone = patio.addModel(person_phone);
         * PhoneNumber.manyToMany("owners", {
         *                      model : Person.tableName,
         *                      joinTable : "person_phone",
         *                      filter : function(){
         *                              //find all the person ids
         *                            var jd = PhoneNumber.dataset
         *                                                .select('person_id')
         *                                                .find({phone_id : this.id});
         *                            //now query person with the ids!
         *                            return Person.filter({id : {"in" : jd}});
         *                      }
         *  });
         * </pre>
         * @param {AssociationPlugin.fetchType.EAGER|AssociationPlugin.fetchType.EAGER} [options.fetchType=AssociationPlugin.fetchType.LAZY]
         *          how fetch the association, if specified to lazy then the association is lazy loaded.
         *          Otherwise the association is loaded when the model is loaded.
         * @param {Object} key this defines the foreign key relationship
         *  <pre class="code">
         *      {thisModelsKey : otherModelsKey}
         *  </pre>
         * @param {String|Object} [options.orderBy] column or columns to order the associated model by.
         */
        manyToMany:function (name, options, filter) {
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
         *         model object can be associated with many current model objects./li>
         * </ul>
         *
         * @param {patio.plugins.AssociationPlugin.ONE_TO_ONE|patio.plugins.AssociationPlugin.ONE_TO_MANY
            *          |patio.plugins.AssociationPlugin.MANY_TO_ONE|patio.plugins.AssociationPlugin.MANY_TO_MANY} type the
         *          type of assiciation that is to be created.
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
         *   <li><b>limit - Limit the number of records to the provided value.  Use
         *     an array with two elements for the value to specify a limit (first element) and an
         *     offset (second element). See {@link patio.Dataset#limit}.</li>
         *   <li><b>order - the column/s order the association dataset by.  Can be a
         *     one or more columns.
         *     See {@link patio.Dataset#order}.</li>
         *   <li><b>readOnly - Do not add a setter method (for MANY_TO_ONE or ONE_TO_ONE associations),
         *     or add/remove/removeAll methods (for ONE_TO_MANY and MANY_TO_MANY associations).</li>
         *   <li><b>select - the columns to select.  Defaults to the associated class's
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
         *             current model's primary key, as a symbol.  Defaults to
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
         *          Can use an array of symbols for a composite key association.
         *      <li><b>leftPrimaryKey - column in current table that <b>leftKey</b> points to.
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
        associate:function (type, name, options, filter) {
            if (comb.isFunction(options)) {
                filter = options;
                options = {};
            }
            this.__associations = this.__associations || {manyToMany:{}, oneToMany:{}, manyToOne:{}, oneToOne:{}};
            var assoc = new associations[type](comb.merge({model:name}, options), this.patio, filter);
            assoc.inject(this, name);
            this.__associations[type][name] = assoc;
            return this;
        },

        __isReciprocalAssociation:function (assoc, pAssoc) {
            var keys = assoc._getAssociationKey(), leftKey = keys[0], rightKey = keys[1];
            var pKeys = pAssoc._getAssociationKey(), pLeftKey = pKeys[0], pRightKey = pKeys[1];
            return leftKey.every(function (k, i) {
                return pRightKey[i] == k
            }) && rightKey.every(function (k, i) {
                return pLeftKey[i] == k
            }) && assoc.parent == pAssoc.model;
        },

        _findAssociation:function (assoc) {
            var type = assoc.type, recipTypes = RECIPROCAL_ASSOC[type];
            for (var i in recipTypes) {
                var recipType = recipTypes[i];
                var potentialAssociations = this.__associations[recipType];
                var ret = null, found = true;
                for (var i in potentialAssociations) {
                    var pAssoc = potentialAssociations[i];
                    if (this.__isReciprocalAssociation(assoc, pAssoc)) {
                        ret = [i, pAssoc], found = true;
                        break;
                    }
                }
                if (found) {
                    break;
                }

            }
            return ret;
        },

        _clearAssociationsForType:function (type, clazz, model) {
            this._findAssociationsForType(type, clazz).forEach(function (assoc) {
                assoc._clearAssociations(model);
            });
        },

        _reloadAssociationsForType:function (type, clazz, model) {
            var pl = this._findAssociationsForType(type, clazz).map(function (assoc) {
                return assoc._forceReloadAssociations(model);
            });
            return pl.length ? new comb.PromiseList(pl) : new comb.Promise().callback();
        },

        _findAssociationsForType:function (type, clazz) {
            var associations = this.__associations[type], ret = [];
            for (var i in associations) {
                var assoc = associations[i];
                if (assoc.model == clazz) {
                    ret.push(assoc);
                }
            }
            return ret;

        },

        getters:{
            associations:function () {
                var ret = [], assoc = this.__associations;
                Object.keys(assoc).forEach(function (k) {
                    ret = ret.concat(Object.keys(assoc[k]));
                });
                return ret;
            },

            hasAssociations:function () {
                return this.associations.length > 0;
            }
        },

        /**
         * @borrows _Association.fetch as fetch
         */
        fetchType:fetch,

        ONE_TO_ONE:"oneToOne",
        ONE_TO_MANY:"oneToMany",
        MANY_TO_ONE:"manyToOne",
        MANY_TO_MANY:"manyToMany"
    }});