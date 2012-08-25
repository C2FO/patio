var comb = require("comb"),
    Promise = comb.Promise,
    PromiseList = comb.PromiseList,
    hitch = comb.hitch;


comb.define(null, {
    instance:{},
    "static":{

        configure:function (model) {

        }
    }
}).as(exports, "SingleTableInheritance");


/**
 * @class This plugin enables
 * <a href="http://www.martinfowler.com/eaaCatalog/classTableInheritance.html" target="patioapi">
 *     class table inheritance
 * </a>.
 *
 *<div>
 *     Consider the following table model.
 * @code
 *          employee
 *            - id
 *            - name (varchar)
 *            - kind (varchar)
 *     /                          \
 * staff                        manager
 *   - id (fk employee)           - id (fk employee)
 *   - manager_id (fk manger)     - numStaff (number)
 *                                 |
 *                              executive
 *                                - id (fk manager)
 *
 *  <ul>
 *      <li><b>employee</b>: This is the parent table of all employee instances.</li>
 *      <li><b>staff</b>: Table that inherits from employee where and represents.</li>
 *      <li><b>manager</b>: Another subclass of employee.</li>
 *      <li><b>executive</b>: Subclass of manager that also inherits from employee through inhertiance</li>
 *  </ul>
 * <p>
 * When setting up you tables the parent table should contain a String column that contains the "kind" of class it is.
 * (i.e. employee, staff, manager, executive). This allows the plugin to return the proper instance type when querying
 * the tables.
 * </p>
 * <p>
 *     All other tables that inherit from employee should contain a foreign key to their direct super class that is the
 *     same name as the primary key of the parent table(<b>employee</b>). So, in the
 *     above example <b>staff</b> and <b>manager</b> both contain foreign keys to employee and <b>executive</b> contains
 *     a foreign key to <b>manager</b> and they are all named <b>id</b>.
 * </p>
 *</div>
 *
 * To set up you models the base super class should contain the ClassTableInheritancePlugin
 *
 * {@code
 * var Employee = (exports.Employee = patio.addModel("employee", {
 *      plugins : [patio.plugins.ClassTableInheritancePlugin],
 *      static:{
 *          init:function () {
 *              this._super(arguments);
 *              this.configure({key : "kind"});
 *          }
 *      }
 *  }));
 * }
 * All sub classes should just inherit their super class
 * {@code
 * var Staff = (exports.Staff =  patio.addModel("staff", Employee, {
 *      static:{
 *          init:function () {
 *              this._super(arguments);
 *              this.manyToOne("manager", {key : "managerId", fetchType : this.fetchType.EAGER});
 *          }
 *      }
 * }));
 * var Manager = (exports.Manager = patio.addModel("manager", Employee, {
 *      static:{
 *          init:function () {
 *              this._super(arguments);
 *              this.oneToMany("staff", {key : "managerId", fetchType : this.fetchType.EAGER});
 *          }
 *      }
 * }));
 * }
 *
 * Executive inherits from manager, and through inheritance will also receive the oneToMany relationship with staff
 * {@code
 * var Executive = (exports.Executive = patio.addModel("executive",  Manager));
 * }
 *
 * Working with models
 *
 * {@code
 * comb.when(
 *     new Employee({name:"Bob"}).save(),
 *     new Staff({name:"Greg"}).save(),
 *     new Manager({name:"Jane"}).save(),
 *     new Executive({name:"Sue"}).save()
 * ).then(function(){
 *      Employee.all().then(function(emps){
 *          var bob = emps[0], greg = emps[1], jane = emps[2], sue = emps[3];
 *          console.log(bob instanceof Employee); //true
 *          console.log(greg instanceof Employee);  //true
 *          console.log(greg instanceof Staff);  //true
 *          console.log(jane instanceof Employee);  //true
 *          console.log(jane instanceof Manager);  //true
 *          console.log(sue instanceof Employee);  //true
 *          console.log(sue instanceof Manager);  //true
 *          console.log(sue instanceof Executive);  //true
 *      });
 * });
 * }
 *
 * @name ClassTableInheritancePlugin
 * @memberof patio.plugins
 */
comb.define(null, {

    instance:{

        // Delete the row from all backing tables, starting from the
        // most recent table and going through all superclasses.
        _remove:function () {
            var q = this._getPrimaryKeyQuery();
            return new PromiseList(this._static.__ctiTables.slice().reverse().map(function (table) {
                return this.db.from(table).filter(q).remove();
            }, this), true).promise();
        },

        // Save each column according to the columns in each table
        _save:function () {
            var thisStatic = this._static;
            var ret = new Promise();
            if (thisStatic === this._static.__ctiBaseModel) {
                return this._super(arguments);
            } else {
                var q = this._getPrimaryKeyQuery(), pk = this.primaryKey[0];
                var tables = thisStatic.__ctiTables, ctiColumns = thisStatic.__ctiColumns;
                var insertTable = comb.hitch(this, function (index) {
                    if (index < tables.length) {
                        var table = tables[index];
                        var isRestricted = this._static.isRestrictedPrimaryKey;
                        var cols = ctiColumns[table], insert = {};
                        cols.forEach(function (c) {
                            var val;
                            if ((index !== 0 || (index == 0 && (!isRestricted || pk.indexOf(c) == -1))) && !comb.isUndefined(val = this[c])) {
                                insert[c] = val;
                            }
                        }, this);
                        this.db.from(table).insert(insert).then(comb.hitch(this, function (id) {
                            if (comb.isUndefined(this.primaryKeyValue) && !comb.isUndefined(id) && index == 0) {
                                this.__ignore = true;
                                //how to handle composite keys.
                                this[pk] = id;
                                this.__ignore = false;
                            }
                            insertTable(++index);
                        }), ret);
                    } else {
                        this.__isNew = false;
                        this.__isChanged = false;
                        this._saveReload().then(comb.hitch(this, function () {
                            ret.callback(this);
                        }), ret);
                    }
                });
                insertTable(0);
            }
            return ret.promise();
        },

        // update each column according to the columns in each table
        _update:function () {
            var q = this._getPrimaryKeyQuery(), changed = this.__changed;
            var ret = new Promise(),
                modelStatic = this._static,
                ctiColumns = modelStatic.__ctiColumns,
                tables = modelStatic.__ctiTables;
            return new PromiseList(tables.map(function (table) {
                var cols = ctiColumns[table], update = {};
                cols.forEach(function (c) {
                    if (!comb.isUndefined(changed[c])) {
                        update[c] = changed[c];
                    }
                }, this);
                return comb.isEmpty(update) ? new Promise().callback() : this.db.from(table).filter(q).update(update);
            }, this), true)
                .chain(comb.hitch(this, "_updateReload"))
                .promise();
        }


    },

    static:{
        /**@lends patio.plugins.ClassTableInheritancePlugin*/

        /**
         * Configures the plugins with the provided options. <b>Note:</b> This should only be called in the
         * initial parent class.
         *
         * @param {Object} [opts] Additional options to configure behavior of the plugin
         * @param {String} [opts.key="key"] the column in the base table that contains the name of the subclass.
         * @param {Funciton} [opts.keyCb] A callback to invoke on on the key returned from the database. This is useful
         * if you are working with other orms that save the keys differently.
         */
        configure:function (opts) {
            this.__configureOpts = opts;
            return this;
        },

        sync:function (cb) {
            var ret = new Promise(), opts = this.__configureOpts;
            if (this.__configureOpts && !this.__configured) {
                this._super().then(hitch(this, function () {
                    if (!this.__configured) {
                        var baseModel = this.__ctiBaseModel = this;
                        var key = this.__ctiKey = opts.key || "key";
                        var keyCallback = opts.keyCb || function (k) {
                            return k;
                        };
                        this.__ctiModels = [this];
                        this.__ctiTables = [this.tableName];
                        var cols = this.__ctiColumns = {};
                        cols[this.tableName] = this.columns;
                        this.dataset.rowCb = comb.hitch(this, function (r) {
                            if (key) {
                                var model = this.patio.getModel(keyCallback(r[key]));
                                if (model !== baseModel) {
                                    var q = {};
                                    model.primaryKey.forEach(function (k) {
                                        q[k] = r[k];
                                    }, this);
                                    return model.dataset.naked().filter(q).one().chain(model.load.bind(model));
                                } else {
                                    return this.load(r);
                                }
                            } else {
                                return this.load(r);
                            }
                        });
                        this.pre("save", function (next) {
                            if (key) {
                                this[key] = this.tableName.toString();
                            }
                            next();
                        });
                        this.__configured = true;
                    }
                    ret.callback(this);
                }), ret);
                ret.classic(cb);
            } else {
                return this._super(arguments);
            }
            return ret.promise();
        },

        /**
         * <b>Not typically called by user code</b>. Sets up subclass inheritance.
         *
         * @param {patio.Model} model model that this class is inheriting from.
         */
        inherits:function (model) {
            this._super(arguments);
            var ctiKey = this.__ctiKey = model.__ctiKey;
            this.__ctiTables = model.__ctiTables.slice();
            this.__ctiModels = model.__ctiModels.slice();
            this.__ctiModels.push(this);
            this.__ctiTables.push(this.tableName);
            this.__ctiColumns = comb.merge({}, model.__ctiColumns);
            this.__ctiColumns[this.tableName] = this.columns;
            this.__ctiBaseModel = model.__ctiBaseModel;
            //copy over our schema
            var newSchema = comb.merge({}, this.__schema);
            var schemas = model.__ctiModels.map(
                function (m) {
                    return m.schema;
                }).reverse();
            schemas.forEach(function (s) {
                for (var i in s) {
                    newSchema[i] = s[i];
                }
            }, this);
            this._setSchema(newSchema);
            this._setDataset(model.dataset.join(this.tableName, this.primaryKey));
            this._setPrimaryKey(this.__ctiBaseModel.primaryKey);
            this.pre("save", function (next) {
                if (ctiKey) {
                    this[ctiKey] = this.tableName.toString();
                }
                next();
            });
        }
    }
}).as(exports, "ClassTableInheritance");