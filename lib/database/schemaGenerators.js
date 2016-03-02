var comb = require("comb-proxy"),
    argsToArray = comb.argsToArray,
    merge = comb.merge,
    isFunction = comb.isFunction,
    isDefined = comb.isDefined,
    isHash = comb.isHash,
    isString = comb.isString,
    isArray = comb.isArray,
    toArray = comb.array.toArray,
    methodMissing = comb.methodMissing,
    define = comb.define;


var Generator = define(null, {
    instance:{
        /**@lends patio.SchemaGenerator.prototype*/

        __primaryKey:null,

        /**
         * An internal class that the user is not expected to instantiate directly.
         * Instances are created by {@link patio.Database#createTable}.
         * It is used to specify table creation parameters.  It takes a Database
         * object and a block of column/index/constraint specifications, and
         * gives the Database a table description, which the database uses to
         * create a table.
         *
         * {@link patio.SchemaGenerator} has some methods but also includes method_missing,
         * allowing users to specify column type as a method instead of using
         * the column method, which makes for a cleaner code.
         * @constructs
         * @example
         *comb.executeInOrder(DB, function(DB){
         *       DB.createTable("airplane_type", function () {
         *          this.primaryKey("id");
         *          this.name(String, {allowNull:false});
         *          this.max_seats(Number, {size:3, allowNull:false});
         *          this.company(String, {allowNull:false});
         *       });
         *      DB.createTable("airplane", function () {
         *          this.primaryKey("id");
         *          this.total_no_of_seats(Number, {size:3, allowNull:false});
         *          this.foreignKey("typeId", "airplane_type", {key:"id"});
         *      });
         *      DB.createTable("flight_leg", function () {
         *          this.primaryKey("id");
         *          this.scheduled_departure_time("time");
         *          this.scheduled_arrival_time("time");
         *          this.foreignKey("departure_code", "airport", {key:"airport_code", type : String, size : 4});
         *          this.foreignKey("arrival_code", "airport", {key:"airport_code", type : String, size : 4});
         *          this.foreignKey("flight_id", "flight", {key:"id"});
         *      });
         *      DB.createTable("leg_instance", function () {
         *          this.primaryKey("id");
         *          this.date("date");
         *          this.arr_time("datetime");
         *          this.dep_time("datetime");
         *          this.foreignKey("airplane_id", "airplane", {key:"id"});
         *          this.foreignKey("flight_leg_id", "flight_leg", {key:"id"});
         *      });
         *});
         * @param {patio.Database} the database this generator is for
         */
        constructor:function (db) {
            this.db = db;
            this.columns = [];
            this.indexes = [];
            this.constraints = [];
            this.__primaryKey = null;
        },

        /**
         * Add an unnamed constraint to the DDL, specified by the given block
         * or args:
         *
         * @example
         * db.createTable("test", function(){
         *   this.check({num : {between : [1,5]}})
         *      //=> CHECK num >= 1 AND num <= 5
         *   this.check(function(){return this.num.gt(5);});
         *      //=> CHECK num > 5
         * });
         **/
        check:function () {
            return this.constraint.apply(this, [null].concat(argsToArray(arguments)));
        },

        /**
         * Add a column with the given name, type, and opts to the DDL.
         *
         * <pre class="code">
         *  DB.createTable("test", function(){
         *      this.column("num", "integer");
         *          //=> num INTEGER
         *      this.column("name", String, {allowNull : false, "default" : "a");
         *          //=> name varchar(255) NOT NULL DEFAULT 'a'
         *      this.column("ip", "inet");
         *          //=> ip inet
         *   });
         * </pre>
         *
         * You can also create columns via method missing, so the following are
         * equivalent:
         * <pre class="code">
         * DB.createTable("test", function(){
         *   this.column("number", "integer");
         *   this.number("integer");
         * });
         * </pre>
         *
         * @param {String|patio.sql.Identifier} name the name of the column
         * @param type the datatype of the column.
         * @param {Object} [opts] additional options
         *
         * @param [opts.default] The default value for the column.
         * @param [opts.deferrable] This ensure Referential Integrity will work even if
         *                reference table will use for its foreign key a value that does not
         *                exists(yet) on referenced table. Basically it adds
         *                DEFERRABLE INITIALLY DEFERRED on key creation.
         * @param {Boolean} [opts.index]  Create an index on this column.
         * @param {String|patio.sql.Identifier} [key] For foreign key columns, the column in the associated table
         *         that this column references.  Unnecessary if this column references the primary key of the
         *         associated table.
         * @param {Boolean} [opts.allowNull]  Mark the column as allowing NULL values (if true),
         *          or not allowing NULL values (if false).  If unspecified, will default
         *          to whatever the database default is.
         * @param {String} [opts.onDelete] Specify the behavior of this column when being deleted
         *               ("restrict", "cascade", "setNull", "setDefault", "noAction").
         * @param {String} [opts.onUpdate] Specify the behavior of this column when being updated
         *               Valid options ("restrict", "cascade", "setNull", "setDefault", "noAction").
         * @param {Boolean} [opts.primaryKey] Make the column as a single primary key column.  This should only
         *                 be used if you have a single, non-autoincrementing primary key column.
         * @param {Number} [opts.size] The size of the column, generally used with string
         *          columns to specify the maximum number of characters the column will hold.
         *          An array of two integers can be provided to set the size and the
         *          precision, respectively, of decimal columns.
         * @param {String} [opts.unique] Mark the column as unique, generally has the same effect as
         *            creating a unique index on the column.
         * @param {String} [opts.unsigned] Make the column type unsigned, only useful for integer
         *              columns.
         * @param {Array} [opts.elements] Available items used for set and enum columns.
         *
         **/
        column:function (name, type, opts) {
            opts = opts || {};
            this.columns.push(merge({name:name, type:type}, opts));
            if (opts.index) {
                this.index(name);
            }
        },

        /**
         * Adds a named constraint (or unnamed if name is nil) to the DDL,
         * with the given block or args.
         * @example
         * DB.createTable("test", function(){
         *      this.constraint("blah", {num : {between : [1,5]}})
         *              //=> CONSTRAINT blah CHECK num >= 1 AND num <= 5
         *      this.check("foo", function(){
         *          return this.num.gt(5);
         *      }); # CONSTRAINT foo CHECK num > 5
         * });
         * @param {String|patio.sql.Identifier} name the name of the constraint
         * @param {...} args variable number of arguments to create the constraint filter.
         *              See {@link patio.Dataset#filter} for valid filter arguments.
         *   */
        constraint:function (name, args) {
            args = argsToArray(arguments).slice(1);
            var block = isFunction(args[args.length - 1]) ? args.pop : null;
            this.constraints.push({name:name, type:"check", check:block || args});
        },

        /**
         *  Add a foreign key in the table that references another table to the DDL. See {@link patio.SchemaGenerator#column{
         * for options.
         *
         * @example
         * DB.createTable("flight_leg", function () {
         *      this.primaryKey("id");
         *      this.scheduled_departure_time("time");
         *      this.scheduled_arrival_time("time");
         *      this.foreignKey("departure_code", "airport", {key:"airport_code", type : String, size : 4});
         *      this.foreignKey("arrival_code", "airport", {key:"airport_code", type : String, size : 4});
         *      this.foreignKey("flight_id", "flight", {key:"id"});
         * });
         **/
        foreignKey:function (name, table, opts) {
            opts = opts || {};
            opts = isHash(table) ? merge({}, table, opts) : isString(table) ? merge({table:table}, opts) : opts;
            if (isArray(name)) {
                return this.__compositeForeignKey(name, opts);
            } else {
                return this.column(name, "integer", opts);
            }
        },

        /**
         *Add a full text index on the given columns to the DDL.
         *
         * @example
         *  DB.createTable("posts", function () {
         *      this.title("text");
         *      this.body("text");
         *      this.fullTextIndex("title");
         *      this.fullTextIndex(["title", "body"]);
         *  });
         */
        fullTextIndex:function (columns, opts) {
            opts = opts || {};
            return this.index(columns, merge({type:"fullText"}, opts));
        },

        /**
         *
         * Check if the DDL includes the creation of a column with the given name.
         * @return {Boolean} true if the DDL includes the creation of a column with the given name.
         */
        hasColumn:function (name) {
            return this.columns.some(function (c) {
                return c.name === name;
            });
        },

        /**
         * Add an index on the given column(s) with the given options to the DDL.
         * The available options are:
         * @example
         * DB.createTable("test", function(table) {
         *       table.primaryKey("id", "integer", {null : false});
         *       table.column("name", "text");
         *       table.index("name", {unique : true});
         * });
         *
         * @param columns the column/n to create the index from.
         * @param {Object}  [opts] Additional options
         * @param {String}  [opts.type] The type of index to use (only supported by some databases)
         * @param {Boolean} [opts.unique] :: Make the index unique, so duplicate values are not allowed.
         * @param [opts.where] :: Create a partial index (only supported by some databases)
         **/
        index:function (columns, opts) {
            this.indexes.push(merge({columns:toArray(columns)}, opts || {}));
        },

        /**
         * Adds an auto-incrementing primary key column or a primary key constraint to the DDL.
         * To create a constraint, the first argument should be an array of columns
         * specifying the primary key columns. To create an auto-incrementing primary key
         * column, a single column can be used. In both cases, an options hash can be used
         * as the second argument.
         *
         * If you want to create a primary key column that is not auto-incrementing, you
         * should not use this method.  Instead, you should use the regular {@link patio.SchemaGenerator#column}
         * method with a {primaryKey : true} option.
         *
         * @example
         * db.createTable("airplane_type", function () {
         *      this.primaryKey("id");
         *          //=> id integer NOT NULL PRIMARY KEY AUTOINCREMENT
         *      this.name(String, {allowNull:false});
         *      this.max_seats(Number, {size:3, allowNull:false});
         *      this.company(String, {allowNull:false});
         * });
         * */
        primaryKey:function (name) {
            if (isArray(name)) {
                return this.__compositePrimaryKey.apply(this, arguments);
            } else {
                var args = argsToArray(arguments, 1), type;
                var opts = args.pop();
                this.__primaryKey = merge({}, this.db.serialPrimaryKeyOptions, {name:name}, opts);
                if (isDefined((type = args.pop()))) {
                    merge(opts, {type:type});
                }
                merge(this.__primaryKey, opts);
                return this.__primaryKey;
            }
        },

        /**
         * Add a spatial index on the given columns to the DDL.
         */
        spatialIndex:function (columns, opts) {
            opts = opts || {};
            return this.index(columns, merge({type:"spatial"}, opts));
        },

        /**
         * Add a unique constraint on the given columns to the DDL. See {@link patio.SchemaGenerator#constraint}
         * for argument types.
         * @example
         * DB.createTable("test", function(){
         *   this.unique("name");
         *          //=> UNIQUE (name)
         * });
         *   */
        unique:function (columns, opts) {
            opts = opts || {};
            this.constraints.push(merge({type:"unique", columns:toArray(columns)}, opts));
        },


        /**
         * @private
         * Add a composite primary key constraint
         */
        __compositePrimaryKey:function (columns) {
            var args = argsToArray(arguments, 1);
            var opts = args.pop() || {};
            this.constraints.push(merge({type:"primaryKey", columns:columns}, opts));
        },

        /**
         * @private
         * Add a composite foreign key constraint
         */
        __compositeForeignKey:function (columns, opts) {
            this.constraints.push(merge({type:"foreignKey", columns:columns}, opts));
        },

        /**@ignore*/
        getters:{
            // The name of the primary key for this generator, if it has a primary key.
            primaryKeyName:function () {
                return this.__primaryKey ? this.__primaryKey.name : null;
            }

        }

    }
});

exports.SchemaGenerator = function (db, block) {
    var gen = new Generator(db);
    var prox = methodMissing(gen, function (name) {
        return function (type, opts) {
            name = name || null;
            opts = opts || {};
            if (name) {
                return this.column(name, type, opts);
            } else {
                throw new TypeError("name required got " + name);
            }
        };
    }, Generator);
    block.apply(prox, [prox]);
    gen.columns = prox.columns;
    if (gen.__primaryKey && !gen.hasColumn(gen.primaryKeyName)) {
        gen.columns.unshift(gen.__primaryKey);
    }
    return gen;
};


var AlterTableGenerator = define(null, {
    instance:{
        /**@lends patio.AlterTableGenerator.prototype*/

        /**
         * An internal class that the user is not expected to instantiate directly.
         * Instances are created by {@link patio.Database#alterTable}.
         * It is used to specify table alteration parameters.  It takes a Database
         * object and a function which is called in the scope of the {@link patio.AlterTableGenerator}
         * to perform on the table, and gives the Database an array of table altering operations,
         * which the database uses to alter a table's description.
         *
         * @example
         * DB.alterTable("xyz", function() {
         *      this.addColumn("aaa", "text", {null : false, unique : true});
         *      this.dropColumn("bbb");
         *      this.renameColumn("ccc", "ddd");
         *      this.setColumnType("eee", "integer");
         *      this.setColumnDefault("hhh", 'abcd');
         *      this.addIndex("fff", {unique : true});
         *      this.dropIndex("ggg");
         * });
         *
         * //or using the passed in generator
         *  DB.alterTable("xyz", function(table) {
         *      table.addColumn("aaa", "text", {null : false, unique : true});
         *      table.dropColumn("bbb");
         *      table.renameColumn("ccc", "ddd");
         *      table.setColumnType("eee", "integer");
         *      table.setColumnDefault("hhh", 'abcd');
         *      table.addIndex("fff", {unique : true});
         *      table.dropIndex("ggg");
         * });
         * @constructs
         *
         * @param {patio.Database} db the database object which is performing the alter table operation.
         * @param {Function} block a block which performs the operations. The block is called in the scope
         * of the {@link patio.AlterTableGenerator} and is passed an instance of {@link patio.AlterTableGenerator}
         * as the first argument.
         */
        constructor:function (db, block) {
            this.db = db;
            this.operations = [];
            block.apply(this, [this]);

        },

        /**
         * Add a column with the given name, type, and opts to the DDL for the table.
         * See {@link patio.SchemaGenerator#column} for the available options.
         *
         * @example
         * DB.alterTable("test", function(){
         *   this.addColumn("name", String);
         *     //=> ADD COLUMN name varchar(255)
         * });
         **/
        addColumn:function (name, type, opts) {
            opts = opts || {};
            this.operations.push(merge({op:"addColumn", name:name, type:type}, opts));
        },

        /**
         * Add a constraint with the given name and args to the DDL for the table.
         * See {@link patio.SchemaGenerator#constraint}.
         *
         * @example
         * var sql = patio.sql;
         * DB.alterTable("test", function(){
         *      this.addConstraint("valid_name", sql.name.like('A%'));
         *          //=>ADD CONSTRAINT valid_name CHECK (name LIKE 'A%')
         * });
         *   */
        addConstraint:function (name) {
            var args = argsToArray(arguments).slice(1);
            var block = isFunction(args[args.length - 1]) ? args[args.length - 1]() : null;
            this.operations.push({op:"addConstraint", name:name, type:"check", check:block || args});
        },

        /**
         * Add a unique constraint to the given column(s).
         * See {@link patio.SchemaGenerator#constraint}.
         * @example
         * DB.alterTable("test", function(){
         *   this.addUniqueConstraint("name");
         *      //=> ADD UNIQUE (name)
         *   this.addUniqueConstraint("name", {name : "uniqueName});
         *      //=> ADD CONSTRAINT uniqueName UNIQUE (name)
         * });
         **/
        addUniqueConstraint:function (columns, opts) {
            opts = opts || {};
            this.operations.push(merge({op:"addConstraint", type:"unique", columns:toArray(columns)}, opts));
        },

        /**
         * Add a foreign key with the given name and referencing the given table
         * to the DDL for the table.  See {@link patio.SchemaGenerator#column}
         * for the available options.
         *
         * You can also pass an array of column names for creating composite foreign
         * keys. In this case, it will assume the columns exists and will only add
         * the constraint.
         *
         * NOTE: If you need to add a foreign key constraint to a single existing column
         * use the composite key syntax even if it is only one column.
         * @example
         * DB.alterTable("albums", function(){
         *   this.addForeignKey("artist_id", "table");
         *          //=>ADD COLUMN artist_id integer REFERENCES table
         *   this.addForeignKey(["name"], "table")
         *          //=>ADD FOREIGN KEY (name) REFERENCES table
         * });
         */
        addForeignKey:function (name, table, opts) {
            opts = opts;
            if (isArray(name)) {
                return this.__addCompositeForeignKey(name, table, opts);
            } else {
                return this.addColumn(name, this.db.defaultPrimaryKeyType, merge({table:table}, opts));
            }
        },

        /**
         * Add a full text index on the given columns to the DDL for the table.
         * See @{link patio.SchemaGenerator#index} for available options.
         */
        addFullTextIndex:function (columns, opts) {
            opts = opts || {};
            return this.addIndex(columns, merge({type:"fullText"}, opts));
        },

        /**
         * Add an index on the given columns to the DDL for the table.  See
         * {@link patio.SchemaGenerator#index} for available options.
         * @example
         * DB.alterTable("table", function(){
         *   this.addIndex("artist_id");
         *          //=> CREATE INDEX table_artist_id_index ON table (artist_id)
         * });
         */
        addIndex:function (columns, opts) {
            opts = opts || {};
            this.operations.push(merge({op:"addIndex", columns:toArray(columns)}, opts));
        },

        /**
         * Add a primary key to the DDL for the table.  See {@link patio.SchemaGenerator#column}
         * for the available options.  Like {@link patio.ALterTableGenerator#addForeignKey}, if you specify
         * the column name as an array, it just creates a constraint:
         *
         * @example
         * DB.alterTable("albums", function(){
         *      this.addPrimaryKey("id");
         *           //=> ADD COLUMN id serial PRIMARY KEY
         *      this.addPrimaryKey(["artist_id", "name"])
         *          //=>ADD PRIMARY KEY (artist_id, name)
         * });
         */
        addPrimaryKey:function (name, opts) {
            opts = opts || {};
            if (isArray(name)) {
                return this.__addCompositePrimaryKey(name, opts);
            } else {
                opts = merge({}, this.db.serialPrimaryKeyOptions, opts);
                delete opts.type;
                return this.addColumn(name, "integer", opts);
            }
        },

        /**
         * Add a spatial index on the given columns to the DDL for the table.
         * See {@link patio.SchemaGenerator#index} for available options.
         * */
        addSpatialIndex:function (columns, opts) {
            opts = opts || {};
            this.addIndex(columns, merge({}, {type:"spatial"}, opts));
        },

        /**
         * Remove a column from the DDL for the table.
         *
         * @example
         * DB.alterTable("albums", function(){
         *   this.dropColumn("artist_id");
         *      //=>DROP COLUMN artist_id
         * });
         *
         * @param {String|patio.sql.Identifier} name the name of the column to drop.
         */
        dropColumn:function (name) {
            this.operations.push({op:"dropColumn", name:name});
        },

        /**
         * Remove a constraint from the DDL for the table.
         * @example
         * DB.alterTable("test", function(){
         *      this.dropConstraint("constraint_name");
         *          //=>DROP CONSTRAINT constraint_name
         * });
         * @param {String|patio.sql.Identifier} name the name of the constraint to drop.
         */
        dropConstraint:function (name) {
            this.operations.push({op:"dropConstraint", name:name});
        },

        /**
         * Remove a child table's inheritance from a parent table.
         * @example
         * DB.alterTable("test", function () {
         *      this.noInherit("parentTable");
         *          //=>ALTER TABLE test NO INHERIT parent_table
         * });
         * @param {String|patio.sql.Identifier} name the name of the table to remove inheritance from.
         */
        noInherit: function (name) {
            this.operations.push({op:"noInherit", name: name});
        },

        /**
         * Remove an index from the DDL for the table.
         *
         * @example
         * DB.alterTable("albums", function(){
         *   this.dropIndex("artist_id")
         *      //=>DROP INDEX table_artist_id_index
         *   this.dropIndex(["a", "b"])
         *      //=>DROP INDEX table_a_b_index
         *   this.dropIndex(["a", "b"], {name : "foo"})
         *          //=>DROP INDEX foo
         * });
         */
        dropIndex:function (columns, opts) {
            opts = opts || {};
            this.operations.push(merge({op:"dropIndex", columns:toArray(columns)}, opts));
        },

        /**
         * Modify a column's name in the DDL for the table.
         *
         * @example
         * DB.alterTable("artist", function(){
         *      this.renameColumn("name", "artistName");
         *          //=> RENAME COLUMN name TO artist_name
         * });
         */
        renameColumn:function (name, newName, opts) {
            opts = opts || {};
            this.operations.push(merge({op:"renameColumn", name:name, newName:newName}, opts));
        },

        /**
         * Modify a column's default value in the DDL for the table.
         * @example
         * DB.alterTable("artist", function(){
         *      //=>this.setColumnDefault("artist_name", "a");
         *          //=> ALTER COLUMN artist_name SET DEFAULT 'a'
         * });
         *   */
        setColumnDefault:function (name, def) {
            this.operations.push({op:"setColumnDefault", name:name, "default":def});
        },

        /**
         * Modify a column's type in the DDL for the table.
         *
         * @example
         * DB.alterTable("artist", function(){
         *   this.setColumnType("artist_name", 'char(10)');
         *      //=> ALTER COLUMN artist_name TYPE char(10)
         * });
         */
        setColumnType:function (name, type, opts) {
            opts = opts || {};
            this.operations.push(merge({op:"setColumnType", name:name, type:type}, opts));
        },

        /**
         * Modify a column's NOT NULL constraint.
         * @example
         * DB.alterTable("artist", function(){
         *   this.setColumnAllowNull("artist_name", false);
         *      //=> ALTER COLUMN artist_name SET NOT NULL
         * });
         **/
        setAllowNull:function (name, allowNull) {
            this.operations.push({op:"setColumnNull", name:name, "null":allowNull});
        },

        /**
         * @private
         * Add a composite primary key constraint
         **/
        __addCompositePrimaryKey:function (columns, opts) {
            this.operations.push(merge({op:"addConstraint", type:"primaryKey", columns:columns}, opts));
        },

        /**
         * @private
         * Add a composite foreign key constraint
         * */
        __addCompositeForeignKey:function (columns, table, opts) {
            this.operations.push(merge({op:"addConstraint", type:"foreignKey", columns:columns, table:table}, opts));
        }
    }
}).as(exports, "AlterTableGenerator");
