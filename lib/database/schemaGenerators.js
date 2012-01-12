var comb = require("comb"),
    hitch = comb.hitch,
    SQL = require('../sql'),
    sql = SQL.sql,
    errors = require("../errors"),
    NotImplemented = errors.NotImplemented;


var AUTOINCREMENT = 'AUTOINCREMENT',
    CASCADE = 'CASCADE',
    COMMA_SEPARATOR = ', ',
    NO_ACTION = 'NO ACTION',
    NOT_NULL = ' NOT NULL',
    NULL = ' NULL',
    PRIMARY_KEY = ' PRIMARY KEY',
    RESTRICT = 'RESTRICT',
    SET_DEFAULT = 'SET DEFAULT',
    SET_NULL = 'SET NULL',
    TEMPORARY = 'TEMPORARY ',
    UNDERSCORE = '_',
    UNIQUE = ' UNIQUE',
    UNSIGNED = ' UNSIGNED';

var GENERIC_TYPES = [String, Number, SQL.Float, SQL.Decimal, Date, SQL.TimeStamp, SQL.Year, Boolean];


var Generator = comb.define(null, {
    instance : {

        __primaryKey : null,

        constructor : function(db, block) {
            this.db = db;
            this.columns = [];
            this.indexes = [];
            this.constraints = [];
            this.__primaryKey = null;
        },

        /*Add an unnamed constraint to the DDL, specified by the given block
         * or args:
         *
         *   check(:num=>1..5) # CHECK num >= 1 AND num <= 5
         *   check{num > 5} # CHECK num > 5
         *   */
        check : function() {
            return this.constraint.apply(this, [null].concat(comb.argsToArray(arguments)));
        },

        /* Add a column with the given name, type, and opts to the DDL.
         *
         *   column :num, :integer
         *   # num INTEGER
         *
         *   column :name, String, :null=>false, :default=>'a'
         *   # name varchar(255) NOT NULL DEFAULT 'a'
         *
         *   inet :ip
         *   # ip inet
         *
         * You can also create columns via method missing, so the following are
         * equivalent:
         *
         *   column :number, :integer
         *   integer :number
         *
         * The following options are supported:
         *
         * :default :: The default value for the column.
         * :deferrable :: This ensure Referential Integrity will work even if
         *                reference table will use for its foreign key a value that does not
         *                exists(yet) on referenced table. Basically it adds
         *                DEFERRABLE INITIALLY DEFERRED on key creation.
         * :index :: Create an index on this column.
         * :key :: For foreign key columns, the column in the associated table
         *         that this column references.  Unnecessary if this column
         *         references the primary key of the associated table.
         * :null :: Mark the column as allowing NULL values (if true),
         *          or not allowing NULL values (if false).  If unspecified, will default
         *          to whatever the database default is.
         * :on_delete :: Specify the behavior of this column when being deleted
         *               (:restrict, cascade, :set_null, :set_default, :no_action).
         * :on_update :: Specify the behavior of this column when being updated
         *               (:restrict, cascade, :set_null, :set_default, :no_action).
         * :primary_key :: Make the column as a single primary key column.  This should only
         *                 be used if you have a single, nonautoincrementing primary key column.
         * :size :: The size of the column, generally used with string
         *          columns to specify the maximum number of characters the column will hold.
         *          An array of two integers can be provided to set the size and the
         *          precision, respectively, of decimal columns.
         * :unique :: Mark the column as unique, generally has the same effect as
         *            creating a unique index on the column.
         * :unsigned :: Make the column type unsigned, only useful for integer
         *              columns.
         *              */
        column : function(name, type, opts) {
            opts = opts || {};
            this.columns.push(comb.merge({name : name, type : type}, opts));
            if (opts.index) {
                this.index(name);
            }
        },

        /* Adds a named constraint (or unnamed if name is nil) to the DDL,
         * with the given block or args.
         *
         *   constraint(:blah, :num=>1..5) # CONSTRAINT blah CHECK num >= 1 AND num <= 5
         *   check(:foo){num > 5} # CONSTRAINT foo CHECK num > 5
         *   */
        constraint : function(name) {
            var args = comb.argsToArray(arguments);
            var block = comb.isFunction(args[args.length - 1]) ? args.pop : null;
            this.constraints.push({name : name, type : "check", check : block || args});
        },

        /* for available options.
         *
         *   foreign_key(:artist_id) # artist_id INTEGER
         *   foreign_key(:artist_id, :artists) # artist_id INTEGER REFERENCES artists
         *   foreign_key(:artist_id, :artists, :key=>:id) # artist_id INTEGER REFERENCES artists(id)
         *   */
        foreignKey : function(name, table, opts) {
            opts = opts || {};
            opts = comb.isHash(table) ? comb.merge({}, table, opts) : comb.isString(table) ? comb.merge({table : table}, opts) : opts;
            if (comb.isArray(name)) {
                return this.__compositeForeignKey(name, opts);
            } else {
                return this.column(name, "integer", opts);
            }
        },

        /**
         *Add a full text index on the given columns to the DDL.
         */

        fullTextIndex : function(columns, opts) {
            opts = opts || {};
            return this.index(columns, comb.merge({type : "fullText"}, opts));
        },

        /**
         * True if the DDL includes the creation of a column with the given name.
         */
        hasColumn : function(name) {
            return this.columns.some(function(c) {
                return c.name == name
            });
        },

        /** Add an index on the given column(s) with the given options to the DDL.
         * The available options are:
         *
         * :type :: The type of index to use (only supported by some databases)
         * :unique :: Make the index unique, so duplicate values are not allowed.
         * :where :: Create a partial index (only supported by some databases)
         *
         *   index :name
         *   # CREATE INDEX table_name_index ON table (name)
         *
         *   index [:artist_id, :name]
         *   # CREATE INDEX table_artist_id_name_index ON table (artist_id, name)
         *   */
        index : function(columns, opts) {
            this.indexes.push(comb.merge({columns : comb.array.toArray(columns)}, opts));
        },

        /* Adds an autoincrementing primary key column or a primary key constraint to the DDL.
         * To create a constraint, the first argument should be an array of column symbols
         * specifying the primary key columns. To create an autoincrementing primary key
         * column, a single symbol can be used. In both cases, an options hash can be used
         * as the second argument.
         *
         * If you want to create a primary key column that is not autoincrementing, you
         * should not use this method.  Instead, you should use the regular +column+ method
         * with a <tt>:primary_key=>true</tt> option.
         *
         * Examples:
         *   primary_key(:id)
         *   primary_key([:street_number, :house_number])
         * */
        primaryKey : function(name) {
            if (comb.isArray(name)) {
                return this.__compositePrimaryKey.apply(this, arguments);
            } else {
                var args = comb.argsToArray(arguments).slice(1);
                var opts = args.pop();
                this.__primaryKey = comb.merge(opts, this.db.serialPrimaryKeyOptions, {name : name});
                opts = comb.isHash(opts) ? {type : opts} : opts;
                if ((type = args.pop()) != null) {
                    comb.merge(opts, {type : type});
                }
                comb.merge(this.__primaryKey, opts);
                return this.__primaryKey;
            }
        },

        /*
         Add a spatial index on the given columns to the DDL.
         */
        spatialIndex : function(columns, opts) {
            opts = opts || {};
            return this.index(columns, comb.merge({type : "spatial"}, opts));
        },

        /* Add a unique constraint on the given columns to the DDL.
         *
         *   unique(:name) # UNIQUE (name)
         *   */
        unique : function(columns, opts) {
            opts = opts || {};
            this.constraints.push(comb.merge({type : "unique", columns : comb.array.toArray(columns)}, opts));
        },



        /*
         Add a composite primary key constraint
         */
        __compositePrimaryKey : function(columns) {
            var args = comb.argsToArray(arguments).slice(1);
            var opts = args.pop() || {};
            this.constraints.push(comb.merge({type : "primaryKey", columns : columns}, opts));
        },

        /*
         Add a composite foreign key constraint
         */
        __compositeForeignKey : function(columns, opts) {
            this.constraints.push(comb.merge({type : "foreignKey", columns : columns}, opts));
        },

        getters : {
            // The name of the primary key for this generator, if it has a primary key.
            primaryKeyName : function() {
                return this.__primaryKey ? this.__primaryKey.name : null;
            }

        }

    },
    static : {
        /*Add a method for each of the given types that creates a column
         * with that type as a constant.  Types given should either already
         * be constants/classes or a capitalized string/symbol with the same name
         * as a constant/class.
         * */
        addTypeMethod : function(types) {
            if (comb.isArray(types)) {
                types.forEach(function(type) {
                    this.prototype[type] = function(name, opts) {
                        opts = opts || {};
                        return this.column(name, type, opts);
                    }
                }, this);
            } else {
                this.addTypeMethod(comb.argsToArray(arguments));
            }
        }
    }

});

Generator.addTypeMethod(GENERIC_TYPES);

exports.SchemaGenerator = function(ds, block) {
    var gen = new Generator(ds, block);
    var prox = comb.methodMissing(gen, function(name) {
        return function(type, opts) {
            name == name || null,opts = opts || {};
            if (name) {
                return this.column(name, type, opts);
            } else {
                throw new TypeError();
            }
        }
    }, Generator);
    block.apply(prox, [prox]);
    gen.columns = prox.columns;
    if (gen.__primaryKey && !gen.hasColumn(gen.primaryKeyName)) {
        gen.columns.unshift(gen.__primaryKey);
    }
    return gen;
}


/*
 *
 * */

var AlterTableGenerator = comb.define(null, {
    instance : {

        constructor : function(db, block) {
            this.db = db;
            this.operations = [];
            block.apply(this, [this]);

        },

        /* Add a column with the given name, type, and opts to the DDL for the table.
         * See Generator#column for the available options.
         *
         *   add_column(:name, String) # ADD COLUMN name varchar(255)
         *   */
        addColumn : function(name, type, opts) {
            opts = opts || {};
            this.operations.push(comb.merge({op : "addColumn", name : name, type : type}, opts));
        },

        /* Add a constraint with the given name and args to the DDL for the table.
         * See Generator#constraint.
         *
         *   add_constraint(:valid_name, :name.like('A%'))
         *   # ADD CONSTRAINT valid_name CHECK (name LIKE 'A%')
         *   */
        addConstraint : function(name) {
            var args = comb.argsToArray(arguments);
            var block = comb.isFunction(args[args.length - 1]) ? args.pop() : null;
            this.operations.push({op : "addConstraint", name : name, type : "check", check : block || args});
        },

        /** Add a unique constraint to the given column(s)
         *
         *   add_unique_constraint(:name) # ADD UNIQUE (name)
         *   add_unique_constraint(:name, :name=>:unique_name) # ADD CONSTRAINT unique_name UNIQUE (name)
         *   */
        addUniqueConstraint : function(columns, opts) {
            opts = opts || {};
            this.operations.push(comb.merge({op : "addConstraint", type : "unique", columns : comb.array.toArray(columns)}, opts));
        },

        /*
         * Add a foreign key with the given name and referencing the given table
         * to the DDL for the table.  See Generator#column for the available options.
         *
         * You can also pass an array of column names for creating composite foreign
         * keys. In this case, it will assume the columns exists and will only add
         * the constraint.
         *
         * NOTE: If you need to add a foreign key constraint to a single existing column
         * use the composite key syntax even if it is only one column.
         *
         *   add_foreign_key(:artist_id, :table) # ADD COLUMN artist_id integer REFERENCES table
         *   add_foreign_key([:name], :table) # ADD FOREIGN KEY (name) REFERENCES table
         */
        addForeignKey : function(name, table, opts) {
            opts = opts;
            if (comb.isArray(name)) {
                return this.__addCompositeForeignKey(name, table, opts);
            } else {
                return this.addColumn(name, "integer", comb.merge({table : table}, opts));
            }
        },
        /*
         * Add a full text index on the given columns to the DDL for the table.
         * See Generator#index for available options.
         */
        addFullTextIndex : function(columns, opts) {
            opts = opts || {};
            return this.addIndex(columns, comb.merge({type : "fullText"}, opts));
        },

        /*
         * Add an index on the given columns to the DDL for the table.  See
         * Generator#index for available options.
         *
         *   add_index(:artist_id) # CREATE INDEX table_artist_id_index ON table (artist_id)
         */
        addIndex : function(columns, opts) {
            opts = opts || {};
            this.operations.push(comb.merge({op : "addIndex", columns : comb.array.toArray(columns)}, opts));
        },

        /*
         * Add a primary key to the DDL for the table.  See Generator#column
         * for the available options.  Like +add_foreign_key+, if you specify
         * the column name as an array, it just creates a constraint:
         *
         *   add_primary_key(:id) # ADD COLUMN id serial PRIMARY KEY
         *   add_primary_key([:artist_id, :name]) # ADD PRIMARY KEY (artist_id, name)
         */
        addPrimaryKey : function(name, opts) {
            opts = opts || {};
            if (comb.isArray(name)) {
                return this.__addCompositePrimaryKey(name, opts);
            } else {
                opts = comb.merge({}, this.db.serialPrimaryKeyOptions, opts);
                delete opts.type;
                return this.addColumn(name, opts, opts);
            }
        },

        /*
         * Add a spatial index on the given columns to the DDL for the table.
         * See Generator#index for available options.
         * */
        addSpatialIndex : function(columns, opts) {
            opts = opts || {};
            addIndex(columns, comb.merge({}, {type : "spatial"}, opts))
        },
        /*
         * Remove a column from the DDL for the table.
         *
         *   drop_column(:artist_id) # DROP COLUMN artist_id
         */
        dropColumn : function(name) {
            this.operations.push({op : "dropColumn", name : name});
        },

        /*
         * Remove a constraint from the DDL for the table.
         *
         *   drop_constraint(:unique_name) # DROP CONSTRAINT unique_name
         */
        dropConstraint : function(name) {
            this.operations.push({op : "dropConstraint", name : name});
        },

        /*
         * Remove an index from the DDL for the table.
         *
         *   drop_index(:artist_id) # DROP INDEX table_artist_id_index
         *   drop_index([:a, :b]) # DROP INDEX table_a_b_index
         *   drop_index([:a, :b], :name=>:foo) # DROP INDEX foo
         */
        dropIndex : function(columns, opts) {
            opts = opts || {};
            this.operations.push(comb.merge({op : "dropIndex", columns : comb.array.toArray(columns)}, opts));
        },

        /*
         * Modify a column's name in the DDL for the table.
         *
         *   rename_column(:name, :artist_name) # RENAME COLUMN name TO artist_name
         */
        renameColumn : function(name, newName, opts) {
            opts = opts || {};
            this.operations.push(comb.merge({op : "renameColumn", name : name, newName : newName}, opts));
        },

        /*
         * Modify a column's default value in the DDL for the table.
         *
         *   set_column_default(:artist_name, 'a') # ALTER COLUMN artist_name SET DEFAULT 'a'
         *   */
        setColumnDefault : function(name, def) {
            this.operations.push({op : "setColumnDefault", name : name, "default" : def});
        },

        /*
         * Modify a column's type in the DDL for the table.
         *
         *   set_column_type(:artist_name, 'char(10)') # ALTER COLUMN artist_name TYPE char(10)
         */
        setColumnType : function(name, type, opts) {
            opts = opts || {}
            this.operations.push(comb.merge({op : "setColumnType", name : name, type : type}, opts));
        },

        /*
         * Modify a column's NOT NULL constraint.
         *
         *   set_column_allow_null(:artist_name, false) # ALTER COLUMN artist_name SET NOT NULL
         *   */
        setAllowNull : function(name, allowNull) {
            this.operations.push({op : "setColumnNull", name : name, "null" : allowNull});
        },
        /*
         * Add a composite primary key constraint
         * */
        __addCompositePrimaryKey : function(columns, opts) {
            this.operations.push(comb.merge({op : "addConstraint", type : "primaryKey", columns : columns}, opts));
        },

        /*
         * Add a composite foreign key constraint
         * */
        __addCompositeForeignKey : function(columns, table, opts) {
            this.operations.push(comb.merge({op : "addConstraint", type : "foreignKey", columns : columns, table : table}, opts));
        }
    }
}).as(exports, "AlterTableGenerator");



