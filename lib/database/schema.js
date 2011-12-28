var comb = require("comb"),
    argsToArray = comb.argsToArray,
    array = comb.array,
    hitch = comb.hitch,
    format = comb.string.format,
    Dataset = require("../dataset"),
    Promise = comb.Promise,
    PromiseList = comb.PromiseList,
    errors = require("../errors"),
    DatabaseError = errors.DatabaseError,
    generators = require("./schemaGenerators"),
    SchemaGenerator = generators.SchemaGenerator,
    AlterTableGenerator = generators.AlterTableGenerator,
    sql = require("../sql").sql,
    Identifier = sql.Identifier;


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

var Database = comb.define(null, {
    instance : {

        AUTOINCREMENT : AUTOINCREMENT,
        CASCADE : CASCADE,
        COMMA_SEPARATOR : COMMA_SEPARATOR,
        NO_ACTION : NO_ACTION,
        NOT_NULL : NOT_NULL,
        NULL : NULL,
        PRIMARY_KEY : PRIMARY_KEY,
        RESTRICT : RESTRICT,
        SET_DEFAULT : SET_DEFAULT,
        SET_NULL : SET_NULL,
        TEMPORARY : TEMPORARY,
        UNDERSCORE : UNDERSCORE,
        UNIQUE : UNIQUE,
        UNSIGNED :  UNSIGNED,


        constructor : function() {
            this._super(arguments);
            this.schemas = {};
        },

        /* Adds a column to the specified table. This method expects a column name,
         * a datatype and optionally a hash with additional constraints and options:
         *
         *   DB.add_column :items, :name, :text, :unique => true, :null => false
         *   DB.add_column :items, :category, :text, :default => 'ruby'
         *
         * See alter_table.
         * */
        addColumn : function(table) {
            var args = argsToArray(arguments).slice(1);
            return this.alterTable(table, function() {
                this.addColumn.apply(this, args)
            });
        },

        /* Adds an index to a table for the given columns:
         *
         *   DB.add_index :posts, :title
         *   DB.add_index :posts, [:author, :title], :unique => true
         *
         * Options:
         * * :ignore_errors - Ignore any DatabaseErrors that are raised
         *
         * See alter_table.
         * */
        addIndex : function(table, columns, options) {
            options = options || {};
            var e = options.ignoreErrors;
            var ret = new Promise();
            this.alterTable(table,
                function() {
                    this.addIndex(columns, options)
                }).then(hitch(ret, "callback"), function(e) {
                    if (e) {
                        ret.errback(e);
                    } else {
                        ret.callback();
                    }
                });
            return ret;
        },

        /* Alters the given table with the specified block. Example:
         *
         *   DB.alter_table :items do
         *     add_column :category, :text, :default => 'ruby'
         *     drop_column :category
         *     rename_column :cntr, :counter
         *     set_column_type :value, :float
         *     set_column_default :value, :float
         *     add_index [:group, :category]
         *     drop_index [:group, :category]
         *   end
         *
         * Note that +add_column+ accepts all the options available for column
         * definitions using create_table, and +add_index+ accepts all the options
         * available for index definition.
         *
         * See Schema::AlterTableGenerator and the {"Migrations and Schema Modification" guide}[link:files/doc/migration_rdoc.html].
         * */
        alterTable : function(name, generator, block) {
            if (comb.isFunction(generator)) {
                block = generator;
                generator = new AlterTableGenerator(this, block);
            }
            var ret = new Promise();
            this.__alterTableSqlList(name, generator.operations).then(hitch(this, function(res) {
                var sqls = array.flatten(res.map(function(r){return r[1]}));
                var l = sqls.length;
                var drop = hitch(this, function(i) {
                    if (i < l) {
                        var sql = sqls[i++];
                        this.executeDdl(sql).then(hitch(this, drop, i), hitch(ret, "errback"));
                    } else {
                        this.removeCachedSchema(name);
                        ret.callback();
                    }
                });
                drop(0);
            }));
            return ret;
        },

        /* Creates a table with the columns given in the provided block:
         *
         *   DB.create_table :posts do
         *     primary_key :id
         *     column :title, :text
         *     String :content
         *     index :title
         *   end
         *
         * Options:
         * :temp :: Create the table as a temporary table.
         * :ignore_index_errors :: Ignore any errors when creating indexes.
         *
         * See Schema::Generator and the {"Migrations and Schema Modification" guide}[link:files/doc/migration_rdoc.html].
         */
        createTable : function(name, options, block) {
            if (comb.isFunction(options)) {
                block = options;
                options = {};
            }
            this.removeCachedSchema(name);
            if (comb.isInstanceOf(options, SchemaGenerator)) {
                options = {generator : options};
            }
            var generator = options.generator || new SchemaGenerator(this, block);
            var ret = new Promise();
            this.__createTableFromGenerator(name, generator, options).chain(hitch(this, "__createTableIndexesFromGenerator", name, generator, options), hitch(ret, "callback")).then(hitch(ret, "callback"), hitch(ret, "callback"));
            return  ret;
        },

        /*Forcibly creates a table, attempting to drop it unconditionally (and catching any errors), then creating it.
         *
         *   DB.create_table!(:a){Integer :a}
         *   # DROP TABLE a
         *   # CREATE TABLE a (a integer)
         *   */
        forceCreateTable : function(name, options, block) {
            var ret = new Promise();
            this.dropTable(name).chainBoth(hitch(this, this.createTable, name, options, block)).then(hitch(ret, "callback"), hitch(ret, "callback"));
            return ret;
        },


        /*
         Creates the table unless the table already exists
         */
        createTableUnlessExists : function(name, options, block) {
            var ret = new Promise();
            this.tableExists(name).then(hitch(this, function(exists) {
                if (!exists) {
                    this.createTable(name, options, block).then(hitch(ret, "callback"), hitch(ret, "callback"));
                } else {
                    ret.callback();
                }
            }));
            return ret;
        },

        /* Creates a view, replacing it if it already exists:
         *
         *   DB.create_or_replace_view(:cheap_items, "SELECT * FROM items WHERE price < 100")
         *   DB.create_or_replace_view(:ruby_items, DB[:items].filter(:category => 'ruby'))
         *   */
        createOrReplaceView : function(name, source) {
            if (comb.isInstanceOf(source, Dataset)) {
                source = source.sql;
            }
            var ret = new Promise();
            this.executeDdl(format("CREATE OR REPLACE VIEW %s AS %s", this.__quoteSchemaTable(name), source)).then(hitch(this, function() {
                this.removeCachedSchema(name);
                ret.callback();
            }), hitch(ret, "callback"));
            return ret;
        },

        /*Creates a view based on a dataset or an SQL string:
         *
         *   DB.create_view(:cheap_items, "SELECT * FROM items WHERE price < 100")
         *   DB.create_view(:ruby_items, DB[:items].filter(:category => 'ruby'))
         *   */
        createView : function(name, source) {
            if (comb.isInstanceOf(source, Dataset)) {
                source = source.sql;
            }
            return this.executeDdl(format("CREATE VIEW %s AS %s", this.__quoteSchemaTable(name), source));
        },

        /* Removes a column from the specified table:
         *
         *   DB.drop_column :items, :category
         *
         * See alter_table.
         * */
        dropColumn : function(table, args) {
            var args = argsToArray(arguments).slice(1);
            return this.alterTable(table, function() {
                this.dropColumn.apply(this, args)
            });
        },

        /*Removes an index for the given table and column/s:
         *
         *   DB.drop_index :posts, :title
         *   DB.drop_index :posts, [:author, :title]
         *
         * See alter_table.
         * */
        dropIndex : function(table, columns, options) {
            var args = argsToArray(arguments).slice(1);
            return this.alterTable(table, function() {
                this.dropIndex.apply(this, args)
            });
        },

        /* Drops one or more tables corresponding to the given names:
         *
         *   DB.drop_table(:posts, :comments)
         *   */
        dropTable : function(names) {
            var ret = new Promise(), l = names.length;
            if (comb.isArray(names)) {
                var drop = hitch(this, function(i) {
                    if (i < l) {
                        var name = names[i++];
                        this.executeDdl(this.__dropTableSql(name)).then(hitch(this, function() {
                            this.removeCachedSchema(name);
                            drop(i);
                        }), hitch(ret, "errback"));
                    } else {
                        ret.callback();
                    }
                });
                drop(0);
                return ret;
            } else {
                return this.dropTable(argsToArray(arguments).filter(function(i) {
                    return comb.isString(i)
                }));

            }
        },

        /* Drops one or more tables corresponding to the given names:
         *
         *   DB.drop_table(:posts, :comments)
         *   */
        forceDropTable : function(names) {
            var ret = new Promise(), l = names.length;
            if (comb.isArray(names)) {
                var drop = hitch(this, function(i) {
                    if (i < l) {
                        var name = names[i++];
                        this.executeDdl(this.__dropTableSql(name)).both(hitch(this, function() {
                            this.removeCachedSchema(name);
                            drop(i);
                        }));
                    } else {
                        ret.callback();
                    }
                });
                drop(0);
                return ret;
            } else {
                return this.dropTable(argsToArray(arguments).filter(function(i) {
                    return comb.isString(i)
                }));

            }
        },

        /* Drops one or more views corresponding to the given names:
         *
         *   DB.drop_view(:cheap_items)
         *   */
        dropView : function(names) {
            var ret = new Promise(), l = names.length;
            if (comb.isArray(names)) {
                var drop = hitch(this, function(i) {
                    if (i < l) {
                        var name = names[i++];
                        this.executeDdl(format("DROP VIEW %s", this.__quoteSchemaTable(name))).then(hitch(this, function() {
                            this.removeCachedSchema(name);
                            drop(i);
                        }), hitch(ret, "errback"));
                    } else {
                        ret.callback();
                    }
                });
                drop(0);
                return ret;
            } else {
                return this.dropView(argsToArray(arguments));

            }
        },

        /* Renames a table:
         *
         *   DB.tables #=> [:items]
         *   DB.rename_table :items, :old_items
         *   DB.tables #=> [:old_items]
         *   */
        renameTable : function(name, newName) {
            var ret = new Promise();
            this.executeDdl(this.__renameTableSql(name, newName)).then(hitch(this, function() {
                this.removeCachedSchema(name);
                ret.callback();
            }), hitch(ret, "errback"));
            return ret;
        },

        /*Renames a column in the specified table. This method expects the current
         * column name and the new column name:
         *
         *   DB.rename_column :items, :cntr, :counter
         *
         * See alter_table.
         * */
        renameColumn : function(table, args) {
            args = argsToArray(arguments).slice(1);
            return this.alterTable(table, function() {
                this.renameColumn.apply(this, args)
            });
        },


        /* Sets the default value for the given column in the given table:
         *
         *   DB.set_column_default :items, :category, 'perl!'
         *
         * See alter_table.
         * */
        setColumnDefault : function(table, args) {
            args = argsToArray(arguments).slice(1);
            return this.alterTable(table, function() {
                this.setColumnDefault.apply(this, args);
            });
        },


        /* Set the data type for the given column in the given table:
         *
         *   DB.set_column_type :items, :price, :float
         *
         * See alter_table.
         * */
        setColumnType : function(table, args) {
            args = argsToArray(arguments).slice(1);
            return this.alterTable(table, function() {
                this.setColumnType.apply(this, args)
            });
        },

        /* The SQL to execute to modify the DDL for the given table name.  op
         * should be one of the operations returned by the AlterTableGenerator.
         * */
        __alterTableSql : function(table, op) {
            var ret = new comb.Promise();
            var quotedName = op.name ? this.__quoteIdentifier(op.name) : null;
            var alterTableOp = null;
            switch (op.op) {
                case "addColumn":
                    alterTableOp = format("ADD COLUMN %s", this.__columnDefinitionSql(op));
                    break;
                case "dropColumn":
                    alterTableOp = format("DROP COLUMN %s", quotedName);
                    break;
                case "renameColumn":
                    alterTableOp = format("RENAME COLUMN %s TO %s", quotedName, this.__quoteIdentifier(op.newName));
                    break;
                case "setColumnType":
                    alterTableOp = format("ALTER COLUMN %s TYPE %s", quotedName, this.typeLiteral(op));
                    break;
                case "setColumnDefault":
                    alterTableOp = format("ALTER COLUMN %s SET DEFAULT %s", quotedName, this.literal(op["default"]));
                    break;
                case "setColumnNull":
                    alterTableOp = format("ALTER COLUMN %s %s NOT NULL", quotedName, op.null ? "DROP" : "SET");
                    break;
                case "addIndex":
                    return ret.callback(this.__indexDefinitionSql(table, op));
                    break;
                case "dropIndex":
                    return ret.callback(this.__dropIndexSql(table, op));
                    break;
                case "addConstraint":
                    alterTableOp = format("ADD %s", this.__constraintDefinitionSql(op));
                    break;
                case "dropConstraint":
                    alterTableOp = format("DROP CONSTRAINT %s", quotedName);
                    break;
                default :
                    throw new DatabaseError("Invalid altertable operator");
            }
            return ret.callback(format("ALTER TABLE %s %s", this.__quoteSchemaTable(table), alterTableOp));
        },

        /*Array of SQL DDL modification statements for the given table,
         * corresponding to the DDL changes specified by the operations.
         * */
        __alterTableSqlList : function(table, operations) {
            return new PromiseList(operations.map(hitch(this, "__alterTableSql", table)));
        },

        /**
         * SQL DDL fragment containing the column creation SQL for the given column.
         *
         * @param column
         */
        __columnDefinitionSql : function(column) {
            var sql = format("%s %s", this.__quoteIdentifier(column.name), this.typeLiteral(column));
            column.unique && (sql += this._static.UNIQUE);
            (column.allowNull === false || column.null == false) && (sql += this._static.NOT_NULL);
            (column.allowNull === true || column.null == true) && (sql += this._static.NULL);
            !comb.isUndefined(column["default"]) && (sql += format(" DEFAULT %s", this.literal(column["default"])));
            column.primaryKey && (sql += this._static.PRIMARY_KEY);
            column.autoIncrement && (sql += " " + this.autoIncrementSql);
            column.table && (sql += this.__columnReferencesColumnConstraintSql(column));
            return sql;
        },

        /*
         SQL DDL fragment containing the column creation
         SQL for all given columns, used inside a CREATE TABLE block.
         */
        __columnListSql : function(generator) {
            return generator.columns.map(hitch(this, "__columnDefinitionSql")).concat(generator.constraints.map(hitch(this, "__constraintDefinitionSql"))).join(this._static.COMMA_SEPARATOR);
        },

        /**
         *SQL DDL fragment for column foreign key references (column constraints)
         */
        __columnReferencesColumnConstraintSql : function(column) {
            return this.__columnReferencesSql(column);
        },

        /**
         *SQL DDL fragment for column foreign key references
         */
        __columnReferencesSql : function(column) {
            var sql = format(" REFERENCES %s", this.__quoteSchemaTable(column.table));
            column.key && (sql += format("(%s)", array.toArray(column.key).map(this.__quoteIdentifier, this).join(this._static.COMMA_SEPARATOR)));
            column.onDelete && (sql += format(" ON DELETE %s", this.__onDeleteClause(column.onDelete)));
            column.onUpdate && (sql += format(" ON UPDATE %s", this.__onUpdateClause(column.onUpdate)));
            column.deferrable && (sql += " DEFERRABLE INITIALLY DEFERRED");
            return sql;
        },

        //SQL DDL fragment for table foreign key references (table constraints)
        __columnReferencesTableConstraintSql : function(constraint) {
            return format("FOREIGN KEY %s%s", this.literal(constraint.columns.map(function(c) {
                return comb.isString(c) ? sql.stringToIdentifier(c) : c;
            })), this.__columnReferencesSql(constraint));
        },

        //SQL DDL fragment specifying a constraint on a table.
        __constraintDefinitionSql : function(constraint) {
            var sql = constraint.name ? format("CONSTRAINT %s ", this.__quoteIdentifier(constraint.name)) : "";
            switch (constraint.type) {
                case "check":
                    var check = constraint.check;
                    sql += format("CHECK %s", this.__filterExpr(comb.isArray(check) && check.length == 1 ? check[0] : check));
                    break;
                case "primaryKey":
                    sql += format("PRIMARY KEY %s", this.literal(constraint.columns));
                    break;
                case "foreignKey":
                    sql += this.__columnReferencesTableConstraintSql(constraint);
                    break;
                case "unique":
                    sql += format("UNIQUE %s", this.literal(constraint.columns));
                    break;
                default:
                    throw new DatabaseError(format("Invalid constriant type %s, should be 'check', 'primaryKey', foreignKey', or 'unique'", contraint.type));
            }
            return sql;
        },

        //Execute the create table statements using the generator.
        __createTableFromGenerator : function(name, generator, options) {
            return this.executeDdl(this.__createTableSql(name, generator, options));
        },

        // Execute the create index statements using the generator.
        __createTableIndexesFromGenerator : function(name, generator, options) {
            var e = options.ignoreIndexErrors;
            var ret = new Promise();
            var promises = generator.indexes.map(function(index) {
                var ps = this.__indexSqlList(name, [index]).map(this.executeDdl, this);
                return new PromiseList(ps);
            }, this);
            if (promises.length) {
                new PromiseList(promises).then(hitch(ret, "callback"), hitch(ret, e ? "callback" : "errback"));
            } else {
                ret.callback();
            }
            return ret;
        },

        // DDL statement for creating a table with the given name, columns, and options
        __createTableSql : function(name, generator, options) {
            return format("CREATE %sTABLE %s (%s)", options.temp ? this.temporaryTableSql : "", this.__quoteSchemaTable(name), this.__columnListSql(generator));
        },

        // Default index name for the table and columns, may be too long
        // for certain databases.
        __defaultIndexName : function(tableName, columns) {
            var parts = this.__schemaAndTable(tableName);
            var schema = parts[0], table = parts[1];
            var index = [];
            if (schema && schema != this.defaultSchema) {
                index.push(schema);
            }
            index.push(table);
            index = index.concat(columns.map(function(c) {
                return comb.isString(c) ? c : this.literal(c).replace(/\W/g, "_")
            }, this));
            index.push("index");
            return index.join(this._static.UNDERSCORE);

        },

        //The SQL to drop an index for the table.
        __dropIndexSql : function(table, op) {
            return format("DROP INDEX %s", this.__quoteIdentifier(op.name || this.__defaultIndexName(table, op.columns)));
        },

        // SQL DDL statement to drop the table with the given name.
        __dropTableSql : function(name) {
            return format("DROP TABLE %s", this.__quoteSchemaTable(name));
        },

        // Proxy the filter_expr call to the dataset, used for creating constraints.
        __filterExpr : function(args, block) {
            var ds = this.__schemaUtiltyDataset;
            ds.literal(ds._filterExpr(ds, arguments));
        },


        // SQL DDL statement for creating an index for the table with the given name
        // and index specifications.
        __indexDefinitionSql : function(tableName, index) {
            var indexName = index.name || this.__defaultIndexName(tableName, index.columns);
            if (index.type) {
                throw new DatabaseError("Index types are not supported for this database");
            } else if (index.where) {
                throw new DatabaseError("Partial indexes are not supported for this database");
            } else {
                return format("CREATE %sINDEX %s ON %s %s", index.unique ? "UNIQUE " : "", this.__quoteIdentifier(indexName), this.__quoteSchemaTable(tableName), this.literal(index.columns.map(function(c) {
                    return comb.isString(c) ? new Identifier(c) : c;
                })));
            }
        },

        // Array of SQL DDL statements, one for each index specification,
        // for the given table.
        __indexSqlList : function(tableName, indexes) {
            return indexes.map(hitch(this, this.__indexDefinitionSql, tableName));
        },

        // SQL DDL ON DELETE fragment to use, based on the given action.
        //The following actions are recognized:
        //
        // * :cascade - Delete rows referencing this row.
        // * :no_action (default) - Raise an error if other rows reference this
        //   row, allow deferring of the integrity check.
        // * :restrict - Raise an error if other rows reference this row,
        //   but do not allow deferring the integrity check.
        // * :set_default - Set columns referencing this row to their default value.
        // * :set_null - Set columns referencing this row to NULL.
        __onDeleteClause : function(action) {
            return this._static[action.toUpperCase()] || this._static.NO_ACTION;
        },

        //Proxy the quote_schema_table method to the dataset
        __quoteSchemaTable : function(table) {
            return this.__schemaUtiltyDataset.quoteSchemaTable(table);
        },

        // Proxy the quote_identifier method to the dataset, used for quoting tables and columns.
        __quoteIdentifier : function(v) {
            return this.__schemaUtiltyDataset.quoteIdentifier(v);
        },

        // SQL DDL statement for renaming a table.
        __renameTableSql : function(name, newName) {
            return format("ALTER TABLE %s RENAME TO %s", this.__quoteSchemaTable(name), this.__quoteSchemaTable(newName));
        },

        // Remove the cached schema_utility_dataset, because the identifier
        // quoting has changed.
        __resetSchemaUtilityDataset : function() {
            this.__schemaUtiltyDs = null;
        },

        // Split the schema information from the table
        __schemaAndTable : function(tableName) {
            return  this.__schemaUtiltyDataset.schemaAndTable(tableName);
        },

        // Return true if the given column schema represents an autoincrementing primary key.
        schemaAutoincrementingPrimaryKey : function(schema) {
            return !!schema.primaryKey;
        },

        // SQL fragment specifying the type of a given column.
        typeLiteral : function(column) {
            return this.__typeLiteralGeneric(column);
        },

        // SQL fragment specifying the full type of a column,
        // consider the type with possible modifiers.
        __typeLiteralGeneric : function(column) {
            var type = column.type;
            var meth = "__typeLiteralGeneric";
            var isString = comb.isString(type);
            if (type === String || (isString && type.match(/string/i))) {
                meth += "String";
            } else if (type === Number || (isString && type.match(/number/i))) {
                meth += "Numeric";
            } else if (type == Date || (isString && type.match(/date/i))) {
                meth += "Date";
            } else if (type == Boolean || (isString && type.match(/boolean/i))) {
                meth += "Boolean";
            } else {
                return this.__typeLiteralSpecific(column);
            }
            return this[meth](column);
        },

        // Sequel uses the date type by default for Dates.
        // if
        // :onlyTime is present then time is used, if
        // :timeStamp is present then timestamp is used, if
        // :dateTime is present then datetime is used
        // else date is used
        __typeLiteralGenericDate : function(column) {
            var type = column.type;
            return column.onlyTime || column.type.match(/^time$/i) ? "time" : column.timeStamp || column.type.match(/^timestamp$/i) ? "timestamp" : column.dateTime || column.type.match(/^datetime/i) ? "datetime" : "date";
        },

        // Sequel uses the boolean type by default for Boolean class
        __typeLiteralGenericBoolean : function(column) {
            return "boolean";
        },

        // Sequel uses the numeric type by default for NumericTypes
        // If a size is given, it is used, otherwise, it will default to whatever
        // the database default is for an unsized value.
        // if
        // :isInt is present the int is used, if
        // :isDouble is present then double precision is used
        __typeLiteralGenericNumeric : function(column) {
            return column.size ? format("numeric(%s)", array.toArray(column.size).join(', ')) : column.isInt ? "integer" : column.isDouble ? "double precision" : "numeric";
        },

        //Sequel uses the varchar type by default for Strings.  If a
        // size isn't present, Sequel assumes a size of 255.  If the
        // :fixed option is used, Sequel uses the char type.  If the
        // :text option is used, Sequel uses the :text type.
        __typeLiteralGenericString : function(column) {
            return column.text ? "text" : format("%s(%s)", column.fixed ? "char" : "varchar", column.size || 255);
        },

        // SQL fragment for the given type of a column if the column is not one of the
        // generic types specified with a native javascript type class.
        __typeLiteralSpecific : function(column) {
            var type = column.type;
            type = type == "double" ? "double precision" : type;
            if (type == "varchar") column.size = comb.isNumber(column.size) ? column.size : 255;
            var elements = column.size || column.elements;
            return format("%s%s%s", type, elements ? this.literal(array.toArray(elements)) : "", column.unsigned ? " UNSIGNED" : "");
        },



        getters : {
            /* The SQL string specify the autoincrement property, generally used by
             * primary keys.
             * */
            autoIncrementSql : function() {
                return this._static.AUTOINCREMENT;
            },

            temporaryTableSql : function() {
                return this._static.TEMPORARY;
            },

            __schemaUtiltyDataset : function() {
                this.__schemaUtiltyDs = this.__schemaUtiltyDs || this.dataset;
                return this.__schemaUtiltyDs;
            }
        }

    },

    static : {
        AUTOINCREMENT : AUTOINCREMENT,
        CASCADE : CASCADE,
        COMMA_SEPARATOR : COMMA_SEPARATOR,
        NO_ACTION : NO_ACTION,
        NOT_NULL : NOT_NULL,
        NULL : NULL,
        PRIMARY_KEY : PRIMARY_KEY,
        RESTRICT : RESTRICT,
        SET_DEFAULT : SET_DEFAULT,
        SET_NULL : SET_NULL,
        TEMPORARY : TEMPORARY,
        UNDERSCORE : UNDERSCORE,
        UNIQUE : UNIQUE,
        UNSIGNED :  UNSIGNED

    }
}).as(module);

//
//      /*loadSchema : function(name) {
//            var promise = null;
//            promise = new Promise();
//            var schema = this.schemas;
//            if (!(name in schema)) {
//                this.schema(name)
//                    .then(hitch(this, function(table) {
//                    if (table) {
//                        var schema;
//                        if ((schema = this.schemas) == null) {
//                            schema = this.schemas = {};
//                        }
//                        //put the schema under the right database
//                        schema[name] = table;
//                    }
//                    promise.callback(table);
//                }), hitch(promise, "errback"));
//            } else {
//                promise.callback(schema[name]);
//            }
//
//            return promise;
//        },
//
//        schema : function() {
//            throw new NotImplemented("schema must be implemented by the adapter");
//        },
//
//        /**
//         * <p>Creates a new table. This function should be used while performing a migration.</p>
//         * <p>If the table should be created in another DB then the table should have the database set on it.</p>
//         *
//         * @example
//         * //default database table creation
//         * moose.createTable("test", function(table){
//         *     table.column("id", types.INT())
//         *     table.primaryKey("id");
//         * });
//         *
//         * //create a table in another database
//         * moose.createTable("test", function(table){
//         *     table.database = "otherDB";
//         *     table.column("id", types.INT())
//         *     table.primaryKey("id");
//         * });
//         *
//         *
//         * @param {String} tableName the name of the table to create
//         * @param {Funciton} cb this funciton is callback with the table
//         *      - All table properties should be specified within this block
//         *
//         * @return {comb.Promise} There are two different results that the promise can be called back with.
//         * <ol>
//         *     <li>If a migration is currently being performed then the promise is called back with a
//         *     function that should be called to actually perform the migration.</li>
//         *     <li>If the called outside of a migration then the table is created immediately and
//         *     the promise is called back with the result.</li>
//         * </ol>
//         *
//         * */
//        createTable : function(tableName, cb) {
//            var table = new Table(tableName, {}), ret = new comb.Promise();
//            cb(table);
//            //add it to the moose schema map
//            var db = this.client.database, schema;
//            if ((schema = this.schemas) == null) {
//                schema = this.schemas = {};
//            }
//            schema[tableName] = table;
//            this.client.query(table.createTableSql).then(hitch(ret, "callback", true), hitch(ret, "errback"));
//            return ret;
//        },
//
//        dropAndCreateTable : function(table, cb) {
//            var ret = new comb.Promise();
//            this.dropTable(table)
//                .chain(hitch(this, "createTable", table, cb), hitch(ret, "errback"))
//                .then(hitch(ret, "callback"), hitch(ret, "errback"));
//            return ret;
//        },
//
//
//        /**
//         * Drops a table
//         *
//         * @example
//         *
//         * //drop table in default database
//         * moose.dropTable("test");
//         *
//         * //drop table in another database.
//         * moose.dropTable("test", "otherDB");
//         *
//         * @param {String} table the name of the table
//         * @param {String} [database] the database that the table resides in, if a database is not
//         *                            provided then the default database is used.
//         * @return {comb.Promise} There are two different results that the promise can be called back with.
//         * <ol>
//         *     <li>If a migration is currently being performed then the promise is called back with a
//         *     function that should be called to actually perform the migration.</li>
//         *     <li>If the called outside of a migration then the table is dropped immediately and
//         *     the promise is called back with the result.</li>
//         * </ol>
//         *
//        dropTable : function(table, database) {
//            table = new Table(table, {database : database}),ret = new comb.Promise();
//            //delete from the moose schema map
//            var db = database || this.client.database;
//            var schema = this.schemas;
//            if (schema && table in schema) {
//                delete schema[table];
//            }
//            this.client.query(table.createTableSql).then(hitch(ret, "callback", true), hitch(ret, "errback"));
//            return ret;
//
//        },
//
//        /**
//         * Alters a table
//         *
//         * @example :
//         *
//         * //alter table in default database
//         * moose.alterTable("test", function(table){
//         *     table.rename("test2");
//         *     table.addColumn("myColumn", types.STRING());
//         * });
//         *
//         * //alter table in another database
//         * moose.alterTable("test", "otherDB", function(table){
//         *     table.rename("test2");
//         *     table.addColumn("myColumn", types.STRING());
//         * });
//         *
//         * @param {String} name The name of the table to alter.
//         * @param {String} [database] the database that the table resides in, if a database is not
//         *                            provided then the default database is used.
//         * @param {Function} cb the function to execute with the table passed in as the first argument.
//         *
//         * @return {comb.Promise} There are two different results that the promise can be called back with.
//         * <ol>
//         *     <li>If a migration is currently being performed then the promise is called back with a
//         *     function that should be called to actually perform the migration.</li>
//         *     <li>If the called outside of a migration then the table is altered immediately and
//         *     the promise is called back with the result.</li>
//         * </ol>
//         *
//        alterTable : function(name, cb) {
//            if (comb.isFunction(database)) {
//                cb = database;
//            }
//            var ret = new Promise();
//            var db = this.client.database;
//            var schema = this.schemas;
//            if (schema && name in schema) {
//                delete schema[name];
//            }
//            this.loadSchema(name, db).then(function(table) {
//                cb(table);
//                this.client.query(table.alterTableSql).then(hitch(ret, "callback", true), hitch(ret, "errback"));
//            }, hitch(ret, "errback"));
//            return ret;
//
//        },
//
//
//        //ALTER TABLE SHORTCUTS
//
//
//        addColumn : function(table) {
//            var ret = new comb.Promise();
//            var args = argsToArray(arguments).slice(1);
//            this.alterTable(table,
//                function(table) {
//                    table.dropColumn.apply(table, args);
//                }).then(hitch(ret, "callback"), hitch(ret, "errback"));
//            return ret;
//        },
//
//        renameColumn : function(table) {
//            var ret = new comb.Promise();
//            var args = argsToArray(arguments).slice(1);
//            this.alterTable(table,
//                function(table) {
//                    table.renameColumn.apply(table, args);
//                }).then(hitch(ret, "callback"), hitch(ret, "errback"));
//            return ret;
//        },
//
//        dropColumn : function(table) {
//            var ret = new comb.Promise();
//            var args = argsToArray(arguments).slice(1);
//            this.alterTable(table,
//                function(table) {
//                    table.dropColumn.apply(table, args);
//                }).then(hitch(ret, "callback"), hitch(ret, "errback"));
//            return ret;
//        },
//
//        renameTable : function(table, newName) {
//            var ret = new comb.Promise();
//            this.alterTable(table,
//                function(table) {
//                    table.rename(newName);
//                }).then(hitch(ret, "callback"), hitch(ret, "errback"));
//            return ret;
//        },
//
//        setColumnDefault : function(table) {
//            var ret = new comb.Promise();
//            var args = argsToArray(arguments).slice(1);
//            this.alterTable(table,
//                function(table) {
//                    table.setColumnDefault.apply(table, args);
//                }).then(hitch(ret, "callback"), hitch(ret, "errback"));
//            return ret;
//        },
//
//        setColumnType : function(table) {
//            var ret = new comb.Promise();
//            var args = argsToArray(arguments).slice(1);
//            this.alterTable(table,
//                function(table) {
//                    table.setColumnType.apply(table, args);
//                }).then(hitch(ret, "callback"), hitch(ret, "errback"));
//            return ret;
//        }  */