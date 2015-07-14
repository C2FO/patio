"use strict";
var comb = require("comb"),
    asyncArray = comb.async.array,
    isFunction = comb.isFunction,
    argsToArray = comb.argsToArray,
    array = comb.array,
    isArray = comb.isArray,
    isString = comb.isString,
    isUndefined = comb.isUndefined,
    isNumber = comb.isNumber,
    toArray = comb.array.toArray,
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
    Time = sql.Time,
    TimeStamp = sql.TimeStamp,
    DateTime = sql.DateTime,
    Year = sql.Year,
    Float = sql.Float,
    Decimal = sql.Decimal,
    Json = sql.Json,
    isInstanceOf = comb.isInstanceOf,
    Identifier = sql.Identifier,
    QualifiedIdentifier = sql.QualifiedIdentifier,
    define = comb.define;


define(null, {
    instance: {
        /**@lends patio.Database.prototype*/

        /**@ignore*/
        constructor: function () {
            this._super(arguments);
            this.schemas = {};
        },

        /**
         * Adds a column to the specified table. This method expects a column name,
         * a datatype and optionally a hash with additional constraints and options:
         *
         * <p>
         *     This method is a shortcut to {@link patio.Database#alterTable} with an
         *     addColumn call.
         * </p>
         *
         *
         * @example
         * //Outside of a table
         * //ALTER TABLE test ADD COLUMN name text UNIQUE'
         * DB.addColumn("test", "name", "text", {unique : true});
         *
         * @param {String} table the table to add the column to.
         * @param {String} column the name of the column to add.
         * @param type datatype of the column
         * @param {Object} [opts] additional options that can be used when adding a column.
         * @param {Boolean} [opts.primaryKey] set to true if this column is a primary key.
         * @param {Boolean} [opts.allowNull] whether or not this column should allow null.
         * @param {Boolean} [opts.unique] set to true to add a UNIQUE constraint to a column,
         *
         * @return {Promise} a promise that is resolved when the ADD COLUMN action is complete.
         **/
        addColumn: function (table, column, type, opts) {
            var args = argsToArray(arguments).slice(1);
            return this.alterTable(table, function () {
                this.addColumn.apply(this, args);
            });
        },

        /**
         * Adds an index to a table for the given columns
         *
         * <p>
         *     This method is a shortcut to {@link patio.Database#alterTable} with an
         *     addIndex call.
         * </p>
         * @example
         * DB.addIndex("test", "name", {unique : true});
         *      //=> 'CREATE UNIQUE INDEX test_name_index ON test (name)'
         * DB.addIndex("test", ["one", "two"]);
         *      //=> ''CREATE INDEX test_one_two_index ON test (one, two)''
         *
         * @param {String} table the table to add the index to.
         * @param {String|String[]} columns the name of the column/s to create an index for.
         * @param {Object} [options] additional options that can be used when adding an index.
         * @param {Boolean} [options.unique] set to true if this this index should have a UNIQUE constraint.
         * @param {Boolean} [options.ignoreErrors] set to true to ignore errors.
         *
         * @return {Promise} a promise that is resolved when the CREATE INDEX action is complete.
         * */
        addIndex: function (table, columns, options) {
            options = options || {};
            var ignoreErrors = options.ignoreErrors === true;
            return this.alterTable(table, function () {
                this.addIndex(columns, options);
            }).chain(function (res) {
                return res;
            }, function (err) {
                if (!ignoreErrors) {
                    throw err;
                }
            });
        },

        /**
         *  Removes a column from the specified table.
         * <p>
         *     This method is a shortcut to {@link patio.Database#alterTable} with an
         *     dropColumn call.
         * </p>
         *
         * @example
         *   DB.dropColumn("items", "category");
         *          //=> 'ALTER TABLE items DROP COLUMN category',
         *
         * @param {String|patio.sql.Identifier} table the table to alter.
         * @param {String|patio.sql.Identifier} column the column to drop.
         *
         * @return {Promise} a promise that is resolved once the DROP COLUMN action is complete.
         * */
        dropColumn: function (table, column) {
            column = argsToArray(arguments).slice(1);
            return this.alterTable(table, function () {
                this.dropColumn.apply(this, column);
            });
        },

        /**
         * Removes an index for the given table and column/s.
         *
         * <p>
         *     This method is a shortcut to {@link patio.Database#alterTable} with an
         *     dropIndex call.
         * </p>
         *
         * @example
         *   DB.dropIndex("posts", "title");
         *      //=>'DROP INDEX posts_title_index
         *   DB.dropIndex("posts", ["author", "title"]);
         *      //'DROP INDEX posts_author_title_index'
         *
         * @param {String|patio.sql.Identifier} table the table to alter.
         * @param {String|patio.sql.Identifier} column the name of the column/s the index was created from.
         *
         * @return {Promise} a promise that is resolved once the DROP INDEX action is complete.
         * */
        dropIndex: function (table, columns, options) {
            var args = argsToArray(arguments).slice(1);
            return this.alterTable(table, function () {
                this.dropIndex.apply(this, args);
            });
        },

        /**
         * Renames a column in the specified table.
         *
         * <p>
         *     This method is a shortcut to {@link patio.Database#alterTable} with an
         *     renameColumn call.
         * </p>
         *
         * @example
         * DB.renameColumn("items", "cntr", "counter");
         *      //=> ALTER TABLE items RENAME COLUMN cntr TO counter
         *
         * @param {String|patio.sql.Identifier} table the table to alter.
         * @param {String|patio.sql.Identifier} column the name of the column to rename.
         * @param {String|patio.sql.Identifier} newColumn the new name of the column.
         *
         * @return {Promise} a promise that is resolved once the RENAME COLUMN action is complete.
         * */
        renameColumn: function (table, column, newColumn) {
            var args = argsToArray(arguments).slice(1);
            return this.alterTable(table, function () {
                this.renameColumn.apply(this, args);
            });
        },


        /**
         *Sets the default value for the given column in the given table:
         *
         * <p>
         *     This method is a shortcut to {@link patio.Database#alterTable} with an
         *     setColumnDefault call.
         * </p>
         *
         * @example
         * DB.setColumnDefault("items", "category", "misc");
         *      //=> ALTER TABLE items ALTER COLUMN category SET DEFAULT 'misc'
         *
         * @param {String|patio.sql.Identifier} table the table to alter.
         * @param {String|patio.sql.Identifier} column the name of the column to set the DEFAULT on.
         * @param  def the new default value of the column.
         *
         * @return {Promise} a promise that is resolved once the SET DEFAULT action is complete.
         * */
        setColumnDefault: function (table, column, def) {
            var args = argsToArray(arguments).slice(1);
            return this.alterTable(table, function () {
                this.setColumnDefault.apply(this, args);
            });
        },


        /**
         *  Set the data type for the given column in the given table:
         * <p>
         *     This method is a shortcut to {@link patio.Database#alterTable} with an
         *     setColumnType call.
         * </p>
         *
         * @example
         * DB.setColumnType("items", "category", String);
         *      //=> ALTER TABLE items ALTER COLUMN category TYPE varchar(255)
         *
         * @param {String|patio.sql.Identifier} table the table to alter.
         * @param {String|patio.sql.Identifier} column the name of the column to set the TYPE on.
         * @param  type the datatype of the column.
         *
         * @return {Promise} a promise that is resolved once the SET TYPE action is complete.
         * */
        setColumnType: function (table, column, type) {
            var args = argsToArray(arguments).slice(1);
            return this.alterTable(table, function () {
                this.setColumnType.apply(this, args);
            });
        },


        /**
         *  Alters the given table with the specified block.
         *  <p>
         *      <b>NOTE:</b> The block is invoked in the scope of the table that is being altered. The block
         *      is also called with the table as the first argument. Within the block you must use
         *      <b>this</b>(If the block has not been bound to a different scope), or the table object
         *      that is passed in for all alter table operations. See {@link patio.AlterTableGenerator} for
         *      avaiable operations.
         *  </p>
         *
         *  <p>
         *      <b>Note</b> that addColumn accepts all the options available for column
         *      definitions using createTable, and addIndex accepts all the options
         *      available for index definition.
         *  </p>
         *
         * @example
         *   //using the table object
         *   DB.alterTable("items", function(table){
         *      //you must use the passed in table object.
         *     table.addColumn("category", "text", {default : 'javascript'});
         *     table.dropColumn("category");
         *     table.renameColumn("cntr", "counter");
         *     table.setColumnType("value", "float");
         *     table.setColumnDefault("value", "float");
         *     table.addIndex(["group", "category"]);
         *     table.dropIndex(["group", "category"]);
         *   });
         *
         *   //using this
         *    DB.alterTable("items", function(){
         *     this.addColumn("category", "text", {default : 'javascript'});
         *     this.dropColumn("category");
         *     this.renameColumn("cntr", "counter");
         *     this.setColumnType("value", "float");
         *     this.setColumnDefault("value", "float");
         *     this.addIndex(["group", "category"]);
         *     this.dropIndex(["group", "category"]);
         *   });
         *
         *   //This will not work
         *   DB.alterTable("items", comb.hitch(someObject, function(){
         *    //This is called in the scope of someObject so this
         *    //will not work and will throw an error
         *     this.addColumn("category", "text", {default : 'javascript'});
         *   }));
         *
         *    //This will work
         *   DB.alterTable("items", comb.hitch(someObject, function(table){
         *    //This is called in the scope of someObject so you must
         *    //use the table argument
         *     table.category("text", {default : 'javascript'});
         *   }));
         *
         *
         * @param {String|patio.sql.Identifier} table to the table to perform the ALTER TABLE operations on.
         * @param {Function} block the block to invoke for the ALTER TABLE operations
         *
         * @return {Promise} a promise that is resolved once all ALTER TABLE operations have completed.
         * */
        alterTable: function (name, generator, block) {
            if (isFunction(generator)) {
                block = generator;
                generator = new AlterTableGenerator(this, block);
            }
            var self = this;
            return this.__alterTableSqlList(name, generator.operations).chain(function (res) {
                return asyncArray(comb(res).pluck("1")
                    .flatten())
                    .forEach(function (sql) {
                        return self.executeDdl(sql);
                    })
                    .chain(function () {
                        return self.removeCachedSchema(name);
                    });
            });
        },

        /**
         * Creates a table with the columns given in the provided block:
         *
         * <p>
         *      <b>NOTE:</b> The block is invoked in the scope of the table that is being created. The block
         *      is also called with the table as the first argument. Within the block you must use
         *      <b>this</b>(If the block has not been bound to a different scope), or the table object
         *      that is passed in for all create table operations. See {@link patio.SchemaGenerator} for
         *      available operations.
         *  </p>
         *
         *
         * @example
         *
         *   //using the table to create the table
         *   DB.createTable("posts", function(table){
         *     table.primaryKey("id");
         *     table.column('title", "text");
         *     //you may also invoke the column name as
         *     //function on the table
         *     table.content(String);
         *     table.index(title);
         *   });
         *
         *   //using this to create the table
         *   DB.createTable("posts", function(){
         *     this.primaryKey("id");
         *     this.column('title", "text");
         *     //you may also invoke the column name as
         *     //function on the table
         *     this.content(String);
         *     this.index(title);
         *   });
         *
         * @param {String|patio.sql.Identifier} name the name of the table to create.
         * @param {Object} [options] an optional options object
         * @param {Boolean} [options.temp] set to true if this table is a TEMPORARY table.
         * @param {Boolean} [options.ignoreIndexErrors] Ignore any errors when creating indexes.
         * @param {Function} block the block to invoke when creating the table.
         *
         * @return {Promise} a promise that is resolved when the CREATE TABLE action is completed.
         *
         */
        createTable: function (name, options, block) {
            if (isFunction(options)) {
                block = options;
                options = {};
            }
            this.removeCachedSchema(name);
            if (isInstanceOf(options, SchemaGenerator)) {
                options = {generator: options};
            }
            var generator = options.generator || new SchemaGenerator(this, block), self = this;
            return this.__createTableFromGenerator(name, generator, options).chain(function () {
                return self.__createTableIndexesFromGenerator(name, generator, options);
            });
        },

        /**
         * Forcibly creates a table, attempting to drop it unconditionally (and catching any errors), then creating it.
         * <p>
         *      See {@link patio.Database#createTable} for parameter types.
         * </p>
         *
         * @example
         *  // DROP TABLE a
         *  // CREATE TABLE a (a integer)
         *   DB.forceCreateTable("a", function(){
         *      this.a("integer");
         *   });
         *
         **/
        forceCreateTable: function (name, options, block) {
            var self = this;
            return this.dropTable(name).chainBoth(function () {
                return self.createTable(name, options, block);
            });
        },


        /**
         * Creates the table unless the table already exists.
         * <p>
         *      See {@link patio.Database#createTable} for parameter types.
         * </p>
         */
        createTableUnlessExists: function (name, options, block) {
            var self = this;
            return this.tableExists(name).chain(function (exists) {
                if (!exists) {
                    return self.createTable(name, options, block);
                }
            });
        },

        /**
         * Creates a view, replacing it if it already exists:
         * @example
         *   DB.createOrReplaceView("cheapItems", "SELECT * FROM items WHERE price < 100");
         *      //=> CREATE OR REPLACE VIEW cheapItems AS SELECT * FROM items WHERE price < 100
         *   DB.createOrReplaceView("miscItems", DB.from("items").filter({category : 'misc'}));
         *     //=> CREATE OR REPLACE VIEW miscItems AS SELECT * FROM items WHERE category = 'misc'
         *
         * @param {String|patio.sql.Identifier} name the name of the view to create.
         * @param {String|patio.Dataset} source the SQL or {@link patio.Dataset} to use as the source of the
         * view.
         *
         * @return {Promise} a promise that is resolved when the CREATE OR REPLACE VIEW action is complete.
         **/
        createOrReplaceView: function (name, source, opts) {
            if (isInstanceOf(source, Dataset)) {
                source = source.sql;
            }
            opts = opts || {};
            opts.replace = true;
            var self = this;
            return this.executeDdl(this.__createViewSql(name, source, opts)).chain(function () {
                return self.removeCachedSchema(name);
            });
        },

        /**
         * Creates a view based on a dataset or an SQL string:
         * @example
         *   DB.createView("cheapItems", "SELECT * FROM items WHERE price < 100");
         *      //=> CREATE VIEW cheapItems AS SELECT * FROM items WHERE price < 100
         *   DB.createView("miscItems", DB.from("items").filter({category : 'misc'}));
         *     //=> CREATE  VIEW miscItems AS SELECT * FROM items WHERE category = 'misc'
         *
         * @param {String|patio.sql.Identifier} name the name of the view to create.
         * @param {String|patio.Dataset} source the SQL or {@link patio.Dataset} to use as the source of the
         * view.
         **/
        createView: function (name, source, opts) {
            if (isInstanceOf(source, Dataset)) {
                source = source.sql;
            }
            return this.executeDdl(this.__createViewSql(name, source, opts));
        },

        /**
         *  Drops one or more tables corresponding to the given names.
         *
         * @example
         * DB.dropTable("test");
         *      //=>'DROP TABLE test'
         * DB.dropTable("a", "bb", "ccc");
         *      //=>'DROP TABLE a',
         *      //=>'DROP TABLE bb',
         *      //=>'DROP TABLE ccc'
         *
         * @param {String|String[]|patio.sql.Identifier|patio.sql.Identifier[]} names the names of the tables
         * to drop.
         *
         * @return {Promise} a promise that is resolved once all tables have been dropped.
         **/
        dropTable: function (names) {
            if (!isArray(names)) {
                names = comb(arguments).toArray();
            }
            names = names.filter(function (t) {
                return isString(t) || isInstanceOf(t, Identifier, QualifiedIdentifier);
            });
            var self = this;
            return asyncArray(names).forEach(function (name) {
                return self.executeDdl(self.__dropTableSql(name)).chain(function () {
                    return self.removeCachedSchema(name);
                });
            }, 1);
        },

        /**
         *  Forcible drops one or more tables corresponding to the given names, ignoring errors.
         *
         * @example
         * DB.dropTable("test");
         *      //=>'DROP TABLE test'
         * DB.dropTable("a", "bb", "ccc");
         *      //=>'DROP TABLE a',
         *      //=>'DROP TABLE bb',
         *      //=>'DROP TABLE ccc'
         *
         * @param {String|String[]|patio.sql.Identifier|patio.sql.Identifier[]} names the names of the tables
         * to drop.
         *
         * @return {Promise} a promise that is resolved once all tables have been dropped.
         **/
        forceDropTable: function (names) {
            if (!isArray(names)) {
                names = comb(arguments).toArray();
            }
            names = names.filter(function (t) {
                return isString(t) || isInstanceOf(t, Identifier, QualifiedIdentifier);
            });
            var l = names.length, ret = new Promise(), self = this;
            var drop = function (i) {
                if (i < l) {
                    var name = names[i++];
                    self.executeDdl(self.__dropTableSql(name)).both(function () {
                        self.removeCachedSchema(name);
                        drop(i);
                    });
                } else {
                    ret.callback();
                }
            };
            drop(0);

            return ret.promise();
        },

        /**
         *  Drops one or more views corresponding to the given names.
         *
         * @example
         * DB.dropView("test_view");
         *      //=>'DROP VIEW test_view'
         * DB.dropTable("test_view_1", "test_view_2", "test_view_3");
         *      //=>'DROP VIEW test_view_1',
         *      //=>'DROP VIEW test_view_2',
         *      //=>'DROP VIEW test_view_3'
         *
         * @param {String|String[]|patio.sql.Identifier|patio.sql.Identifier[]} names the names of the views
         * to drop.
         * @param {Hash} [opts={}] Additional options that very based on the database adapter.
         *
         * @return {Promise} a promise that is resolved once the view/s have been dropped.
         **/
        dropView: function (names, opts) {
            if (isArray(names)) {
                opts = opts || {};
                var self = this;
                return asyncArray(names).forEach(function (name) {
                    return self.executeDdl(self.__dropViewSql(name, opts)).chain(function () {
                        self.removeCachedSchema(name);
                    });
                }, null, 1).chain(function () {
                    return null;
                });
            } else {
                var args = argsToArray(arguments);
                if (comb.isHash(args[args.length - 1])) {
                    opts = args.pop();
                }
                return this.dropView(args.filter(function (t) {
                    return isString(t) || isInstanceOf(t, Identifier, QualifiedIdentifier);
                }), opts);

            }
        },

        /**
         * Renames a table.
         *
         * @example
         * comb.executeInOrder(DB, function(DB){
         *   DB.tables(); //=> ["items"]
         *   DB.renameTable("items", "old_items");
         *          //=>'ALTER TABLE items RENAME TO old_items'
         *   DB.tables; //=> ["old_items"]
         *});
         *
         * @param {String|patio.sql.Identifier} name the name of the table to rename
         * @param {String|patio.sql.Identifier} newName the new name of the table
         * @return {Promise} a promise that is resolved once the table is renamed.
         **/
        renameTable: function (name, newName) {
            var self = this;
            return this.executeDdl(this.__renameTableSql(name, newName)).chain(function () {
                self.removeCachedSchema(name);
            }).promise();

        },

        /**
         * @private
         *  The SQL to execute to modify the DDL for the given table name.  op
         * should be one of the operations returned by the AlterTableGenerator.
         * */
        __alterTableSql: function (table, op) {
            var ret = new Promise();
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
                alterTableOp = format("ALTER COLUMN %s %s NOT NULL", quotedName, op["null"] ? "DROP" : "SET");
                break;
            case "addIndex":
                return ret.callback(this.__indexDefinitionSql(table, op)).promise();
            case "dropIndex":
                return ret.callback(this.__dropIndexSql(table, op)).promise();
            case "addConstraint":
                alterTableOp = format("ADD %s", this.__constraintDefinitionSql(op));
                break;
            case "dropConstraint":
                alterTableOp = format("DROP CONSTRAINT %s", quotedName);
                break;
            case "noInherit":
                alterTableOp = format("NO INHERIT %s", quotedName);
                break;
            default :
                throw new DatabaseError("Invalid altertable operator");
            }
            return ret.callback(format("ALTER TABLE %s %s", this.__quoteSchemaTable(table), alterTableOp)).promise();
        },

        /**
         * @private
         *
         * Creates the DROP VIEW SQL fragment.
         */
        __dropViewSql: function (name) {
            return format("DROP VIEW %s", this.__quoteSchemaTable(name));
        },

        /**
         * @private
         *
         * Creates a view sql
         */
        __createViewSql: function (name, source, opts) {
            var sql = "CREATE";
            opts = opts || {};
            if (opts.replace) {
                sql += " OR REPLACE";
            }
            sql += " VIEW %s AS %s";
            return format(sql, this.__quoteSchemaTable(name), source);
        },

        /**
         * @private
         * Array of SQL DDL modification statements for the given table,
         * corresponding to the DDL changes specified by the operations.
         * */
        __alterTableSqlList: function (table, operations) {
            var self = this;
            return new PromiseList(operations.map(function (operation) {
                return self.__alterTableSql(table, operation);
            }));
        },

        /**
         * @private
         * SQL DDL fragment containing the column creation SQL for the given column.
         *
         * @param column
         */
        __columnDefinitionSql: function (column) {
            var sql = [format("%s %s", this.__quoteIdentifier(column.name), this.typeLiteral(column))];
            column.unique && sql.push(this._static.UNIQUE);
            (column.allowNull === false || column["null"] === false) && sql.push(this._static.NOT_NULL);
            (column.allowNull === true || column["null"] === true) && sql.push(this._static.NULL);
            !isUndefined(column["default"]) && sql.push(format(" DEFAULT %s", this.literal(column["default"])));
            column.primaryKey && sql.push(this._static.PRIMARY_KEY);
            column.autoIncrement && sql.push(" " + this.autoIncrementSql);
            column.table && sql.push(this.__columnReferencesColumnConstraintSql(column));
            return sql.join("");
        },

        /**
         * @private
         * SQL DDL fragment containing the column creation
         * SQL for all given columns, used inside a CREATE TABLE block.
         */
        __columnListSql: function (generator) {
            var self = this;
            return generator.columns.map(function (column) {
                return self.__columnDefinitionSql(column);
            }).concat(generator.constraints.map(function (constraint) {
                return self.__constraintDefinitionSql(constraint);
            })).join(this._static.COMMA_SEPARATOR);
        },

        /**
         * @private
         *SQL DDL fragment for column foreign key references (column constraints)
         */
        __columnReferencesColumnConstraintSql: function (column) {
            return this.__columnReferencesSql(column);
        },

        /**
         * @private
         * SQL DDL fragment for column foreign key references
         */
        __columnReferencesSql: function (column) {
            var sql = format(" REFERENCES %s", this.__quoteSchemaTable(column.table));
            column.key && (sql += format("(%s)", array.toArray(column.key).map(this.__quoteIdentifier, this).join(this._static.COMMA_SEPARATOR)));
            column.onDelete && (sql += format(" ON DELETE %s", this.__onDeleteClause(column.onDelete)));
            column.onUpdate && (sql += format(" ON UPDATE %s", this.__onUpdateClause(column.onUpdate)));
            column.deferrable && (sql += " DEFERRABLE INITIALLY DEFERRED");
            return sql;
        },

        /**
         * @private
         * SQL DDL fragment for table foreign key references (table constraints)
         * */
        __columnReferencesTableConstraintSql: function (constraint) {
            return format("FOREIGN KEY %s%s", this.literal(constraint.columns.map(function (c) {
                return isString(c) ? sql.stringToIdentifier(c) : c;
            })), this.__columnReferencesSql(constraint));
        },

        /**
         * @private
         * SQL DDL fragment specifying a constraint on a table.
         */
        __constraintDefinitionSql: function (constraint) {
            var ret = [constraint.name ? format("CONSTRAINT %s ", this.__quoteIdentifier(constraint.name)) : ""];
            switch (constraint.type) {
            case "check":
                var check = constraint.check;
                ret.push(format("CHECK %s", this.__filterExpr(isArray(check) && check.length === 1 ? check[0] : check)));
                break;
            case "primaryKey":
                ret.push(format("PRIMARY KEY %s", this.literal(constraint.columns.map(function (c) {
                    return isString(c) ? sql.stringToIdentifier(c) : c;
                }))));
                break;
            case "foreignKey":
                ret.push(this.__columnReferencesTableConstraintSql(constraint));
                break;
            case "unique":
                ret.push(format("UNIQUE %s", this.literal(constraint.columns.map(function (c) {
                    return isString(c) ? sql.stringToIdentifier(c) : c;
                }))));
                break;
            default:
                throw new DatabaseError(format("Invalid constriant type %s, should be 'check', 'primaryKey', foreignKey', or 'unique'", constraint.type));
            }
            return ret.join("");
        },

        /**
         * @private
         * Execute the create table statements using the generator.
         * */
        __createTableFromGenerator: function (name, generator, options) {
            return this.executeDdl(this.__createTableSql(name, generator, options));
        },

        /**
         * @private
         * Execute the create index statements using the generator.
         * */
        __createTableIndexesFromGenerator: function (name, generator, options) {
            var e = options.ignoreIndexErrors;
            var ret;
            var promises = generator.indexes.map(function (index) {
                var ps = this.__indexSqlList(name, [index]).map(this.executeDdl, this);
                return new PromiseList(ps);
            }, this);
            if (promises.length) {
                ret = new PromiseList(promises).chain(function (res) {
                    return res;
                }, function (err) {
                    if (!e) {
                        throw err;
                    }
                });
            } else {
                ret = new Promise().callback();
            }
            return ret.promise();
        },

        /**
         * @private
         * DDL statement for creating a table with the given name, columns, and options
         * */
        __createTableSql: function (name, generator, options) {
            return format("CREATE %sTABLE %s (%s)", options.temp ? this.temporaryTableSql : "", this.__quoteSchemaTable(name), this.__columnListSql(generator));
        },

        /**
         * @private
         * Default index name for the table and columns, may be too long
         * for certain databases.
         */
        __defaultIndexName: function (tableName, columns) {
            var parts = this.__schemaAndTable(tableName);
            var schema = parts[0], table = parts[1];
            var index = [];
            if (schema && schema !== this.defaultSchema) {
                index.push(schema);
            }
            index.push(table);
            index = index.concat(columns.map(function (c) {
                return isString(c) ? c : this.literal(c).replace(/\W/g, "");
            }, this));
            index.push("index");
            return index.join(this._static.UNDERSCORE);

        },

        /**
         * @private
         * The SQL to drop an index for the table.
         * */
        __dropIndexSql: function (table, op) {
            return format("DROP INDEX %s", this.__quoteIdentifier(op.name || this.__defaultIndexName(table, op.columns)));
        },

        /**
         * @private
         *
         *  SQL DDL statement to drop the table with the given name.
         **/
        __dropTableSql: function (name) {
            return format("DROP TABLE %s", this.__quoteSchemaTable(name));
        },

        /**
         * @private
         * Proxy the filterExpr call to the dataset, used for creating constraints.
         * */
        __filterExpr: function (args, block) {
            var ds = this.__schemaUtiltyDataset;
            return ds.literal(ds._filterExpr.apply(ds, arguments));
        },


        /**
         * @private
         * SQL DDL statement for creating an index for the table with the given name
         * and index specifications.
         */
        __indexDefinitionSql: function (tableName, index) {
            var indexName = index.name || this.__defaultIndexName(tableName, index.columns);
            if (index.type) {
                throw new DatabaseError("Index types are not supported for this database");
            } else if (index.where) {
                throw new DatabaseError("Partial indexes are not supported for this database");
            } else {
                return format("CREATE %sINDEX %s ON %s %s", index.unique ? "UNIQUE " : "", this.__quoteIdentifier(indexName), this.__quoteSchemaTable(tableName), this.literal(index.columns.map(function (c) {
                    return isString(c) ? new Identifier(c) : c;
                })));
            }
        },

        /**
         * Array of SQL DDL statements, one for each index specification,
         * for the given table.
         */
        __indexSqlList: function (tableName, indexes) {
            var self = this;
            return indexes.map(function (index) {
                return self.__indexDefinitionSql(tableName, index);
            });
        },

        /**
         * @private
         * SQL DDL ON DELETE fragment to use, based on the given action.
         *The following actions are recognized:
         * <ul>
         *  <li>cascade - Delete rows referencing this row.</li>
         *  <li>noAction (default) - Raise an error if other rows reference this
         *   row, allow deferring of the integrity check.
         *  </li>
         *  <li>restrict - Raise an error if other rows reference this row,
         *   but do not allow deferring the integrity check.</li>
         *  <li> setDefault - Set columns referencing this row to their default value.</li>
         *   <li>setNull - Set columns referencing this row to NULL.</li>
         * </ul>
         */
        __onDeleteClause: function (action) {
            return this._static[action.toUpperCase()] || this._static.NO_ACTION;
        },

        /**
         * @private
         * SQL DDL ON UPDATE fragment to use, based on the given action.
         *The following actions are recognized:
         * <ul>
         *  <li>cascade - Delete rows referencing this row.</li>
         *  <li>noAction (default) - Raise an error if other rows reference this
         *   row, allow deferring of the integrity check.
         *  </li>
         *  <li>restrict - Raise an error if other rows reference this row,
         *   but do not allow deferring the integrity check.</li>
         *  <li> setDefault - Set columns referencing this row to their default value.</li>
         *   <li>setNull - Set columns referencing this row to NULL.</li>
         * </ul>
         */
        __onUpdateClause: function (action) {
            return this._static[action.toUpperCase()] || this._static.NO_ACTION;
        },

        /**
         * @private
         * Proxy the quoteSchemaTable method to the dataset
         * */
        __quoteSchemaTable: function (table) {
            return this.__schemaUtiltyDataset.quoteSchemaTable(table);
        },

        /**
         * @private
         * Proxy the quoteIdentifier method to the dataset, used for quoting tables and columns.
         * */
        __quoteIdentifier: function (v) {
            return this.__schemaUtiltyDataset.quoteIdentifier(v);
        },

        /**
         * @private
         * SQL DDL statement for renaming a table.
         * */
        __renameTableSql: function (name, newName) {
            return format("ALTER TABLE %s RENAME TO %s", this.__quoteSchemaTable(name), this.__quoteSchemaTable(newName));
        },

        /**
         * @private
         * Remove the cached schemaUtilityDataset, because the identifier
         * quoting has changed.
         */
        __resetSchemaUtilityDataset: function () {
            this.__schemaUtiltyDs = null;
        },

        /**
         * @private
         * Split the schema information from the table
         * */
        __schemaAndTable: function (tableName) {
            return this.__schemaUtiltyDataset.schemaAndTable(tableName);
        },

        /**
         * @private
         * Return true if the given column schema represents an autoincrementing primary key.
         *
         */
        _schemaAutoincrementingPrimaryKey: function (schema) {
            return !!schema.primaryKey;
        },

        /**
         * @private
         * SQL fragment specifying the type of a given column.
         * */
        typeLiteral: function (column) {
            return this.__typeLiteralGeneric(column);
        },

        /**
         * @private
         * SQL fragment specifying the full type of a column,
         * consider the type with possible modifiers.
         */
        __typeLiteralGeneric: function (column) {
            var type = column.type;
            var meth = "__typeLiteralGeneric";
            var isStr = isString(type);
            var proper = isStr ? type.charAt(0).toUpperCase() + type.substr(1) : null;
            if (type === String || (isStr && type.match(/string/i))) {
                meth += "String";
            } else if ((isStr && type.match(/number/i)) || type === Number) {
                meth += "Numeric";
            } else if ((isStr && type.match(/datetime/i)) || type === DateTime) {
                meth += "DateTime";
            } else if ((isStr && type.match(/date/i)) || type === Date) {
                meth += "Date";
            } else if ((isStr && type.match(/year/i)) || type === Year) {
                meth += "Year";
            } else if ((isStr && type.match(/timestamp/i)) || type === TimeStamp) {
                meth += "Timestamp";
            } else if ((isStr && type.match(/time/i)) || type === Time) {
                meth += "Time";
            } else if ((isStr && type.match(/decimal/i)) || type === Decimal) {
                meth += "Decimal";
            } else if ((isStr && type.match(/float/i)) || type === Float) {
                meth += "Float";
            } else if ((isStr && type.match(/boolean/i)) || type === Boolean) {
                meth += "Boolean";
            } else if ((isStr && type.match(/buffer/i)) || type === Buffer) {
                meth += "Blob";
            } else if ((isStr && type.match(/json/i)) || type === Json) {
                meth += "Json";
            } else if (isStr && isFunction(this[meth + proper])) {
                meth += proper;
            } else {
                return this.__typeLiteralSpecific(column);
            }
            return this[meth](column);
        },

        /**
         * @private
         * patio uses the date type by default for Dates.
         * <ul>
         *      <li>if onlyTime is present then time is used</li>
         *      <li>if timeStamp is present then timestamp is used,</li>
         *      <li>if dateTime is present then datetime is used</li>
         *      <li>if yearOnly is present then year is used</li>
         *       <li>else date is used</li>
         * </ul>
         */
        __typeLiteralGenericDate: function (column) {
            var type = column.type, ret = "date";
            if (column.onlyTime) {
                ret = "time";
            } else if (column.timeStamp) {
                ret = "timestamp";
            } else if (column.dateTime) {
                ret = "datetime";
            } else if (column.yearOnly) {
                ret = "year";
            }
            return ret;
        },

        /**
         * @private
         * * patio uses the blob type by default for Buffers.
         */
        __typeLiteralGenericBlob: function (column) {
            return "blob";
        },

        /**
         * @private
         * * patio uses the year type by default for {@link patio.sql.DateTime}.
         */
        __typeLiteralGenericDateTime: function (column) {
            return "datetime";
        },

        /**
         * @private
         * patio uses the timestamp type by default for {@link patio.sql.TimeStamp}.
         */
        __typeLiteralGenericTimestamp: function (column) {
            return "timestamp";
        },

        /**
         * @private
         * patio uses the time type by default for {@link patio.sql.Time}.
         */
        __typeLiteralGenericTime: function (column) {
            return "time";
        },

        /**
         * @private
         * patio uses the year type by default for {@link patio.sql.Year}.
         */
        __typeLiteralGenericYear: function (column) {
            return "year";
        },

        /**
         * @private
         * patio uses the boolean type by default for Boolean class
         * */
        __typeLiteralGenericBoolean: function (column) {
            return "boolean";
        },

        /**
         * @private
         * patio uses the numeric type by default for NumericTypes
         * If a size is given, it is used, otherwise, it will default to whatever
         * the database default is for an unsized value.
         * <ul>
         * <li> if isInt is present the int is used</li>
         * <li> if isDouble is present then double precision is used</li>
         * </ul>
         */
        __typeLiteralGenericNumeric: function (column) {
            return column.size ? format("numeric(%s)", array.toArray(column.size).join(', ')) : column.isInt ? "integer" : column.isDouble ? "double precision" : "numeric";
        },


        /**
         * @private
         */
        __typeLiteralGenericFloat: function (column) {
            return "double precision";
        },

        /**
         * @private
         */
        __typeLiteralGenericDecimal: function (column) {
            return "double precision";
        },

        /**
         * @private
         */
        __typeLiteralGenericJson: function (column) {
            return "json";
        },


        /**
         * @private
         * patio uses the varchar type by default for Strings.  If a
         * size isn't present, patio assumes a size of 255.  If the
         * fixed option is used, patio uses the char type.  If the
         * text option is used, patio uses the `text` type.
         */
        __typeLiteralGenericString: function (column) {
            return column.text ? "text" : format("%s(%s)", column.fixed ? "char" : "varchar", column.size || 255);
        },

        /**
         * @private
         * SQL fragment for the given type of a column if the column is not one of the
         * generic types specified with a native javascript type class.
         */
        __typeLiteralSpecific: function (column) {
            var type = column.type;
            type = type === "double" ? "double precision" : type;
            if (type === "varchar") {
                column.size = isNumber(column.size) ? column.size : 255;
            }
            var elements = column.size || column.elements;
            return format("%s%s%s", type, elements ? this.literal(toArray(elements)) : "", column.unsigned ? " UNSIGNED" : "");
        },

        /**@ignore*/
        getters: {
            /**@lends patio.Database.prototype*/
            /**
             * @private
             *  The SQL string specify the autoincrement property, generally used by
             * primary keys.
             *
             * @field
             * */
            autoIncrementSql: function () {
                return this._static.AUTOINCREMENT;
            },

            /**
             * @private
             * @field
             * */
            temporaryTableSql: function () {
                return this._static.TEMPORARY;
            },

            /**
             * @private
             * @field
             * */
            __schemaUtiltyDataset: function () {
                this.__schemaUtiltyDs = this.__schemaUtiltyDs || this.dataset;
                return this.__schemaUtiltyDs;
            }
        }

    },

    "static": {
        /**@lends patio.Database*/

        /**
         *Default  AUTO INCREMENT SQL
         */
        AUTOINCREMENT: 'AUTOINCREMENT',

        /**
         *Default  CASCACDE SQL
         */
        CASCADE: 'CASCADE',

        /**
         *Default comma
         */
        COMMA_SEPARATOR: ', ',

        /**
         *Default  NO ACTION SQL
         */
        NO_ACTION: 'NO ACTION',

        /**
         *Default  NOT NULL SQL
         */
        NOT_NULL: ' NOT NULL',

        /**
         * Default NULL SQL
         */
        NULL: ' NULL',

        /**
         *Default  PRIMARY KEY SQL
         */
        PRIMARY_KEY: ' PRIMARY KEY',

        /**
         *Default  RESTRICT SQL
         */
        RESTRICT: 'RESTRICT',

        /**
         *Default  SET DEFAULT SQL
         */
        SET_DEFAULT: 'SET DEFAULT',

        /**
         *Default  SET NULL SQL
         */
        SET_NULL: 'SET NULL',

        /**
         *Default  TEMPORARY SQL
         */
        TEMPORARY: 'TEMPORARY ',

        /**
         *Default  UNDERSCORE SQL, used in index creation.
         */
        UNDERSCORE: '_',

        /**
         *Default  UNIQUE SQL
         */
        UNIQUE: ' UNIQUE',

        /**
         * Default  UNSIGNED SQL
         */
        UNSIGNED: ' UNSIGNED'

    }
}).as(module);

