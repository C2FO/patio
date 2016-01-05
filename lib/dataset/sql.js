var comb = require("comb"),
    define = comb.define,
    array = comb.array,
    toArray = array.toArray,
    intersect = array.intersect,
    compact = array.compact,
    string = comb.string,
    format = string.format,
    argsToArray = comb.argsToArray,
    isInstanceOf = comb.isInstanceOf,
    isArray = comb.isArray,
    isNumber = comb.isNumber,
    isDate = comb.isDate,
    isNull = comb.isNull,
    isBoolean = comb.isBoolean,
    isFunction = comb.isFunction,
    isUndefined = comb.isUndefined,
    isObject = comb.isObject,
    isHash = comb.isHash,
    isEmpty = comb.isEmpty,
    merge = comb.merge,
    hitch = comb.hitch,
    isUndefinedOrNull = comb.isUndefinedOrNull,
    isString = comb.isString,
    sql = require("../sql").sql,
    Json = sql.Json,
    JsonArray = sql.JsonArray,
    Expression = sql.Expression,
    ComplexExpression = sql.ComplexExpression,
    AliasedExpression = sql.AliasedExpression,
    Identifier = sql.Identifier,
    QualifiedIdentifier = sql.QualifiedIdentifier,
    OrderedExpression = sql.OrderedExpression,
    CaseExpression = sql.CaseExpression,
    SubScript = sql.SubScript,
    NumericExpression = sql.NumericExpression,
    ColumnAll = sql.ColumnAll,
    Cast = sql.Cast,
    StringExpression = sql.StringExpression,
    BooleanExpression = sql.BooleanExpression,
    SQLFunction = sql.SQLFunction,
    LiteralString = sql.LiteralString,
    PlaceHolderLiteralString = sql.PlaceHolderLiteralString,
    QueryError = require("../errors").QueryError, patio;

var Dataset;

var clauseMethods = function (type, clauses) {
    if (isString(clauses)) {
        clauses = clauses.split(" ");
    }
    return clauses.map(function (clause) {
        return ["_", type, clause.charAt(0).toUpperCase(), clause.substr(1), "Sql"].join("");
    });
};

define({

    instance: {
        /**@lends patio.Dataset.prototype*/

        /**
         * Dataset mixin that provides functions to the {@link patio.dataset.Dataset} to
         * create SELECT, UPDATE, CREATE, and DELETE SQL statements, based off of the the
         * methods invoked in the {@link patio.dataset._Query}
         * mixin. This class should not be used directly by
         *
         * @constructs
         * @memberOf patio.dataset
         * @name _Query
         *
         * @property {String} sql Readonly property that returns a SELECT statement.
         * @property {String} deleteSql DELETE SQL query string.  See {@link patio.dataset._Actions#delete}.
         *
         *      <pre class="code">
         *       dataset.filter(function(){
         *           return this.price.gte(100);
         *       }).deleteSql;
         *           // => "DELETE FROM items WHERE (price >= 100)"
         *      </pre>
         * @property {String} selectSql  Returns a SELECT SQL query string.
         *
         *      <pre class="code">
         *
         *       DB.from("items").selectSql;
         *              //=> "SELECT * FROM items"
         *      </pre>
         * @property {String} truncateSql Returns a TRUNCATE SQL query string.  See {@link patio.dataset._Actions#truncate}
         *
         *      <pre class="code">
         *
         *       DB.from("items").truncateSql();
         *              //=> 'TRUNCATE items'
         *      </pre>
         * @property {String} exists  Returns an EXISTS clause for the dataset as a {@link patio.sql.LiteralString}.
         *
         *      <pre class="code">
         *
         *       var ds = DB.from("test");
         *       ds.filter(ds.filter(sql.price.lt(100))).exists()).sql;
         *              //=> 'SELECT * FROM test WHERE (EXISTS (SELECT * FROM test WHERE (price < 100)))'
         *
         *      </pre>
         **/
        constructor: function () {
            //We initialize these here because otherwise
            //the will be blank because of recursive dependencies.
            !patio && (patio = require("../index"));
            !Dataset && (Dataset = patio.Dataset);
            this.outputIdentifier = hitch(this, this.outputIdentifier);
            this._super(arguments);
        },

        /**
         * Returns an INSERT SQL query string.  See {@link patio.dataset._Actions#insert}
         *
         * @example
         *
         *
         * DB.from("items").insertSql({a : 1});
         *      //=> INSERT INTO items (a) VALUES (1)
         *
         * var ds = DB.from("test");
         *
         * //default values
         * ds.insertSql();
         *      //=> INSERT INTO test DEFAULT VALUES
         *
         * //with hash
         * ds.insertSql({name:'wxyz', price:342});
         *      //=> INSERT INTO test (name, price) VALUES ('wxyz', 342)
         * ds.insertSql({});
         *      //=> INSERT INTO test DEFAULT VALUES
         *
         * //object that has a values property
         * ds.insertSql({values:{a:1}});
         *      //=> INSERT INTO test (a) VALUES (1)
         *
         * //arbitrary value
         * ds.insertSql(123);
         *      //=> INSERT INTO test VALUES (123)
         *
         * //with dataset
         * ds.insertSql(DB.from("something").filter({x:2}));
         *      //=> INSERT INTO test SELECT * FROM something WHERE (x = 2)
         *
         * //with array
         * ds.insertSql('a', 2, 6.5);
         *      //=> INSERT INTO test VALUES ('a', 2, 6.5)
         *
         * @throws {patio.QueryError} if there are Different number of values and columns given to insertSql or
         *                                  if an invalid BooleanExpresion is given.
         *
         * @param {patio.Dataset|patio.sql.LiteralString|Array|Object|patio.sql.BooleanExpression|...} values  values to
         *      insert into the database. The INSERT statement generated depends on the type.
         *      <ul>
         *          <li>Empty object| Or no arugments: then DEFAULT VALUES is used.</li>
         *          <li>Object: the keys will be used as the columns, and values will be the values inserted.</li>
         *          <li>Single {@link patio.Dataset} : an insert with subselect will be performed.</li>
         *          <li>Array with {@link patio.Dataset} : The array will be used for columns and a subselect will performed with the dataset for the values.</li>
         *          <li>{@link patio.sql.LiteralString} : the literal value will be used.</li>
         *          <li>Single Array : the values in the array will be used as the VALUES clause.</li>
         *          <li>Two Arrays: the first array is the columns the second array is the values.</li>
         *          <li>{@link patio.sql.BooleanExpression} : the expression will be used as the values.
         *          <li>An arbitrary number of arguments : the {@link patio.Dataset#literal} version of the values will be used</li>
         *      </ul>
         *
         *
         * @return {String} a INSERT SQL query string
         */
        insertSql: function (values) {
            values = argsToArray(arguments);
            var opts = this.__opts;
            if (opts.sql) {
                return this._staticSql(opts.sql);
            }
            this.__checkModificationAllowed();

            var columns = [];

            switch (values.length) {
            case 0 :
                //we have no values
                return this.insertSql({});
            case 1 :
                var vals = values[0], v;
                if (isInstanceOf(vals, Dataset, LiteralString) || isArray(vals)) {
                    values = vals;
                } else if (vals.hasOwnProperty("values") && isObject((v = vals.values))) {
                    return this.insertSql(v);
                } else if (isHash(vals)) {
                    vals = merge({}, opts.defaults || {}, vals);
                    vals = merge({}, vals, opts.overrides || {});
                    values = [];
                    for (var i in vals) {
                        columns.push(i);
                        values.push(vals[i]);
                    }
                } else if (isInstanceOf(vals, BooleanExpression)) {
                    var op = vals.op;
                    values = [];
                    if (!isUndefinedOrNull(this._static.TWO_ARITY_OPERATORS[op])) {
                        var args = vals.args;
                        columns.push(args[0]);
                        values.push(args[1]);
                    } else {
                        throw new QueryError("Invalid Expression op: " + op);
                    }
                }
                break;
            case 2 :
                var v0 = values[0], v1 = values[1];
                if (isArray(v0) && isArray(v1) || isInstanceOf(v1, Dataset, LiteralString)) {
                    columns = v0, values = v1;
                    if (isArray(values) && columns.length !== values.length) {
                        throw new QueryError("Different number of values and columns given to insertSql");
                    }
                }
                break;
            }
            columns = columns.map(function (k) {
                return isString(k) ? new Identifier(k) : k;
            }, this);
            return this.mergeOptions({columns: columns, values: values})._insertSql();
        },

        /**
         * Returns an array of insert statements for inserting multiple records.
         * This method is used by {@link patio.dataset._Actions#multiInsert} to format insert statements.
         * <b>This method is not typically used directly.</b>
         *
         * <p>
         *      <b>Note:</b>This method should be overridden by descendants if there is support for
         *                      inserting multiple records in a single SQL statement.
         * </p>
         *
         * @param {Array} columns The columns to insert values for.
         *                  This array will be used as the base for each values item in the values array.
         * @param {Array[Array]} values Array of arrays of values to insert into the columns.
         *
         * @return {String[]} array of insert statements.
         */
        multiInsertSql: function (columns, values) {
            return values.map(function (r) {
                return this.insertSql(columns, r);
            }, this);
        },

        /**
         *Formats an UPDATE statement using the given values.  See {@link patio.dataset._Actions#update}.
         *
         * @example
         *
         * DB.from("items").updateSql({price : 100, category : 'software'});
         *      //=> "UPDATE items SET price = 100, category = 'software'
         *
         * @throw {QueryError} If the dataset is grouped or includes more than one table.
         *
         * @param {*...} Variable number of values to update the table with.
         *  The UPDATE statement created depends on the values passed in.
         *          <ul>
         *              <li>Object : the keys will be used as the columns and the values will be the values to set to columns to</li>
         *              <li>{@link patio.sql.Expression} : the {@link patio.dataset._Sql#literal} representation of the
         *                      {@link patio.sql.Expression} will be used as the value
         *              </li>
         *              </li> Other : the {@link patio.dataset._Sql#literal} value will be used as the value</li>
         *          </ul>
         *
         * @return {String} the UPDATE statement.
         */
        updateSql: function (values) {
            values = argsToArray(arguments);
            var update;
            if (this.__opts.sql) {
                update = this._staticSql(this.__opts.sql);
            } else {
                this.__checkModificationAllowed();
                update = this.mergeOptions({values: values})._updateSql();
            }
            return update;
        },


        /**
         * Returns a qualified column name (including a table name) if the column
         * name isn't already qualified.
         *
         * @example
         *
         * dataset.qualifiedColumnName("b1", "items");
         *      //=> items.b1
         *
         * dataset.qualifiedColumnName("ccc__b"));
         *      //=> 'ccc.b'
         *
         * dataset.qualifiedColumnName("ccc__b", "items"));
         *      //=> 'ccc.b'
         *
         * @param {String} column the column to qualify. If the column is already qualified (e.g. ccc__b) then the
         *                        table name (e.g. ccc) will override the provided table.
         *
         * @param {String} table the name of the table to qualify the column to.
         *
         * @return {String} the qualified column name..
         *
         */
        qualifiedColumnName: function (column, table) {
            if (isString(column)) {
                var parts = this._splitString(column);
                var columnTable = parts[0], alias = parts[2], schema, tableAlias;
                column = parts[1];
                if (!columnTable) {
                    if (isInstanceOf(table, Identifier)) {
                        table = table.value;
                    }
                    if (isInstanceOf(table, AliasedExpression)) {
                        tableAlias = table.alias;
                    } else if (isInstanceOf(table, QualifiedIdentifier)) {
                        tableAlias = table;
                    } else {
                        parts = this._splitString(table);
                        schema = parts[0];
                        tableAlias = parts[2];
                        table = parts[1];
                        if (schema) {
                            tableAlias = new Identifier(tableAlias) || new QualifiedIdentifier(schema, table);
                        }
                    }
                    columnTable = tableAlias || table;
                }
                return new QualifiedIdentifier(columnTable, column);
            } else if (isInstanceOf(column, Identifier)) {
                return column.qualify(table);
            } else {
                return column;
            }
        },

        /**
         * Creates a unique table alias that hasn't already been used in this dataset.
         *
         * @example
         *
         * DB.from("table").unusedTableAlias("t");
         *       //=> "t"
         *
         * DB.from("table").unusedTableAlias("table");
         *   //=> "table0"
         *
         * DB.from("table", "table0"]).unusedTableAlias("table");
         *   //=> "table1"
         *
         * @param {String|patio.sql.Identifier} tableAlias the table to get an unused alias for.
         *
         * @return {String} the implicit alias that is in tableAlias with a possible "N"
         *                  if the alias has already been used, where N is an integer starting at 0.
         */
        unusedTableAlias: function (tableAlias) {
            tableAlias = this._toTableName(tableAlias);
            var usedAliases = [], from, join;
            if ((from = this.__opts.from) != null) {
                usedAliases = usedAliases.concat(from.map(function (n) {
                    return this._toTableName(n);
                }, this));
            }
            if ((join = this.__opts.join) != null) {
                usedAliases = usedAliases.concat(join.map(function (join) {
                    if (join.tableAlias) {
                        return this.__toAliasedTableName(join.tableAlias);
                    } else {
                        return this._toTableName(join.table);
                    }
                }, this));
            }
            if (usedAliases.indexOf(tableAlias) !== -1) {
                var base = tableAlias, i = 0;
                do {
                    tableAlias = string.format("%s%d", base, i++);
                } while (usedAliases.indexOf(tableAlias) !== -1);
            }
            return tableAlias;
        },

        /**
         * Returns a literal representation of a value to be used as part
         * of an SQL expression.
         *
         * @example
         *
         *  DB.from("items").literal("abc'def\\") //=> "'abc''def\\\\'"
         *  DB.from("items").literal("items__id") //=> "items.id"
         *  DB.from("items").literal([1, 2, 3]) //=> "(1, 2, 3)"
         *  DB.from("items").literal(DB.from("items")) //=> "(SELECT * FROM items)"
         *  DB.from("items").literal(sql.x.plus(1).gt("y")); //=> "((x + 1) > y)"
         *
         * @throws {patio.QueryError} If an unsupported object is given.
         * @param {*} v the value to convert the the SQL literal representation
         *
         * @return {String} a literal representation of the value.
         */
        literal: function (v) {
            if (isInstanceOf(v, Json, JsonArray)) {
                return this._literalJson(v);
            } else if (isInstanceOf(v, LiteralString)) {
                return "" + v;
            } else if (isString(v)) {
                return this._literalString(v);
            } else if (isNumber(v)) {
                return this._literalNumber(v);
            } else if (isInstanceOf(v, Expression)) {
                return this._literalExpression(v);
            } else if (isInstanceOf(v, Dataset)) {
                return this._literalDataset(v);
            } else if (isArray(v)) {
                return this._literalArray(v);
            } else if (isInstanceOf(v, sql.Year)) {
                return this._literalYear(v);
            } else if (isInstanceOf(v, sql.TimeStamp, sql.DateTime)) {
                return this._literalTimestamp(v);
            } else if (isDate(v)) {
                return this._literalDate(v);
            } else if (isInstanceOf(v, sql.Time)) {
                return this._literalTime(v);
            } else if (Buffer.isBuffer(v)) {
                return this._literalBuffer(v);
            } else if (isNull(v)) {
                return this._literalNull();
            } else if (isBoolean(v)) {
                return this._literalBoolean(v);
            } else if (isHash(v)) {
                return this._literalObject(v);
            } else {
                return this._literalOther(v);
            }
        },

        //BEGIN PROTECTED


        /**
         *
         * Qualify the given expression to the given table.
         * @param {patio.sql.Expression} column the expression to qualify
         * @param table the table to qualify the expression to
         */
        _qualifiedExpression: function (e, table) {
            var h, i, args;
            if (isString(e)) {
                //this should not be hit but here just for completeness
                return this.stringToIdentifier(e);
            } else if (isArray(e)) {
                return e.map(function (exp) {
                    return this._qualifiedExpression(exp, table);
                }, this);
            } else if (isInstanceOf(e, Identifier)) {
                return new QualifiedIdentifier(table, e);
            } else if (isInstanceOf(e, OrderedExpression)) {
                return new OrderedExpression(this._qualifiedExpression(e.expression, table), e.descending,
                    {nulls: e.nulls});
            } else if (isInstanceOf(e, AliasedExpression)) {
                return new AliasedExpression(this._qualifiedExpression(e.expression, table), e.alias);
            } else if (isInstanceOf(e, CaseExpression)) {
                args = [this._qualifiedExpression(e.conditions, table), this._qualifiedExpression(e.def, table)];
                if (e.hasExpression) {
                    args.push(this._qualifiedExpression(e.expression, table));
                }
                return CaseExpression.fromArgs(args);
            } else if (isInstanceOf(e, Cast)) {
                return new Cast(this._qualifiedExpression(e.expr, table), e.type);
            } else if (isInstanceOf(e, SQLFunction)) {
                return SQLFunction.fromArgs([e.f].concat(this._qualifiedExpression(e.args, table)));
            } else if (isInstanceOf(e, ComplexExpression)) {
                return ComplexExpression.fromArgs([e.op].concat(this._qualifiedExpression(e.args, table)));
            } else if (isInstanceOf(e, SubScript)) {
                return new SubScript(this._qualifiedExpression(e.f, table), this._qualifiedExpression(e.sub, table));
            } else if (isInstanceOf(e, PlaceHolderLiteralString)) {
                args = [];
                var eArgs = e.args;
                if (isHash(eArgs)) {
                    h = {};
                    for (i in eArgs) {
                        h[i] = this._qualifiedExpression(eArgs[i], table);
                    }
                    args = h;
                } else {
                    args = this._qualifiedExpression(eArgs, table);
                }
                return new PlaceHolderLiteralString(e.str, args, e.parens);

            } else if (isHash(e)) {
                h = {};
                for (i in e) {
                    h[this._qualifiedExpression(i, table) + ""] = this._qualifiedExpression(e[i], table);
                }
                return h;
            } else {
                return e;
            }
        },

        /**
         * Returns a string that is the name of the table.
         *
         * @throws {patio.QueryError} If the name is not a String {@link patio.sql.Identifier},
         * {@link patio.sql.QualifiedIdentifier} or {@link patio.sql.AliasedExpression}.
         *
         * @param {String|patio.sql.Identifier|patio.sql.QualifiedIdentifier|patio.sql.AliasedExpression} name
         *          the object to get the table name from.
         *
         * @return {String} the name of the table.
         */
        _toTableName: function (name) {
            var ret;
            if (isString(name)) {
                var parts = this._splitString(name);
                var schema = parts[0], table = parts[1], alias = parts[2];
                ret = (schema || alias) ? alias || table : table;
            } else if (isInstanceOf(name, Identifier)) {
                ret = name.value;
            } else if (isInstanceOf(name, QualifiedIdentifier)) {
                ret = this._toTableName(name.column);
            } else if (isInstanceOf(name, AliasedExpression)) {
                ret = this.__toAliasedTableName(name.alias);
            } else {
                throw new QueryError("Invalid object to retrieve the table name from");
            }
            return ret;
        },

        /**
         * Return the unaliased part of the identifier.  Handles both
         * implicit aliases in strings, as well as {@link patio.sql.AliasedExpression}s.
         * Other objects are returned as is.
         *
         * @param {String|patio.sql.AliasedExpression|*} tableAlias the object to un alias
         *
         * @return {patio.sql.QualifiedIdentifier|String|*} the unaliased portion of the identifier
         */
        _unaliasedIdentifier: function (c) {
            if (isString(c)) {
                var parts = this._splitString(c);
                var table = parts[0], column = parts[1];
                if (table) {
                    return new QualifiedIdentifier(table, column);
                }
                return column;

            } else if (isInstanceOf(c, AliasedExpression)) {
                return c.expression;
            } else {
                return c;
            }
        },

        /**
         * Return a [@link patio.sql._Query#fromSelf} dataset if an order or limit is specified, so it works as expected
         * with UNION, EXCEPT, and INTERSECT clauses.
         */
        _compoundFromSelf: function () {
            var opts = this.__opts;
            return (opts["limit"] || opts["order"]) ? this.fromSelf() : this;
        },

        /**
         * Return true if the dataset has a non-null value for any key in opts.
         * @param opts the options to compate this datasets options to
         *
         * @return {Boolean} true if the dataset has a non-null value for any key in opts.
         */
        _optionsOverlap: function (opts) {
            var o = [];
            for (var i in this.__opts) {
                if (!isUndefinedOrNull(this.__opts[i])) {
                    o.push(i);
                }
            }
            return intersect(compact(o), opts).length !== 0;
        },

        //Formats in INSERT statement using the stored columns and values.
        _insertSql: function () {
            return this._clauseSql("insert");
        },

        //Formats an UPDATE statement using the stored values.
        _updateSql: function () {
            return this._clauseSql("update");
        },
        //Formats the truncate statement.  Assumes the table given has already been
        //literalized.
        _truncateSql: function (table) {
            return "TRUNCATE TABLE" + table;
        },


        //Prepares an SQL statement by calling all clause methods for the given statement type.
        _clauseSql: function (type) {
            var sql = [("" + type).toUpperCase()];
            try {
                this._static[sql + "_CLAUSE_METHODS"].forEach(function (m) {
                    if (m.match("With")) {
                        this[m](sql);
                    } else {
                        var sqlRet = this[m]();
                        if (sqlRet) {
                            sql.push(sqlRet);
                        }
                    }
                }, this);
            } catch (e) {
                throw e;
            }
            return sql.join("");
        },


        //SQL fragment specifying the table to insert INTO
        _insertIntoSql: function (sql) {
            return string.format(" INTO%s", this._sourceList(this.__opts.from));
        },

        //SQL fragment specifying the columns to insert into
        _insertColumnsSql: function (sql) {
            var columns = this.__opts.columns, ret = "";
            if (columns && columns.length) {
                ret = " (" + columns.map(
                    function (c) {
                        return c.toString(this);
                    }, this).join(this._static.COMMA_SEPARATOR) + ")";
            }
            return ret;
        },

        //SQL fragment specifying the values to insert.
        _insertValuesSql: function () {
            var values = this.__opts.values, ret = [];
            if (isArray(values)) {
                ret.push(values.length === 0 ? " DEFAULT VALUES" : " VALUES " + this.literal(values));
            } else if (isInstanceOf(values, Dataset)) {
                ret.push(" " + this._subselectSql(values));
            } else if (isInstanceOf(values, LiteralString)) {
                ret.push(" " + values.toString(this));
            } else {
                throw new QueryError("Unsupported INSERT values type, should be an array or dataset");
            }
            return ret.join("");
        },

        //SQL fragment for Array
        _arraySql: function (a) {
            return !a.length ? '(NULL)' : "(" + this.__expressionList(a) + ")";
        },

        //This method quotes the given name with the SQL standard double quote.
        //should be overridden by subclasses to provide quoting not matching the
        //SQL standard, such as backtick (used by MySQL and SQLite).
        _quotedIdentifier: function (name) {
            return string.format("\"%s\"", ("" + name).replace('"', '""'));
        },


        /*
         This section is for easier adapter overrides of sql formatting. These metthods are used by patio.sql.* toString
         methods to generate sql.
         */

        /**
         * @private  For internal use by patio
         *
         * SQL fragment for AliasedExpression
         */
        aliasedExpressionSql: function (ae) {
            return this.__asSql(this.literal(ae.expression), ae.alias);
        },

        /**
         * @private For internal use by patio
         * SQL fragment for BooleanConstants
         * */
        booleanConstantSql: function (constant) {
            return this.literal(constant);
        },

        /**
         * @private For internal use by patio
         * SQL fragment for CaseExpression
         */
        caseExpressionSql: function (ce) {
            var sql = ['(CASE '];
            if (ce.expression) {
                sql.push(this.literal(ce.expression), " ");
            }
            var conds = ce.conditions;
            if (isArray(conds)) {
                conds.forEach(function (cond) {
                    sql.push(format("WHEN %s THEN %s", this.literal(cond[0]), this.literal(cond[1])));
                }, this);
            } else if (isHash(conds)) {
                for (var i in conds) {
                    sql.push(format("WHEN %s THEN %s", this.literal(i), this.literal(conds[i])));
                }
            }
            return format("%s ELSE %s END)", sql.join(""), this.literal(ce.def));
        },

        /**
         * @private For internal use by patio
         * SQL fragment for the SQL CAST expression
         * */
        castSql: function (expr, type) {
            return string.format("CAST(%s AS %s)", this.literal(expr), this.db.castTypeLiteral(type));
        },

        /**
         * @private For internal use by patio
         * SQL fragment for specifying all columns in a given table
         **/
        columnAllSql: function (ca) {
            return string.format("%s.*", this.quoteSchemaTable(ca.table));
        },

        /**
         * @private For internal use by patio
         * SQL fragment for complex expressions
         **/
        complexExpressionSql: function (op, args) {
            var newOp;
            var isOperators = this._static.IS_OPERATORS, isLiterals = this._static.IS_LITERALS, l;
            if ((newOp = isOperators[op]) != null) {
                var r = args[1], v = isNull(r) ? isLiterals.NULL : isLiterals[r];
                if (r == null || this.supportsIsTrue) {
                    if (isUndefined(v)) {
                        throw new QueryError(string.format("Invalid argument('%s') used for IS operator", r));
                    }
                    l = args[0];
                    return string.format("(%s %s %s)", isString(l) ? l : this.literal(l), newOp, v);
                } else if (op === "IS") {
                    return this.complexExpressionSql("EQ", args);
                } else {
                    return this.complexExpressionSql("OR",
                        [BooleanExpression.fromArgs(["NEQ"].concat(args)), new BooleanExpression("IS", args[0],
                            null)]);
                }

            } else if (["IN", "NOTIN"].indexOf(op) !== -1) {
                var cols = args[0], vals = args[1], colArray = isArray(cols), valArray = false, emptyValArray = false;

                if (isArray(vals)) {
                    valArray = true;
                    emptyValArray = vals.length === 0;
                }
                if (colArray) {
                    if (emptyValArray) {
                        if (op === "IN") {
                            return this.literal(BooleanExpression.fromValuePairs(cols.map(function (x) {
                                return [x, x];
                            }), "AND", true));
                        } else {
                            return this.literal({1: 1});
                        }
                    } else if (!this.supportsMultipleColumnIn) {
                        if (valArray) {
                            var expr = BooleanExpression.fromArgs(["OR"].concat(vals.map(function (vs) {
                                return BooleanExpression.fromValuePairs(array.zip(cols, vs));
                            })));
                            return this.literal(op === "IN" ? expr : expr.invert());
                        }
                    } else {
                        //If the columns and values are both arrays, use _arraySql instead of
                        //literal so that if values is an array of two element arrays, it
                        //will be treated as a value list instead of a condition specifier.
                        return format("(%s %s %s)", isString(cols) ? cols : this.literal(cols),
                            ComplexExpression.IN_OPERATORS[op],
                            valArray ? this._arraySql(vals) : this.literal(vals));
                    }
                }
                else {
                    if (emptyValArray) {
                        if (op === "IN") {
                            return this.literal(BooleanExpression.fromValuePairs([
                                [cols, cols]
                            ], "AND", true));
                        } else {
                            return this.literal({1: 1});
                        }
                    } else {
                        return format("(%s %s %s)", isString(cols) ? cols : this.literal(cols),
                            ComplexExpression.IN_OPERATORS[op], this.literal(vals));
                    }
                }
            } else if ((newOp = this._static.TWO_ARITY_OPERATORS[op]) != null) {
                l = args[0];
                return format("(%s %s %s)", isString(l) ? l : this.literal(l), newOp,
                    this.literal(args[1]));
            } else if ((newOp = this._static.N_ARITY_OPERATORS[op]) != null) {
                return string.format("(%s)", args.map(this.literal, this).join(" " + newOp + " "));
            } else if (op === "NOT") {
                return string.format("NOT %s", this.literal(args[0]));
            } else if (op === "NOOP") {
                return this.literal(args[0]);
            } else {
                throw new QueryError("Invalid operator " + op);
            }
        },

        /**
         * @private For internal use by patio
         *
         * SQL fragment for constants
         * */
        constantSql: function (constant) {
            return "" + constant;
        },

        /**
         * @private For internal use by patio
         *
         * SQL fragment specifying an SQL function call
         * */
        functionSql: function (f) {
            var args = f.args;
            return string.format("%s%s", f.f, args.length === 0 ? '()' : this.literal(args));
        },

        /**
         * @private For internal use by patio
         * SQL fragment specifying a JOIN clause without ON or USING.
         * */
        joinClauseSql: function (jc) {
            var table = jc.table,
                tableAlias = jc.tableAlias;
            if (table === tableAlias) {
                tableAlias = null;
            }
            var tref = this.__tableRef(table);
            return string.format(" %s %s", this._joinTypeSql(jc.joinType), tableAlias ? this.__asSql(tref, tableAlias) : tref);
        },

        /**
         * @private For internal use by patio
         * SQL fragment specifying a JOIN clause with ON.
         **/
        joinOnClauseSql: function (jc) {
            return string.format("%s ON %s", this.joinClauseSql(jc), this.literal(this._filterExpr(jc.on)));
        },

        /**
         * @private For internal use by patio
         * SQL fragment specifying a JOIN clause with USING.
         **/
        joinUsingClauseSql: function (jc) {
            return string.format("%s USING (%s)", this.joinClauseSql(jc), this.__columnList(jc.using));
        },

        /**
         * @private For internal use by patio
         * SQL fragment for NegativeBooleanConstants.
         **/
        negativeBooleanConstantSql: function (constant) {
            return string.format("NOT %s", this.booleanConstantSql(constant));
        },

        /**
         * @private For internal use by patio
         *
         * SQL fragment for the ordered expression, used in the ORDER BY
         * clause.
         */
        orderedExpressionSql: function (oe) {
            var s = string.format("%s %s", this.literal(oe.expression), oe.descending ? "DESC" : "ASC");
            if (oe.nulls) {
                s = string.format("%s NULLS %s", s, oe.nulls === "first" ? "FIRST" : "LAST");
            }
            return s;
        },

        /**
         * @private For internal use by patio
         * SQL fragment for a literal string with placeholders
         * */
        placeholderLiteralStringSql: function (pls) {
            var args = pls.args;
            var s;
            if (isHash(args)) {
                for (var i in args) {
                    args[i] = this.literal(args[i]);
                }
                s = string.format(pls.str, args);
            } else {
                s = pls.str.replace(this._static.QUESTION_MARK, "%s");
                args = toArray(args).map(this.literal, this);
                s = string.format(s, args);

            }
            if (pls.parens) {
                s = string.format("(%s)", s);
            }
            return s;
        },


        /**
         * @private For internal use by patio
         * SQL fragment for the qualifed identifier, specifying
         * a table and a column (or schema and table).
         */
        qualifiedIdentifierSql: function (qcr) {
            return [qcr.table, qcr.column].map(function (x) {
                var isLiteral = [QualifiedIdentifier, Identifier, String].some(function (c) {
                    return x instanceof c;
                }), ret;
                if (isLiteral) {
                    ret = this.literal(x);
                } else {
                    ret = this.quoteIdentifier(x);
                }
                return ret;
            }, this).join('.');
        },

        /**
         * @private For internal use by patio
         *
         * Adds quoting to identifiers (columns and tables). If identifiers are not
         * being quoted, returns name as a string.  If identifiers are being quoted
         * quote the name with {@link patio.dataset._Sql#_quotedIdentifier}.
         */
        quoteIdentifier: function (name) {
            if (isInstanceOf(name, LiteralString)) {
                return name;
            } else {
                if (isInstanceOf(name, Identifier)) {
                    name = name.value;
                }
                name = this.inputIdentifier(name);
                if (this.quoteIdentifiers) {
                    name = this._quotedIdentifier(name);
                }
            }
            return name;
        },

        /**
         * @private For internal use by patio
         *
         * Modify the identifier returned from the database based on the
         * identifierOutputMethod.
         */
        inputIdentifier: function (v) {
            var i = this.__identifierInputMethod;
            v = v.toString(this);
            return !isUndefinedOrNull(i) ?
                isFunction(v[i]) ?
                    v[i]() :
                    isFunction(comb[i]) ?
                        comb[i](v)
                        : v
                : v;
        },

        /**
         * @private For internal use by patio
         *
         * Modify the identifier sent to the database based on the
         * identifierOutputMethod.
         */
        outputIdentifier: function (v) {
            (v === '' && (v = 'untitled'));
            var i = this.__identifierOutputMethod;
            return !isUndefinedOrNull(i) ?
                isFunction(v[i]) ?
                    v[i]() :
                    isFunction(comb[i]) ?
                        comb[i](v)
                        : v
                : v;
        },

        /**
         * @private For For internal use by patio
         *
         * Separates the schema from the table and returns a string with them
         * quoted (if quoting identifiers)
         */
        quoteSchemaTable: function (table) {
            var parts = this.schemaAndTable(table);
            var schema = parts[0];
            table = parts[1];
            return string.format("%s%s", schema ? this.quoteIdentifier(schema) + "." : "", this.quoteIdentifier(table));
        },


        /**
         * @private For For internal use by patio
         * Split the schema information from the table
         */
        schemaAndTable: function (tableName) {
            var sch = this.db ? this.db.defaultSchema || null : null;
            if (isString(tableName)) {
                var parts = this._splitString(tableName);
                var s = parts[0], table = parts[1];
                return [s || sch, table];
            } else if (isInstanceOf(tableName, QualifiedIdentifier)) {
                return [tableName.table, tableName.column];
            } else if (isInstanceOf(tableName, Identifier)) {
                return [null, tableName.value];
            } else if (isInstanceOf(tableName, LiteralString)) {
                return [null, tableName];
            } else {
                throw new QueryError("table should be a QualifiedIdentifier, Identifier, or String");
            }
        },

        /**
         * @private For For internal use by patio
         * SQL fragment for specifying subscripts (SQL array accesses)
         * */
        subscriptSql: function (s) {
            return string.format("%s[%s]", this.literal(s.f), this.__expressionList(s.sub));
        },


        /**
         * Do a simple join of the arguments (which should be strings) separated by commas
         * */
        __argumentList: function (args) {
            return args.join(this._static.COMMA_SEPARATOR);
        },

        /**
         * SQL fragment for specifying an alias.  expression should already be literalized.
         */
        __asSql: function (expression, alias) {
            return string.format("%s AS %s", expression, this.quoteIdentifier(alias));
        },

        /**
         * Converts an array of column names into a comma seperated string of
         * column names. If the array is empty, a wildcard (*) is returned.
         */
        __columnList: function (columns) {
            return (!columns || columns.length === 0) ? this._static.WILDCARD : this.__expressionList(columns);
        },

        /**
         * The alias to use for datasets, takes a number to make sure the name is unique.
         * */
        _datasetAlias: function (number) {
            return this._static.DATASET_ALIAS_BASE_NAME + number;
        },

        /**
         * Converts an array of expressions into a comma separated string of
         * expressions.
         */
        __expressionList: function (columns) {
            return columns.map(this.literal, this).join(this._static.COMMA_SEPARATOR);
        },

        //Format the timestamp based on the default_timestamp_format, with a couple
        //of modifiers.  First, allow %N to be used for fractions seconds (if the
        //database supports them), and override %z to always use a numeric offset
        //of hours and minutes.
        formatTimestamp: function (v, format) {
            return this.literal(patio.dateToString(v, format));
        },

        /**
         * SQL fragment specifying a JOIN type, splits a camelCased join type
         * and converts to uppercase/
         */
        _joinTypeSql: function (joinType) {
            return (joinType || "").replace(/([a-z]+)|([A-Z][a-z]+)/g, function (m) {
                    return m.toUpperCase() + " ";
                }).trimRight() + " JOIN";
        },

        /*
         Methods for converting types to a SQL .
         */

        /**
         * @return SQL fragment for a type of object not handled by {@link patio.dataset._Sql#literal}.
         * If object has a method sqlLiteral then it is called with this dataset as the first argument,
         * otherwise raises an error. Classes implementing sqlLiteral should call a class-specific method
         * on the dataset provided and should add that method to {@link patio.dataset.Dataset}, allowing for adapters
         * to provide customized literalizations.
         * If a database specific type is allowed, this should be overriden in a subclass.
         */
        _literalOther: function (v) {
            if (isFunction(v.sqlLiteral)) {
                return v.sqlLiteral(this);
            } else {
                throw new QueryError(string.format("can't express %j as a SQL literal", [v]));
            }
        },

        /**
         *@return SQL fragment for Buffer, treated as an expression
         * */
        _literalBuffer: function (b) {
            return "X'" + b.toString("hex") + "'";
        },

        /**
         *@return SQL fragment for Hash, treated as an expression
         * */
        _literalObject: function (v) {
            return this._literalExpression(BooleanExpression.fromValuePairs(v));
        },


        /**
         * @return SQL fragment for Array.  Treats as an expression if an array of all two pairs, or as a SQL array otherwise.
         */
        _literalArray: function (v) {
            return Expression.isConditionSpecifier(v) ? this._literalExpression(BooleanExpression.fromValuePairs(v)) : this._arraySql(v);
        },

        /**
         * @return SQL fragment for a number.
         */
        _literalNumber: function (num) {
            var ret = "" + num;
            if (isNaN(num) || num === Infinity) {
                ret = string.format("'%s'", ret);
            }
            return ret;
        },

        /**
         * @return SQL fragment for Dataset.  Does a subselect inside parantheses.
         */
        _literalDataset: function (dataset) {
            return string.format("(%s)", this._subselectSql(dataset));
        },

        /**
         * @return SQL fragment for Date, using the ISO8601 format.
         */
        _literalDate: function (date) {
            return (this.requiresSqlStandardDateTimes ? "DATE '" : "'") + patio.dateToString(date) + "'";
        },

        /**
         *@return SQL fragment for a year.
         */
        _literalYear: function (o) {
            return patio.dateToString(o, this._static.YEAR_FORMAT);
        },

        /**
         *@return SQL fragment for a timestamp, using the ISO8601 format.
         */
        _literalTimestamp: function (v) {
            return this.formatTimestamp(v, this._static.TIMESTAMP_FORMAT);
        },

        /**
         *@return SQL fragment for a timestamp, using the ISO8601 format.
         */
        _literalTime: function (v) {
            return this.formatTimestamp(v, this._static.TIME_FORMAT);
        },

        /**
         * @return SQL fragment for a boolean.
         */
        _literalBoolean: function (b) {
            return b ? this._static.BOOL_TRUE : this._static.BOOL_FALSE;
        },

        /**
         * @return SQL fragment for SQL::Expression, result depends on the specific type of expression.
         * */
        _literalExpression: function (v) {
            return v.toString(this);
        },

        /**
         *@return SQL fragment for Hash, treated as an expression
         * */
        _literalHash: function (v) {
            return this._literalExpression(BooleanExpression.fromValuePairs(v));
        },

        /**@return SQL fragment for null*/
        _literalNull: function () {
            return this._static.NULL;
        },

        /**
         * @return SQL fragment for String.  Doubles \ and ' by default.
         * */
        _literalString: function (v) {
            var parts = this._splitString(v);
            var table = parts[0], column = parts[1], alias = parts[2], ret;
            if (!alias) {
                if (column && table) {
                    ret = this._literalExpression(QualifiedIdentifier.fromArgs([table, column]));
                } else {
                    ret = "'" + v.replace(/\\/g, "\\\\").replace(/'/g, "''") + "'";
                }
            } else {
                if (column && table) {
                    ret = new AliasedExpression(QualifiedIdentifier.fromArgs([table, column]), alias);
                } else {
                    ret = new AliasedExpression(new Identifier(column), alias);
                }
                ret = this.literal(ret);
            }
            return ret;
        },

        /**
         * @return SQL fragment for json.  Doubles ' by default.
         * */
        _literalJson: function (v) {
            throw new QueryError("Json not supported.");
        },

        /*SQL STATEMENT CREATION METHODS*/

        _selectQualifySql: function () {
            var o = this.__opts;
            var table = this.__opts.alwaysQualify;
            if (table && !o.sql) {
                array.intersect(Object.keys(o), this._static.QUALIFY_KEYS).forEach(function (k) {
                    o[k] = this._qualifiedExpression(o[k], table);
                }, this);
                if (!o.select || isEmpty(o.select)) {
                    o.select = [new ColumnAll(table)];
                }
            }
        },

        _deleteQualifySql: function () {
            return this._selectQualifySql.apply(this, arguments);
        },

        /**
         * @return the columns selected
         * */
        _selectColumnsSql: function () {
            return " " + this.__columnList(this.__opts.select);
        },

        /**@return the DISTINCT clause.*/
        _selectDistinctSql: function () {
            var distinct = this.__opts.distinct, ret = [];
            if (distinct) {
                ret.push(" DISTINCT");
                if (distinct.length) {
                    ret.push(format(" ON (%s)", this.__expressionList(distinct)));
                }
            }
            return ret.join("");
        },

        /**
         * @return the EXCEPT, INTERSECT, or UNION clause.
         * This uses a subselect for the compound datasets used, because using parantheses doesn't
         * work on all databases.
         **/
        _selectCompoundsSql: function () {
            var opts = this.__opts, compounds = opts.compounds, ret = [];
            if (compounds) {
                compounds.forEach(function (c) {
                    var type = c[0], dataset = c[1], all = c[2];
                    ret.push(string.format(" %s%s %s", type.toUpperCase(), all ? " ALL" : "", this._subselectSql(dataset)));
                }, this);
            }
            return ret.join("");
        },

        /**
         * @return the sql to add the list of tables to select FROM
         **/
        _selectFromSql: function () {
            var from = this.__opts.from;
            return from ? string.format(" %s%s", this._static.FROM, this._sourceList(from)) : "";
        },

        /**
         * @return the GROUP BY clause
         **/
        _selectGroupSql: function () {
            var group = this.__opts.group;
            return group ? string.format(" GROUP BY %s", this.__expressionList(group)) : "";
        },


        /**
         *@return the sql to add the filter criteria in the HAVING clause
         **/
        _selectHavingSql: function () {
            var having = this.__opts.having;
            return having ? string.format(" HAVING %s", this.literal(having)) : "";
        },

        /**
         * @return the JOIN clause.
         **/
        _selectJoinSql: function () {
            var join = this.__opts.join, ret = [];
            if (join) {
                join.forEach(function (j) {
                    ret.push(this.literal(j));
                }, this);
            }
            return ret.join("");
        },

        /**
         * @return the LIMIT and OFFSET clauses.
         * */
        _selectLimitSql: function () {
            var ret = [], limit = this.__opts.limit, offset = this.__opts.offset;
            !isUndefined(limit) && !isNull(limit) && (ret.push(format(" LIMIT %s", this.literal(limit))));
            !isUndefined(offset) && !isNull(offset) && (ret.push(format(" OFFSET %s", this.literal(offset))));
            return ret.join("");
        },

        /**
         * @return SQL for different locking modes.
         **/
        _selectLockSql: function () {
            var lock = this.__opts.lock, ret = [];
            if (lock) {
                if (lock === "update") {
                    ret.push(this._static.FOR_UPDATE);
                } else {
                    ret.push(" ", lock);
                }
            }
            return ret.join("");
        },

        /**
         * @return the SQL ORDER BY clause fragment.
         */
        _selectOrderSql: function () {
            var order = this.__opts.order;
            return order ? string.format(" ORDER BY %s", this.__expressionList(order)) : "";
        },

        /**
         * @return the SQL WHERE clause fragment.
         */
        _selectWhereSql: function () {
            var where = this.__opts.where;
            return where ? string.format(" WHERE %s", this.literal(where)) : "";
        },

        /**
         * @return SQL WITH clause fragment.
         * @param sql
         */
        _selectWithSql: function (sql) {
            var wit = this.__opts["with"];
            if (wit && wit.length) {
                //sql.length = 0;
                var base = sql.join("");
                sql.length = 0;
                sql.push([this._selectWithSqlBase(), wit.map(function (w) {
                    return [
                        this.quoteIdentifier(w.name),
                        (w.args ? ("(" + this.__argumentList(w.args) + ")") : ""),
                        " AS ",
                        this._literalDataset(w.dataset)
                    ].join("");
                }, this).join(this._static.COMMA_SEPARATOR), base].join(" "));
            }
        },

        _deleteWithSql: function () {
            return this._selectWithSql.apply(this, arguments);
        },

        _insertWithSql: function () {
            return this._selectWithSql.apply(this, arguments);
        },

        _updateWithSql: function () {
            return this._selectWithSql.apply(this, arguments);
        },

        _insertReturningSql: function (sql) {
            var opts = this.__opts, ret = "";
            if (opts.hasOwnProperty("returning")) {
                return [this._static.RETURNING, this.__columnList(array.toArray(opts.returning))].join("");
            }
            return ret;
        },

        _deleteReturningSql: function () {
            return this._insertReturningSql.apply(this, arguments);
        },

        _updateReturningSql: function () {
            return this._insertReturningSql.apply(this, arguments);
        },

        /**
         * @return The base keyword to use for the SQL WITH clause
         **/
        _selectWithSqlBase: function () {
            return this._static.SQL_WITH;
        },

        /**
         * @see patio.dataset._Sql#_selectFromSql
         */
        _deleteFromSql: function () {
            return this._selectFromSql();
        },

        /**
         * @see patio.dataset._Sql#_selectOrderSql
         */
        _deleteOrderSql: function () {
            return this._selectOrderSql();
        },

        /**
         * @see patio.dataset._Sql#_selectWhereSql
         */
        _deleteWhereSql: function () {
            return this._selectWhereSql();
        },

        /**
         * @see patio.dataset._Sql#_selectOrderSql
         */
        _updateOrderSql: function () {
            return this._selectOrderSql();
        },

        /**
         * @see patio.dataset._Sql#_selectWhereSql
         */
        _updateWhereSql: function () {
            return this._selectWhereSql();
        },

        /**
         * @return SQL fragment specifying the tables to delete from.
         * Includes join table if modifying joins is allowed.
         */
        _updateTableSql: function (sql) {
            var ret = [this._sourceList(this.__opts.from)];
            if (this.supportsModifyingJoins) {
                ret.push(this._selectJoinSql());
            }
            return ret.join("");
        },


        /**
         * @returns The SQL fragment specifying the columns and values to SET.
         * */
        _updateSetSql: function () {
            var values = this.__opts.values, defs = this.__opts.defaults, overrides = this.__opts.overrides;
            var st = [" SET "];
            if (isArray(values)) {
                var v = [], mergedDefsAndOverrides = false, length = values.length, ident, val;

                for (var i = 0; i < length; i++) {
                    val = values[i];
                    if (isHash(val)) {
                        mergedDefsAndOverrides = true;
                        val = merge({}, defs || {}, val);
                        val = merge({}, val, overrides || {});
                        for (var j in val) {
                            ident = this.stringToIdentifier(j);
                            v.push(this.quoteIdentifier(ident) + " = " + this.literal(val[j]));
                        }
                    } else if (isInstanceOf(val, Expression)) {
                        v.push(this._literalExpression(val).replace(/^\(|\)$/g, ""));
                    } else {
                        v.push(val);
                    }
                }
                if (!mergedDefsAndOverrides) {
                    val = merge({}, defs || {});
                    val = merge({}, val, overrides || {});
                    for (i in val) {
                        ident = this.stringToIdentifier(i);
                        v.push(this.quoteIdentifier(ident) + " = " + this.literal(val[i]));
                    }
                }
                st.push(v.join(this._static.COMMA_SEPARATOR));
            } else {
                st.push(values);
            }

            return st.join("");
        },

        /**
         * Converts an array of source names into into a comma separated list.
         **/
        _sourceList: function (source) {
            if (!Array.isArray(source)) {
                source = [source];
            }
            if (!source || !source.length) {
                throw new QueryError("No source specified for the query");
            }
            return " " + source.map(
                    function (s) {
                        return this.__tableRef(s);
                    }, this).join(this._static.COMMA_SEPARATOR);
        },

        /**
         * @return SQL to use if this dataset uses static SQL.  Since static SQL
         * can be a PlaceholderLiteralString in addition to a String,
         * we literalize nonstrings.
         **/
        _staticSql: function (sql) {
            return isString(sql) ? sql : this.literal(sql);

        },

        /**
         * @return SQL fragment for a subselect using the given database's SQL.
         **/
        _subselectSql: function (ds) {
            return ds.sql;
        },

        /**
         * @returns SQL fragment specifying a table name.
         **/
        __tableRef: function (t) {
            return isString(t) ? this._quotedIdentifier(t) : this.literal(t);
        },


        //Raise an InvalidOperation exception if deletion is not allowed
        //for this dataset
        __checkModificationAllowed: function () {
            if (this.__opts.group) {
                throw new QueryError("Grouped datasets cannot be modified");
            }
            if (!this.supportsModifyingJoins && this._joinedDataset) {
                throw new QueryError("Joined datasets cannot be modified");
            }
        },

        __toAliasedTableName: function (alias) {
            var ret;
            if (isString(alias)) {
                ret = alias;
            } else if (isInstanceOf(alias, Identifier)) {
                ret = alias.value;
            } else {
                throw new QueryError("Invalid table alias");
            }
            return ret;
        },

        getters: {
            //Same as selectS, not aliased directly to make subclassing simpler.
            sql: function () {
                return this.selectSql;
            },

            selectSql: function () {
                var selectSql;
                if (this.__opts.sql) {
                    selectSql = this._staticSql(this.__opts.sql);
                } else {
                    selectSql = this._clauseSql("select");
                }
                return selectSql;
            },

            deleteSql: function () {
                var opts = this.__opts;
                if (opts.sql) {
                    return this._staticSql(this.sql);
                } else {
                    this.__checkModificationAllowed();
                    return this._clauseSql("delete");
                }
            },

            truncateSql: function () {
                if (this.__opts.sql) {
                    return this._staticSql(this.__opts.sql);
                } else {
                    this.__checkModificationAllowed();
                    if (this.__opts.where) {
                        throw new QueryError("cant truncate filtered datasets");
                    }
                    return this._truncateSql(this._sourceList(this.__opts.from));
                }
            },

            exists: function () {
                return new LiteralString("EXISTS (" + this.selectSql + ")");
            },

            //Whether this dataset is a joined dataset
            _joinedDataset: function () {
                var from = this.__opts.from;
                return (isArray(from) && from.length > 1) || this.__opts.join;
            }
        }
    },

    static: {
        /**@lends patio.Dataset*/

        /**
         * Default FROM clause
         */
        FROM: "FROM",
        /**
         * Default SQL AND separator.
         */
        AND_SEPARATOR: " AND ",
        /**
         * Default SQL boolean false operator.
         */
        BOOL_FALSE: "'f'",
        /**
         * Default SQL boolean true operator.
         */
        BOOL_TRUE: "'t'",
        /**
         * Default SQL comma sperator.
         */
        COMMA_SEPARATOR: ', ',
        /**
         * Default COUNT expression.
         */
        COUNT_OF_ALL_AS_COUNT: sql.count(sql.literal('*')).as("count"),
        /**
         * Default alias for datasets.
         */
        DATASET_ALIAS_BASE_NAME: 't',
        /**
         * Default FOR UPDATE SQL fragment.
         */
        FOR_UPDATE: ' FOR UPDATE',
        /**
         * Hash of IS literals
         */
        IS_LITERALS: {NULL: 'NULL', true: 'TRUE', false: 'FALSE'},
        /**
         * Defaults IS OPERATORS. See {@link patio.sql.ComplexExpression.IS_OPERATORS}.
         */
        IS_OPERATORS: ComplexExpression.IS_OPERATORS,
        /**
         * Defaults N(Multi arity) OPERATORS. See {@link patio.sql.ComplexExpression.N_ARITY_OPERATORS}.
         */
        N_ARITY_OPERATORS: ComplexExpression.N_ARITY_OPERATORS,
        /**
         * Defaults TWO OPERATORS. See {@link patio.sql.ComplexExpression.TWO_ARITY_OPERATORS}.
         */
        TWO_ARITY_OPERATORS: ComplexExpression.TWO_ARITY_OPERATORS,
        /**
         * Defaults SQL NULL.
         */
        NULL: "NULL",
        /**
         * Default SQL clauses that need qualifying. This may be overrode by adapters.
         */
        QUALIFY_KEYS: ["select", "where", "having", "order", "group"],
        /**
         * Regexp used to replace '?' in {@link patio.sql.PlaceHolderLiteralString}
         */
        QUESTION_MARK: /\?/g,
        /**
         * Default SQL DELETE clause methods. This may be overrode by adapters.
         */
        DELETE_CLAUSE_METHODS: clauseMethods("delete", "qualify from where"),
        /**
         * Default SQL INSERT clause. This may be overrode by adapters.
         */
        INSERT_CLAUSE_METHODS: clauseMethods("insert", "into columns values"),
        /**
         * Default SQL SELECT clause. This may be overrode by adapters.
         */
        SELECT_CLAUSE_METHODS: clauseMethods("select", "qualify with distinct columns from join where group having compounds order limit lock"),
        /**
         * Default SQL UPDATE clause. This may be overrode by adapters.
         */
        UPDATE_CLAUSE_METHODS: clauseMethods("update", "table set where"),
        /**
         * Default SQL '*' literal string.
         */
        WILDCARD: new LiteralString('*'),

        /**
         * Default SQL 'RETURNING' literal string
         */
        RETURNING: " RETURNING ",

        /**
         * Default SQL WITH base. This may be overrode by adapters.
         */
        SQL_WITH: "WITH",

        /**
         * Default space to use when building SQL queries
         */
        SPACE: " ",

        clauseMethods: clauseMethods

    }
}).as(module);
