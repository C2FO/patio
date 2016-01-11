var comb = require("comb"),
    logging = comb.logging,
    Logger = logging.Logger,
    errors = require("../errors"),
    QueryError = errors.QueryError,
    DatasetError = errors.DatasetError,
    Promise = comb.Promise,
    PromiseList = comb.PromiseList,
    isUndefined = comb.isUndefined,
    isUndefinedOrNull = comb.isUndefinedOrNull,
    isString = comb.isString,
    isInstanceOf = comb.isInstanceOf,
    isString = comb.isString,
    isFunction = comb.isFunction,
    isNull = comb.isNull,
    merge = comb.merge,
    define = comb.define,
    graph = require("./graph"),
    actions = require("./actions"),
    features = require("./features"),
    query = require("./query"),
    sql = require("./sql"),
    SQL = require("../sql").sql,
    AliasedExpression = SQL.AliasedExpression,
    Identifier = SQL.Identifier,
    QualifiedIdentifier = SQL.QualifiedIdentifier;


var LOGGER = comb.logger("patio.Dataset");

define([actions, graph, features, query, sql], {
    instance: {

        /**@lends patio.Dataset.prototype*/

        /**
         * Class that is used for querying/retrieving datasets from a database.
         *
         * <p> Dynamically generated methods include
         * <ul>
         *     <li>Join methods from {@link patio.Dataset.CONDITIONED_JOIN_TYPES} and
         *         {@link  patio.Dataset.UNCONDITIONED_JOIN_TYPES}, these methods handle the type call
         *         to {@link patio.Dataset#joinTable}, so to invoke include all arguments that
         *         {@link patio.Dataset#joinTable} requires except the type parameter. The default list includes.
         *         <ul>
         *             <li>Conditioned join types that accept conditions.
         *                  <ul>
         *                      <li>inner - INNER JOIN</li>
         *                      <li>fullOuter - FULL OUTER</li>
         *                      <li>rightOuter - RIGHT OUTER JOIN</li>
         *                      <li>leftOuter - LEFT OUTER JOIN</li>
         *                      <li>full - FULL JOIN</li>
         *                      <li>right - RIGHT JOIN</li>
         *                      <li>left - LEFT JOIN</li>
         *                  </ul>
         *             </li>
         *             <li>Unconditioned join types that do not accept join conditions
         *                  <ul>
         *                      <li>natural - NATURAL JOIN</li>
         *                      <li>naturalLeft - NATURAL LEFT JOIN</li>
         *                      <li>naturalRight - NATURAL RIGHT JOIN</li>
         *                      <li>naturalFull - NATURA FULLL JOIN</li>
         *                      <li>cross - CROSS JOIN</li>
         *                  </ul>
         *             </li>
         *         </ul>
         *      </li>
         *  </li>
         * </ul>
         *
         * <p>
         *     <h4>Features:</h4>
         *     <p>
         *           Features that a particular {@link patio.Dataset} supports are shown in the example below.
         *           If you wish to implement an adapter please override these values depending on the database that
         *           you are developing the adapter for.
         *      </p>
         *      <pre class="code">
         *          var ds = DB.from("test");
         *
         *          //The default values returned
         *
         *          //Whether this dataset quotes identifiers.
         *          //Whether this dataset quotes identifiers.
         *          ds.quoteIdentifiers //=>true
         *
         *          //Whether this dataset will provide accurate number of rows matched for
         *          //delete and update statements.  Accurate in this case is the number of
         *          //rows matched by the dataset's filter.
         *          ds.providesAccurateRowsMatched; //=>true
         *
         *          //Times Whether the dataset requires SQL standard datetimes (false by default,
         *          // as most allow strings with ISO 8601 format).
         *          ds.requiresSqlStandardDate; //=>false
         *
         *          //Whether the dataset supports common table expressions (the WITH clause).
         *          ds.supportsCte; //=>true
         *
         *          //Whether the dataset supports the DISTINCT ON clause, false by default.
         *          ds.supportsDistinctOn; //=>false
         *
         *          //Whether the dataset supports the INTERSECT and EXCEPT compound operations, true by default.
         *          ds.supportsIntersectExcept; //=>true
         *
         *          //Whether the dataset supports the INTERSECT ALL and EXCEPT ALL compound operations, true by default
         *          ds.supportsIntersectExceptAll; //=>true
         *
         *          //Whether the dataset supports the IS TRUE syntax.
         *          ds.supportsIsTrue; //=>true
         *
         *          //Whether the dataset supports the JOIN table USING (column1, ...) syntax.
         *          ds.supportsJoinUsing; //=>true
         *
         *          //Whether modifying joined datasets is supported.
         *          ds.supportsModifyingJoin; //=>false
         *
         *          //Whether the IN/NOT IN operators support multiple columns when an
         *          ds.supportsMultipleColumnIn; //=>true
         *
         *          //Whether the dataset supports timezones in literal timestamps
         *          ds.supportsTimestampTimezone; //=>false
         *
         *          //Whether the dataset supports fractional seconds in literal timestamps
         *          ds.supportsTimestampUsecs; //=>true
         *
         *          //Whether the dataset supports window functions.
         *          ds.supportsWindowFunctions; //=>false
         *       </pre>
         * <p>
         * <p>
         *     <h4>Actions</h4>
         *     <p>
         *         Each dataset does not actually send any query to the database until an action method has
         *         been called upon it(with the exception of {@link patio.Dataset#graph} because columns
         *         from the other table might need retrived in order to set up the graph). Each action
         *         returns a <i>comb.Promise</i> that will be resolved with the result or errback, it is important
         *         that you account for errors otherwise it can be difficult to track down issues.
         *         The list of action methods is:
         *         <ul>
         *             <li>{@link patio.Dataset#all}</li>
         *             <li>{@link patio.Dataset#one}</li>
         *             <li>{@link patio.Dataset#avg}</li>
         *             <li>{@link patio.Dataset#count}</li>
         *             <li>{@link patio.Dataset#columns}</li>
         *             <li>{@link patio.Dataset#remove}</li>
         *             <li>{@link patio.Dataset#forEach}</li>
         *             <li>{@link patio.Dataset#empty}</li>
         *             <li>{@link patio.Dataset#first}</li>
         *             <li>{@link patio.Dataset#get}</li>
         *             <li>{@link patio.Dataset#import}</li>
         *             <li>{@link patio.Dataset#insert}</li>
         *             <li>{@link patio.Dataset#save}</li>
         *             <li>{@link patio.Dataset#insertMultiple}</li>
         *             <li>{@link patio.Dataset#saveMultiple}</li>
         *             <li>{@link patio.Dataset#interval}</li>
         *             <li>{@link patio.Dataset#last}</li>
         *             <li>{@link patio.Dataset#map}</li>
         *             <li>{@link patio.Dataset#max}</li>
         *             <li>{@link patio.Dataset#min}</li>
         *             <li>{@link patio.Dataset#multiInsert}</li>
         *             <li>{@link patio.Dataset#range}</li>
         *             <li>{@link patio.Dataset#selectHash}</li>
         *             <li>{@link patio.Dataset#selectMap}</li>
         *             <li>{@link patio.Dataset#selectOrderMap}</li>
         *             <li>{@link patio.Dataset#set}</li>
         *             <li>{@link patio.Dataset#singleRecord}</li>
         *             <li>{@link patio.Dataset#singleValue}</li>
         *             <li>{@link patio.Dataset#sum}</li>
         *             <li>{@link patio.Dataset#toCsv}</li>
         *             <li>{@link patio.Dataset#toHash}</li>
         *             <li>{@link patio.Dataset#truncate}</li>
         *             <li>{@link patio.Dataset#update}</li>
         *         </ul>
         *
         *     </p>
         * </p>
         *
         * @constructs
         *
         *
         * @param {patio.Database} db the database this dataset should use when querying for data.
         * @param {Object} opts options to set on this dataset instance
         *
         * @property {Function} rowCb callback to be invoked for each row returned from the database.
         *                      the return value will be used as the result of query. The rowCb can also return a promise,
         *                      The resolved value of the promise will be used as result.
         *
         * @property {String} identifierInputMethod this is the method that will be called on each identifier returned from the database.
         *                                          This value will be defaulted to whatever the identifierInputMethod
         *                                          is on the database used in initialization.
         *
         * @property {String} identifierOutputMethod this is the method that will be called on each identifier sent to the database.
         *                                          This value will be defaulted to whatever the identifierOutputMethod
         *                                          is on the database used in initialization.
         * @property {String} firstSourceAlias The first source (primary table) for this dataset. If the table is aliased, returns the aliased name.
         *                                     throws a {patio.DatasetError} tf the dataset doesn't have a table.
         * <pre class="code">
         *   DB.from("table").firstSourceAlias;
         *   //=> "table"
         *
         *   DB.from("table___t").firstSourceAlias;
         *   //=> "t"
         * </pre>
         *
         * @property {String} firstSourceTable  The first source (primary table) for this dataset.  If the dataset doesn't
         *                                      have a table, raises a {@link patio.erros.DatasetError}.
         *<pre class="code">
         *
         *  DB.from("table").firstSourceTable;
         *         //=> "table"
         *
         *  DB.from("table___t").firstSourceTable;
         *         //=> "t"
         * </pre>
         * @property {Boolean} isSimpleSelectAll Returns true if this dataset is a simple SELECT * FROM {table}, otherwise false.
         * <pre class="code">
         *     DB.from("items").isSimpleSelectAll; //=> true
         *     DB.from("items").filter({a : 1}).isSimpleSelectAll; //=> false
         * </pre>
         * @property {boolean} [quoteIdentifiers=true]  Whether this dataset quotes identifiers.
         * @property {boolean} [providesAccurateRowsMatched=true] Whether this dataset will provide accurate number of rows matched for
         *   delete and update statements.  Accurate in this case is the number of
         *   rows matched by the dataset's filter.
         * @property {boolean} [requiresSqlStandardDate=false] Whether the dataset requires SQL standard datetimes (false by default,
         *                                                     as most allow strings with ISO 8601 format).
         * @property {boolean} [supportsCte=true] Whether the dataset supports common table expressions (the WITH clause).
         * @property {boolean} [supportsDistinctOn=false] Whether the dataset supports the DISTINCT ON clause, false by default.
         * @property {boolean} [supportsIntersectExcept=true] Whether the dataset supports the INTERSECT and EXCEPT compound operations, true by default.
         * @property {boolean} [supportsIntersectExceptAll=true] Whether the dataset supports the INTERSECT ALL and EXCEPT ALL compound operations, true by default.
         * @property {boolean} [supportsIsTrue=true] Whether the dataset supports the IS TRUE syntax.
         * @property {boolean} [supportsJoinUsing=true] Whether the dataset supports the JOIN table USING (column1, ...) syntax.
         * @property {boolean} [supportsModifyingJoin=false] Whether modifying joined datasets is supported.
         * @property {boolean} [supportsMultipleColumnIn=true] Whether the IN/NOT IN operators support multiple columns when an
         * @property {boolean} [supportsTimestampTimezone=false] Whether the dataset supports timezones in literal timestamps
         * @property {boolean} [supportsTimestampUsecs=true] Whether the dataset supports fractional seconds in literal timestamps
         * @property {boolean} [supportsWindowFunctions=false] Whether the dataset supports window functions.
         * @property {patio.sql.Identifier[]} [sourceList=[]] a list of sources for this dataset.
         * @property {patio.sql.Identifier[]} [joinSourceList=[]] a list of join sources
         * @property {Boolean} hasSelectSource true if this dataset already has a select sources.
         */
        constructor: function (db, opts) {
            this._super(arguments);
            this.db = db;
            this.__opts = {};
            this.__rowCb = null;
            if (db) {
                this.__quoteIdentifiers = db.quoteIdentifiers;
                this.__identifierInputMethod = db.identifierInputMethod;
                this.__identifierOutputMethod = db.identifierOutputMethod;
            }
        },


        /**
         * Returns a new clone of the dataset with with the given options merged into the current datasets options.
         * If the options changed include options in {@link patio.dataset.Query#COLUMN_CHANGE_OPTS}, the cached
         * columns are deleted.  This method should generally not be called
         * directly by user code.
         *
         * @param {Object} opts options to merge into the curred datasets options and applied to the returned dataset.
         * @return [patio.Dataset] a cloned dataset with the merged options
         **/
        mergeOptions: function (opts) {
            opts = isUndefined(opts) ? {} : opts;
            var ds = new this._static(this.db, {});
            ds.rowCb = this.rowCb;
            this._static.FEATURES.forEach(function (f) {
                ds[f] = this[f];
            }, this);
            var dsOpts = ds.__opts = merge({}, this.__opts, opts);
            ds.identifierInputMethod = this.identifierInputMethod;
            ds.identifierOutputMethod = this.identifierOutputMethod;
            var columnChangeOpts = this._static.COLUMN_CHANGE_OPTS;
            if (Object.keys(opts).some(function (o) {
                    return columnChangeOpts.indexOf(o) !== -1;
                })) {
                dsOpts.columns = null;
            }
            return ds;
        },


        /**
         * Converts a string to an {@link patio.sql.Identifier}, {@link patio.sql.QualifiedIdentifier},
         * or {@link patio.sql.AliasedExpression}, depending on the format:
         *
         * <ul>
         *      <li>For columns : table__column___alias.</li>
         *      <li>For tables : schema__table___alias.</li>
         * </ul>
         * each portion of the identifier is optional. See example below
         *
         * @example
         *
         * ds.stringToIdentifier("a") //= > new patio.sql.Identifier("a");
         * ds.stringToIdentifier("table__column"); //=> new patio.sql.QualifiedIdentifier(table, column);
         * ds.stringToIdentifier("table__column___alias");
         *      //=> new patio.sql.AliasedExpression(new patio.sql.QualifiedIdentifier(table, column), alias);
         *
         * @param {String} name the name to covert to an an {@link patio.sql.Identifier}, {@link patio.sql.QualifiedIdentifier},
         * or {@link patio.sql.AliasedExpression}.
         *
         * @return  {patio.sql.Identifier|patio.sql.QualifiedIdentifier|patio.sql.AliasedExpression} an identifier generated based on the name string.
         */
        stringToIdentifier: function (name) {
            if (isString(name)) {
                var parts = this._splitString(name),
                    schema = parts[0], table = parts[1], alias = parts[2],
                    identifier;
                if (schema && table && alias) {
                    identifier = new AliasedExpression(new QualifiedIdentifier(schema, table), alias);
                } else if (schema && table) {
                    identifier = new QualifiedIdentifier(schema, table);
                } else if (table && alias) {
                    identifier = new AliasedExpression(new Identifier(table), alias);
                } else {
                    identifier = new Identifier(table);
                }
                return identifier;
            } else {
                return name;
            }
        },

        /**
         * Can either be a string or null.
         *
         *
         * @example
         * //columns
         *  table__column___alias //=> table.column as alias
         *  table__column //=> table.column
         *  //tables
         *  schema__table___alias //=> schema.table as alias
         *  schema__table //=> schema.table
         *
         * //name and alias
         * columnOrTable___alias //=> columnOrTable as alias
         *
         *
         *
         * @return {String[]} an array with the elements being:
         * <ul>
         *      <li>For columns :[table, column, alias].</li>
         *      <li>For tables : [schema, table, alias].</li>
         * </ul>
         */
        _splitString: function (s) {
            var ret, m;
            if ((m = s.match(this._static.COLUMN_REF_RE1)) !== null) {
                ret = m.slice(1);
            }
            else if ((m = s.match(this._static.COLUMN_REF_RE2)) !== null) {
                ret = [null, m[1], m[2]];
            }
            else if ((m = s.match(this._static.COLUMN_REF_RE3)) !== null) {
                ret = [m[1], m[2], null];
            }
            else {
                ret = [null, s, null];
            }
            return ret;
        },

        /**
         * @ignore
         **/
        getters: {

            rowCb: function () {
                return this.__rowCb;
            },

            identifierInputMethod: function () {
                return this.__identifierInputMethod;
            },

            identifierOutputMethod: function () {
                return this.__identifierOutputMethod;
            },

            firstSourceAlias: function () {
                var source = this.__opts.from;
                if (isUndefinedOrNull(source) || !source.length) {
                    throw new DatasetError("No source specified for the query");
                }
                source = source[0];
                if (isInstanceOf(source, AliasedExpression)) {
                    return source.alias;
                } else if (isString(source)) {
                    var parts = this._splitString(source);
                    var alias = parts[2];
                    return alias ? alias : source;
                } else {
                    return source;
                }
            },

            firstSourceTable: function () {
                var source = this.__opts.from;
                if (isUndefinedOrNull(source) || !source.length) {
                    throw new QueryError("No source specified for the query");
                }
                source = source[0];
                if (isInstanceOf(source, AliasedExpression)) {
                    return source.expression;
                } else if (isString(source)) {
                    var parts = this._splitString(source);
                    return source;
                } else {
                    return source;
                }
            },

            sourceList: function () {
                return (this.__opts.from || []).map(this.stringToIdentifier, this);
            },

            joinSourceList: function () {
                return (this.__opts.join || []).map(function (join) {
                    return this.stringToIdentifier(join.tableAlias || join.table);
                }, this);
            },

            hasSelectSource: function () {
                var select = this.__opts.select;
                return !(isUndefinedOrNull(select) || select.length === 0);
            }
        },

        /**
         * @ignore
         **/
        setters: {
            /**@lends patio.Dataset.prototype*/

            identifierInputMethod: function (meth) {
                this.__identifierInputMethod = meth;
            },

            identifierOutputMethod: function (meth) {
                this.__identifierOutputMethod = meth;
            },

            rowCb: function (cb) {
                if (isFunction(cb) || isNull(cb)) {
                    this.__rowCb = cb;
                } else {
                    throw new DatasetError("rowCb mus be a function");
                }
            }
        }
    },

    static: {
        COLUMN_REF_RE1: /^(\w+)__(\w+)___(\w+)$/,
        COLUMN_REF_RE2: /^(\w+)___(\w+)$/,
        COLUMN_REF_RE3: /^(\w+)__(\w+)$/
    }
}).as(module);
