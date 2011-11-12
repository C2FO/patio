var comb = require("comb"),
    array = comb.array,
    sql = require("../sql").sql,
    LiteralString = sql.LiteralString,
    Expression = sql.Expression,
    ComplexExpression = sql.ComplexExpression,
    BooleanExpression = sql.BooleanExpression,
    PlaceHolderLiteralString = sql.PlaceHolderLiteralString,
    Identifier = sql.Identifier,
    QualifiedIdentifier = sql.QualifiedIdentifier,
    AliasedExpression = sql.AliasedExpression,
    StringExpression = sql.StringExpression,
    NumericExpression = sql.NumericExpression,
    OrderedExpression = sql.OrderedExpression,
    JoinClause = sql.JoinClause,
    JoinOnClause = sql.JoinOnClause,
    JoinUsingClause = sql.JoinUsingClause,
    ColumnAll = sql.ColumnAll,
    QueryError = require("../errors").QueryError;

var Dataset;

// The dataset options that require the removal of cached columns
// if changed.
var COLUMN_CHANGE_OPTS = ["select", "sql", "from", "join"]

// Which options don't affect the SQL generation.  Used by simple_select_all?
// to determine if this is a simple SELECT * FROM table.
var NON_SQL_OPTIONS = ["server", "defaults", "overrides", "graph", "eagerGraph", "graphAliases"];

// These symbols have _join methods created (e.g. inner_join) that
// call join_table with the symbol, passing along the arguments and
// block from the method call.
var CONDITIONED_JOIN_TYPES = ["inner", "fullOuter", "rightOuter", "leftOuter", "full", "right", "left"];

// These symbols have _join methods created (e.g. natural_join) that
// call join_table with the symbol.  They only accept a single table
// argument which is passed to join_table, and they raise an error
// if called with a block.
var UNCONDITIONED_JOIN_TYPES = ["natural", "naturalLeft", "naturalRight", "naturalFull", "cross"];

// All methods that return modified datasets with a joined table added.
var JOIN_METHODS = CONDITIONED_JOIN_TYPES.concat(UNCONDITIONED_JOIN_TYPES).map(
    function(joinType) {
        return joinType + "Join"
    }).concat(["join", "joinTable"]);

// Methods that return modified datasets
var QUERY_METHODS = ['addGraphAliases', "and","distinct","except","exclude", "filter","forUpdate","from",
    "fromSelf","graph","grep","group","groupAndCount","groupBy","having","intersect","invert",
    "limit","lockStyle","naked","or","order","orderAppend","orderBy","orderMore","orderPrepend",
    "paginate","qualify","query", "reverse","reverseOrder","select","selectAll","select_append",
    "selectMore","server", "setDefaults","setGraphAliases","setOverrides","unfiltered","ungraphed",
    "ungrouped","union", "unlimited","unordered","where","with","withRecursive","withSql"].concat(JOIN_METHODS);

var Dataset = comb.define(null, {
    instance : {

        constructor : function() {
            this.super(arguments);
            Dataset = require("../../lib").Dataset;
            this.static.CONDITIONED_JOIN_TYPES.forEach(function(type) {
                if (!this[type + "Join"]) {
                    this[type + "Join"] = function() {
                        var args = comb.argsToArray(arguments);
                        return this.joinTable.apply(this, [type].concat(args));
                    };
                }

            }, this);
            this.static.UNCONDITIONED_JOIN_TYPES.forEach(function(type) {
                if (!this[type + "Join"]) {
                    this[type + "Join"] = function(table) {
                        return this.joinTable.apply(this, [type, table]);
                    };
                }

            }, this);
        },
        // Adds an further filter to an existing filter using AND. If no filter
        // exists an error is raised. This method is identical to //filter except
        // it expects an existing filter.
        //
        //   DB[:table].filter(:a).and(:b) # SELECT * FROM table WHERE a AND b
        and : function(cond, block) {
            var tOpts = this.__opts, clauseObj = tOpts[tOpts.having ? "having" : "where"];
            if (clauseObj) {
                return this.filter.apply(this, arguments);
            } else {
                throw new QueryError("No existing filter found");
            }
        },

        as : function(alias) {
            return new AliasedExpression(this, alias);
        },

        // Adds an alternate filter to an existing filter using OR. If no filter
        // exists an +Error+ is raised.
        //
        //   DB[:items].filter(:a).or(:b) # SELECT * FROM items WHERE a OR b
        or : function() {
            var tOpts = this.__opts;
            var clause = (tOpts.having ? "having" : "where"), clauseObj = tOpts[clause];
            if (clauseObj) {
                var args = comb.argsToArray(arguments);
                args = args.length == 1 ? args[0] : args;
                var opts = {};
                opts[clause] = new BooleanExpression("OR", clauseObj, this._filterExpr(args))
                return this.mergeOptions(opts);
            } else {
                throw new QueryError("No existing filter found");
            }
        },


        // Returns a new clone of the dataset with with the given options merged.
        // If the options changed include options in COLUMN_CHANGE_OPTS, the cached
        // columns are deleted.  This method should generally not be called
        // directly by user code.
        mergeOptions : function(opts) {
            opts = comb.isUndefined(opts) ? {} : opts;
            var ds = new this.static(this.db, {});
            ds.rowCb = this.rowCb;
            this.static.FEATURES.forEach(function(f) {
                ds[f] = this[f]
            }, this);
            ds.__opts = comb.merge({}, this.__opts, opts);
            var columnChangeOpts = this.static.COLUMN_CHANGE_OPTS;
            if (Object.keys(opts).some(function(o) {
                return columnChangeOpts.indexOf(o) != -1;
            })) {
                ds.__opts.columns = null;
            }
            return ds;
        },

        // Returns a copy of the dataset with the SQL DISTINCT clause.
        // The DISTINCT clause is used to remove duplicate rows from the
        // output.  If arguments are provided, uses a DISTINCT ON clause,
        // in which case it will only be distinct on those columns, instead
        // of all returned columns.  Raises an error if arguments
        // are given and DISTINCT ON is not supported.
        //
        //  DB[:items].distinct => SQL: SELECT DISTINCT * FROM items
        //  DB[:items].order(:id).distinct(:id) # SQL: SELECT DISTINCT ON (id) * FROM items ORDER BY id
        distinct : function() {
            var args = comb.argsToArray(arguments);
            if (args.length && !this.supportsDistinctOn) {
                throw new QueryError("DISTICT ON is not supported");
            }
            args = args.map(function(a) {
                return comb.isString(a) ? new Identifier(a) : a;
            });
            return this.mergeOptions({distinct : args});
        },

        //Adds an EXCEPT clause using a second dataset object.
        // An EXCEPT compound dataset returns all rows in the current dataset
        // that are not in the given dataset.
        // Raises an +InvalidOperation+ if the operation is not supported.
        // Options:
        // :alias :: Use the given value as the from_self alias
        // :all :: Set to true to use EXCEPT ALL instead of EXCEPT, so duplicate rows can occur
        // :from_self :: Set to false to not wrap the returned dataset in a from_self, use with care.
        //
        //   DB[:items].except(DB[:other_items])
        //   // SELECT * FROM items EXCEPT SELECT * FROM other_items
        //
        //   DB[:items].except(DB[:other_items], :all=>true, :from_self=>false)
        //   // SELECT * FROM items EXCEPT ALL SELECT * FROM other_items
        //
        //   DB[:items].except(DB[:other_items], :alias=>:i)
        //   // SELECT * FROM (SELECT * FROM items EXCEPT SELECT * FROM other_items) AS i
        except : function(dataset, opts) {
            opts = comb.isUndefined(opts) ? {} : opts;
            if (!comb.isHash(opts)) {
                opts = {all : true};
            }
            if (!this.supportsIntersectExcept) {
                throw new QueryError("EXCEPT not supoorted");
            } else if (opts.hasOwnProperty("all") && !this.supportsIntersectExceptAll) {
                throw new QueryError("EXCEPT ALL not supported");
            }
            return this.compoundClone("except", dataset, opts);
        },

        // Performs the inverse of Dataset#filter.  Note that if you have multiple filter
        // conditions, this is not the same as a negation of all conditions.
        //
        //   DB[:items].exclude(:category => 'software')
        //   // SELECT * FROM items WHERE (category != 'software')
        //
        //   DB[:items].exclude(:category => 'software', :id=>3)
        //   // SELECT * FROM items WHERE ((category != 'software') OR (id != 3))
        exclude : function() {
            var cond = comb.argsToArray(arguments), tOpts = this.__opts;
            var clause = (tOpts["having"] ? "having" : "where"), clauseObj = tOpts[clause];
            cond = cond.length > 1 ? cond : cond[0];
            cond = this._filterExpr.call(this, cond);
            cond = BooleanExpression.invert(cond);
            if (clauseObj) {
                cond = new BooleanExpression("AND", clauseObj, cond)
            }
            var opts = {};
            opts[clause] = cond;
            return this.mergeOptions(opts);
        },

        // Returns a copy of the dataset with the given conditions imposed upon it.
        // If the query already has a HAVING clause, then the conditions are imposed in the
        // HAVING clause. If not, then they are imposed in the WHERE clause.
        //
        // filter accepts the following argument types:
        //
        // * Hash - list of equality/inclusion expressions
        // * Array - depends:
        //   * If first member is a string, assumes the rest of the arguments
        //     are parameters and interpolates them into the string.
        //   * If all members are arrays of length two, treats the same way
        //     as a hash, except it allows for duplicate keys to be
        //     specified.
        //   * Otherwise, treats each argument as a separate condition.
        // * String - taken literally
        // * Symbol - taken as a boolean column argument (e.g. WHERE active)
        // * Sequel::SQL::BooleanExpression - an existing condition expression,
        //   probably created using the Sequel expression filter DSL.
        //
        // filter also takes a block, which should return one of the above argument
        // types, and is treated the same way.  This block yields a virtual row object,
        // which is easy to use to create identifiers and functions.  For more details
        // on the virtual row support, see the {"Virtual Rows" guide}[link:files/doc/virtual_rows_rdoc.html]
        //
        // If both a block and regular argument are provided, they get ANDed together.
        //
        // Examples:
        //
        //   DB[:items].filter(:id => 3)
        //   // SELECT * FROM items WHERE (id = 3)
        //
        //   DB[:items].filter('price < ?', 100)
        //   // SELECT * FROM items WHERE price < 100
        //
        //   DB[:items].filter([[:id, (1,2,3)], [:id, 0..10]])
        //   // SELECT * FROM items WHERE ((id IN (1, 2, 3)) AND ((id >= 0) AND (id <= 10)))
        //
        //   DB[:items].filter('price < 100')
        //   // SELECT * FROM items WHERE price < 100
        //
        //   DB[:items].filter(:active)
        //   // SELECT * FROM items WHERE :active
        //
        //   DB[:items].filter{price < 100}
        //   // SELECT * FROM items WHERE (price < 100)
        //
        // Multiple filter calls can be chained for scoping:
        //
        //   software = dataset.filter(:category => 'software').filter{price < 100}
        //   // SELECT * FROM items WHERE ((category = 'software') AND (price < 100))
        //
        // See the the {"Dataset Filtering" guide}[link:files/doc/dataset_filtering_rdoc.html] for more examples and details.
        filter : function() {
            var args = [this.__opts["having"] ? "having" : "where"].concat(comb.argsToArray(arguments));
            return this._filter.apply(this, args);
        },

        //Returns a cloned dataset with a :update lock style.
        //
        //   DB[:table].for_update # SELECT * FROM table FOR UPDATE
        forUpdate : function() {
            return this.lockStyle("update");
        },

        // Returns a copy of the dataset with the source changed. If no
        // source is given, removes all tables.  If multiple sources
        // are given, it is the same as using a CROSS JOIN (cartesian product) between all tables.
        //
        //   DB[:items].from # SQL: SELECT *
        //   DB[:items].from(:blah) # SQL: SELECT * FROM blah
        //   DB[:items].from(:blah, :foo) # SQL: SELECT * FROM blah, foo
        from : function(source) {
            source = comb.argsToArray(arguments);
            var tableAliasNum = 0, sources = [];
            source.forEach(function(s) {
                if (comb.isInstanceOf(s, Dataset)) {
                    sources.push(new AliasedExpression(s, this.datasetAlias(++tableAliasNum)));
                } else if (comb.isHash(s)) {
                    for (var i in s) {
                        sources.push(new AliasedExpression(new Identifier(i), s[i]));
                    }
                } else if (comb.isString(s)) {
                    sources.push(this.stringToIdentifier(s))
                } else {
                    sources.push(s);
                }
            }, this);

            var o = {from : sources.length ? sources : null}
            if (tableAliasNum) {
                o.numDatasetSources = tableAliasNum;
            }
            return this.mergeOptions(o)
        },

        // Returns a dataset selecting from the current dataset.
        // Supplying the :alias option controls the alias of the result.
        //
        //   ds = DB[:items].order(:name).select(:id, :name)
        //   # SELECT id,name FROM items ORDER BY name
        //
        //   ds.from_self
        //   # SELECT * FROM (SELECT id, name FROM items ORDER BY name) AS t1
        //
        //   ds.from_self(:alias=>:foo)
        //   # SELECT * FROM (SELECT id, name FROM items ORDER BY name) AS foo
        fromSelf : function(opts) {
            opts = comb.isUndefined(opts) ? {} : opts;
            var fs = {};
            var nonSqlOptions = this.static.NON_SQL_OPTIONS;
            Object.keys(this.__opts).forEach(function(k) {
                if (nonSqlOptions.indexOf(k) == -1) {
                    fs[k] = null;
                }
            });
            return this.mergeOptions(fs).from(opts["alias"] ? this.as(opts["alias"]) : this);
        },

        // Match any of the columns to any of the patterns. The terms can be
        // strings (which use LIKE) or regular expressions (which are only
        // supported on MySQL and PostgreSQL).  Note that the total number of
        // pattern matches will be Array(columns).length * Array(terms).length,
        // which could cause performance issues.
        //
        // Options (all are boolean):
        //
        // :all_columns :: All columns must be matched to any of the given patterns.
        // :all_patterns :: All patterns must match at least one of the columns.
        // :case_insensitive :: Use a case insensitive pattern match (the default is
        //                      case sensitive if the database supports it).
        //
        // If both :all_columns and :all_patterns are true, all columns must match all patterns.
        //
        // Examples:
        //
        //   dataset.grep(:a, '%test%')
        //   // SELECT * FROM items WHERE (a LIKE '%test%')
        //
        //   dataset.grep([:a, :b], %w'%test% foo')
        //   // SELECT * FROM items WHERE ((a LIKE '%test%') OR (a LIKE 'foo') OR (b LIKE '%test%') OR (b LIKE 'foo'))
        //
        //   dataset.grep([:a, :b], %w'%foo% %bar%', :all_patterns=>true)
        //   // SELECT * FROM a WHERE (((a LIKE '%foo%') OR (b LIKE '%foo%')) AND ((a LIKE '%bar%') OR (b LIKE '%bar%')))
        //
        //   dataset.grep([:a, :b], %w'%foo% %bar%', :all_columns=>true)
        //   // SELECT * FROM a WHERE (((a LIKE '%foo%') OR (a LIKE '%bar%')) AND ((b LIKE '%foo%') OR (b LIKE '%bar%')))
        //
        //   dataset.grep([:a, :b], %w'%foo% %bar%', :all_patterns=>true, :all_columns=>true)
        //   // SELECT * FROM a WHERE ((a LIKE '%foo%') AND (b LIKE '%foo%') AND (a LIKE '%bar%') AND (b LIKE '%bar%'))
        grep : function(columns, patterns, opts) {
            opts = comb.isUndefined(opts) ? {} : opts;
            var conds;
            if (opts.hasOwnProperty("allPatterns")) {
                conds = array.toArray(patterns).map(function(pat) {
                    return BooleanExpression.fromArgs(
                        [(opts.allColumns ? "AND" : "OR")]
                            .concat(array.toArray(columns)
                            .map(function(c) {
                                return StringExpression.like(c, pat, opts);
                            })));
                });
                return this.filter(BooleanExpression.fromArgs([opts.allPatterns ? "AND" : "OR"].concat(conds)));
            } else {
                conds = array.toArray(columns)
                    .map(function(c) {
                        return BooleanExpression.fromArgs(["OR"].concat(array.toArray(patterns).map(function(pat) {
                            return StringExpression.like(c, pat, opts);
                        })));
                    });
                return this.filter(BooleanExpression.fromArgs([opts.allColumns ? "AND" : "OR"].concat(conds)));
            }
        },

        like : function() {
            return this.grep.apply(this, arguments);
        },


        // Returns a copy of the dataset with the results grouped by the value of
        // the given columns.
        //
        //   DB[:items].group(:id) # SELECT * FROM items GROUP BY id
        //   DB[:items].group(:id, :name) # SELECT * FROM items GROUP BY id, name
        group : function(columns) {
            columns = comb.argsToArray(arguments);
            return this.mergeOptions({group : (array.compact(columns).length == 0 ? null : columns.map(function(c) {
                return comb.isString(c) ? new Identifier(c) : c;
            }))});
        },

        // Alias of group
        groupBy : function() {
            return this.group.apply(this, arguments);
        },


        // Returns a dataset grouped by the given column with count by group.
        // Column aliases may be supplied, and will be included in the select clause.
        //
        // Examples:
        //
        //   DB[:items].group_and_count(:name).all
        //   # SELECT name, count(*) AS count FROM items GROUP BY name
        //   # => [{:name=>'a', :count=>1}, ...]
        //
        //   DB[:items].group_and_count(:first_name, :last_name).all
        //   # SELECT first_name, last_name, count(*) AS count FROM items GROUP BY first_name, last_name
        //   # => [{:first_name=>'a', :last_name=>'b', :count=>1}, ...]
        //
        //   DB[:items].group_and_count(:first_name___name).all
        //   # SELECT first_name AS name, count(*) AS count FROM items GROUP BY first_name
        //   # => [{:name=>'a', :count=>1}, ...]
        groupAndCount : function() {
            var columns = comb.argsToArray(arguments);
            var group = this.group.apply(this, columns.map(function(c) {
                return this.unaliasedIdentifier(c);
            }, this));
            return group.select.apply(group, columns.concat([this.static.COUNT_OF_ALL_AS_COUNT]));

        },

        // Returns a copy of the dataset with the HAVING conditions changed. See #filter for argument types.
        //
        //   DB[:items].group(:sum).having(:sum=>10)
        //   # SELECT * FROM items GROUP BY sum HAVING (sum = 10)
        having : function(cond) {
            cond = comb.argsToArray(arguments).map(function(s) {
                return comb.isString(s) && s !== '' ? this.stringToIdentifier(s) : s
            }, this);
            return this._filter.apply(this, ["having"].concat(cond));
        },

        // Adds an INTERSECT clause using a second dataset object.
        // An INTERSECT compound dataset returns all rows in both the current dataset
        // and the given dataset.
        // Raises an +InvalidOperation+ if the operation is not supported.
        // Options:
        // :alias :: Use the given value as the from_self alias
        // :all :: Set to true to use INTERSECT ALL instead of INTERSECT, so duplicate rows can occur
        // :from_self :: Set to false to not wrap the returned dataset in a from_self, use with care.
        //
        //   DB[:items].intersect(DB[:other_items])
        //   // SELECT * FROM (SELECT * FROM items INTERSECT SELECT * FROM other_items) AS t1
        //
        //   DB[:items].intersect(DB[:other_items], :all=>true, :from_self=>false)
        //   // SELECT * FROM items INTERSECT ALL SELECT * FROM other_items
        //
        //   DB[:items].intersect(DB[:other_items], :alias=>:i)
        //   // SELECT * FROM (SELECT * FROM items INTERSECT SELECT * FROM other_items) AS i
        intersect : function(dataset, opts) {
            opts = comb.isUndefined(opts) ? {} : opts;
            if (!comb.isHash(opts)) {
                opts = {all : opts};
            }
            if (!this.supportsIntersectExcept) {
                throw new QueryError("INTERSECT not supported");
            } else if (opts.all && !this.supportsIntersectExceptAll) {
                throw new QueryError("INTERSECT ALL not supported");
            }
            return this.compoundClone("intersect", dataset, opts);
        },

        // Inverts the current filter.
        //
        //   DB[:items].filter(:category => 'software').invert
        //   # SELECT * FROM items WHERE (category != 'software')
        //
        //   DB[:items].filter(:category => 'software', :id=>3).invert
        //   # SELECT * FROM items WHERE ((category != 'software') OR (id != 3))
        invert : function() {
            var having = this.__opts.having, where = this.__opts.where;
            if (!(having || where)) {
                throw new QueryError("No current filter");
            }
            var o = {}
            if (having) {
                o.having = BooleanExpression.invert(having);
            }
            if (where) {
                o.where = BooleanExpression.invert(where);
            }
            return this.mergeOptions(o);
        },

        // Alias of +inner_join+
        join : function() {
            return this.innerJoin.apply(this, arguments);
        },

        // Returns a joined dataset.  Uses the following arguments:
        //
        // * type - The type of join to do (e.g. :inner)
        /// * table - Depends on type:
        ///   * Dataset - a subselect is performed with an alias of tN for some value of N
        ///   * Model (or anything responding to :table_name) - table.table_name
        ///   * String, Symbol: table
        // * expr - specifies conditions, depends on type:
        //   * Hash, Array of two element arrays - Assumes key (1st arg) is column of joined table (unless already
        //     qualified), and value (2nd arg) is column of the last joined or primary table (or the
        //     :implicit_qualifier option).
        //     To specify multiple conditions on a single joined table column, you must use an array.
        //     Uses a JOIN with an ON clause.
        //   * Array - If all members of the array are symbols, considers them as columns and
        //     uses a JOIN with a USING clause.  Most databases will remove duplicate columns from
        //     the result set if this is used.
        //   * nil - If a block is not given, doesn't use ON or USING, so the JOIN should be a NATURAL
        //     or CROSS join. If a block is given, uses an ON clause based on the block, see below.
        //   * Everything else - pretty much the same as a using the argument in a call to filter,
        //     so strings are considered literal, symbols specify boolean columns, and Sequel
        ///     expressions can be used. Uses a JOIN with an ON clause.
        // * options - a hash of options, with any of the following keys:
        //   * :table_alias - the name of the table's alias when joining, necessary for joining
        //     to the same table more than once.  No alias is used by default.
        //   * :implicit_qualifier - The name to use for qualifying implicit conditions.  By default,
        //     the last joined or primary table is used.
        // * block - The block argument should only be given if a JOIN with an ON clause is used,
        //   in which case it yields the table alias/name for the table currently being joined,
        //   the table alias/name for the last joined (or first table), and an array of previous
        //   SQL::JoinClause. Unlike +filter+, this block is not treated as a virtual row block.
        joinTable : function() {
            var args = comb.argsToArray(arguments), cb, type, table, expr, options;
            if (comb.isFunction(args[args.length - 1])) {
                cb = args[args.length - 1];
                args.pop();
            } else {
                cb = null;
            }
            type = args.shift(),table = args.shift(),expr = args.shift(),options = args.shift();
            expr = comb.isUndefined(expr) ? null : expr,options = comb.isUndefined(options) ? {} : options;

            var h;
            var usingJoin = comb.isArray(expr) && expr.length && expr.every(function(x) {
                return comb.isString(x) || comb.isInstanceOf(x, Identifier)
            });
            if (usingJoin && !this.supportsJoinUsing) {
                h = {};
                expr.forEach(function(s) {
                    h[s] = s;
                })
                return this.joinTable(type, table, h, options);
            }
            var tableAlias, lastAlias;
            if (comb.isHash(options)) {
                tableAlias = options.tableAlias;
                lastAlias = options.implicitQualifier;
            } else if (comb.isString(options) || comb.isInstanceOf(options, Identifier)) {
                tableAlias = options;
                lastAlias = null;
            } else {
                throw new QueryError("Invalid options format for joinTable %j4", [options]);
            }
            var tableAliasNum, tableName;
            if (comb.isInstanceOf(table, Dataset)) {
                if (!tableAlias) {
                    tableAliasNum = (this.__opts.numDatasetSources || 0) + 1;
                    tableAlias = this.datasetAlias(tableAliasNum);
                }
                tableName = tableAlias;
            } else {
                if (table.hasOwnProperty("tableName")) {
                    table = table.tableName;
                }
                if (comb.isArray(table)) {
                    table = table.map(this.stringToIdentifier, this);
                } else {
                    table = comb.isString(table) ? this.stringToIdentifier(table) : table;
                    var parts = this.splitAlias(table), implicitTableAlias = parts[1];
                    table = parts[0]
                    tableAlias = tableAlias || implicitTableAlias;
                    tableName = tableAlias || table;
                }
            }
            var join;
            if (!expr && !cb) {
                join = new JoinClause(type, table, tableAlias);
            } else if (usingJoin) {
                if (cb) {
                    throw new QueryError("cant use a cb if an array is given");
                }
                join = new JoinUsingClause(expr, type, table, tableAlias);
            } else {
                lastAlias = lastAlias || this.__opts["lastJoinedTable"] || this.firstSourceAlias();
                if (Expression.isConditionSpecifier(expr)) {
                    var newExpr = [];
                    for (var i in expr) {
                        var val = expr[i];
                        if (comb.isArray(val) && val.length == 2) {
                            i = val[0],val = val[1];
                        }
                        var k = this.qualifiedColumnName(i, tableName), v;
                        if (comb.isString(val)) {
                            v = this.qualifiedColumnName(val, lastAlias);
                        } else {
                            v = val;
                        }
                        newExpr.push([k, v]);
                    }
                    expr = newExpr;
                }
                if (comb.isFunction(cb)) {
                    var expr2 = cb.apply(sql, [tableName, lastAlias, this.__opts.join || []]);
                    expr = expr ? new BooleanExpression("AND", expr, expr2) : expr2;
                }
                join = new JoinOnClause(expr, type, table, tableAlias);
            }
            var opts = {join : (this.__opts.join || []).concat([join]), lastJoinedTable :  tableName};
            if (tableAliasNum) {
                opts.numDatasetSources = tableAliasNum;
            }
            return this.mergeOptions(opts);

        },


        // If given an integer, the dataset will contain only the first l results.
        // If given a range, it will contain only those at offsets within that
        // range. If a second argument is given, it is used as an offset. To use
        // an offset without a limit, pass nil as the first argument.
        //
        //   DB[:items].limit(10) # SELECT * FROM items LIMIT 10
        //   DB[:items].limit(10, 20) # SELECT * FROM items LIMIT 10 OFFSET 20
        //   DB[:items].limit(10...20) # SELECT * FROM items LIMIT 10 OFFSET 10
        //   DB[:items].limit(10..20) # SELECT * FROM items LIMIT 11 OFFSET 10
        //   DB[:items].limit(nil, 20) # SELECT * FROM items OFFSET 20
        limit : function(limit, offset) {
            if (this.__opts.sql) {
                return this.fromSelf().limit(limit, offset);
            }
            if (comb.isArray(limit) && limit.length == 2) {
                offset = limit[0];
                limit = limit[1] - limit[0] + 1;
            }
            if (comb.isString(limit) || comb.isInstanceOf(limit, LiteralString)) {
                limit = parseInt("" + limit, 10);
            }
            if (comb.isNumber(limit) && limit < 1) {
                throw new QueryError("Limit must be >= 1");
            }
            var opts = {limit : limit};
            if (offset) {
                if (comb.isString(offset) || comb.isInstanceOf(offset, LiteralString)) {
                    offset = parseInt("" + offset, 10);
                    isNaN(offset) && (offset = 0);
                }
                if (comb.isNumber(offset) && offset < 0) {
                    throw new QueryError("Offset must be >= 0");
                }
                opts.offset = offset;
            }
            return this.mergeOptions(opts);
        },

        __createBoolExpression : function(op, obj) {
            var pairs = [];
            for (var i in obj) {
                pairs.push(new BooleanExpression(op, new Identifier(i), obj[i]));
            }
            return pairs.length == 1 ? pairs[0] : BooleanExpression.fromArgs(["AND"].concat(pairs));
        },

        __createBetweenExpression : function(obj, invert) {
            var pairs = [];
            for (var i in obj) {
                var v = obj[i];
                if (comb.isArray(v) && v.length) {
                    var ident = this.stringToIdentifier(i);
                    pairs.push(new BooleanExpression("AND", new BooleanExpression("gte", ident, v[0]), new BooleanExpression("lte", ident, v[1])));
                } else {
                    throw new QueryError("Between requires an array for the value");
                }
            }
            var ret = pairs.length == 1 ? pairs[0] : BooleanExpression.fromArgs(["AND"].concat(pairs))
            return invert ? BooleanExpression.invert(ret) : ret;
        },

        neq : function(obj) {
            return this.filter(this.__createBetweenExpression("neq", obj));
        },

        eq : function(obj) {
            return this.filter(this.__createBetweenExpression("eq", obj));
        },

        gt : function(obj) {
            return this.filter(this.__createBoolExpression("gt", obj));
        },
        lt : function(obj) {
            return this.filter(this.__createBoolExpression("lt", obj));
        },
        gte : function(obj) {
            return this.filter(this.__createBoolExpression("gte", obj));
        },
        lte : function(obj) {
            return this.filter(this.__createBoolExpression("lte", obj));
        },

        between : function(obj) {
            return this.filter(this.__createBetweenExpression(obj));
        },

        notBetween : function(obj) {
            return this.filter(this.__createBetweenExpression(obj, true));
        },

        // Returns a cloned dataset with the given lock style.  If style is a
        // string, it will be used directly.  Otherwise, a symbol may be used
        // for database independent locking.  Currently :update is respected
        // by most databases, and :share is supported by some.
        //
        //   DB[:items].lock_style('FOR SHARE') # SELECT * FROM items FOR SHARE
        lockStyle : function(style) {
            return this.mergeOptions({lock : style});
        },


        // Returns a copy of the dataset with the order changed. If the dataset has an
        // existing order, it is ignored and overwritten with this order. If a nil is given
        // the returned dataset has no order. This can accept multiple arguments
        // of varying kinds, such as SQL functions.  If a block is given, it is treated
        // as a virtual row block, similar to +filter+.
        //
        //  DB[:items].order(:name) # SELECT * FROM items ORDER BY name
        //   DB[:items].order(:a, :b) # SELECT * FROM items ORDER BY a, b
        //   DB[:items].order('a + b'.lit) # SELECT * FROM items ORDER BY a + b
        //   DB[:items].order(:a + :b) # SELECT * FROM items ORDER BY (a + b)
        //  DB[:items].order(:name.desc) # SELECT * FROM items ORDER BY name DESC
        //   DB[:items].order(:name.asc(:nulls=>:last)) # SELECT * FROM items ORDER BY name ASC NULLS LAST
        //   DB[:items].order{sum(name).desc} # SELECT * FROM items ORDER BY sum(name) DESC
        //   DB[:items].order(nil) # SELECT * FROM items
        order : function() {
            var args = comb.argsToArray(arguments);
            var order = [];
            args = array.compact(args).length ? args : null;
            if (args) {
                args.forEach(function(a) {
                    if (comb.isString(a)) {
                        order.push(this.stringToIdentifier(a));
                    } else if (comb.isFunction(a)) {
                        var res = a.apply(sql, [sql]);
                        order = order.concat(comb.isArray(res) ? res : [res]);
                    } else {
                        order.push(a);
                    }
                }, this);
            } else {
                order = null;
            }
            return this.mergeOptions({order : order});
        },

        // Alias of order_more, for naming consistency with order_prepend.
        orderAppend : function() {
            return this.orderMore.apply(this, arguments);
        },

        //alias to order
        orderBy : function() {
            return this.order.apply(this, arguments);
        },

        // Returns a copy of the dataset with the order columns added
        // to the end of the existing order.
        //
        //   DB[:items].order(:a).order(:b) # SELECT * FROM items ORDER BY b
        //   DB[:items].order(:a).order_more(:b) # SELECT * FROM items ORDER BY a, b
        orderMore : function() {
            var args = comb.argsToArray(arguments);
            if (this.__opts.order) {
                args = this.__opts.order.concat(args);
            }
            return this.order.apply(this, args);
        },

        // Returns a copy of the dataset with the order columns added
        // to the beginning of the existing order.
        //
        //   DB[:items].order(:a).order(:b) # SELECT * FROM items ORDER BY b
        //   DB[:items].order(:a).order_prepend(:b) # SELECT * FROM items ORDER BY b, a
        orderPrepend : function() {
            var ds = this.order.apply(this, arguments);
            return this.__opts.order ? ds.orderMore.apply(ds, this.__opts.order) : ds;
        },

        // Qualify to the given table, or first source if not table is given.
        //
        //   DB[:items].filter(:id=>1).qualify
        //   # SELECT items.* FROM items WHERE (items.id = 1)
        //
        //   DB[:items].filter(:id=>1).qualify(:i)
        //   # SELECT i.* FROM items WHERE (i.id = 1)
        qualify : function(table) {
            table = table || this.firstSourceAlias();
            return this.qualifyTo(table);
        },

        //# Return a copy of the dataset with unqualified identifiers in the
        // SELECT, WHERE, GROUP, HAVING, and ORDER clauses qualified by the
        // given table. If no columns are currently selected, select all
        // columns of the given table.
        //
        //   DB[:items].filter(:id=>1).qualify_to(:i)
        //   # SELECT i.* FROM items WHERE (i.id = 1)
        qualifyTo : function(table) {
            var o = this.__opts;
            if (o.sql) {
                return this.mergeOptions();
            }
            var h = {};
            array.intersect(Object.keys(o), this.static.QUALIFY_KEYS).forEach(function(k) {
                h[k] = this.qualifiedExpression(o[k], table);
            }, this);
            if (!o.select || comb.isEmpty(o.select)) {
                h.select = [new ColumnAll(table)];
            }
            return this.mergeOptions(h);
        },

        // Qualify the dataset to its current first source.  This is useful
        // if you have unqualified identifiers in the query that all refer to
        // the first source, and you want to join to another table which
        // has columns with the same name as columns in the current dataset.
        // See +qualify_to+.
        //
        //   DB[:items].filter(:id=>1).qualify_to_first_source
        //   # SELECT items.* FROM items WHERE (items.id = 1)
        qualifyToFirstSource : function() {
            return this.qualifyTo(this.firstSourceAlias());
        },

        // Returns a copy of the dataset with the order reversed. If no order is
        // given, the existing order is inverted.
        //
        //   DB[:items].reverse(:id) # SELECT * FROM items ORDER BY id DESC
        //   DB[:items].order(:id).reverse # SELECT * FROM items ORDER BY id DESC
        //   DB[:items].order(:id).reverse(:name.asc) # SELECT * FROM items ORDER BY name ASC
        reverse : function() {
            var args = comb.argsToArray(arguments);
            return this.order.apply(this, this._invertOrder(args.length ? args : this.__opts.order));
        },

        //alias to reverse;
        reverseOrder : function() {
            return this.reverse.apply(this, arguments);
        },

        // Returns a copy of the dataset with the columns selected changed
        // to the given columns. This also takes a virtual row block,
        // similar to +filter+.
        //
        //   DB[:items].select(:a) # SELECT a FROM items
        //   DB[:items].select(:a, :b) # SELECT a, b FROM items
        //   DB[:items].select{[a, sum(b)]} # SELECT a, sum(b) FROM items
        select : function() {
            var args = comb.argsToArray(arguments);
            var columns = [];
            args.forEach(function(c) {
                if (comb.isFunction(c)) {
                    var res = c.apply(sql, [sql]);
                    columns = columns.concat(comb.isArray(res) ? res : [res]);
                } else {
                    columns.push(c);
                }
            });
            var select = [];
            columns.forEach(function(c) {
                if (comb.isHash(c)) {
                    for (var i in c) {
                        select.push(new AliasedExpression(new Identifier(i), c[i]));
                    }
                } else if (comb.isString(c)) {
                    select.push(this.stringToIdentifier(c));
                } else {
                    select.push(c);
                }
            }, this);
            return this.mergeOptions({select : select});

        },

        // Returns a copy of the dataset selecting the wildcard.
        //
        //   DB[:items].select(:a).select_all # SELECT * FROM items
        selectAll : function() {
            return this.mergeOptions({select : null});
        },

        // Returns a copy of the dataset with the given columns added
        // to the existing selected columns.  If no columns are currently selected,
        // it will select the columns given in addition to *.
        //
        //   DB[:items].select(:a).select(:b) # SELECT b FROM items
        //   DB[:items].select(:a).select_append(:b) # SELECT a, b FROM items
        //   DB[:items].select_append(:b) # SELECT *, b FROM items
        selectAppend : function() {
            var args = comb.argsToArray(arguments);
            var currentSelect = this.__opts.select;
            if (!currentSelect || !currentSelect.length) {
                currentSelect = [this.static.WILDCARD];
            }
            return this.select.apply(this, currentSelect.concat(args));
        },

        /**
         * # Returns a copy of the dataset with the given columns added
         # to the existing selected columns. If no columns are currently selected
         # it will just select the columns given.
         #
         #   DB[:items].select(:a).select(:b) # SELECT b FROM items
         #   DB[:items].select(:a).select_more(:b) # SELECT a, b FROM items
         #   DB[:items].select_more(:b) # SELECT b FROM items
         */
        selectMore : function() {
            var args = comb.argsToArray(arguments);
            var currentSelect = this.__opts.select;
            return this.select.apply(this, (currentSelect || []).concat(args));
        },

        // Set the default values for insert and update statements.  The values hash passed
        // to insert or update are merged into this hash, so any values in the hash passed
        // to insert or update will override values passed to this method.
        //
        //   DB[:items].set_defaults(:a=>'a', :c=>'c').insert(:a=>'d', :b=>'b')
        //   # INSERT INTO items (a, c, b) VALUES ('d', 'c', 'b')
        setDefaults : function(hash) {
            return this.mergeOptions({defaults : comb.merge({}, this.__opts.defaults || {}, hash)});
        },

        /**
         * # Set values that override hash arguments given to insert and update statements.
         # This hash is merged into the hash provided to insert or update, so values
         # will override any values given in the insert/update hashes.
         #
         #   DB[:items].set_overrides(:a=>'a', :c=>'c').insert(:a=>'d', :b=>'b')
         #   # INSERT INTO items (a, c, b) VALUES ('a', 'c', 'b')
         def set_overrides(hash)
         clone(:overrides=>hash.merge(@opts[:overrides]||{}))
         end
         */
        setOverrides : function(hash) {
            return this.mergeOptions({overrides : comb.merge({}, this.__opts.overrides || {}, hash)});
        },

        /**
         *  # Returns a copy of the dataset with no filters (HAVING or WHERE clause) applied.
         #
         #   DB[:items].group(:a).having(:a=>1).where(:b).unfiltered
         #   # SELECT * FROM items GROUP BY a
         */

        unfiltered : function() {
            return this.mergeOptions({where : null, having : null});
        },

        /*# Returns a copy of the dataset with no grouping (GROUP or HAVING clause) applied.
         #
         #   DB[:items].group(:a).having(:a=>1).where(:b).ungrouped
         #   # SELECT * FROM items WHERE b

         */
        ungrouped : function() {
            return this.mergeOptions({group : null, having  : null});
        },
        /*
         # Adds a UNION clause using a second dataset object.
         # A UNION compound dataset returns all rows in either the current dataset
         # or the given dataset.
         # Options:
         # :alias :: Use the given value as the from_self alias
         # :all :: Set to true to use UNION ALL instead of UNION, so duplicate rows can occur
         # :from_self :: Set to false to not wrap the returned dataset in a from_self, use with care.
         #
         #   DB[:items].union(DB[:other_items]).sql
         #   #=> "SELECT * FROM items UNION SELECT * FROM other_items"
         #
         #   DB[:items].union(DB[:other_items], :all=>true, :from_self=>false)
         #   # SELECT * FROM items UNION ALL SELECT * FROM other_items
         #
         #   DB[:items].union(DB[:other_items], :alias=>:i)
         #   # SELECT * FROM (SELECT * FROM items UNION SELECT * FROM other_items) AS i
         */
        union : function(dataset, opts) {
            opts = comb.isUndefined(opts) ? {} : opts;
            if (!comb.isHash(opts)) {
                opts = {all : opts};
            }
            return this.compoundClone("union", dataset, opts);
        },

        /**
         * # Returns a copy of the dataset with no limit or offset.
         #
         #   DB[:items].limit(10, 20).unlimited # SELECT * FROM items
         def unlimited
         clone(:limit=>nil, :offset=>nil)
         end */
        unlimited : function() {
            return this.mergeOptions({limit : null, offset : null});
        },
        /*
         # Returns a copy of the dataset with no order.
         #
         #   DB[:items].order(:a).unordered # SELECT * FROM items
         def unordered
         order(nil)
         end*/
        unordered : function() {
            return this.order(null);
        },
        /*
         # Add a condition to the WHERE clause.  See +filter+ for argument types.
         #
         #   DB[:items].group(:a).having(:a).filter(:b)
         #   # SELECT * FROM items GROUP BY a HAVING a AND b
         #
         #   DB[:items].group(:a).having(:a).where(:b)
         #   # SELECT * FROM items WHERE b GROUP BY a HAVING a
         def where(*cond, &block)
         _filter(:where, *cond, &block)
         end

         */
        where : function() {
            return this._filter.apply(this, ["where"].concat(comb.argsToArray(arguments)));
        },
        /*

         # Add a common table expression (CTE) with the given name and a dataset that defines the CTE.
         # A common table expression acts as an inline view for the query.
         # Options:
         # :args :: Specify the arguments/columns for the CTE, should be an array of symbols.
         # :recursive :: Specify that this is a recursive CTE
         #
         #   DB[:items].with(:items, DB[:syx].filter(:name.like('A%')))
         #   # WITH items AS (SELECT * FROM syx WHERE (name LIKE 'A%')) SELECT * FROM items
         def with(name, dataset, opts={})
         raise(Error, 'This datatset does not support common table expressions') unless supports_cte?
         clone(:with=>(@opts[:with]||[]) + [opts.merge(:name=>name, :dataset=>dataset)])
         end
         */

        "with" :function(name, dataset, opts) {
            if (!this.supportsCte) {
                throw new QueryError("this dataset does not support common table expressions");
            }
            return this.mergeOptions({
                "with" : (this.__opts["with"] || []).concat([comb.merge(opts || {}, {name : this.stringToIdentifier(name), dataset : dataset})])
            });
        },

        /**
         *  # Add a recursive common table expression (CTE) with the given name, a dataset that
         # defines the nonrecursive part of the CTE, and a dataset that defines the recursive part
         # of the CTE.  Options:
         # :args :: Specify the arguments/columns for the CTE, should be an array of symbols.
         # :union_all :: Set to false to use UNION instead of UNION ALL combining the nonrecursive and recursive parts.
         #
         #   DB[:t].select(:i___id, :pi___parent_id).
         #    with_recursive(:t,
         #                   DB[:i1].filter(:parent_id=>nil),
         #                   DB[:t].join(:t, :i=>:parent_id).select(:i1__id, :i1__parent_id),
         #                   :args=>[:i, :pi])
         #   # WITH RECURSIVE t(i, pi) AS (
         #   #   SELECT * FROM i1 WHERE (parent_id IS NULL)
         #   #   UNION ALL
         #   #   SELECT i1.id, i1.parent_id FROM t INNER JOIN t ON (t.i = t.parent_id)
         #   # )
         #   # SELECT i AS id, pi AS parent_id FROM t
         def with_recursive(name, nonrecursive, recursive, opts={})
         raise(Error, 'This datatset does not support common table expressions') unless supports_cte?
         clone(:with=>(@opts[:with]||[]) + [opts.merge(:recursive=>true, :name=>name, :dataset=>nonrecursive.union(recursive, {:all=>opts[:union_all] != false, :from_self=>false}))])
         end*/
        withRecursive : function(name, nonRecursive, recursive, opts) {
            if (!this.supportsCte) {
                throw new QueryError("This dataset does not support common table expressions");
            }
            opts = opts || {};
            var wit = (this.__opts["with"] || []).concat([comb.merge(opts, {recursive : true, name : this.stringToIdentifier(name), dataset : nonRecursive.union(recursive, {all : opts.unionAll != false, fromSelf : false})})]);
            return this.mergeOptions({"with" : wit});
        },
        /*

         # Returns a copy of the dataset with the static SQL used.  This is useful if you want
         # to keep the same row_proc/graph, but change the SQL used to custom SQL.
         #
         #   DB[:items].with_sql('SELECT * FROM foo') # SELECT * FROM foo
         def with_sql(sql, *args)
         sql = SQL::PlaceholderLiteralString.new(sql, args) unless args.empty?
         clone(:sql=>sql)
         end */

        withSql : function(sql) {
            var args = comb.argsToArray(arguments).slice(1);
            if (args.length) {
                sql = new PlaceHolderLiteralString(sql, args)
            }
            return this.mergeOptions({sql : sql});
        },

        //# Internal filter method so it works on either the having or where clauses.
        _filter : function(clause) {
            var cond = comb.argsToArray(arguments).slice(1), cb;
            if (cond.length && comb.isFunction(cond[cond.length - 1])) {
                cb = cond.pop();
            }
            cond = cond.length == 1 ? cond[0] : cond
            if ((cond == null || cond == undefined || cond === "") || (comb.isArray(cond) && cond.length == 0 && !cb) || (comb.isObject(cond) && comb.isEmpty(cond) && !cb)) {
                return this.mergeOptions();
            } else {
                cond = this._filterExpr(cond, cb);
                var cl = this.__opts[clause];
                cl && (cond = new BooleanExpression("AND", cl, cond));
                var opts = {};
                opts[clause] = cond;
                return this.mergeOptions(opts);
            }
        },

        /**
         *  Add the dataset to the list of compounds
         */
        compoundClone : function(type, dataset, options) {
            var ds = this._compoundFromSelf().mergeOptions({compounds : (comb.array.toArray(this.__opts.compounds || [])).concat([
                [type, dataset._compoundFromSelf(), options.all]
            ])});
            return options.fromSelf === false ? ds : ds.fromSelf(options);
        },

        naked : function() {
            return this.mergeOptions({});
            ds.rowCb = null;
            return ds;
        },

        /*

         # Inverts the given order by breaking it into a list of column references
         # and inverting them.
         #
         #   DB[:items].invert_order([:id.desc]]) #=> [:id]
         #   DB[:items].invert_order(:category, :price.desc]) #=> [:category.desc, :price]
         */

        _invertOrder : function(order) {
            var ret = order;
            if (order) {
                ret = order.map(function(o) {
                    if (comb.isInstanceOf(o, OrderedExpression)) {
                        return o.invert();
                    } else {
                        return new OrderedExpression(comb.isString(o) ? new Identifier(o) : o);
                    }
                }, this);
            }
            return ret;
        },

        /*
         # SQL expression object based on the expr type.  See +filter+.
         */
        _filterExpr : function(expr, cb) {
            expr = (comb.isUndefined(expr) || comb.isNull(expr) || (comb.isArray(expr) && !expr.length)) ? null : expr;
            if (expr && cb) {
                return new BooleanExpression("AND", this._filterExpr(expr), this._filterExpr(cb))
            } else if (cb) {
                expr = cb
            }
            if (comb.isInstanceOf(expr, Expression)) {
                if (comb.isInstanceOf(expr, NumericExpression) || comb.isInstanceOf(expr, StringExpression)) {
                    throw new QueryError("Invalid SQL Expression type : " + expr);
                }
                return expr;
            } else if (comb.isArray(expr)) {
                if (expr.length) {
                    var first = expr[0];
                    if (comb.isString(first)) {
                        return new PlaceHolderLiteralString(first, expr.slice(1), true);
                    } else if (Expression.isConditionSpecifier(expr)) {
                        return BooleanExpression.fromValuePairs(expr)
                    } else {
                        return BooleanExpression.fromArgs(["AND"].concat(expr.map(function(e) {
                            return this._filterExpr(e);
                        }, this)));
                    }
                }
            } else if (comb.isFunction(expr)) {
                return this._filterExpr(expr.call(sql, sql));
            } else if (comb.isBoolean(expr)) {
                return new BooleanExpression("NOOP", expr);
            } else if (comb.isString(expr) || comb.isInstanceOf(expr, LiteralString)) {
                return new LiteralString("(" + expr + ")");
            } else if (comb.isHash(expr)) {
                if (!comb.isEmpty(expr)) {
                    return this.__filterObject(expr);
                }
            } else {
                throw new QueryError("Invalid filter argument");
            }
        },

        __filterObject : function(expr, key) {
            var pairs = [], opts;
            var twoArityOperators = this.static.TWO_ARITY_OPERATORS;
            for (var k in expr) {
                var v = expr[k];
                if (comb.isHash(v)) {
                    pairs.push(this.__filterObject(v, k));
                } else if (key && (twoArityOperators[k.toUpperCase()] || k.match(/between/i))) {
                    key = key.split(",");
                    if (key.length > 1) {
                        opts = [
                            [key, v]
                        ];
                        pairs.push(BooleanExpression.fromValuePairs(opts));
                    } else {
                        opts = {};
                        opts[key[0]] = v;
                        if (k.match(/like/i)) {
                            pairs.push(StringExpression.like.apply(StringExpression, (key.concat(comb.isArray(v) ? v : [v]))));
                        } else if (k.match(/between/i)) {
                            pairs.push(this.__createBetweenExpression(opts, k == "notBetween"));
                        } else {
                            pairs.push(this.__createBoolExpression(k, opts));
                        }
                    }
                } else {
                    k = k.split(",");
                    if (k.length == 1) {
                        k = new Identifier(k[0]);
                    }
                    opts = [
                        [k, v]
                    ];
                    pairs.push(BooleanExpression.fromValuePairs(opts));
                }
            }
            return pairs.length == 1 ? pairs[0] : BooleanExpression.fromArgs(["AND"].concat(pairs));
        },



        getters : {
            isSimpleSelectAll : function() {
                var o = {}, opts = this.__opts, count = 0, f;
                for (var i in opts) {
                    if (opts[i] != null && this.static.NON_SQL_OPTIONS.indexOf(i) == -1) {
                        o[i] = opts[i];
                        count++;
                    }
                }
                f = o.from;
                return count == 1 && f.length == 1 && (comb.isString(f[0]) || comb.isInstanceOf(f[0], AliasedExpression) || comb.isInstanceOf(f[0], Identifier));
            }
        }
    },

    static : {
        CONDITIONED_JOIN_TYPES : CONDITIONED_JOIN_TYPES,
        UNCONDITIONED_JOIN_TYPES : UNCONDITIONED_JOIN_TYPES,
        COLUMN_CHANGE_OPTS : COLUMN_CHANGE_OPTS,
        NON_SQL_OPTIONS : NON_SQL_OPTIONS,
        JOIN_METHODS :JOIN_METHODS,
        QUERY_METHODS:QUERY_METHODS
    }
}).export(module);
