var comb = require("comb"),
    Promise = comb.Promise,
    string = comb.string,
    format = string.format,
    SQL = require("../sql"),
    sql = SQL.sql,
    isUndefinedOrNull = comb.isUndefinedOrNull,
    define = comb.define,
    merge = comb.merge,
    argsToArray = comb.argsToArray,
    isString = comb.isString,
    isHash = comb.isHash,
    isArray = comb.isArray,
    isObject = comb.isObject,
    isFunction = comb.isFunction,
    isBoolean = comb.isBoolean,
    isEmpty = comb.isEmpty,
    isInstanceOf = comb.isInstanceOf,
    toArray = comb.array.toArray,
    errors = require("../errors"),
    QueryError = errors.QueryError;

/*
 * This file contains dataset graphing related features. All methods return
 * a copy of the dataset.
 */


//leave for later initialization
var Dataset;


//checks for a table alies if there isnt one throw an error.
var raiseAliasError = function (options) {
    var isAlias = isUndefinedOrNull(options.tableAlias);
    throw new QueryError(format("this %s has already been used, please specify %s", isAlias ? "alias" : "table", isAlias ? "a different alias" : "an alias in options"));
};


define({
    /**@ignore*/
    instance: {

        /**
         * @lends patio.Dataset.prototype
         */

        /**
         * @ignore
         */
        constructor: function () {
            !Dataset && (Dataset = require("../index").Dataset);
            this._super(arguments);
        },

        /**
         * Adds the given graph aliases to the list of graph aliases to use,
         * unlike {@link patio.Dataset#setGraphAliases}, which replaces the list (the equivalent
         * of {@link patio.Dataset#selectMore} when graphing).  See {@link patio.Dataset#setGraphAliases}.
         *
         * @example
         *  var DB = patio.defaultDatabase;
         *  // SELECT ..., table.column AS someAlias
         *   DB.from("table").addGraphAliases({someAlias : ["table", "column"]);
         *   //returns from graphing
         *   // => {table : {column : someAlias_value, ...}, ...}
         *
         *   @param {Object} graphAliases the graph aliases to use.
         *          Where key is the alias name and the value is an array where arr[0] = 'tableName' arr[1] = "colName'.
         *
         *   @return {patio.Dataset} deep copy of the original dataset with the added graphAliases.
         */
        addGraphAliases: function (graphAliases) {
            var ds = this.selectMore.apply(this, this.__graphAliasColumns(graphAliases));
            ds.__opts.graphAliases = merge((ds.__opts.graphAliases || (ds.__opts.graph ? ds.__opts.graph.columnAliases : {}) || {}), graphAliases);
            return ds;
        },

        /**
         * Allows you to join multiple datasets/tables and have the result set
         * split into component tables.
         *
         * This differs from the usual usage of join, which returns the result set
         * as a single hash.
         *
         * <pre class="code">
         *
         * //CREATE TABLE artists (id INTEGER, name TEXT);
         * //CREATE TABLE albums (id INTEGER, name TEXT, artist_id INTEGER);
         *
         * var DB = patio.defaultDatabase, ds = db.from("artists");
         * ds.leftOuterJoin("albums", {artistId : "id"}).first
         *   //=> {id : albums.id, name : albums.name, artist_id : albums.artist_id}
         *
         * var p = comb.executeInOrder(ds, function(ds){
         *   var graphedDs = ds.graph("albums", {artist_id : "id"});
         *   return graphedDs.first();
         * });
         * p.chain(function(obj){
         *    //obj == {artists : {
         *                  id : artists.id,
         *                  name : artists.name
         *                  },
         *             albums : {
         *                  id : albums.id,
         *                  name : albums.name,
         *                  artist_id=>albums.artist_id
         *                  }
         *             }
         * });
         * </pre>
         *
         * Using a join such as leftOuterJoin, the attribute names that are shared between
         * the tables are combined in the single return hash.  You can get around that by
         * using {@link patio.Dataset#select} with correct aliases for all of the columns, but it is simpler to
         * use {@link patio.Dataset#graph} and have the result set split for you.  In addition, {@link patio.Dataset#graph} respects
         * any {@link patio.Dataset#rowCb} of the current dataset and the datasets you use with {@link patio.Dataset#graph}.
         *
         * If you are graphing a table and all columns for that table are null, this
         * indicates that no matching rows existed in the table, so graph will return null.
         * instead of a hash with all nil values:
         *
         * <pre class="code">
         *   // Psuedo code there will be promises returned
         *   /// If the artist doesn't have any albums
         *
         *   var DB = patio.defaultDatabase, ds = db.from("artists");
         *   var obj = ds.graph(:albums, :artist_id=>:id).first()
         *   //obj == { artists : {id : artists.id, name : artists.name}, albums : null};
         * </pre>
         *
         *
         * @parameter {String|patio.Dataset|Object} dataset This can be a string (representing a table), another {@link patio.Dataset},
         *            or an object that has a dataset property that returns a string or dataset.
         * @parameter joinConditions Any condition(s) allowed by {@link patio.Dataset#joinTable}.
         *
         * @parameter {Object} [options] options to use when creating the graph
         *
         * @parameter {String|sql.LiteralString|sql.Identifier} [options.fromSelfAlias] The alias to use when the
         *                      receiver is not a graphed dataset but it contains multiple FROM tables or a JOIN.
         *                      In this case, the receiver is wrapped in a {@link patio.Dataset#fromSelf} before graphing,
         *                      and this option determines the alias to use.
         * @parameter {String|sql.LiteralString|sql.Identifier} [options.implicitQualifier] The qualifier of implicit conditions,
         *              see {@link patio.Dataset#joinTable}.
         * @parameter [String] [options.joinType="leftOuter"] The type of join to use (passed to {@link patio.Dataset#joinTable}.).
         * @parameter [String[]|sql.LiteralString[]|sql.Identifier[]|Boolean] [options.select] An array of columns to select.  When not used, selects
         *            all columns in the given dataset.  When set to false, selects no
         *            columns and is like simply joining the tables, though graph keeps
         *            some metadata about the join that makes it important to use {@link patio.Dataset#graph} instead
         *            of {@link patio.Dataset#joinTable}
         * @parameter {String|sql.LiteralString|sql.Identifier} [options.tableAlias] The alias to use for the table.  If not specified, doesn't
         *                 alias the table.  You will get an error if the the alias (or table) name is
         *                 used more than once.

         * @parameter {Function} block  A function that is passed to {@link patio.Dataset#joinTable}.
         **/
        graph: function (dataset, joinConditions, options, block) {
            var ret = new Promise();
            var args = argsToArray(arguments, 1);
            block = isFunction(args[args.length - 1]) ? args.pop() : null;
            joinConditions = args.shift() || null;
            options = args.shift() || {};
            // Allow the use of a model, dataset, or string as the first argument
            // Find the table name/dataset based on the argument
            dataset.hasOwnProperty("dataset") && (dataset = dataset.dataset);
            var tableAlias = options.tableAlias, table;
            if (isString(dataset)) {
                table = sql.identifier(dataset);
                dataset = this.db.from(dataset);
                isUndefinedOrNull(tableAlias) && (tableAlias = table);
            } else if (isInstanceOf(dataset, Dataset)) {
                if (dataset.isSimpleSelectAll) {
                    table = dataset.__opts.from[0];
                    isUndefinedOrNull(tableAlias) && (tableAlias = table);
                } else {
                    table = dataset;
                    isUndefinedOrNull(tableAlias) && (tableAlias = this._datasetAlias((this.__opts.numDatasetSources || 0) + 1));
                }
            } else {
                throw new QueryError("The dataset arg should be a string, dataset or model");
            }

            var aliases;
            // Only allow table aliases that haven't been used
            var thisOpts = this.__opts, thisOptsGraph = thisOpts.graph;
            if (isObject(thisOptsGraph) && isHash((aliases = thisOptsGraph.tableAliases)) && !isUndefinedOrNull(aliases[tableAlias.value])) {
                raiseAliasError(options);
            }
            // Use a from_self if this is already a joined table
            var ds = (!thisOptsGraph && (thisOpts.from.length > 1 || thisOpts.join)) ? this.fromSelf({alias: options.fromSelfAlias || this.firstSourceAlias}) : this;

            // Join the table early in order to avoid cloning the dataset twice
            ds = ds.joinTable(options.joinType || "leftOuter", table, joinConditions, {tableAlias: tableAlias, implicitQualifier: options.implicitQualifier}, block);
            var opts = ds.__opts;

            // Whether to include the table in the result set
            var addTable = isBoolean(options.select) ? options.select : true;
            // Whether to add the columns to the list of column aliases
            var addColumns = isUndefinedOrNull(opts.graphAliases);
            // Setup the initial graph data structure if it doesn't exist
            var graph;
            var populateGraphPromise;
            if (isUndefinedOrNull((graph = opts.graph))) {
                var master = this._toTableName(ds.firstSourceAlias);
                ("" + master === "" + tableAlias) && raiseAliasError(options);
                // Master hash storing all .graph related information
                graph = opts.graph = {};
                // Associates column aliases back to tables and columns
                var columnAliases = graph.columnAliases = {};
                // Associates table alias (the master is never aliased)
                var tableAliases = graph.tableAliases = {};
                tableAliases[master] = this;

                // All columns in the master table are never
                // aliased, but are not included if set_graph_aliases
                // has been used.
                if (addColumns) {
                    var select = opts.select = [];
                    populateGraphPromise = this.columns.chain(function (cols) {
                        cols.forEach(function (column) {
                            columnAliases[column] = [master, column];
                            select.push(new sql.QualifiedIdentifier(master, column));
                        });
                        return graph;
                    });
                } else {
                    populateGraphPromise = new Promise().callback(graph);
                }
            } else {
                populateGraphPromise = new Promise().callback(graph);
            }
            return populateGraphPromise.chain(function (graph) {
                var ret;
                // Add the table alias to the list of aliases
                // Even if it isn't been used in the result set,
                // we add a key for it with a nil value so we can check if it
                // is used more than once
                var tableAliases = graph.tableAliases;
                tableAliases[tableAlias] = addTable ? dataset : null;

                // Add the columns to the selection unless we are ignoring them
                if (addTable && addColumns) {
                    var select = opts.select;
                    var columnAliases = graph.columnAliases;

                    // Which columns to add to the result set
                    var dsColPromise;
                    if (options.select) {
                        dsColPromise = new Promise().callback(options.select);
                    } else {
                        dsColPromise = dataset.columns;
                    }
                    // If the column hasn't been used yet, don't alias it.
                    // If it has been used, try tableColumn.
                    ret = dsColPromise.chain(function (cols) {
                        cols.forEach(function (column) {
                            var colAlias, identifier;
                            if (columnAliases[column]) {
                                var columnAlias = format("%s_%s", [tableAlias, column]);
                                colAlias = columnAlias;
                                identifier = new sql.QualifiedIdentifier(tableAlias, column).as(columnAlias);
                            } else {
                                colAlias = column;
                                identifier = new sql.QualifiedIdentifier(tableAlias, column);
                            }
                            columnAliases[colAlias] = [tableAlias, column];
                            select.push(identifier);
                        });
                        return ds;
                    });
                } else {
                    ret = ds;
                }
                return ret;
            });
        },


        /**
         * This allows you to manually specify the graph aliases to use
         * when using graph.  You can use it to only select certain
         * columns, and have those columns mapped to specific aliases
         * in the result set.  This is the equivalent of {@link patio.Dataset#select} for a
         * graphed dataset, and must be used instead of {@link patio.Dataset#select} whenever
         * graphing is used.
         *
         * @example
         *
         * var DB = patio.defaultDatabase, ds = DB.from("artists");
         * var p = comb.executeInOrder(ds, function(ds){
         *     var graphedDs = ds.graph("albums", {artist_id : id});
         *     //SELECT artists.name AS artist_name, albums.name AS album_name, 42 AS forty_two FROM table
         *     return graphedDs.setGraphAliases({artist_name : ["artists", "name"],
         *                       album_name : ["albums", "name"],
         *                       forty_two : ["albums", "fourtwo", 42]).first();
         * });
         *
         * p.chain(function(obj){
         *    //obj == {artists : {name : artists.name}, albums : {name : albums.name, fourtwo : 42}}
         * });
         *
         * @parameter {Object} graphAliases Should be a hash with keys being  column aliases, and values being
         *                  arrays with two or three elements. The first element of the array should be the table alias,
         *                  and the second should be the actual column name. If the array
         *                  has a third element, it is used as the value returned, instead of
         *                  tableAlias.columnName.
         *
         **/
        setGraphAliases: function (graphAliases) {
            var ds = this.select.apply(this, this.__graphAliasColumns(graphAliases));
            ds.__opts.graphAliases = graphAliases;
            return ds;
        },


        /**
         * Remove the splitting of results into subhashes, and all metadata
         * related to the current graph (if any).
         */
        ungraphed: function () {
            return this.mergeOptions({graph: null});
        },

        //Transform the hash of graph aliases to an array of columns
        __graphAliasColumns: function (graphAliases) {
            var ret = [];
            if (isArray(graphAliases)) {
                var newGraphAliases = {};
                for (var i in graphAliases) {
                    newGraphAliases[graphAliases[i][0]] = graphAliases[i][1];
                }
                graphAliases = newGraphAliases;
            }
            for (var colAlias in graphAliases) {
                var tc = graphAliases[colAlias];
                var identifier = tc[2] || new sql.QualifiedIdentifier(tc[0], tc[1]);
                if (tc[2] || "" + tc[1] !==  "" +colAlias) {
                    identifier = new sql.AliasedExpression(identifier, colAlias);
                }
                ret.push(identifier);
            }
            return ret;
        },


        /**
         * Fetch the rows, split them into component table parts,
         * transform and run the {@link patio.Dataset#rowCb} on each part (if applicable),
         * and yield a hash of the parts.
         * */
        graphEach: function (cb) {
            // Reject tables with nil datasets, as they are excluded from
            // the result set
            var datasets = toArray(this.__opts.graph.tableAliases).filter(function (e) {
                return !isUndefinedOrNull(e[1]);
            });
            // Get just the list of table aliases into a local variable, for speed
            var tableAliases = datasets.map(function (e) {
                return e[0];
            });

            datasets = datasets.map(function (e) {
                return [e[0], e[1], e[1].rowCb];
            });
            // Use the manually set graph aliases, if any, otherwise
            // use the ones automatically created by .graph
            var columnAliases = this.__opts.graphAliases || this.__opts.graph.columnAliases;
            var ret = this.fetchRows(this.selectSql).map(function (r) {
                var graph = {};
                // Create the sub hashes, one per table
                tableAliases.forEach(function (ta) {
                    graph[ta] = {};
                });
                // Split the result set based on the column aliases
                // If there are columns in the result set that are
                // not in column_aliases, they are ignored
                for (var colAlias in columnAliases) {
                    var tc = columnAliases[colAlias];
                    var ta = tc[0], column = tc[1];
                    !graph[ta] && (graph[ta] = {});
                    graph[ta][column] = r[colAlias];
                }
                datasets.forEach(function (d) {
                    var ta = d[0], ds = d[1], dsCb = d[2];
                    var g = graph[ta];
                    if (!isEmpty(g) && Object.keys(g).some(function (x) {
                            return !isUndefinedOrNull(g[x]);
                        })) {
                        graph[ta] = dsCb ? dsCb(g) : g;
                    } else {
                        graph[ta] = null;
                    }
                });
                return graph;
            });
            if (cb) {
                ret.forEach(cb);
            }
            return ret;
        }
    }
}).as(module);