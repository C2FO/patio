var comb = require("comb"), string = comb.string, format = string.format,  SQL = require("../sql"), sql = SQL.sql, hitch = comb.hitch, errors = require("../errors"), QueryError = errors.QueryError;

var Dataset;
/**
 * :section: Methods related to dataset graphing
 * Dataset graphing changes the dataset to yield hashes where keys are table
 * name symbols and values are hashes representing the columns related to
 * that table.  All of these methods return modified copies of the receiver.
 */
comb.define(null, {
    instance :{

        constructor : function() {
            this.super(arguments);
            Dataset = require("../../lib").Dataset;
        },

        /**
         * Adds the given graph aliases to the list of graph aliases to use,
         * unlike +set_graph_aliases+, which replaces the list (the equivalent
         * of +select_more+ when graphing).  See +set_graph_aliases+.
         *
         *   DB[:table].add_graph_aliases(:some_alias=>[:table, :column])
         *   # SELECT ..., table.column AS some_alias
         *   # => {:table=>{:column=>some_alias_value, ...}, ...}
         *   */
        addGraphAliases : function(graphAliases) {
            var ds = this.selectMore.apply(this, this.__graphAliasColumns(graphAliases));
            ds.__opts.graphAliases = comb.merge((ds.__opts.graphAliases || (ds.__opts.graph ? ds.__opts.graph.columnAliases : {}) || {}), graphAliases);
            return ds;
        },

        /*
         * Allows you to join multiple datasets/tables and have the result set
         * split into component tables.
         *
         * This differs from the usual usage of join, which returns the result set
         * as a single hash.  For example:
         *
         *   * CREATE TABLE artists (id INTEGER, name TEXT);
         *   * CREATE TABLE albums (id INTEGER, name TEXT, artist_id INTEGER);
         *
         *   DB[:artists].left_outer_join(:albums, :artist_id=>:id).first
         *   *=> {:id=>albums.id, :name=>albums.name, :artist_id=>albums.artist_id}
         *
         *   DB[:artists].graph(:albums, :artist_id=>:id).first
         *   *=> {:artists=>{:id=>artists.id, :name=>artists.name}, :albums=>{:id=>albums.id, :name=>albums.name, :artist_id=>albums.artist_id}}
         *
         * Using a join such as left_outer_join, the attribute names that are shared between
         * the tables are combined in the single return hash.  You can get around that by
         * using +select+ with correct aliases for all of the columns, but it is simpler to
         * use +graph+ and have the result set split for you.  In addition, +graph+ respects
         * any +row_proc+ of the current dataset and the datasets you use with +graph+.
         *
         * If you are graphing a table and all columns for that table are nil, this
         * indicates that no matching rows existed in the table, so graph will return nil
         * instead of a hash with all nil values:
         *
         *   * If the artist doesn't have any albums
         *   DB[:artists].graph(:albums, :artist_id=>:id).first
         *   => {:artists=>{:id=>artists.id, :name=>artists.name}, :albums=>nil}
         *
         * Arguments:
         * dataset :: Can be a symbol (specifying a table), another dataset,
         *            or an object that responds to +dataset+ and returns a symbol or a dataset
         * join_conditions :: Any condition(s) allowed by +join_table+.
         * block :: A block that is passed to +join_table+.
         *
         * Options:
         * :from_self_alias :: The alias to use when the receiver is not a graphed
         *                     dataset but it contains multiple FROM tables or a JOIN.  In this case,
         *                     the receiver is wrapped in a from_self before graphing, and this option
         *                     determines the alias to use.
         * :implicit_qualifier :: The qualifier of implicit conditions, see *join_table.
         * :join_type :: The type of join to use (passed to +join_table+).  Defaults to :left_outer.
         * :select :: An array of columns to select.  When not used, selects
         *            all columns in the given dataset.  When set to false, selects no
         *            columns and is like simply joining the tables, though graph keeps
         *            some metadata about the join that makes it important to use +graph+ instead
         *            of +join_table+.
         * :table_alias :: The alias to use for the table.  If not specified, doesn't
         *                 alias the table.  You will get an error if the the alias (or table) name is
         *                 used more than once.
         *                 */
        graph : function(dataset, joinConditions, options, block) {
            var ret = new comb.Promise();
            var args = comb.argsToArray(arguments).slice(1);
            block = comb.isFunction(args[args.length - 1]) ? args.pop() : null;
            joinConditions = args.shift() || null;
            options = args.shift() || {};
            // Allow the use of a model, dataset, or symbol as the first argument
            // Find the table name/dataset based on the argument
            dataset.hasOwnProperty("dataset") && (dataset = dataset.dataset);
            var tableAlias = options.tableAlias, table;
            if (comb.isString(dataset)) {
                table = new sql.Identifier(dataset);
                dataset = this.db.from(dataset);
                comb.isUndefinedOrNull(tableAlias) && (tableAlias = table);
            } else if (comb.isInstanceOf(dataset, Dataset)) {
                if (dataset.isSimpleSelectAll) {
                    table = dataset.__opts.from[0];
                    comb.isUndefinedOrNull(tableAlias) && (tableAlias = table);
                } else {
                    table = dataset;
                    comb.isUndefinedOrNull(tableAlias) && (tableAlias = this.datasetAlias((this.__opts.numDatasetSources || 0) + 1));
                }
            } else {
                throw new QueryError("The dataset arg should be a string, dataset or model");
            }
            var raiseAliasError = function() {
                var isAlias = comb.isUndefinedOrNull(options.tableAlias);
                throw new QueryError(format("this %s has already been used, please specify %s", isAlias ? "alias" : "table", isAlias ? "a different alias" : "an alias in options"));
            }
            var aliases;
            // Only allow table aliases that haven't been used
            var thisOpts = this.__opts;
            if (comb.isObject(thisOpts.graph) && comb.isHash((aliases = thisOpts.graph.tableAliases)) && !comb.isUndefinedOrNull(aliases[tableAlias.value])) {
                raiseAliasError();
            }
            // Use a from_self if this is already a joined table
            var ds = (!thisOpts.graph && (thisOpts.from.length > 1 || thisOpts.join)) ? this.fromSelf({alias : options.fromSelfAlias || this.firstSourceAlias()}) : this;

            // Join the table early in order to avoid cloning the dataset twice
            ds = ds.joinTable(options.joinType || "leftOuter", table, joinConditions, {tableAlias : tableAlias, implicitQualifier : options.implicitQualifier}, block);
            var opts = ds.__opts;

            // Whether to include the table in the result set
            var addTable = comb.isBoolean(options.select) ? options.select : true;
            // Whether to add the columns to the list of column aliases
            var addColumns = comb.isUndefinedOrNull(opts.graphAliases);
            // Setup the initial graph data structure if it doesn't exist
            var graph;
            var populateGraphPromise = new comb.Promise();
            if (comb.isUndefinedOrNull((graph = opts.graph))) {
                var master = this.toTableName(ds.firstSourceAlias());
                (master == tableAlias) && raiseAliasError();
                // Master hash storing all .graph related information
                graph = opts.graph = {};
                // Associates column aliases back to tables and columns
                var columnAliases = graph.columnAliases = {};
                // Associates table alias (the master is never aliased)
                var tableAliases = graph.tableAliases = {};
                tableAliases[master] = this;

                // Keep track of the alias numbers used
                var columnAliasNum = graph.columnAliasNum = {};
                // All columns in the master table are never
                // aliased, but are not included if set_graph_aliases
                // has been used.            
                if (addColumns) {
                    var select = opts.select = [];
                    this.columns.then(hitch(this, function(cols) {
                        cols.forEach(function(column) {
                            columnAliases[column] = [master, column];
                            select.push(new sql.QualifiedIdentifier(master, column));
                        });
                        populateGraphPromise.callback(graph);
                    }), hitch(ret, "errback"));
                } else {
                    populateGraphPromise.callback(graph);
                }
            } else {
                populateGraphPromise.callback(graph);
            }
            populateGraphPromise.then(hitch(this, function(graph) {
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
                    var caNum = graph.columnAliasNum;

                    var getDefCaNum = function(a) {
                        if (!caNum.hasOwnProperty(a)) caNum[a] = 0;
                        return caNum[a];
                    };
                    // Which columns to add to the result set
                    var dsColPromise;
                    if (options.select) {
                        dsColPromise = new comb.Promise().callback(options.select)
                    } else {
                        dsColPromise = dataset.columns;
                    }
                    // If the column hasn't been used yet, don't alias it.
                    // If it has been used, try table_column.
                    // If that has been used, try table_column_N
                    // using the next value of N that we know hasn't been
                    // used
                    dsColPromise.then(hitch(this, function(cols) {
                        cols.forEach(function(column) {
                            var colAlias, identifier;
                            if (columnAliases[column]) {
                                var columnAlias = format("%s_%s", [tableAlias, column]);
                                if (columnAliases[columnAlias]) {
                                    var columnAliasNum = getDefCaNum(columnAlias);
                                    columnAlias = format("%s_%s", [columnAlias, columnAliasNum]);
                                    getDefCaNum(columnAlias);
                                    caNum[columnAlias] += 1;
                                }
                                colAlias = columnAlias,identifier = new sql.QualifiedIdentifier(tableAlias, column).as(columnAlias);
                            } else {
                                colAlias = column,identifier = new sql.QualifiedIdentifier(tableAlias, column);
                            }
                            columnAliases[colAlias] = [tableAlias, column];
                            select.push(identifier)
                        });
                        ret.callback(ds);
                    }), hitch(ret, "errback"));
                } else {
                    ret.callback(ds);
                }
            }), hitch(ret, "errback"));
            return ret;
        },



        /**
         * This allows you to manually specify the graph aliases to use
         * when using graph.  You can use it to only select certain
         * columns, and have those columns mapped to specific aliases
         * in the result set.  This is the equivalent of +select+ for a
         * graphed dataset, and must be used instead of +select+ whenever
         * graphing is used.
         *
         * graph_aliases :: Should be a hash with keys being symbols of
         *                  column aliases, and values being arrays with two or three elements.
         *                  The first element of the array should be the table alias symbol,
         *                  and the second should be the actual column name symbol. If the array
         *                  has a third element, it is used as the value returned, instead of
         *                  table_alias.column_name.
         *
         *   DB[:artists].graph(:albums, :artist_id=>:id).
         *     set_graph_aliases(:artist_name=>[:artists, :name],
         *                       :album_name=>[:albums, :name],
         *                       :forty_two=>[:albums, :fourtwo, 42]).first
         *   # SELECT artists.name AS artist_name, albums.name AS album_name, 42 AS forty_two FROM table
         *   # => {:artists=>{:name=>artists.name}, :albums=>{:name=>albums.name, :fourtwo=>42}}
         *   */
        setGraphAliases : function(graphAliases) {
            var ds = this.select.apply(this, this.__graphAliasColumns(graphAliases));
            ds.__opts.graphAliases = graphAliases;
            return ds;
        },


        /**
         # Remove the splitting of results into subhashes, and all metadata
         # related to the current graph (if any).
         */
        ungraphed : function() {
            return this.mergeOptions({graph : null});
        },

        //Transform the hash of graph aliases to an array of columns
        __graphAliasColumns : function(graphAliases) {
            var ret = [];
            if(comb.isArray(graphAliases)){
                var newGraphAliases = {};
                for(var i in graphAliases){
                    newGraphAliases[graphAliases[i][0]] = graphAliases[i][1];
                }
                graphAliases = newGraphAliases;
            }
            for (var colAlias in graphAliases) {
                var tc = graphAliases[colAlias];
                var identifier = tc[2] || new sql.QualifiedIdentifier(tc[0], tc[1]);
                if (tc[2] || tc[1] != colAlias) {
                    identifier = new sql.AliasedExpression(identifier, colAlias);
                }
                ret.push(identifier);
            }
            return ret;
        },


        /*
         * Fetch the rows, split them into component table parts,
         * tranform and run the row_proc on each part (if applicable),
         * and yield a hash of the parts.
         * */
        graphEach : function(cb) {
            // Reject tables with nil datasets, as they are excluded from
            // the result set
            var datasets = comb.array.toArray(this.__opts.graph.tableAliases).filter(function(e) {
                return !comb.isUndefinedOrNull(e[1])
            });
            // Get just the list of table aliases into a local variable, for speed
            var tableAliases = datasets.map(function(e) {
                return e[0]
            });

            datasets = datasets.map(function(e) {
                return [e[0],e[1], e[1].rowCb]
            });
            // Use the manually set graph aliases, if any, otherwise
            // use the ones automatically created by .graph
            var columnAliases = this.__opts.graphAliases || this.__opts.graph.columnAliases;
            var ret = new comb.Promise();
            return this.fetchRows(this.selectSql(), function(r) {
                var graph = {};
                // Create the sub hashes, one per table
                tableAliases.forEach(function(ta) {
                    graph[ta] = {};
                });
                // Split the result set based on the column aliases
                // If there are columns in the result set that are
                // not in column_aliases, they are ignored
                for(var colAlias in columnAliases){
                     var  tc = columnAliases[colAlias];
                    var ta = tc[0], column = tc[1];
                    if(!graph[ta]){
                        graph[ta] = {};
                    }
                    graph[ta][column] = r[colAlias];
                }
                datasets.forEach(function(d) {
                    var ta = d[0], ds = d[1], dsCb = d[2];
                    var g = graph[ta];
                    if (!comb.isEmpty(g) && Object.keys(g).some(function(x) {
                        return !comb.isUndefinedOrNull(g[x]);
                    })) {
                        graph[ta] = dsCb ? dsCb(g) : g;
                    } else {
                        graph[ta] = null;
                    }
                });
                cb(graph);
            });
        }
    }
}).export(module);