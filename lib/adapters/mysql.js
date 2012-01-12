var mysql = require("mysql"),
    comb = require("comb"),
    string = comb.string,
    format = string.format,
    hitch = comb.hitch,
    Promise = comb.Promise,
    PromiseList = comb.PromiseList,
    QueryError = require("../errors").QueryError,
    Dataset = require("../dataset"),
    Database = require("../database"),
    sql = require("../sql").sql,
    ConnectionPool = require("../ConnectionPool"),
    moose;

var convertDate = function(v, m, convertDateTime) {
    try {
        return moose[m](v);
    } catch(e) {
        if (convertDateTime == null) {
            return null;
        } else if (convertDateTime == String || (comb.isString(convertDateTime) && convertDateTime.match(/string/i))) {
            return v;
        } else {
            throw e;
        }
    }
}

var Connection = comb.define(null, {
    instance : {
        /**@lends moose.mysql.Query.prototype*/
        connection : null,

        constructor : function(conn) {
            this.connection = conn
        },

        closeConnection : function() {
            var ret = new Promise();
            this.connection.end(function() {
                ret.callback();
            });

            return ret;
        },

        /**
         * Queries the database.
         *
         * @param {String} query query to perform
         *
         * @return {comb.Promise} promise that is called back with the results, or error backs with an error.
         */
        query : function(query) {
            var ret = new Promise();
            this.connection.query(query, hitch(this, function(err, results, info) {
                if (err) {
                    ret.errback(err);
                } else {
                    ret.callback(results, info);
                }
            }));
            return ret;
        }
    }
});

var DS = comb.define(Dataset, {

    instance : {

        __providesAccurateRowsMatched : false,

        __supportsDistinctOn : true,

        __supportsIntersectExcept : false,

        __supportsModifyingJoins : true,

        __supportsTimestampUsecs : false,


        // MySQL specific syntax for LIKE/REGEXP searches, as well as
        // string concatenation.
        complexExpressionSql : function(op, args) {
            var likeOps = ["~", "~*", "LIKE", "ILIKE"];
            var notLikeOps = ["!~", "!~*", "NOT LIKE", "NOT ILIKE"];
            var regExpOps = ["~", "!~", "~*", "!~*"];
            var binaryOps = ["~", "!~", "LIKE", "NOT LIKE"];
            if (likeOps.indexOf(op) != -1 || notLikeOps.indexOf(op) != -1) {
                return format("(%s%s %s%s %s)", this.literal(args[0]), notLikeOps.indexOf(op) != -1 ? " NOT" : "", regExpOps.indexOf(op) != -1 ? "REGEXP" : "LIKE", binaryOps.indexOf(op) != -1 ? " BINARY" : "", this.literal(args[1]));
            } else if (op == "||") {
                if (args.length > 1) {
                    return format("CONCAT(%s)", args.map(this.literal, this).join(", "));
                } else {
                    return this.literal(args[0]);
                }
            } else if (op == "B~") {
                return format("CAST(~%s AS SIGNED INTEGER)", this.literal(args[0]));
            } else {
                return this._super(arguments);
            }
        },

        // Use GROUP BY instead of DISTINCT ON if arguments are provided.
        distinct : function(args) {
            args = comb.argsToArray(arguments);
            return args.length == 0 ? this._super(arguments) : this.group.apply(this, args);
        },

        //Return a cloned dataset which will use LOCK IN SHARE MODE to lock returned rows.
        forShare : function() {
            return this.lockStyle("share");
        },

        //Adds full text filter
        fullTextSearch : function(cols, terms, opts) {
            opts = opts || {};
            cols = comb.array.toArray(cols).map(this.stringToIdentifier, this);
            return this.filter(this.fullTextSql(cols, terms, opts));
        },

        //MySQL specific full text search syntax.
        fullTextSql : function(cols, term, opts) {
            opts = opts || {};
            return format("MATCH %s AGAINST (%s%s)", this.literal(comb.array.toArray(cols)), this.literal(comb.array.toArray(term).join(" ")), opts.boolean ? " IN BOOLEAN MODE" : "");
        },

        //MySQL allows HAVING clause on ungrouped datasets.
        having : function(cond, cb) {
            var args = comb.argsToArray(arguments);
            return this._filter.apply(this, ["having"].concat(args));
        },

        // Transforms an CROSS JOIN to an INNER JOIN if the expr is not nil.
        //Raises an error on use of :full_outer type, since MySQL doesn't support it.
        joinTable : function(type, table, expr, tableAlias) {
            tableAlias = tableAlias || {};
            type == "cross" && !comb.isUndefinedOrNull(expr) && (type = "inner");
            if (type == "fullOuter") {
                throw new QueryError("MySQL does not support FULL OUTER JOIN");
            }
            return this._super(arguments, [type,table,expr,tableAlias]);
        },

        // Transforms :natural_inner to NATURAL LEFT JOIN and straight to
        //STRAIGHT_JOIN.
        _joinTypeSql : function(joinType) {
            if (joinType == "straight") {
                return "STRAIGHT_JOIN";
            } else if (joinType == "naturalInner") {
                return "NATURAL LEFT JOIN";
            } else {
                return this._super(arguments);
            }
        },
        /*
         * Sets up the insert methods to use INSERT IGNORE.
         * Useful if you have a unique key and want to just skip
         * inserting rows that violate the unique key restriction.
         *
         *   dataset.insert_ignore.multi_insert(
         *    [{:name => 'a', :value => 1}, {:name => 'b', :value => 2}]
         *   )
         *   # INSERT IGNORE INTO tablename (name, value) VALUES (a, 1), (b, 2)
         */
        insertIgnore : function() {
            return this.mergeOptions({insertIgnore : true});
        },

        /*
         * Sets up the insert methods to use ON DUPLICATE KEY UPDATE
         * If you pass no arguments, ALL fields will be
         * updated with the new values.  If you pass the fields you
         * want then ONLY those field will be updated.
         *
         * Useful if you have a unique key and want to update
         * inserting rows that violate the unique key restriction.
         *
         *   dataset.on_duplicate_key_update.multi_insert(
         *    [{:name => 'a', :value => 1}, {:name => 'b', :value => 2}]
         *   )
         *   # INSERT INTO tablename (name, value) VALUES (a, 1), (b, 2)
         *   # ON DUPLICATE KEY UPDATE name=VALUES(name), value=VALUES(value)
         *
         *   dataset.on_duplicate_key_update(:value).multi_insert(
         *     [{:name => 'a', :value => 1}, {:name => 'b', :value => 2}]
         *   )
         *   # INSERT INTO tablename (name, value) VALUES (a, 1), (b, 2)
         *   # ON DUPLICATE KEY UPDATE value=VALUES(value)
         *   */
        onDuplicateKeyUpdate : function(args) {
            var args = comb.argsToArray(arguments).map(function(c) {
                return comb.isString(c) ? this.stringToIdentifier(c) : c
            }, this);
            return this.mergeOptions({onDuplicateKeyUpdate : args});
        },

        // MySQL specific syntax for inserting multiple values at once.
        multiInsertSql : function(columns, values) {
            return [this.insertSql(columns, sql.literal('VALUES ' + values.map(
                function(r) {
                    return this.literal(comb.array.toArray(r));
                }, this).join(this._static.COMMA_SEPARATOR)))];
        },

        //MySQL uses the nonstandard ` (backtick) for quoting identifiers.
        _quotedIdentifier : function(c) {
            return format("`%s`", c);
        },

        // MySQL specific syntax for REPLACE (aka UPSERT, or update if exists,
        //insert if it doesn't).
        replaceSql : function(values) {
            var ds = this.mergeOptions({replace : true});
            return ds.insertSql.apply(ds, comb.argsToArray(arguments));
        },

        //If this is an replace instead of an insert, use replace instead
        _insertSql : function() {
            return this.__opts.replace ? this._clauseSql("replace") : this._super(arguments);
        },

        //Consider the first table in the joined dataset is the table to delete
        //from, but include the others for the purposes of selecting rows.
        _deleteFromSql : function(sql) {
            if (this._joinedDataset) {
                return format(" %s FROM %s", this._sourceList(this.__opts.from[0]), this._sourceList(this.__opts.from)) + this._selectJoinSql();
            } else {
                return this._super(arguments);
            }
        },

        //alias replace_clause_methods insert_clause_methods
        //MySQL doesn't use the standard DEFAULT VALUES for empty values.
        _insertColumnsSql : function(sql) {
            var values = this.__opts.values;
            if (comb.isArray(values) && values.length == 0) {
                return " ()";
            } else {
                return this._super(arguments);
            }
        },

        //MySQL supports INSERT IGNORE INTO
        _insertIgnoreSql : function(sql) {
            return this.__opts.insertIgnore ? " IGNORE" : "";
        },

        //MySQL supports INSERT ... ON DUPLICATE KEY UPDATE
        _insertOnDuplicateKeyUpdateSql : function(sql) {
            return this.__opts.onDuplicateKeyUpdate ? this.onDuplicateKeyUpdateSql() : "";
        },

        //MySQL doesn't use the standard DEFAULT VALUES for empty values.
        _insertValuesSql : function(sql) {
            var values = this.__opts.values;
            if (comb.isArray(values) && values.length == 0) {
                return " VALUES ()";
            } else {
                return this._super(arguments);
            }
        },

        //MySQL allows a LIMIT in DELETE and UPDATE statements.
        limitSql : function(sql) {
            return this.__opts.limit ? format(" LIMIT %s", this.__opts.limit) : "";
        },

        _deleteLimitSql : function() {
            return this.limitSql.apply(this, arguments);
        },

        _updateLimitSql : function() {
            return this.limitSql.apply(this, arguments);
        },



        //MySQL specific syntax for ON DUPLICATE KEY UPDATE
        onDuplicateKeyUpdateSql : function() {
            var ret = "";
            var updateCols = this.__opts.onDuplicateKeyUpdate;
            if (updateCols) {
                var updateVals = null, l, last;
                if ((l = updateCols.length) > 0 && comb.isHash((last = updateCols[l - 1]))) {
                    updateVals = last;
                    updateCols = l == 2 ? [updateCols[0]] : updateCols.slice(0, l - 2);
                }
                var updating = updateCols.map(function(c) {
                    var quoted = this.quoteIdentifier(c);
                    return format("%s=VALUES(%s)", quoted, quoted);
                }, this);
                for (var i in updateVals) {
                    updating.push(format("%s=%s", this.quoteIdentifier(i), this.literal(updateVals[i])));
                }
                (updating || updateVals) && (ret = format(" ON DUPLICATE KEY UPDATE %s", updating.join(this._static.COMMA_SEPARATOR)));
            }
            return ret;
        },

        //Support FOR SHARE locking when using the :share lock style.
        _selectLockSql : function(sql) {
            return this.__opts.lock == "share" ? this._static.FOR_SHARE : this._super(arguments);
        },

        // Delete rows matching this dataset
        delete : function() {
            return  this.executeDui(this.deleteSql, function(c, info) {
                return c.affectedRows;
            });
        },

        // Yield all rows matching this dataset.  If the dataset is set to
        //split multiple statements, yield arrays of hashes one per statement
        //instead of yielding results for all statements as hashes.
        fetchRows : function(sql, block) {
            return this.execute(sql, hitch(this, function(r, fields) {
                var i = -1;
                var cols;

                cols = Object.keys(fields).map(function(c) {
                    var col = fields[c];
                    var type = col.fieldType;
                    var length = col.fieldLength;
                    return [this.outputIdentifier(c), DB.convertMysqlType(type == 1 && length != 1 ? 2 : type), c];
                }, this);

                this.__columns = cols.map(function(c) {
                    return c[0]
                });
                if (block) {
                    if (this.__opts.splitMultipleResultSets) {
                        var s = [];
                        return block(this.__processRows(r, cols, function(h) {
                            return h;
                        }));
                    } else {
                        return this.__processRows(r, cols, block)
                    }
                }
            }));
        },

        __processRows : function(rows, cols, cb) {
            return comb.when.apply(comb, rows.map(function(row) {
                var h = {};
                cols.forEach(function(col) {
                    h[col[0]] = col[1](row[col[2]]);
                }, this);
                return cb(h);
            }, this));
        },

        //Don't allow graphing a dataset that splits multiple statements
        graph : function() {
            if (this.__opts.splitMultipleResultSels) {
                throw new QueryError("Can't graph a dataset that splits multiple result sets")
            }
            this._super(arguments);
        },

        //Insert a new value into this dataset
        insert : function() {
            return  this.executeDui(this.insertSql.apply(this, arguments), function(c, info) {
                return c.insertId
            });
        },

        // Replace (update or insert) the matching row.
        replace : function() {
            return this.executeDui(this.replaceSql.apply(this, arguments), function(c, info) {
                return c.insertId
            });
        },

        /*
         * Makes each yield arrays of rows, with each array containing the rows
         * for a given result set.  Does not work with graphing.  So you can submit
         * SQL with multiple statements and easily determine which statement
         * returned which results.
         *
         * Modifies the row_proc of the returned dataset so that it still works
         * as expected (running on the hashes instead of on the arrays of hashes).
         * If you modify the row_proc afterward, note that it will receive an array
         * of hashes instead of a hash.
         * **/
        splitMultipleResultSets : function() {
            if (this.__opts.graph) {
                throw new QueryError("Can't split multiple statements on a graphed dataset");
            }
            var ds = this.mergeOptions({splitMultipleResultSets : true});
            var rowCb = this.rowCb;
            if (rowCb) {
                ds.rowCb = function(x) {
                    return x.map(rowCb, this);
                };
            }
            return ds;
        },

        //Update the matching rows.
        update : function() {
            return this.executeDui(this.updateSql.apply(this, arguments), function(c, info) {
                return c.affectedRows;
            });
        },

        //Set the :type option to :select if it hasn't been set.
        execute : function(sql, opts, block) {
            if (comb.isFunction(opts)) {
                block = opts;
                opts = {};
            } else {
                opts = opts || {};
            }
            return this._super(arguments, [sql, comb.merge({type : "select"}, opts), block]);
        },

        //Set the :type option to :select if it hasn't been set.
        executeDui : function(sql, opts, block) {
            if (comb.isFunction(opts)) {
                block = opts;
                opts = {};
            } else {
                opts = opts || {};
            }
            return this._super(arguments, [sql, comb.merge({type : "dui"}, opts), block]);
        },

        _literalString : function(v) {
            return "'" + v.replace(/[\0\n\r\t\\\'\"\x1a]/g, function(s) {
                switch (s) {
                    case "\0":
                        return "\\0";
                    case "\n":
                        return "\\n";
                    case "\r":
                        return "\\r";
                    case "\b":
                        return "\\b";
                    case "\t":
                        return "\\t";
                    case "\x1a":
                        return "\\Z";
                    default:
                        return "\\" + s;
                }
            }) + "'";
        }
    },

    static : {
        BOOL_TRUE : '1',
        BOOL_FALSE : '0',
        COMMA_SEPARATOR : ', ',
        FOR_SHARE : ' LOCK IN SHARE MODE',
        DELETE_CLAUSE_METHODS :  ["_deleteFromSql", "_deleteWhereSql", "_deleteOrderSql", "_deleteLimitSql"],
        INSERT_CLAUSE_METHODS :  ["_insertIgnoreSql", "_insertIntoSql", "_insertColumnsSql", "_insertValuesSql",
            "_insertOnDuplicateKeyUpdateSql"],
        REPLACE_CLAUSE_METHODS : ["_insertIgnoreSql", "_insertIntoSql", "_insertColumnsSql", "_insertValuesSql",
            "_insertOnDuplicateKeyUpdateSql"],
        SELECT_CLAUSE_METHODS :  ["_selectDistinctSql", "_selectColumnsSql", "_selectFromSql", "_selectJoinSql",
            "_selectWhereSql", "_selectGroupSql", "_selectHavingSql", "_selectCompoundsSql", "_selectOrderSql",
            "_selectLimitSql", "_selectLockSql"],
        UPDATE_CLAUSE_METHODS :  ["_updateTableSql", "_updateSetSql", "_updateWhereSql", "_updateOrderSql", "_updateLimitSql"]

    }

});

var DB = comb.define(Database, {
    instance : {

        CAST_TYPES : {String : "CHAR", Integer : "SIGNED", Time : "DATETIME", DateTime : "DATETIME", Numeric : "DECIMAL", BigDecimal: "DECIMAL", File : "BINARY"},
        PRIMARY : 'PRIMARY',

        type : "mysql",

        __supportsSavePoints : true,

        __supportsTransactionIsolationLevels : true,


        createConnection : function(opts) {
            delete opts.query;
            var conn = mysql.createClient(comb.merge({}, opts, {typeCast : false}));
            conn.useDatabase(opts.database)
            return new Connection(conn);
        },

        closeConnection : function(conn) {
            return conn.closeConnection();
        },

        validate : function(conn) {
            return new Promise().callback(true);
        },

        execute : function(sql, opts, cb) {
            var ret = new Promise();
            this.pool.getConnection().then(hitch(this, function(conn) {
                this.__execute(conn, sql, opts, cb).then(hitch(ret, "callback"), hitch(ret, "errback"));
            }), hitch(ret, "errback"));
            return ret;
        },

        // Execute the given SQL on the given connection.  If the :type
        //option is :select, yield the result of the query, otherwise
        // yield the connection if a block is given.
        __execute : function(conn, sql, opts, cb) {
            var ret = new comb.Promise();
            this.__logAndExecute(sql,
                function() {
                    return conn.query(sql);
                }).then(hitch(this, function(r, info) {
                var type = opts.type;
                this.pool.returnConnection(conn);
                try {
                    if (type == "select") {
                        comb.when(r ? cb(r, info) : null, hitch(ret, "callback"), hitch(ret, "errback"));
                    } else if (comb.isFunction(cb)) {
                        comb.when(cb(r, info),hitch(ret, "callback"), hitch(ret, "errback"));
                    } else {
                        ret.callback();
                    }
                } catch(e) {
                    ret.errback(e);
                }
            }), hitch(this, function(err) {
                this.pool.returnConnection(conn);
                ret.errback(err);
            }));
            return ret;

        },


        // MySQL's cast rules are restrictive in that you can't just cast to any possible
        // database type.
        castTypeLiteral : function(type) {
            return  this._static.CAST_TYPES[type] || this._super(arguments);
        },

        // Use SHOW INDEX FROM to get the index information for the table.
        indexes : function(table, opts) {
            var indexes = {};
            var removeIndexes = []
            var m = this.outputIdentifierMeth;
            var im = this.inputIdentifierMeth;
            var ret = new comb.Promise();
            this.metadataDataset.withSql("SHOW INDEX FROM ?", comb.isInstanceOf(table, sql.Identifier) ? table : sql.identifier(im(table))).forEach(
                function(r) {
                    var name = r.Key_name;
                    if (name != "PRIMARY") {
                        name = m(name);
                        if (r.Sub_part) {
                            removeIndexes.push(name);
                        }
                        var i = indexes[name] || (indexes[name] = {columns : [], unique : r.Non_unique != 1});
                        i.columns.push(m(r.Column_name));
                    }
                }).then(function() {
                    var r = {};
                    for (var i in indexes) {
                        if (removeIndexes.indexOf(i) == -1) {
                            r[i] = indexes[i];
                        }
                    }
                    ret.callback(r);
                }, hitch(ret, "errback"));
            return ret;

        },

        // Get version of MySQL server, used for determined capabilities.
        serverVersion  : function() {
            var ret = new comb.Promise();
            if (!this.__serverVersion) {
                this.get(sql.version().sqlFunction).then(hitch(this, function(version) {
                    var m = version.match(/(\d+)\.(\d+)\.(\d+)/);
                    this._serverVersion = (parseInt(m[1]) * 10000) + (parseInt(m[2]) * 100) + parseInt(m[3]);
                    ret.callback(this._serverVersion);
                }), hitch(ret, "errback"));
            } else {
                ret.callback(this._serverVersion);
            }
            return ret;
        },

        //Return an array of symbols specifying table names in the current database.
        //
        //Options:
        //* :server - Set the server to use
        tables : function(opts) {
            var m = this.outputIdentifierMeth;
            return this.metadataDataset.withWql('SHOW TABLES').map(function(r) {
                return  m(r[Object.keys(r)[0]]);
            });
        },



        //Changes the database in use by issuing a USE statement.  I would be
        //very careful if I used this.
        use : function(dbName) {
            var ret = new comb.Promise();
            this.disconnect().then(hitch(this, function() {
                this.run("USE " + dbName).then(hitch(this, function() {
                    this.opts.database = dbName;
                    this.schemas = {};
                    ret.callback(this);
                }));
            }), hitch(ret, "errback"));

            return ret;
        },

        //Use MySQL specific syntax for rename column, set column type, and
        // drop index cases.
        __alterTableSql : function(table, op) {
            var ret = new comb.Promise();
            if (op.op == "addColumn") {
                var related = op.table;
                if (related) {
                    delete op.table;
                    this._super(arguments).then(hitch(this, function(sql) {
                        op.table = related;
                        ret.callback([sql, format("ALTER TABLE %s ADD FOREIGN KEY (%s)%s", this.__quoteSchemaTable(table), this.__quoteIdentifier(op.name), this.__columnReferencesSql(op))]);
                    }), hitch(ret, "errback"));
                    return ret;
                } else {
                    return this._super(arguments);
                }
            } else if (['renameColumn', "setColumnType", "setColumnNull", "setColumnDefault"].indexOf(op.op) != -1) {
                var o = op.op;
                this.schema(table).then(hitch(this, function(schema) {
                    var name = op.name;
                    var opts = schema[Object.keys(schema).filter(function(i) {
                        return i == name
                    })[0]];
                    opts = comb.merge({}, opts || {});
                    opts.name = op.newName || name;
                    opts.type = op.type || opts.dbType;
                    opts["allowNull"] = comb.isUndefined(op["null"]) ? opts.allowNull : op["null"];
                    opts["default"] = op["default"] || opts["jsDefault"];
                    if (comb.isUndefinedOrNull(opts["default"])) {
                        delete opts["default"];
                    }
                    ret.callback(format("ALTER TABLE %s CHANGE COLUMN %s %s", this.__quoteSchemaTable(table), this.__quoteIdentifier(op.name), this.__columnDefinitionSql(comb.merge(op, opts))));
                }), hitch(ret, "errback"));
                return ret;
            } else if (op.op == "dropIndex") {
                return ret.callback(format("%s ON %s", this.__dropIndexSql(table, op), this.__quoteSchemaTable(table)));
            } else {
                return this._super(arguments);
            }
        },

        //MySQL needs to set transaction isolation before begining a transaction
        __beginNewTransaction : function(conn, opts) {
            var ret = new comb.Promise();
            this.__setTransactionIsolation(conn, opts)
                .chain(hitch(this, this.__logConnectionExecute, conn, this.beginTransactionSql), hitch(ret, "errback"))
                .then(hitch(ret, "callback"), hitch(ret, "errback"));
            return ret;
        },

        // Use XA START to start a new prepared transaction if the :prepare
        //option is given.
        __beginTransaction : function(conn, opts) {
            opts = opts || {};
            var s;
            if (s = opts.prepare) {
                return this.__logConnectionExecute(conn, format("XA START %s", this.literal(s)));
            } else {
                return this._super(arguments);
            }
        },

        // MySQL doesn't allow default values on text columns, so ignore if it the
        // generic text type is used
        __columnDefinitionSql : function(column) {
            if (comb.isString(column.type) && column.type.match(/string/i) && column.text == true) {
                delete column["default"];
            }
            return this._super(arguments, [column]);
        },

        // Prepare the XA transaction for a two-phase commit if the
        // :prepare option is given.
        __commitTransaction : function(conn, opts) {
            opts = opts || {};
            var s = opts.prepare;
            if (s) {
                var ret = new comb.Promise();
                this.__logConnectionExecute(conn, format("XA END %s", this.literal(s)))
                    .chain(hitch(this, "__logConnectionExecute", format("XA PREPARE %s", this.literal(s))), hitch(ret, "errback"))
                    .then(hitch(ret, "callback"), hitch(ret, "errback"));
                return ret;
            } else {
                return this._super(arguments);
            }
        },

        //Use MySQL specific syntax for engine type and character encoding
        __createTableSql : function(name, generator, options) {
            options = options || {};
            var engine = options.engine, charset = options.charset, collate = options.collate;
            comb.isUndefined(engine) && (engine = this._static.defaultEngine);
            comb.isUndefined(charset) && (charset = this._static.defaultCharset);
            comb.isUndefined(collate) && (collate = this._static.defaultCollate);
            generator.columns.forEach(function(c) {
                var t = c.table;
                if (t) {
                    delete c.table;
                    generator.foreignKey([c.name], t, comb.merge({}, c, {name : null, type : "foreignKey"}));
                }
            });
            return format(" %s%s%s%s", this._super(arguments), engine ? " ENGINE=" + engine : "", charset ? " DEFAULT CHARSET=" + charset : "", collate ? " DEFAULT COLLATE=" + collate : "");
        },

        //Handle MySQL specific index SQL syntax
        __indexDefinitionSql : function(tableName, index) {
            var indexName = this.__quoteIdentifier(index.name || this.__defaultIndexName(tableName, index.columns)), t = index.type, using = "";
            var indexType = "";
            if (t == "fullText") {
                indexType = "FULLTEXT ";
            } else if (t == "spatial") {
                indexType = "SPATIAL ";
            } else {
                indexType = index.unique ? "UNIQUE " : "";
                using = t != null ? " USING " + t : "";
            }
            return format("CREATE %sINDEX %s%s ON %s %s", indexType, indexName, using, this.__quoteSchemaTable(tableName), this.literal(index.columns.map(function(c) {
                return comb.isString(c) ? sql.identifier(c) : c;
            })));
        },

        // Rollback the currently open XA transaction
        __rollbackTransaction : function(conn, opts) {
            opts = opts || {};
            var s = opts.prepare;
            if (s) {
                s = this.literal(s);
                var ret = new comb.Promise();
                this.__logConnectionExecute(conn, "XA END " + s).chain(hitch(this, "__logConnectionExecute", conn, "XA PREPARE " + s), hitch(ret, "errback")).then(hitch(this, "__logConnectionExecute", conn, "XA ROLLBACK " + s), hitch(ret, "errback"));
                return ret;
            } else {
                return this._super(arguments);
            }
        },

        // MySQL treats integer primary keys as autoincrementing.
        schemaAutoincrementingPrimaryKey : function(schema) {
            return this._super(arguments) && schema.dbType.match(/int/i);
        },

        //Use the MySQL specific DESCRIBE syntax to get a table description.
        schemaParseTable : function(tableName, opts) {
            var m = this.outputIdentifierMeth;
            var im = this.inputIdentifierMeth;
            return this.metadataDataset.withSql("DESCRIBE ?", sql.identifier(im(tableName))).map(hitch(this, function(row) {
                var ret = {};
                var e = row.Extra;
                ret.autoIncrement = e.match(/auto_increment/i) != null;
                delete row.Extra;
                ret.allowNull = row.Null.match(/Yes/i) != null;
                delete row.Null;
                ret.primaryKey = row.Key.match(/PRI/i) != null;
                delete row.Key;
                ret["default"] = row.Default;
                comb.isEmpty(row["default"]) && (row["default"] = null);
                delete row.Default;
                ret.dbType = row.Type;
                delete row.Type;
                ret.type = this.schemaColumnType(ret.dbType);
                var fieldName = m(row.Field);
                delete row.Field;
                return [fieldName, ret];
            }));
        },

        //Convert tinyint(1) type to boolean if convert_tinyint_to_bool is true
        schemaColumnType : function(dbType) {
            return this._static.convertTinyintToBool && dbType == 'tinyint(1)' ? "boolean" : this._super(arguments);
        },


        //MySQL doesn't have a true boolean class, so it uses tinyint(1)
        __typeLiteralGenericBoolean : function(column) {
            return 'tinyint(1)';
        },

        getters : {

            identifierInputMethodDefault : function() {
                return null;
            },

            identifierOutputMethodDefault : function() {
                return null;
            },

            connectionExecuteMethod : function() {
                return "query";
            },

            dataset : function() {
                return new DS(this);
            }
        }
    },

    static : {
        __convertTinyintToBool : true,

        __convertInvalidDateTime : false,

        AUTOINCREMENT : 'AUTO_INCREMENT',

        init : function(){
            this.setAdapterType("mysql");
        },

        FIELD_TYPES : {
            FIELD_TYPE_DECIMAL : 0x00,
            FIELD_TYPE_TINY : 0x01,
            FIELD_TYPE_SHORT : 0x02,
            FIELD_TYPE_LONG : 0x03,
            FIELD_TYPE_FLOAT : 0x04,
            FIELD_TYPE_DOUBLE : 0x05,
            FIELD_TYPE_NULL : 0x06,
            FIELD_TYPE_TIMESTAMP : 0x07,
            FIELD_TYPE_LONGLONG : 0x08,
            FIELD_TYPE_INT24 : 0x09,
            FIELD_TYPE_DATE : 0x0a,
            FIELD_TYPE_TIME : 0x0b,
            FIELD_TYPE_DATETIME : 0x0c,
            FIELD_TYPE_YEAR : 0x0d,
            FIELD_TYPE_NEWDATE : 0x0e,
            FIELD_TYPE_VARCHAR : 0x0f,
            FIELD_TYPE_BIT : 0x10,
            FIELD_TYPE_NEWDECIMAL : 0xf6,
            FIELD_TYPE_ENUM : 0xf7,
            FIELD_TYPE_SET : 0xf8,
            FIELD_TYPE_TINY_BLOB : 0xf9,
            FIELD_TYPE_MEDIUM_BLOB : 0xfa,
            FIELD_TYPE_LONG_BLOB : 0xfb,
            FIELD_TYPE_BLOB : 0xfc,
            FIELD_TYPE_VAR_STRING : 0xfd,
            FIELD_TYPE_STRING : 0xfe,
            FIELD_TYPE_GEOMETRY : 0xff

        },

        convertMysqlType : function(type) {
            var convert = this.convertTinyintToBool, convertDateTime = this.__convertInvalidDateTime;
            !moose && (moose = require("../index"));
            return hitch(this.FIELD_TYPES, function(o) {
                if (o != null) {
                    switch (type) {
                        case this.FIELD_TYPE_TIMESTAMP:
                        case this.FIELD_TYPE_DATETIME:
                            return convertDate(o, "stringToDateTime", convertDateTime);
                            break;
                        case this.FIELD_TYPE_DATE:
                        case this.FIELD_TYPE_NEWDATE:
                            return convertDate(o, "stringToDate", convertDateTime);
                            break;
                        case this.FIELD_TYPE_TIME:
                            return convertDate(o, "stringToTime", convertDateTime);
                            break;
                        case this.FIELD_TYPE_TINY:
                            return convert ? parseInt(o, 10) == 1 : parseInt(o, 10);
                            break;
                        case this.FIELD_TYPE_SHORT:
                        case this.FIELD_TYPE_LONG:
                        case this.FIELD_TYPE_LONGLONG:
                        case this.FIELD_TYPE_INT24:
                        case this.FIELD_TYPE_YEAR:
                            return parseInt(o, 10);
                            break;
                        case this.FIELD_TYPE_FLOAT:
                        case this.FIELD_TYPE_DOUBLE:
                            // decimal types cannot be parsed as floats because
                            // V8 Numbers have less precision than some MySQL Decimals
                            return parseFloat(o);
                            break;
                    }
                }
                return o;
            });
        },

        getters : {
            convertTinyintToBool : function() {
                return this.__convertTinyintToBool;
            },

            convertInvalidDateTime : function() {
                return this.__convertInvalidDateTime;
            }

        },

        setters : {
            convertTinyintToBool : function(convert) {
                this.__convertTinyintToBool = convert;
            },

            convertInvalidDateTime : function(convert) {
                this.__convertInvalidDateTime = convert;
            }

        }
    }
}).as(exports, "MySQLDatabase");
