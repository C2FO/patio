var mysql = require("mysql"),
    comb = require("comb"),
    hitch = comb.hitch,
    asyncArray = comb.async.array,
    define = comb.define,
    merge = comb.merge,
    string = comb.string,
    argsToArray = comb.argsToArray,
    format = string.format,
    Promise = comb.Promise,
    isString = comb.isString,
    array = comb.array,
    toArray = array.toArray,
    isArray = comb.isArray,
    isHash = comb.isHash,
    when = comb.when,
    isInstanceOf = comb.isInstanceOf,
    isFunction = comb.isFunction,
    isUndefinedOrNull = comb.isUndefinedOrNull,
    isUndefined = comb.isUndefined,
    isEmpty = comb.isEmpty,
    QueryError = require("../errors").QueryError,
    Dataset = require("../dataset"),
    Database = require("../database"),
    sql = require("../sql").sql,
    DateTime = sql.DateTime,
    Time = sql.Time,
    Year = sql.Year,
    Double = sql.Double,
    stream = require("stream"),
    PassThroughStream = stream.PassThrough,
    pipeAll = require("../utils").pipeAll,
    patio, DB;

var convertDate = function (v, m, convertDateTime) {
    v = ("" + v);
    try {
        return patio[m](v);
    } catch (e) {
        if (convertDateTime === null) {
            return null;
        } else if (convertDateTime === String || (isString(convertDateTime) && convertDateTime.match(/string/i))) {
            return v;
        } else {
            throw e;
        }
    }
};

var Connection = define(null, {
    instance: {

        connection: null,

        errored: false,

        closed: false,


        constructor: function (conn) {
            this.connection = conn;
        },

        closeConnection: function () {
            var ret = new Promise();
            this.closed = true;
            this.connection.end(hitch(ret, ret.resolve));
            return ret.promise();
        },


        stream: function (query) {
            var ret;
            if (!this.closed) {
                try {
                    ret = this.connection.query(query).stream();
                } catch (e) {
                    patio.logError(e);
                }
            } else {
                ret = new PassThroughStream();
                setImmediate(function () {
                    ret.emit("error", new Error("Connection already closed"));
                });
            }
            return ret;
        },


        query: function (query) {
            var ret = new Promise();
            if (!this.closed) {
                try {
                    this.connection.setMaxListeners(0);
                    this.connection.query(query, hitch(ret, ret.resolve));
                } catch (e) {
                    patio.logError(e);
                    ret.errback(e);
                }
            } else {
                ret.errback(new Error("Connection already closed"));
            }
            return ret.promise();
        }
    }
});

var DS = define(Dataset, {

    instance: {

        __providesAccurateRowsMatched: false,

        __supportsDistinctOn: true,

        __supportsIntersectExcept: false,

        __supportsModifyingJoins: true,

        __supportsTimestampUsecs: false,


        // MySQL specific syntax for LIKE/REGEXP searches, as well as
        // string concatenation.
        complexExpressionSql: function (op, args) {
            var likeOps = ["~", "~*", "LIKE", "ILIKE"];
            var notLikeOps = ["!~", "!~*", "NOT LIKE", "NOT ILIKE"];
            var regExpOps = ["~", "!~", "~*", "!~*"];
            var binaryOps = ["~", "!~", "LIKE", "NOT LIKE"];
            if (likeOps.indexOf(op) !== -1 || notLikeOps.indexOf(op) !== -1) {
                return format("(%s%s %s%s %s)", this.literal(args[0]), notLikeOps.indexOf(op) !== -1 ? " NOT" : "",
                    regExpOps.indexOf(op) !== -1 ? "REGEXP" : "LIKE", binaryOps.indexOf(op) !== -1 ? " BINARY" : "",
                    this.literal(args[1]));
            } else if (op === "||") {
                if (args.length > 1) {
                    return format("CONCAT(%s)", args.map(this.literal, this).join(", "));
                } else {
                    return this.literal(args[0]);
                }
            } else if (op === "B~") {
                return format("CAST(~%s AS SIGNED INTEGER)", this.literal(args[0]));
            } else {
                return this._super(arguments);
            }
        },

        // Use GROUP BY instead of DISTINCT ON if arguments are provided.
        distinct: function (args) {
            args = argsToArray(arguments);
            return !args.length ? this._super(arguments) : this.group.apply(this, args);
        },

        //Return a cloned dataset which will use LOCK IN SHARE MODE to lock returned rows.
        forShare: function () {
            return this.lockStyle("share");
        },

        //Adds full text filter
        fullTextSearch: function (cols, terms, opts) {
            opts = opts || {};
            cols = toArray(cols).map(this.stringToIdentifier, this);
            return this.filter(sql.literal(this.fullTextSql(cols, terms, opts)));
        },

        //MySQL specific full text search syntax.
        fullTextSql: function (cols, term, opts) {
            opts = opts || {};
            return format("MATCH %s AGAINST (%s%s)", this.literal(toArray(cols)),
                this.literal(toArray(term).join(" ")), opts.boolean ? " IN BOOLEAN MODE" : "");
        },

        //MySQL allows HAVING clause on ungrouped datasets.
        having: function (cond, cb) {
            var args = argsToArray(arguments);
            return this._filter.apply(this, ["having"].concat(args));
        },

        // Transforms an CROSS JOIN to an INNER JOIN if the expr is not nil.
        //Raises an error on use of :full_outer type, since MySQL doesn't support it.
        joinTable: function (type, table, expr, tableAlias) {
            tableAlias = tableAlias || {};
            if (type === "cross" && !isUndefinedOrNull(expr)) {
                type = "inner";
            }
            if (type === "fullOuter") {
                throw new QueryError("MySQL does not support FULL OUTER JOIN");
            }
            return this._super(arguments, [type, table, expr, tableAlias]);
        },

        // Transforms :natural_inner to NATURAL LEFT JOIN and straight to
        //STRAIGHT_JOIN.
        _joinTypeSql: function (joinType) {
            if (joinType === "straight") {
                return "STRAIGHT_JOIN";
            } else if (joinType === "naturalInner") {
                return "NATURAL LEFT JOIN";
            } else {
                return this._super(arguments);
            }
        },

        insertIgnore: function () {
            return this.mergeOptions({insertIgnore: true});
        },


        onDuplicateKeyUpdate: function (args) {
            args = argsToArray(arguments).map(function (c) {
                return isString(c) ? this.stringToIdentifier(c) : c;
            }, this);
            return this.mergeOptions({onDuplicateKeyUpdate: args});
        },

        // MySQL specific syntax for inserting multiple values at once.
        multiInsertSql: function (columns, values) {
            return [this.insertSql(columns, sql.literal('VALUES ' + values.map(
                function (r) {
                    return this.literal(toArray(r));
                }, this).join(this._static.COMMA_SEPARATOR)))];
        },

        //MySQL uses the nonstandard ` (backtick) for quoting identifiers.
        _quotedIdentifier: function (c) {
            return format("`%s`", c);
        },

        // MySQL specific syntax for REPLACE (aka UPSERT, or update if exists,
        //insert if it doesn't).
        replaceSql: function (values) {
            var ds = this.mergeOptions({replace: true});
            return ds.insertSql.apply(ds, argsToArray(arguments));
        },

        //If this is an replace instead of an insert, use replace instead
        _insertSql: function () {
            return this.__opts.replace ? this._clauseSql("replace") : this._super(arguments);
        },

        //Consider the first table in the joined dataset is the table to delete
        //from, but include the others for the purposes of selecting rows.
        _deleteFromSql: function (sql) {
            if (this._joinedDataset) {
                return format(" %s FROM %s%s", this._sourceList(this.__opts.from[0]), this._sourceList(this.__opts.from), this._selectJoinSql());
            } else {
                return this._super(arguments);
            }
        },

        //alias replace_clause_methods insert_clause_methods
        //MySQL doesn't use the standard DEFAULT VALUES for empty values.
        _insertColumnsSql: function (sql) {
            var values = this.__opts.values;
            if (isArray(values) && !values.length) {
                return " ()";
            } else {
                return this._super(arguments);
            }
        },

        //MySQL supports INSERT IGNORE INTO
        _insertIgnoreSql: function (sql) {
            return this.__opts.insertIgnore ? " IGNORE" : "";
        },

        //MySQL supports INSERT ... ON DUPLICATE KEY UPDATE
        _insertOnDuplicateKeyUpdateSql: function (sql) {
            return this.__opts.onDuplicateKeyUpdate ? this.onDuplicateKeyUpdateSql() : "";
        },

        //MySQL doesn't use the standard DEFAULT VALUES for empty values.
        _insertValuesSql: function (sql) {
            var values = this.__opts.values;
            if (isArray(values) && !values.length) {
                return " VALUES ()";
            } else {
                return this._super(arguments);
            }
        },

        //MySQL allows a LIMIT in DELETE and UPDATE statements.
        limitSql: function (sql) {
            return this.__opts.limit ? format(" LIMIT %s", this.__opts.limit) : "";
        },

        _deleteLimitSql: function () {
            return this.limitSql.apply(this, arguments);
        },

        _updateLimitSql: function () {
            return this.limitSql.apply(this, arguments);
        },


        //MySQL specific syntax for ON DUPLICATE KEY UPDATE
        onDuplicateKeyUpdateSql: function () {
            var ret = "";
            var updateCols = this.__opts.onDuplicateKeyUpdate;
            if (updateCols) {
                var updateVals = null, l, last;
                if ((l = updateCols.length) > 0 && isHash((last = updateCols[l - 1]))) {
                    updateVals = last;
                    updateCols = l === 2 ? [updateCols[0]] : updateCols.slice(0, l - 2);
                }
                var updating = updateCols.map(function (c) {
                    var quoted = this.quoteIdentifier(c);
                    return format("%s=VALUES(%s)", quoted, quoted);
                }, this);
                for (var i in updateVals) {
                    if (i in updateVals) {
                        updating.push(format("%s=%s", this.quoteIdentifier(i), this.literal(updateVals[i])));
                    }
                }
                if (updating || updateVals) {
                    ret =
                        format(" ON DUPLICATE KEY UPDATE %s", updating.join(this._static.COMMA_SEPARATOR));
                }
            }
            return ret;
        },

        //Support FOR SHARE locking when using the :share lock style.
        _selectLockSql: function (sql) {
            return this.__opts.lock === "share" ? this._static.FOR_SHARE : this._super(arguments);
        },

        // Delete rows matching this dataset
        remove: function () {
            return this.executeDui(this.deleteSql).chain(function (c, info) {
                return c.affectedRows;
            });
        },

        __processFields: function (fields) {
            var cols = [], i = -1, l = fields.length, col, fieldName, type, length, colIdentifier,
                outputIdentifier = this.outputIdentifier,
                selfCols = ( this.__columns = []);
            while (++i < l) {
                col = fields[i];
                fieldName = col.name;
                type = col.type;
                length = col.fieldLength;
                colIdentifier = outputIdentifier(fieldName);
                selfCols[i] = colIdentifier;
                cols[i] = [colIdentifier, DB.convertMysqlType(type === 1 && length !== 1 ? 2 : type), fieldName];
            }
            return cols;
        },

        //Don't allow graphing a dataset that splits multiple statements
        graph: function () {
            if (this.__opts.splitMultipleResultSels) {
                throw new QueryError("Can't graph a dataset that splits multiple result sets");
            }
            this._super(arguments);
        },

        //Insert a new value into this dataset
        insert: function () {
            return this.executeDui(this.insertSql.apply(this, arguments)).chain(function (c, info) {
                return c.insertId;
            });
        },

        // Replace (update or insert) the matching row.
        replace: function () {
            return this.executeDui(this.replaceSql.apply(this, arguments)).chain(function (c, info) {
                return c.insertId;
            });
        },

        splitMultipleResultSets: function () {
            if (this.__opts.graph) {
                throw new QueryError("Can't split multiple statements on a graphed dataset");
            }
            var ds = this.mergeOptions({splitMultipleResultSets: true});
            var rowCb = this.rowCb;
            if (rowCb) {
                ds.rowCb = function (x) {
                    return x.map(rowCb, this);
                };
            }
            return ds;
        },

        //Update the matching rows.
        update: function () {
            return this.executeDui(this.updateSql.apply(this, arguments)).chain(function (c, info) {
                return c.affectedRows;
            });
        },

        //Set the :type option to select if it hasn't been set.
        execute: function (sql, opts) {
            opts = opts || {};
            return this._super([sql, merge({type: "select"}, opts)]);
        },

        //Set the :type option to :select if it hasn't been set.
        executeDui: function (sql, opts) {
            opts = opts || {};
            return this._super([sql, merge({type: "dui"}, opts)]);
        },

        _literalString: function (v) {
            return "'" + v.replace(/[\0\n\r\t\\\'\"\x1a]/g, function (s) {
                    switch (s) {
                    case "0":
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

    "static": {
        BOOL_TRUE: '1',
        BOOL_FALSE: '0',
        COMMA_SEPARATOR: ', ',
        FOR_SHARE: ' LOCK IN SHARE MODE',
        DELETE_CLAUSE_METHODS: Dataset.clauseMethods("delete", "qualify from where order limit"),
        INSERT_CLAUSE_METHODS: Dataset.clauseMethods("insert", "ignore into columns values onDuplicateKeyUpdate"),
        REPLACE_CLAUSE_METHODS: Dataset.clauseMethods("insert", "ignore into columns values onDuplicateKeyUpdate"),
        SELECT_CLAUSE_METHODS: Dataset.clauseMethods("select", "qualify distinct columns from join where group having compounds order limit lock"),
        UPDATE_CLAUSE_METHODS: Dataset.clauseMethods("update", "table set where order limit")
    }

});

DB = define(Database, {
    instance: {

        PRIMARY: 'PRIMARY',

        type: "mysql",

        __supportsSavePoints: true,

        __supportsTransactionIsolationLevels: true,

        createConnection: function (opts) {
            delete opts.query;
            var self = this, ret;
            var conn = mysql.createConnection(merge({}, opts, {typeCast: false}));
            conn.on("error", function (err) {
                self.logWarn("Connection from " + self.uri + " errored removing from pool and reconnecting");
                self.logWarn(err.stack);
                ret.errored = true;
                self.pool.removeConnection(ret);
            });
            conn.connect();
            ret = new Connection(conn);
            return ret;
        },

        closeConnection: function (conn) {
            return conn.closeConnection();
        },

        validate: function (conn) {
            return new Promise().callback(!(conn.errored)).promise();
        },

        // MySQL's cast rules are restrictive in that you can't just cast to any possible
        // database type.
        castTypeLiteral: function (type) {
            var ret = null, meth;
            if (isString(type)) {
                ret = this._static.CAST_TYPES[type] || this._super(arguments);
            } else if (type === String) {
                meth += "CHAR";
            } else if (type === Number) {
                meth += "DECIMAL";
            } else if (type === DateTime) {
                meth += "DATETIME";
            } else if (type === Year) {
                meth += "Year";
            } else if (type === Time) {
                meth += "DATETIME";
            } else if (type === Double) {
                meth += "DECIMAL";
            } else {
                ret = this._super(arguments);
            }
            return ret;
        },

        // Use SHOW INDEX FROM to get the index information for the table.
        indexes: function (table, opts) {
            var indexes = {};
            var removeIndexes = [];
            var m = this.outputIdentifierFunc;
            var im = this.inputIdentifierFunc;
            return this.metadataDataset.withSql("SHOW INDEX FROM ?", isInstanceOf(table, sql.Identifier) ? table : sql.identifier(im(table)))
                .forEach(function (r) {
                    var name = r[m("Key_name")];
                    if (name !== "PRIMARY") {
                        name = m(name);
                        if (r[m("Sub_part")]) {
                            removeIndexes.push(name);
                        }
                        var i = indexes[name] || (indexes[name] = {columns: [], unique: r[m("Non_unique")] !== 1});
                        i.columns.push(m(r[m("Column_name")]));
                    }
                }).chain(function () {
                    var r = {};
                    for (var i in indexes) {
                        if (removeIndexes.indexOf(i) === -1) {
                            r[i] = indexes[i];
                        }
                    }
                    return r;
                });

        },

        // Get version of MySQL server, used for determined capabilities.
        serverVersion: function () {
            var ret;
            if (!this.__serverVersion) {
                var self = this;
                ret = this.get(sql.version().sqlFunction).chain(function (version) {
                    var m = version.match(/(\d+)\.(\d+)\.(\d+)/);
                    return (self._serverVersion = (parseInt(m[1], 10) * 10000) + (parseInt(m[2], 10) * 100) + parseInt(m[3], 10));
                });
            } else {
                ret = new Promise().callback(this._serverVersion);
            }
            return ret.promise();
        },

        //Return an array of strings specifying table names in the current database.
        tables: function (opts) {
            var m = this.outputIdentifierFunc;
            return this.metadataDataset.withSql('SHOW TABLES').map(function (r) {
                return m(r[Object.keys(r)[0]]);
            });
        },


        use: function (dbName) {
            var self = this;
            return this.disconnect().chain(function () {
                return self.run("USE " + dbName).chain(function () {
                    self.opts.database = dbName;
                    self.schemas = {};
                    return self;
                });
            });
        },

        //Use MySQL specific syntax for rename column, set column type, and
        // drop index cases.
        __alterTableSql: function (table, op) {
            var ret = new Promise(), self = this;
            if (op.op === "addColumn") {
                var related = op.table;
                if (related) {
                    delete op.table;
                    ret = this._super(arguments).chain(function (sql) {
                        op.table = related;
                        return [sql, format("ALTER TABLE %s ADD FOREIGN KEY (%s)%s",
                            self.__quoteSchemaTable(table), self.__quoteIdentifier(op.name),
                            self.__columnReferencesSql(op))];
                    });
                } else {
                    ret = this._super(arguments);
                }
            } else if (['renameColumn', "setColumnType", "setColumnNull", "setColumnDefault"].indexOf(op.op) !== -1) {
                ret = this.schema(table).chain(function (schema) {
                    var name = op.name;
                    var opts = schema[Object.keys(schema).filter(function (i) {
                        return i === name;
                    })[0]];
                    opts = merge({}, opts || {});
                    opts.name = op.newName || name;
                    opts.type = op.type || opts.dbType;
                    opts.allowNull = isUndefined(op["null"]) ? opts.allowNull : op["null"];
                    opts["default"] = op["default"] || opts.jsDefault;
                    if (isUndefinedOrNull(opts["default"])) {
                        delete opts["default"];
                    }
                    return format("ALTER TABLE %s CHANGE COLUMN %s %s", self.__quoteSchemaTable(table),
                        self.__quoteIdentifier(op.name), self.__columnDefinitionSql(merge(op, opts)));
                });
            } else if (op.op === "dropIndex") {
                ret = when(format("%s ON %s", this.__dropIndexSql(table, op), this.__quoteSchemaTable(table)));
            } else {
                ret = this._super(arguments);
            }
            return ret.promise();
        },

        //MySQL needs to set transaction isolation before beginning a transaction
        __beginNewTransaction: function (conn, opts) {
            var self = this;
            return this.__setTransactionIsolation(conn, opts).chain(function () {
                return self.__logConnectionExecute(conn, self.beginTransactionSql);
            });
        },

        // Use XA START to start a new prepared transaction if the :prepare
        //option is given.
        __beginTransaction: function (conn, opts) {
            opts = opts || {};
            var s;
            if ((s = opts.prepare)) {
                return this.__logConnectionExecute(conn, comb("XA START %s").format(this.literal(s)));
            } else {
                return this._super(arguments);
            }
        },

        // MySQL doesn't allow default values on text columns, so ignore if it the
        // generic text type is used
        __columnDefinitionSql: function (column) {
            if (isString(column.type) && column.type.match(/string/i) && column.text) {
                delete column["default"];
            }
            return this._super(arguments, [column]);
        },

        // Prepare the XA transaction for a two-phase commit if the
        // prepare option is given.
        __commitTransaction: function (conn, opts) {
            opts = opts || {};
            var s = opts.prepare, self = this;
            if (s) {
                return this.__logConnectionExecute(conn, comb("XA END %s").format(this.literal(s))).chain(function () {
                    return self.__logConnectionExecute(comb("XA PREPARE %s").format(self.literal(s)));
                });
            } else {
                return this._super(arguments);
            }
        },

        //Use MySQL specific syntax for engine type and character encoding
        __createTableSql: function (name, generator, options) {
            options = options || {};
            var engine = options.engine, charset = options.charset, collate = options.collate;
            if (isUndefined(engine)) {
                engine = this._static.defaultEngine;
            }
            if (isUndefined(charset)) {
                charset = this._static.defaultCharset;
            }
            if (isUndefined(collate)) {
                collate = this._static.defaultCollate;
            }
            generator.columns.forEach(function (c) {
                var t = c.table;
                if (t) {
                    delete c.table;
                    generator.foreignKey([c.name], t, merge({}, c, {name: null, type: "foreignKey"}));
                }
            });
            return format(" %s%s%s%s", this._super(arguments), engine ? " ENGINE=" + engine : "",
                charset ? " DEFAULT CHARSET=" + charset : "", collate ? " DEFAULT COLLATE=" + collate : "");
        },

        //Handle MySQL specific index SQL syntax
        __indexDefinitionSql: function (tableName, index) {
            var indexName = this.__quoteIdentifier(index.name || this.__defaultIndexName(tableName,
                index.columns)), t = index.type, using = "";
            var indexType = "";
            if (t === "fullText") {
                indexType = "FULLTEXT ";
            } else if (t === "spatial") {
                indexType = "SPATIAL ";
            } else {
                indexType = index.unique ? "UNIQUE " : "";
                using = t ? " USING " + t : "";
            }
            return format("CREATE %sINDEX %s%s ON %s %s", indexType, indexName, using,
                this.__quoteSchemaTable(tableName), this.literal(index.columns.map(function (c) {
                    return isString(c) ? sql.identifier(c) : c;
                })));
        },

        // Rollback the currently open XA transaction
        __rollbackTransaction: function (conn, opts) {
            opts = opts || {};
            var s = opts.prepare;
            var logConnectionExecute = comb("__logConnectionExecute");
            if (s) {
                s = this.literal(s);
                var self = this;
                return this.__logConnectionExecute(conn, "XA END " + s)
                    .chain(function () {
                        return self.__logConnectionExecute(conn, "XA PREPARE " + s);
                    })
                    .chain(function () {
                        return self.__logConnectionExecute(conn, "XA ROLLBACK " + s);
                    });
            } else {
                return this._super(arguments);
            }
        },

        // MySQL treats integer primary keys as autoincrementing.
        _schemaAutoincrementingPrimaryKey: function (schema) {
            return this._super(arguments) && schema.dbType.match(/int/i);
        },

        //Use the MySQL specific DESCRIBE syntax to get a table description.
        schemaParseTable: function (tableName, opts) {
            var m = this.outputIdentifierFunc, im = this.inputIdentifierFunc, self = this;
            return this.metadataDataset.withSql("DESCRIBE ?", sql.identifier(im(tableName))).map(function (row) {
                var ret = {};
                var e = row[m("Extra")];
                var allowNull = row[m("Null")];
                var key = row[m("Key")];
                ret.autoIncrement = e.match(/auto_increment/i) !== null;
                ret.allowNull = allowNull.match(/Yes/i) !== null;
                ret.primaryKey = key.match(/PRI/i) !== null;
                var defaultValue = row[m("Default")];
                ret["default"] = Buffer.isBuffer(defaultValue) ? defaultValue.toString() : defaultValue;
                if (isEmpty(row["default"])) {
                    row["default"] = null;
                }
                ret.dbType = row[m("Type")];
                if (Buffer.isBuffer(ret.dbType)) {
                    //handle case for field type being returned at 252  (i.e. BLOB)
                    ret.dbType = ret.dbType.toString();
                }
                ret.type = self.schemaColumnType(ret.dbType.toString("utf8"));
                var fieldName = m(row[m("Field")]);
                return [fieldName, ret];
            });
        },

        //Convert tinyint(1) type to boolean if convert_tinyint_to_bool is true
        schemaColumnType: function (dbType) {
            return this._static.convertTinyintToBool && dbType === 'tinyint(1)' ? "boolean" : this._super(arguments);
        },


        //MySQL doesn't have a true boolean class, so it uses tinyint(1)
        __typeLiteralGenericBoolean: function (column) {
            return 'tinyint(1)';
        },

        getters: {

            identifierInputMethodDefault: function () {
                return null;
            },

            identifierOutputMethodDefault: function () {
                return null;
            },

            connectionExecuteMethod: function () {
                return "query";
            },

            dataset: function () {
                return new DS(this);
            }
        }
    },

    "static": {
        __convertTinyintToBool: true,

        __convertInvalidDateTime: false,

        CAST_TYPES: {string: "CHAR", integer: "SIGNED", time: "DATETIME", datetime: "DATETIME", numeric: "DECIMAL"},

        AUTOINCREMENT: 'AUTO_INCREMENT',

        init: function () {
            this.setAdapterType("mysql");
        },

        FIELD_TYPES: {
            FIELD_TYPE_DECIMAL: 0x00,
            FIELD_TYPE_TINY: 0x01,
            FIELD_TYPE_SHORT: 0x02,
            FIELD_TYPE_LONG: 0x03,
            FIELD_TYPE_FLOAT: 0x04,
            FIELD_TYPE_DOUBLE: 0x05,
            FIELD_TYPE_NULL: 0x06,
            FIELD_TYPE_TIMESTAMP: 0x07,
            FIELD_TYPE_LONGLONG: 0x08,
            FIELD_TYPE_INT24: 0x09,
            FIELD_TYPE_DATE: 0x0a,
            FIELD_TYPE_TIME: 0x0b,
            FIELD_TYPE_DATETIME: 0x0c,
            FIELD_TYPE_YEAR: 0x0d,
            FIELD_TYPE_NEWDATE: 0x0e,
            FIELD_TYPE_VARCHAR: 0x0f,
            FIELD_TYPE_BIT: 0x10,
            FIELD_TYPE_NEWDECIMAL: 0xf6,
            FIELD_TYPE_ENUM: 0xf7,
            FIELD_TYPE_SET: 0xf8,
            FIELD_TYPE_TINY_BLOB: 0xf9,
            FIELD_TYPE_MEDIUM_BLOB: 0xfa,
            FIELD_TYPE_LONG_BLOB: 0xfb,
            FIELD_TYPE_BLOB: 0xfc,
            FIELD_TYPE_VAR_STRING: 0xfd,
            FIELD_TYPE_STRING: 0xfe,
            FIELD_TYPE_GEOMETRY: 0xff

        },

        convertMysqlType: function (type) {
            var convert = this.convertTinyintToBool, convertDateTime = this.__convertInvalidDateTime, types = this.FIELD_TYPES;
            if (!patio) {
                patio = require("../index");
            }
            return function (o) {
                var ret = o;
                if (o !== null) {
                    switch (type) {
                    case types.FIELD_TYPE_TIMESTAMP:
                    case types.FIELD_TYPE_DATETIME:
                        ret = convertDate(o, "stringToDateTime", convertDateTime);
                        break;
                    case types.FIELD_TYPE_DATE:
                    case types.FIELD_TYPE_NEWDATE:
                        ret = convertDate(o, "stringToDate", convertDateTime);
                        break;
                    case types.FIELD_TYPE_TIME:
                        ret = convertDate(o, "stringToTime", convertDateTime);
                        break;
                    case types.FIELD_TYPE_TINY:
                        ret = convert ? parseInt(o, 10) === 1 : parseInt(o, 10);
                        break;
                    case types.FIELD_TYPE_YEAR:
                        ret = convertDate(o, "stringToYear", convertDateTime);
                        break;
                    case types.FIELD_TYPE_SHORT:
                    case types.FIELD_TYPE_LONG:
                    case types.FIELD_TYPE_LONGLONG:
                    case types.FIELD_TYPE_INT24:
                        ret = parseInt(o, 10);
                        break;
                    case types.FIELD_TYPE_FLOAT:
                    case types.FIELD_TYPE_DOUBLE:
                    case types.FIELD_TYPE_DECIMAL:
                        // decimal types cannot be parsed as floats because
                        // V8 Numbers have less precision than some MySQL Decimals
                        ret = parseFloat(o);
                        break;
                    case types.FIELD_TYPE_TINY_BLOB:
                    case types.FIELD_TYPE_MEDIUM_BLOB:
                    case types.FIELD_TYPE_LONG_BLOB:
                    case types.FIELD_TYPE_BLOB:
                        ret = new Buffer(o);
                        break;
                    }
                }
                return ret;
            };
        },

        getters: {
            convertTinyintToBool: function () {
                return this.__convertTinyintToBool;
            },

            convertInvalidDateTime: function () {
                return this.__convertInvalidDateTime;
            }

        },

        setters: {
            convertTinyintToBool: function (convert) {
                this.__convertTinyintToBool = convert;
            },

            convertInvalidDateTime: function (convert) {
                this.__convertInvalidDateTime = convert;
            }

        }
    }
}).as(exports, "MySQLDatabase");
