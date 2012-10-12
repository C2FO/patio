var pg = require("pg"),
    PgTypes = require("pg/lib/types"),
    StringDecoder = require("string_decoder").StringDecoder,
    comb = require("comb"),
    asyncArray = comb.async.array,
    string = comb.string,
    pad = string.pad,
    format = string.format,
    hitch = comb.hitch,
    when = comb.when,
    serial = comb.serial,
    array = comb.array,
    toArray = array.toArray,
    zip = array.zip,
    flatten = array.flatten,
    Promise = comb.Promise,
    isUndefinedOrNull = comb.isUndefinedOrNull,
    isString = comb.isString,
    isArray = comb.isArray,
    isEmpty = comb.isEmpty,
    isBoolean = comb.isBoolean,
    isObject = comb.isObject,
    isFunction = comb.isFunction,
    define = comb.define,
    merge = comb.merge,
    isDefined = comb.isDefined,
    isInstanceOf = comb.isInstanceOf,
    QueryError = require("../errors").QueryError,
    Dataset = require("../dataset"),
    Database = require("../database"),
    sql = require("../sql").sql,
    stringToIdentifier = sql.stringToIdentifier,
    DateTime = sql.DateTime,
    Time = sql.Time,
    Year = sql.Year,
    literal = sql.literal,
    StringExpression = sql.StringExpression,
    Double = sql.Double,
    identifier = sql.identifier,
    BooleanExpression = sql.BooleanExpression,
    LiteralString = sql.LiteralString,
    Subscript = sql.Subscript,
    patio;

var getPatio = function () {
    return patio || (patio = require("../index.js"));
};

var isBlank = function (obj) {
    var ret = false;
    if (isUndefinedOrNull(obj)) {
        ret = true;
    } else if (isString(obj) || isArray(obj)) {
        ret = obj.length === 0;
    } else if (isBoolean(obj) && !obj) {
        ret = true;
    } else if (isObject(obj) && isEmpty(obj)) {
        ret = true;
    }
    return ret;
};

var byteaParser = function (val) {
    if (val.toString().indexOf("\\x") === 0) {
        val = val.toString().replace(/^\\x/, "");
        return new Buffer(val, "hex");
    } else {
        val = val.toString().replace(/\\([0-7]{3})/g,function (full_match, code) {
            return String.fromCharCode(parseInt(code, 8));
        }).replace(/\\\\/g, "\\");
        return new Buffer(val, "binary");
    }
};

PgTypes.setTypeParser(17, "text", byteaParser);
var timestampOrig = PgTypes.getTypeParser(1114, "text");
//PgTypes.setTypeParser(25, "text", byteaParser);
PgTypes.setTypeParser(1114, "text", function (val) {
    val = String(val);
    if (!val.match(/\.(\d{0,3})/)) {
        val += ".000";
    } else {
        val = val.replace(/\.(\d{0,3})$/, function (m, m1) {
            return "." + pad(m1, 3, "0", true);
        });
    }
    return getPatio().stringToTimeStamp(val.toString(), DS.TIMESTAMP_FORMAT).date;
});
PgTypes.setTypeParser(1184, "text", function (val) {
    return getPatio().stringToDate(val.toString());
});

PgTypes.setTypeParser(1082, "text", function (val) {
    return getPatio().stringToDate(val.toString());
});

PgTypes.setTypeParser(1083, "text", function (val) {
    return getPatio().stringToTime(val.toString(), DS.TIME_FORMAT);
});


var Connection = define(null, {
    instance:{

        connection:null,


        constructor:function (conn) {
            this.connection = conn;
        },

        closeConnection:function () {
            this.connection.end();
            return new Promise().callback().promise();
        },

        query:function (query) {
            var ret = new Promise();
            try {
                this.connection.setMaxListeners(0);
                var fields = [];
                var q = this.connection.query(query, hitch(this, function (err, results) {
                    q.handleRowDescription = orig;
                    if (err) {
                        return ret.errback(err);
                    } else {
                        return ret.callback(results.rows, fields);
                    }
                }));
                var orig = q.handleRowDescription;
                q.handleRowDescription = function (msg) {
                    fields = msg.fields;
                    return orig.apply(q, arguments);
                };
            } catch (e) {
                patio.logError(e);
            }
            return ret.promise();
        }
    }
});

var DS = define(Dataset, {
    instance:{

        complexExpressionSql:function (op, args) {
            var ret = "";
            if (op === "^") {
                var j = this._static.XOR_OP, c = false;
                args.forEach(function (a) {
                    if (c) {
                        ret += j;
                    }
                    ret += this.literal(a);
                    c = true;
                }, true);
            } else {
                return this._super(arguments);
            }
            return ret;
        },

        forShare:function () {
            return this.lockStyle("share");
        },

        fullTextSearch:function (cols, terms, opts) {
            opts = opts || {};
            var lang = opts.language || 'simple';
            if (Array.isArray(terms)) {
                terms = terms.join(' | ');
            }
            return this.filter("to_tsvector(?, ?) @@ to_tsquery(?, ?)", lang, this.__fullTextStringJoin(toArray(cols).map(function (c) {
                return stringToIdentifier(c);
            })), lang, terms);
        },

        /**
         * Lock all tables in the datasets from clause (but not in JOINs), in the specified mode. If
         * a function is passed in as the last argument
         * @para {String} mode the lock mode (e.g. 'EXCLUSIVE').
         * @param {Object} [opts] see {@link patio.Database#transaction} for options.
         * @param {Function} [cb] of provided then a new {@link patio.Database} transaction is started.
         *
         */
        lock:function (mode, opts, cb) {
            if (isFunction(opts)) {
                cb = opts;
                opts = null;
            } else {
                opts = opts || {};
            }
            if (isFunction(cb)) {
                return this.db.transaction(opts, function () {
                    return serial([
                        this.lock.bind(this, mode, opts),
                        cb.bind(this)
                    ]);

                }.bind(this));
            } else {
                return this.db.execute(format(this._static.LOCK, [this._sourceList(this.__opts.from), mode]), opts);
            }
        },

        multiInsertSql:function (columns, values) {
            var ret = literal('VALUES ');
            ret += this.__expressionList(values.map(function (r) {
                return toArray(r);
            }));
            return [this.insertSql(columns.map(function (c) {
                return stringToIdentifier(c);
            }), literal(ret))];
        },


        _deleteFromSql:function () {
            var self = this._static, space = self.SPACE;
            return [space, self.FROM, space, this._sourceList(this.__opts.from[0])].join("");
        },


        _deleteUsingSql:function () {
            return this._joinFromSql("USING");
        },

        _joinFromSql:function (type) {
            var from = this.__opts.from.slice(1), join = this.__opts.join, ret = "";
            if (!from.length) {
                if (!isEmpty(join)) {
                    throw new QueryError("Need multiple FROM tables if updating/deleteing a dataset with joins");
                }
            } else {
                var space = this._static.SPACE;
                ret = [space, type.toString(), space, this._sourceList(from), this._selectJoinSql()].join("");
            }
            return ret;
        },

        _selectLockSql:function () {
            if (this.__opts.lock === "share") {
                return this._static.FOR_SHARE;
            } else {
                return this._super(arguments);
            }
        },

        _selectWithSql:function () {
            var optsWith = this.__opts["with"];
            if (!isEmpty(optsWith) && optsWith.some(function (w) {
                return w.recursive;
            })) {
                return this._static.SQL_WITH_RECURSIVE;
            } else {
                return this._super(arguments);
            }
        },

        _updateFromSql:function () {
            return this._joinFromSql("FROM");
        },

        _updateTableSql:function () {
            return [this._static.SPACE, this._sourceList(this.__opts.from.slice(0, 1))].join("");
        },

        _quotedIdentifier:function (c) {
            return format('"%s"', c);
        },

        __fullTextStringJoin:function (cols) {
            var EMPTY_STRING = this._static.EMPTY_STRING;
            cols = toArray(cols).map(function (x) {
                return  sql.COALESCE(x, EMPTY_STRING);
            });
            cols = flatten(zip(cols, array.multiply([this._static.SPACE], cols.length)));
            cols.pop();
            return StringExpression.fromArgs(['||'].concat(cols));
        },

        insert:function () {
            var args = arguments;
            if (this.__opts.returning) {
                return this._super(arguments);
            } else {
                var ret = new Promise();
                this.primaryKey(this.__opts.from).then(function (res) {
                    var pks = res.map(function (r) {
                        return r.name;
                    });
                    var ds = this.returning.apply(this, pks);
                    var dsPromise = ds.insert.apply(ds, args), l = res.length;
                    if (l) {
                        dsPromise.then(function (insertRes) {
                            if (l === 1) {
                                ret.callback(insertRes.map(function (i) {
                                    return i[pks[0]];
                                }).pop());
                            } else {
                                ret.callback(insertRes.pop());
                            }

                        }, ret);
                    } else {
                        dsPromise.then(ret);
                    }
                }.bind(this), ret);
                return ret.promise();
            }
        },

        primaryKey:function () {
            return this.db.primaryKey(this.__opts.from[0]);
        },

        fetchRows:function (sql) {
            var oi = this.outputIdentifier.bind(this);
            return asyncArray(this.execute(sql).chain(function (rows, fields) {
                var cols = [];
                if (rows && rows.length) {
                    cols = this.__columns = fields && fields.length ? fields.map(function (f) {
                        return f.name;
                    }) : Object.keys(rows[0]);
                    //the pg driver does auto type coercion
                    cols = cols.map(function (c) {
                        return [oi(c), function (o) {
                            return o;
                        }, c];
                    }, this);
                }
                return this.__processRows(rows, cols);
            }.bind(this)));
        },

        __processRows:function (rows, cols) {
            //dp this so the callbacks are called in appropriate order also.
            return comb(rows).map(function (row, i) {
                var h = {};
                cols.forEach(function (col) {
                    h[col[0]] = col[1](row[col[2]]);
                });
                return h;
            });
        },


        _literalTimestamp:function (v) {
            return this.literal(literal("TIMESTAMP " + this._super(arguments) + ""));
        },

        _literalBuffer:function (b) {
            return this.literal(literal("decode('" + b.toString("hex") + "', 'hex')"));
        },

        getters:{

            columns:function () {
                var ret = new Promise();
                if (this.__columns) {
                    ret.callback(this.__columns);
                } else {
                    this.db.schema(this.firstSourceTable).then(function (schema) {
                        this.__columns = schema ? Object.keys(schema) : [];
                        ret.callback(this.__columns);
                    }, ret);
                }
                return ret.promise();
            },

            supportsCteInSubqueries:function () {
                return true;
            },

            supportsDistinctOn:function () {
                return true;
            },

            supportsModifyingJoins:function () {
                return true;
            },

            supportsTimestampTimezones:function () {
                return true;
            }
        }


    },
    "static":{
        ACCESS_SHARE:'ACCESS SHARE',
        ACCESS_EXCLUSIVE:'ACCESS EXCLUSIVE',
        BOOL_FALSE:'false',
        BOOL_TRUE:'true',
        COMMA_SEPARATOR:', ',
        DELETE_CLAUSE_METHODS:Dataset.clauseMethods("delete", 'qualify with from using where returning'),
        EXCLUSIVE:'EXCLUSIVE',
        EXPLAIN:'EXPLAIN ',
        EXPLAIN_ANALYZE:'EXPLAIN ANALYZE ',
        FOR_SHARE:' FOR SHARE',
        INSERT_CLAUSE_METHODS:Dataset.clauseMethods("insert", 'with into columns values returning'),
        LOCK:'LOCK TABLE %s IN %s MODE',
        NULL:literal('NULL'),
        QUERY_PLAN:'QUERY PLAN',
        ROW_EXCLUSIVE:'ROW EXCLUSIVE',
        ROW_SHARE:'ROW SHARE',
        SELECT_CLAUSE_METHODS:Dataset.clauseMethods("select", '' +
            'qualify with distinct columns from join where group having compounds order limit lock'),
        SHARE:'SHARE',
        SHARE_ROW_EXCLUSIVE:'SHARE ROW EXCLUSIVE',
        SHARE_UPDATE_EXCLUSIVE:'SHARE UPDATE EXCLUSIVE',
        SQL_WITH_RECURSIVE:"WITH RECURSIVE ",
        TIMESTAMP_FORMAT:"yyyy-MM-dd HH:mm:ss.SSS",
        TIME_FORMAT:"HH:mm:ss.SSS",
        UPDATE_CLAUSE_METHODS:Dataset.clauseMethods("update", 'with table set from where returning'),
        XOR_OP:' # ',
        CRLF:"\r\n",
        BLOB_RE:/[\000-\037\047\134\177-\377]/,
        WINDOW:" WINDOW ",
        EMPTY_STRING:literal("''")


    }
});

var DB = define(Database, {
    instance:{

        EXCLUDE_SCHEMAS:/pg_*|information_schema/i,
        PREsPARED_ARG_PLACEHOLDER:new LiteralString('$'),
        RE_CURRVAL_ERROR:/currval of sequence "(.*)" is not yet defined in this session|relation "(.*)" does not exist/,
        SYSTEM_TABLE_REGEXP:/^pg|sql/,

        type:"postgres",

        constructor:function () {
            this._super(arguments);
            this.__primaryKeys = {};
        },

        createConnection:function (opts) {
            delete opts.query;
            var conn = new pg.Client(merge({}, opts, {typeCast:false}));
            conn.connect();
            //conn.useDatabase(opts.database)
            return new Connection(conn);
        },

        closeConnection:function (conn) {
            return conn.closeConnection();
        },

        validate:function (conn) {
            return new Promise().callback(true).promise();
        },

        execute:function (sql, opts, conn) {
            return when(conn || this._getConnection())
                .chain(function (conn) {
                return this.__execute(conn, sql, opts);
            }.bind(this));
        },

        __execute:function (conn, sql, opts, cb) {
            return this.__logAndExecute(sql, comb("query").bindIgnore(conn, sql))
                .both(comb("_returnConnection").bindIgnore(this, conn));


        },

        // Use the pg_* system tables to determine indexes on a table
        indexes:function (table, opts) {
            opts = opts || {};
            var m = this.outputIdentifierFunc;
            var im = this.inputIdentifierFunc;
            var parts = this.__schemaAndTable(table), schema = parts[0];
            table = parts[1];
            var ret = new Promise();
            when(this.serverVersion()).then(function (version) {
                var attNums;
                if (version >= 80100) {
                    attNums = sql.ANY("ind__indkey");
                } else {
                    attNums = [];
                    for (var i = 0; i < 32; i++) {
                        attNums.push(Subscript("ind__indkey", [i]));
                    }
                }
                var orderRange = [];
                for (var j = 0; j < 32; j++) {
                    orderRange.push(new Subscript("ind__indkey", [j]));
                }
                orderRange = sql["case"](orderRange, 32, "att__attnum");
                var ds = this.metadataDataset.from("pg_class___tab")
                    .join("pg_index___ind", [
                    [identifier("indrelid"), identifier("oid")],
                    [im(table), "relname"]
                ])
                    .join("pg_class___indc", [
                    [identifier("oid"), identifier("indexrelid")]
                ])
                    .join("pg_attribute___att", [
                    [identifier("attrelid"), identifier("tab__oid")],
                    [identifier("attnum"), attNums]
                ])
                    .filter({indc__relkind:'i', ind__indisprimary:false, indexprs:null, indpred:null})
                    .order("indc__relname", orderRange)
                    .select("indc__relname___name", "ind__indisunique___unique", "att__attname___column");

                if (schema) {
                    ds = ds.join("pg_namespace___nsp", {oid:identifier("tab__relnamespace"), nspname:schema.toString()});
                }
                if (version >= 80200) {
                    ds = ds.filter({indisvalid:true});
                }
                if (version >= 80300) {
                    ds = ds.filter({indisready:true, indcheckxmin:false});
                }
                var indexes = {};
                ds.forEach(function (r) {
                    var ident = m(r.name), i = indexes[ident];
                    if (!i) {
                        i = indexes[ident] = {columns:[], unique:r.unique};
                    }
                    i.columns.push(r.column);
                }).then(function () {
                        ret.callback(indexes);
                    }, ret);
            }, ret);

            return ret.promise();
        },

        locks:function () {
            return this.dataset.from("pg_class").join("pg_locks", {relation:identifier("relfilenode")}).select("pg_class__relname", identifier("pg_locks").all());
        },

        // Get version of postgres server, used for determined capabilities.
        serverVersion:function () {
            var ret = new Promise();
            if (!this.__serverVersion) {
                this.get(identifier("version").sqlFunction).then(hitch(this, function (version) {
                    var m = version.match(/PostgreSQL (\d+)\.(\d+)(?:(?:rc\d+)|\.(\d+))?/);
                    this._serverVersion = (parseInt(m[1], 10) * 10000) + (parseInt(m[2], 10) * 100) + parseInt(m[3], 10);
                    ret.callback(this._serverVersion);
                }), ret);
            } else {
                ret.callback(this._serverVersion);
            }
            return ret.promise();
        },

        /**
         * Return an array of table names in the current database.
         * The dataset used is passed to the block if one is provided,
         * otherwise, an a promise resolved with an array of table names.
         *
         * Options:
         * @param {Object} [opts = {}] options
         * @param {String|patio.sql.Identifier} [opts.schema] The schema to search (default_schema by default)
         * @param {Function} [cb = null] an optional callback that is invoked with the dataset to retrieve tables.
         * @return {Promise} a promise resolved with the table names or the result of the cb if one is provided.
         */
        tables:function (opts, cb) {
            return this.__pgClassRelname('r', opts, cb);
        },

        /**
         * Return an array of view names in the current database.
         *
         * Options:
         * @param {Object} [opts = {}] options
         * @param {String|patio.sql.Identifier} [opts.schema] The schema to search (default_schema by default)
         * @return {Promise} a promise resolved with the view names.
         */
        views:function (opts) {
            return this.__pgClassRelname('v', opts);
        },

        primaryKey:function (table, opts) {
            var ret, quotedTable = this.__quoteSchemaTable(table).toString(), pks = this.__primaryKeys;
            if (pks.hasOwnProperty(quotedTable.toString())) {
                ret = pks[quotedTable];
            } else {
                ret = (pks[quotedTable] = this.__primarykey(table));
            }
            return ret.promise();
        },

        __primarykey:function (table) {
            var parts = this.__schemaAndTable(table);
            var m2 = this.inputIdentifierFunc;
            var schema = parts[0];
            table = parts[1];
            var ds = this.from(table)
                .select("pg_attribute__attname___name")
                .from("pg_index", "pg_class", "pg_attribute", "pg_namespace")
                .where([
                [identifier("pg_class__oid"), identifier("pg_attribute__attrelid")],
                [identifier("pg_class__relnamespace"), identifier("pg_namespace__oid")],
                [identifier("pg_class__oid"), identifier("pg_index__indrelid")],
                [identifier("pg_index__indkey").sqlSubscript(0), identifier("pg_attribute__attnum")],
                [identifier("indisprimary"), true],
                [identifier("pg_class__relname"), m2(table.toString())]
            ]);
            if (schema) {
                ds.filter({pg_namespace__nspname:m2(schema)});
            }
            return ds.all();

        },


        schemaParseTable:function (tableName, opts) {
            var m = this.outputIdentifierFunc,
                m2 = this.inputIdentifierFunc;
            var ds = this.metadataDataset
                .select(
                "pg_attribute__attname___name",
                sql.format_type("pg_type__oid", "pg_attribute__atttypmod").as(literal('"dbType"')),
                sql.pg_get_expr("pg_attrdef__adbin", "pg_class__oid").as(literal('"default"')),
                sql.NOT("pg_attribute__attnotnull").as(literal('"allowNull"')),
                sql.COALESCE(BooleanExpression.fromValuePairs({pg_attribute__attnum:sql.ANY("pg_index__indkey")}), false).as(literal('"primaryKey"')),
                "pg_namespace__nspname"
            ).from("pg_class")
                .join("pg_attribute", {attrelid:identifier("oid")})
                .join("pg_type", {oid:identifier("atttypid")})
                .join("pg_namespace", {oid:identifier("pg_class__relnamespace")})
                .leftOuterJoin("pg_attrdef", {adrelid:identifier("pg_class__oid"), adnum:identifier("pg_attribute__attnum")})
                .leftOuterJoin("pg_index", {indrelid:identifier("pg_class__oid"), indisprimary:true})
                .filter({pg_attribute__attisdropped:false})
                .filter({pg_attribute__attnum:{gt:0}})
                .filter({pg_class__relname:m2(tableName)})
                .order("pg_attribute__attnum");
            ds = this.__filterSchema(ds, opts);
            var currentSchema = null;
            return ds.map(function (row) {
                var sch = row.nspname;
                delete row.nspname;
                if (currentSchema) {
                    if (sch !== currentSchema) {
                        var error = new Error("columns from two tables were returned please specify a schema");
                        this.logError(error);
                    }
                } else {
                    currentSchema = sch;
                }
                if (isBlank(row["default"])) {
                    row["default"] = null;
                }
                row.type = this.schemaColumnType(row.dbType);
                var fieldName = m(row.name);
                delete row.name;
                return [fieldName, row];

            }.bind(this));

        },

        __commitTransaction:function (conn, opts) {
            opts = opts || {};
            var s = opts.prepare;
            if (s && this.__transactionDepth <= 1) {
                return this.__logConnectionExecute(conn, ["PREPARE TRANSACTION ", this.literal(s)].join(""));
            } else {
                return this._super(arguments);
            }
        },

        //Backbone of the tables and views support.
        __pgClassRelname:function (type, opts, cb) {
            var ret = new Promise();
            var ds = this.metadataDataset.from("pg_class")
                .filter({relkind:type}).select("relname")
                .exclude({relname:{like:this.SYSTEM_TABLE_REGEXP}})
                .join("pg_namespace", {oid:identifier("relnamespace")});
            ds = this.__filterSchema(ds, opts);
            var m = this.outputIdentifierFunc;
            if (cb) {
                when(cb(ds)).then(ret);
            } else {
                ds.map(function (r) {
                    return m(r.relname);
                }).then(ret);
            }
            return ret.promise();
        },

        //If opts includes a :schema option, or a default schema is used, restrict the dataset to
        // that schema.  Otherwise, just exclude the default PostgreSQL schemas except for public.
        __filterSchema:function (ds, opts) {
            opts = opts || {};
            var schema = opts.schema, ret = ds;
            if (schema) {
                ds = ds.filter({pg_namespace__nspname:schema});
            } else {
                ds = ds.exclude({pg_namespace__nspname:this.EXCLUDE_SCHEMAS});
            }
            return ds;
        },

        __indexDefinitionSql:function (tableName, index) {
            tableName = stringToIdentifier(tableName);
            var cols = index.columns.map(function (col) {
                    return stringToIdentifier(col);
                }),
                indexName = index.name || this.__defaultIndexName(tableName, cols),
                o = index.opclass,
                indexType = index.type,
                unique = index.unique ? "UNIQUE" : "",
                filter = index.where || index.filter,
                expr;
            filter = filter ? ["WHERE ", this.__filterExpr(filter)].join("") : "";
            if (isDefined(o)) {
                expr = ["(", cols.map(function (c) {
                    return [this.literal(c), o].join(" ");
                }, this).join(", "), ")"].join("");
            } else {
                expr = this.literal(toArray(cols));
            }
            switch (indexType) {
                case "fullText":
                    expr = ["(to_tsvector(", this.literal(index.language || "simple"), ", ", this.literal(this.dataset.__fullTextStringJoin(cols)), "))"].join("");
                    indexType = "gin";
                    break;
                case "spatial" :
                    indexType = "gist";
                    break;
            }
            return ["CREATE", unique, "INDEX", this.__quoteIdentifier(indexName), "ON", this.__quoteSchemaTable(tableName), indexType ? "USING " + indexType : "", expr, filter ].join(" ");

        },
        /*
         todo might need this?
         __insertResult:function (conn, table, values) {
         },
         */

        __renameTableSql:function (name, newName) {
            return ["ALTER TABLE ", this.__quoteSchemaTable(name), " RENAME TO ", this.__quoteIdentifier(this.__schemaAndTable(newName).pop())].join("");
        },

        __schemaAutoincrementingPrimaryKey:function (schema) {
            return this._super(arguments) && schema.dbType.match(/^(?:integer|bigint)$/i) && schema["default"].match(/^nextval/i);
        },

        __typeLiteralGenericNumeric:function (column) {
            return column.size ? format("numeric(%s)", array.toArray(column.size).join(', ')) : column.isInt ? "integer" : column.isDouble ? "double precision" : "numeric";
        },

        __typeLiteralGenericDateTime:function (column) {
            return "timestamp";
        },

        //handle bigserial
        __typeLiteralGenericBigint:function (column) {
            return column.serial ? "bigserial" : this.__typeLiteralSpecific(column);
        },

        __typeLiteralGenericBlob:function (column) {
            return "bytea";
        },

        //handle serial type
        __typeLiteralGenericInteger:function (column) {
            return column.serial ? "serial" : this.__typeLiteralSpecific(column);
        },

        // PostgreSQL prefers the text datatype.  If a fixed size is requested,
        // the char type is used.  If the text type is specifically
        // disallowed or there is a size specified, use the varchar type.
        // Otherwise use the type type.
        __typeLiteralGenericString:function (column) {
            if (column.fixed) {
                return ["char(", column.size || 255, ")"].join("");
            } else if (column.text === false || column.size) {
                return ["varchar(", column.size || 255, ")"].join("");
            } else {
                return 'text';
            }

        },


        getters:{

            connectionExecuteMethod:function () {
                return "query";
            },

            dataset:function () {
                return new DS(this);
            },

            serialPrimaryKeyOptions:function () {
                return {primaryKey:true, serial:true, type:"integer"};
            },

            supportsSavepoints:function () {
                return true;
            },

            supportsTransactionIsolationLevels:function () {
                return true;
            },

            identifierInputMethodDefault:function () {
                return null;
            },

            identifierOutputMethodDefault:function () {
                return null;
            }
        }
    },

    "static":{

        init:function () {
            this.setAdapterType("pg");
        }

    }
}).
    as(exports, "PostgresDatabase");
