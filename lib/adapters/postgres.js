var pg = require("pg"),
    comb = require("comb"),
    string = comb.string,
    format = string.format,
    hitch = comb.hitch,
    when = comb.when,
    serial = comb.serial,
    array = comb.array,
    toArray = array.toArray,
    zip = array.zip,
    flatten = array.flatten,
    Promise = comb.Promise,
    QueryError = require("../errors").QueryError,
    Dataset = require("../dataset"),
    Database = require("../database"),
    sql = require("../sql").sql,
    DateTime = sql.DateTime,
    Time = sql.Time,
    Year = sql.Year,
    Double = sql.Double,
    patio;

var isBlank = function (obj) {
    var ret = false;
    if (comb.isUndefinedOrNull(obj)) {
        ret = true;
    } else if (comb.isString(obj) || comb.isArray(obj)) {
        ret = obj.length === 0;
    } else if (comb.isBoolean(obj) && !obj) {
        ret = true;
    } else if (comb.isObject(obj) && comb.isEmpty(obj)) {
        ret = true;
    }
    return ret;
};

var Connection = comb.define(null, {
    instance:{

        connection:null,


        constructor:function (conn) {
            this.connection = conn;
        },

        closeConnection:function () {
            this.connection.end();
            return new Promise().callback();
        },

        query:function (query) {
            var ret = new Promise();
            try {
                this.connection.setMaxListeners(0);
                this.connection.query(query, hitch(this, function (err, results) {
                    if (err) {
                        return ret.errback(err);
                    } else {
                        return ret.callback(results, info);
                    }
                }));
            } catch (e) {
                patio.logError(e);
            }
            return ret;
        }
    }
});

var DS = comb.define(Dataset, {
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
            return this.filter("to_tsvector(?, ?) @@ to_tsquery(?, ?)", lang, this.__fullTextStringJoin(cols), lang, terms)
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
            if (comb.isFunction(opts)) {
                cb = opts;
                opts = null;
            } else {
                opts = opts || {};
            }
            if (comb.isFunction(cb)) {
                return this.db.transaction(opts, function () {
                    return serial([
                        this.lock.bind(this, mode, opts),
                        cb.bind(this)
                    ]);

                }.bind(this));
            } else {
                return this.db.execute(format(this._static.LOCK, [this._sourceList(this._opts.from), mode]), opts);
            }
        },

        multiInsertSql:function (columns, values) {
            var sql = sql.literal('VALUES ');
            sql += this.__expressionList(values.map(function (r) {
                return toArray(r)
            }));
            return [this.insertSql(columns, sql)];
        },


        _deleteFromSql:function () {
            return this._static.FROM += this._sourceList(this._opts.from[0]);
        },


        _deleteUsingSql:function () {
            return this._joinFromSql("USING");
        },

        _joinFromSql:function (type) {
            var from = this._opts.from, join = this._opts.join;
            if (from.slice(1).length = 0 && !comb.isEmpty(join)) {
                throw new QueryError("Need multiple FROM tables if updating/deleteing a dataset with joins");
            } else {
                var space = this._static.SPACE;
                return [space, type.toString(), space, this._sourceList(from), this._selectJoinSql()].join("");
            }
        },

        __fullTextStringJoin:function (cols) {
            var EMPTY_STRING = this._static.EMPTY_STRING;
            cols = toArray(cols).map(function (x) {
                return  sql.COALESCE(x, EMPTY_STRING);
            });
            cols = flatten(zip(cols, array.multiply([this._static.SPACE], cols.length)));
            cols.pop();
            return new sql.StringExpression.fromArgs(['||'].concat(cols));
        },

        getters:{
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
        DELETE_CLAUSE_METHODS:Dataset.clauseMethods("delete", ['with delete from using where returning']),
        EXCLUSIVE:'EXCLUSIVE',
        EXPLAIN:'EXPLAIN ',
        EXPLAIN_ANALYZE:'EXPLAIN ANALYZE ',
        FOR_SHARE:' FOR SHARE',
        INSERT_CLAUSE_METHODS_91:Dataset.clauseMethods("insert", 'with insert into columns values returning'),
        LOCK:'LOCK TABLE %s IN %s MODE',
        NULL:sql.literal('NULL'),
        PG_TIMESTAMP_FORMAT:"TIMESTAMP 'yyyy-m-d hh:MM:ss",
        QUERY_PLAN:'QUERY PLAN',
        ROW_EXCLUSIVE:'ROW EXCLUSIVE',
        ROW_SHARE:'ROW SHARE',
        SELECT_CLAUSE_METHODS:Dataset.clauseMethods("select", 'select distinct columns from join where group having compounds order limit lock'),
        SELECT_CLAUSE_METHODS_84:Dataset.clauseMethods("select", 'with select distinct columns from join where group having window compounds order limit lock'),
        SHARE:'SHARE',
        SHARE_ROW_EXCLUSIVE:'SHARE ROW EXCLUSIVE',
        SHARE_UPDATE_EXCLUSIVE:'SHARE UPDATE EXCLUSIVE',
        SQL_WITH_RECURSIVE:"WITH RECURSIVE ",
        UPDATE_CLAUSE_METHODS:Dataset.clauseMethods("update", 'with update table set from where returning'),
        XOR_OP:' # ',
        CRLF:"\r\n",
        BLOB_RE:/[\000-\037\047\134\177-\377]/,
        WINDOW:" WINDOW ",
        EMPTY_STRING:''


    }
});

var DB = comb.define(Database, {
    instance:{

        EXCLUDE_SCHEMAS:/pg_*|information_schema/i,
        PREsPARED_ARG_PLACEHOLDER:new sql.LiteralString('$'),
        RE_CURRVAL_ERROR:/currval of sequence "(.*)" is not yet defined in this session|relation "(.*)" does not exist/,
        SYSTEM_TABLE_REGEXP:/^pg|sql/,

        type:"postgres",

        // Use the pg_* system tables to determine indexes on a table
        indexes:function (table, opts) {
            opts = opts || {};
            var m = this.outputIdentifierFunc;
            var im = this.inputIdentifierFunc;
            var parts = this.__schemaAndTable(table), schema = parts[0], table = parts[1];
            var ret = new Promise();
            when(this.serverVersion()).then(function (version) {
                var attNums;
                if (version >= 80100) {
                    attNums = sql.ANY("ind__indkey");
                } else {
                    attNums = [];
                    for (var i = 0; i < 32; i++) {
                        attNums.push(sql.Subscript("ind__indkey", [i]));
                    }
                }
                var orderRange = [];
                for (var j = 0; j < 32; j++) {
                    orderRange.push(new sql.Subscript("ind__indkey", [j]));
                }
                orderRange = sql["case"](orderRange, 32, "att__attnum");
                var ds = this.metadataDataset.from("pg_class___tab")
                    .join("pg_index___ind", [
                    [sql.indrelid, sql.oid],
                    [im(table), "relname"]
                ])
                    .join("pg_class___indc", [
                    [sql.oid, sql.indexrelid]
                ])
                    .join("pg_attribute___att", [
                    [sql.attrelid, sql.tab__oid],
                    [sql.attnum, attNums]
                ])
                    .filter({indc__relkind:'i', ind__indisprimary:false, indexprs:null, indpred:null})
                    .order("indc__relname", orderRange)
                    .select("indc__relname___name", "ind__indisunique___unique", "att__attname___column");

                schema && (ds = ds.join("pg_namespace___nsp", {oid:sql.tab__relnamespace, nspname:schema.toString()}));
                version >= 80200 && (ds = ds.filter({indisvalid:true}));
                version >= 80300 && (ds = ds.filter({indisready:true, indcheckxmin:false}));
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

            return ret;
        },

        locks:function () {
            this.dataset.from("pg_class").join("pg_locks", {relation:sql.relfilenode}).select("pg_class__relname", sql.pg_locks.all());
        },

        // Get version of postgres server, used for determined capabilities.
        serverVersion:function () {
            var ret = new comb.Promise();
            if (!this.__serverVersion) {
                this.get(sql.version().sqlFunction).then(hitch(this, function (version) {
                    var m = version.match(/PostgreSQL (\d+)\.(\d+)(?:(?:rc\d+)|\.(\d+))?/);
                    this._serverVersion = (parseInt(m[1], 10) * 10000) + (parseInt(m[2], 10) * 100) + parseInt(m[3], 10);
                    ret.callback(this._serverVersion);
                }), ret);
            } else {
                ret.callback(this._serverVersion);
            }
            return ret;
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

        schemaParseTable:function (tableName, opts) {
            var m = this.outputIdentifierFunc,
                m2 = this.inputIdentifierFunc;
            var ds = this.metadataDataset
                .select(
                "pg_attribute__attname___name",
                sql.format_type("pg_type__oid", "pg_attribute__atttypmod").as("dbType"),
                sql.pg_get_expr("pg_attrdef__adbin", "pg_class__oid").as("default"),
                sql.NOT("pg_attribute__attnotnull").as("allowNull"),
                sql.COALESCE(sql.BooleanExpression.fromValuePairs({pg_attribute__attnum:sql.ANY("pg_index__indkey")}), false).as("primaryKey"),
                "pg_namespace__nspname"
            ).from("pg_class")
                .join("pg_attribute", {attrelid:sql.oid})
                .join("pg_type", {oid:sql.atttypid})
                .join("pg_namespace", {oid:sql.pg_class__relnamespace})
                .leftOuterJoin("pg_attrdef", {adrelid:sql.pg_class__oid, adnum:sql.pg_attribute__attnum})
                .leftOuterJoin("pg_index", {indrelid:sql.pg_class__oid, indisprimary:true})
                .filter({pg_attribute__attisdropped:false})
                .filter({pg_attribute__attnum:{gt:0}})
                .filter({pg_class__relname:m2(tableName)})
                .order("pg_attribute__attnum");
            ds = this.__filterSchema(ds, opts);
            var currentSchema = null;
            var ret = new Promise();
            return ds.map(function (row) {
                var sch = row.nspname;
                delete row.nspname;
                if (currentSchema) {
                    if (sch !== currentSchema) {
                        var error = new Error("columns from two tables were returned please specify a schema");
                        this.logError(error);
                        ret.errback(error);
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

            });

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
                .join("pg_namespace", {oid:sql.relnamespace});
            ds = this.__filterSchema(ds, opts);
            var m = this.outputIdentifierFunc;
            if (cb) {
                when(cb(ds)).then(ret);
            } else {
                ds.map(function (r) {
                    return m(r.relname);
                }).then(ret);
            }
            return ret;
        },

        //If opts includes a :schema option, or a default schema is used, restrict the dataset to
        // that schema.  Otherwise, just exclude the default PostgreSQL schemas except for public.
        __filterSchema:function (ds, opts) {
            opts = opts || {};
            var shema = opts.schema, ret = ds;
            if (schema) {
                ds = ds.filter({pg_namespace__nspname:schema});
            } else {
                ds = ds.exclude({pg_namespace__nspname:this.EXCLUDE_SCHEMAS});
            }
            return ds;
        },

        __indexDefinitionSql:function (tableName, index) {
            var cols = index.columns,
                indexName = index.name || this.__defaultIndexName(tableName, cols),
                o = index.opclass,
                indexType = index.type,
                unique = index.unique ? "UNIQUE" : "",
                filter = index.where || index.filter,
                expr;
            filter = filter ? ["WHERE ", this.__filterExpr(filter)].join("") : "";
            if (comb.isDefined(o)) {
                expr = ["(", cols.map(this.literal.bind(this)).join(", "), ")"].join("");
            } else {
                expr = this.literal(toArray(cols));
            }
            switch (indexType) {
                case "fullText":
                    expr = ["(to_tsvector(", this.literal(index.language || "simple"), ", ", this.literal(this.dataset.fullTextStringJoin(cols)), "))"].join("");
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
            return ["ALTER TABLE ", this.__quoteSchemaTable(name), " RENAME TO ", this.__quoteIdentifier(this.__schemaAndTable(newName).pop())].join("")
        },

        __schemaAutoincrementingPrimaryKey:function (schema) {
            return this._super(arguments) && schema.dbType.match(/^(?:integer|bigint)$/i) && schema["default"].match(/^nextval/i);
        },

        //handle bigserial
        __typeLiteralGenericBignum:function (column) {
            return column.serial ? "bigserial" : this._super(arguments);
        },

        __typeLiteralGenericFile:function (column) {
            return "bytea";
        },

        //handle serial type
        __typeLiterGenericInteger:function (column) {
            return column.serial ? "serial" : this._super(arguments);
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
}).as(exports, "PostgresDatabase");
