var pg = require("pg"),
    PgTypes = require("pg-types"),
    QueryStream = require('pg-query-stream'),
    comb = require("comb"),
    asyncArray = comb.async.array,
    string = comb.string,
    isHash = comb.isHash,
    argsToArray = comb.argsToArray,
    pad = string.pad,
    format = string.format,
    when = comb.when,
    array = comb.array,
    toArray = array.toArray,
    zip = array.zip,
    flatten = array.flatten,
    Promise = comb.Promise,
    isUndefinedOrNull = comb.isUndefinedOrNull,
    isUndefined = comb.isUndefined,
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
    identifier = sql.identifier,
    BooleanExpression = sql.BooleanExpression,
    LiteralString = sql.LiteralString,
    Subscript = sql.Subscript,
    patio, DS,
    stream = require("stream"),
    PassThroughStream = stream.PassThrough,
    pipeAll = require("../utils").pipeAll,
    hashPick = comb.hash.pick,
    isSafeInteger = Number.isSafeInteger || require('is-safe-integer');

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
        val = val.toString().replace(/\\([0-7]{3})/g, function (fullMatch, code) {
            return String.fromCharCode(parseInt(code, 8));
        }).replace(/\\\\/g, "\\");
        return new Buffer(val, "binary");
    }
};

PgTypes.setTypeParser(17, "text", byteaParser);

PgTypes.setTypeParser(20, 'text', function (val) {
    if (!getPatio().parseInt8) {
        return val;
    }

    var i = parseInt(val, 10);
    if (!isSafeInteger(i)) {
        throw new Error(format("The value '%s' cannot be represented by a javascript number.", val));
    }
    return i;
});

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
    val = String(val);
    if (!val.match(/\.(\d{0,3})/)) {
        val += ".000";
    } else {
        val = val.replace(/\.(\d{0,3})$/, function (m, m1) {
            return "." + pad(m1, 3, "0", true);
        });
    }
    return getPatio().stringToTime(val.toString(), DS.TIME_FORMAT);
});

PgTypes.setTypeParser(1700, "text", parseFloat);

PgTypes.setTypeParser(114, "text", function (data) {
    return getPatio().sql.json(JSON.parse(data));
});


var Connection = define(null, {
    instance: {

        connection: null,

        errored: false,

        closed: false,


        constructor: function (conn) {
            this.connection = conn;
        },

        closeConnection: function () {
            this.closed = true;
            this.connection.end();
            return new Promise().callback().promise();
        },

        stream: function (query, opts) {
            var ret;
            if (!this.closed) {
                try {
                    opts = hashPick(opts || {}, ["batchSize", "highWaterMark"]);
                    this.connection.setMaxListeners(0);
                    var fields = [];
                    query = new QueryStream(query, null, opts);
                    ret = this.connection.query(query);
                    var orig = ret.handleRowDescription;
                    ret.handleRowDescription = function (msg) {
                        ret.emit("fields", msg.fields);
                        ret.handleRowDescription = orig;
                        return orig.apply(ret, arguments);
                    };
                } catch (e) {
                    ret = new PassThroughStream();
                    setImmediate(function () {
                        ret.emit("error", e);
                    });
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
                    var fields = [], rows = [];
                    var q = this.connection.query(query)
                        .on("error", ret.errback)
                        .on("row", function(row){
                            rows.push(row);
                        })
                        .on("end", function(){
                        ret.callback(rows, fields);
                    });
                    var orig = q.handleRowDescription;
                    q.handleRowDescription = function (msg) {
                        fields = msg.fields;
                        q.handleRowDescription = orig;
                        return orig.apply(q, arguments);
                    };
                } catch (e) {
                    ret.errback(e);
                }
            } else {
                ret.errback(new Error("Connection already closed"));
            }
            return ret.promise();
        }
    }
});

function colCallback(o) {
    return o;
}

DS = define(Dataset, {
    instance: {

        complexExpressionSql: function (op, args) {
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

        forShare: function () {
            return this.lockStyle("share");
        },

        fullTextSearch: function (cols, terms, opts) {
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
        lock: function (mode, opts, cb) {
            if (isFunction(opts)) {
                cb = opts;
                opts = null;
            } else {
                opts = opts || {};
            }
            if (isFunction(cb)) {
                var self = this;
                return this.db.transaction(opts, function () {
                    return self.lock(mode, opts)
                        .chain(function () {
                            return cb.call(self);
                        });
                });
            } else {
                return this.db.execute(format(this._static.LOCK, [this._sourceList(this.__opts.from), mode]), opts);
            }
        },

        multiInsertSql: function (columns, values) {
            var ret = literal('VALUES ');
            ret += this.__expressionList(values.map(function (r) {
                return toArray(r);
            }));
            return [this.insertSql(columns.map(function (c) {
                return stringToIdentifier(c);
            }), literal(ret))];
        },

        _literalString: function (v) {
            return "'" + v.replace(/'/g, "''") + "'";
        },

        _literalJson: function (v) {
            return "'" + JSON.stringify(v).replace(/'/g, "''") + "'";
        },

        _deleteFromSql: function () {
            var self = this._static, space = self.SPACE;
            return [space, self.FROM, space, this._sourceList(this.__opts.from[0])].join("");
        },


        _deleteUsingSql: function () {
            return this._joinFromSql("USING");
        },

        _joinFromSql: function (type) {
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

        _selectLockSql: function () {
            if (this.__opts.lock === "share") {
                return this._static.FOR_SHARE;
            } else {
                return this._super(arguments);
            }
        },

        _selectWithSql: function () {
            var optsWith = this.__opts["with"];
            if (!isEmpty(optsWith) && optsWith.some(function (w) {
                    return w.recursive;
                })) {
                return this._static.SQL_WITH_RECURSIVE;
            } else {
                return this._super(arguments);
            }
        },

        _updateFromSql: function () {
            return this._joinFromSql("FROM");
        },

        _updateTableSql: function () {
            return [this._static.SPACE, this._sourceList(this.__opts.from.slice(0, 1))].join("");
        },

        _quotedIdentifier: function (c) {
            return format('"%s"', c);
        },

        __fullTextStringJoin: function (cols) {
            var EMPTY_STRING = this._static.EMPTY_STRING;
            cols = toArray(cols).map(function (x) {
                return sql.COALESCE(x, EMPTY_STRING);
            });
            cols = flatten(zip(cols, array.multiply([this._static.SPACE], cols.length)));
            cols.pop();
            return StringExpression.fromArgs(['||'].concat(cols));
        },

        insert: function () {
            var args = arguments;
            if (this.__opts.returning) {
                return this._super(arguments);
            } else {
                var self = this;
                return this.primaryKey(this.__opts.from).chain(function (res) {
                    var pks = res.map(function (r) {
                        return r.name;
                    });
                    var ds = self.returning.apply(self, pks);
                    var dsPromise = ds.insert.apply(ds, args), l = res.length;
                    if (l) {
                        return dsPromise.chain(function (insertRes) {
                            if (l === 1) {
                                return insertRes.map(function (i) {
                                    return i[pks[0]];
                                }).pop();
                            } else {
                                return insertRes.pop();
                            }
                        });
                    } else {
                        return dsPromise;
                    }
                });
            }
        },

        primaryKey: function () {
            return this.db.primaryKey(this.__opts.from[0]);
        },

        __processFields: function (fields) {
            var col, colOutputIdentifier, i = -1, l, cols = [],
                outputIdentifier = this.outputIdentifier,
                selfCols = ( this.__columns = []);
            if (fields && fields.length) {
                l = fields.length;
                while (++i < l) {
                    colOutputIdentifier = outputIdentifier(col = fields[i].name);
                    selfCols[i] = colOutputIdentifier;
                    cols[i] = [colOutputIdentifier, colCallback, col];
                }
            }
            return cols;
        },

        _literalTimestamp: function (v) {
            return this.literal(literal("TIMESTAMP " + this._super(arguments) + ""));
        },

        _literalBuffer: function (b) {
            return this.literal(literal("decode('" + b.toString("hex") + "', 'hex')"));
        },

        getters: {

            columns: function () {
                var ret;
                if (this.__columns) {
                    ret = when(this.__columns);
                } else {
                    var self = this;
                    ret = this.db.schema(this.firstSourceTable).chain(function (schema) {
                        var columns = (schema ? Object.keys(schema) : []);
                        self.__columns = columns;
                        return columns;
                    });
                }
                return ret.promise();
            },

            supportsCteInSubqueries: function () {
                return true;
            },

            supportsDistinctOn: function () {
                return true;
            },

            supportsModifyingJoins: function () {
                return true;
            },

            supportsTimestampTimezones: function () {
                return true;
            }
        }


    },
    "static": {
        ACCESS_SHARE: 'ACCESS SHARE',
        ACCESS_EXCLUSIVE: 'ACCESS EXCLUSIVE',
        BOOL_FALSE: 'false',
        BOOL_TRUE: 'true',
        COMMA_SEPARATOR: ', ',
        DELETE_CLAUSE_METHODS: Dataset.clauseMethods("delete", 'qualify with from using where returning'),
        EXCLUSIVE: 'EXCLUSIVE',
        EXPLAIN: 'EXPLAIN ',
        EXPLAIN_ANALYZE: 'EXPLAIN ANALYZE ',
        FOR_SHARE: ' FOR SHARE',
        INSERT_CLAUSE_METHODS: Dataset.clauseMethods("insert", 'with into columns values returning'),
        LOCK: 'LOCK TABLE %s IN %s MODE',
        NULL: literal('NULL'),
        QUERY_PLAN: 'QUERY PLAN',
        ROW_EXCLUSIVE: 'ROW EXCLUSIVE',
        ROW_SHARE: 'ROW SHARE',
        SELECT_CLAUSE_METHODS: Dataset.clauseMethods("select", '' +
        'qualify with distinct columns from join where group having compounds order limit lock'),
        SHARE: 'SHARE',
        SHARE_ROW_EXCLUSIVE: 'SHARE ROW EXCLUSIVE',
        SHARE_UPDATE_EXCLUSIVE: 'SHARE UPDATE EXCLUSIVE',
        SQL_WITH_RECURSIVE: "WITH RECURSIVE ",
        TIMESTAMP_FORMAT: "yyyy-MM-dd HH:mm:ss.SSS",
        TIME_FORMAT: "HH:mm:ss.SSS",
        UPDATE_CLAUSE_METHODS: Dataset.clauseMethods("update", 'with table set from where returning'),
        XOR_OP: ' # ',
        CRLF: "\r\n",
        BLOB_RE: /[\000-\037\047\134\177-\377]/,
        WINDOW: " WINDOW ",
        EMPTY_STRING: literal("''")


    }
}).as(exports, "PostgresDataset");

var DB = define(Database, {
    instance: {

        EXCLUDE_SCHEMAS: /pg_*|information_schema/i,
        PREPARED_ARG_PLACEHOLDER: new LiteralString('$'),
        RE_CURRVAL_ERROR: /currval of sequence "(.*)" is not yet defined in this session|relation "(.*)" does not exist/,
        SYSTEM_TABLE_REGEXP: /^pg|sql/,

        type: "postgres",

        constructor: function () {
            this._super(arguments);
            this.__primaryKeys = {};
            this.__listeners = {};
        },

        createConnection: function (opts) {
            delete opts.query;
            var self = this, ret;
            var conn = new pg.Client(merge({}, opts, {typeCast: false}));

            conn.on("error", function (err) {
                self.logWarn("Connection from " + self.uri + " errored removing from pool and reconnecting");
                self.logWarn(err.stack);
                ret.errored = true;
                self.pool.removeConnection(ret);
                getPatio().emit('error', err);
            });

            conn.on("end", function () {
                if (!ret.closed) {
                    self.logWarn("Connection from " + self.uri + " unexpectedly ended");
                    self.pool.removeConnection(ret);
                    ret.closed = true;
                }
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

        listen: function (channel, cb, opts) {
            opts = opts || {};
            channel = this.__quoteSchemaTable(channel);
            var listeningChannel = channel.toLowerCase();
            var timeout = opts.timeout || 30000,
                ret,
                connected = true, errored = false;

            var self = this;
            if (this.quoteIdentifiers) {
                listeningChannel = listeningChannel.replace(/^"|"$/g, "");
            }
            var connectionTimeout = setTimeout(function () {
                if (!connected) {
                    errored = true;
                    ret.errback(new Error("Listen: Unable to connect to " + channel));
                }
            }, timeout);
            ret = this._getConnection().chain(function (conn) {
                function __listener(message) {
                    if (message.channel === listeningChannel) {
                        cb(JSON.parse(message.payload));
                    }
                }

                if (!errored) {
                    connected = true;
                    clearTimeout(connectionTimeout);

                    conn.connection.on('notification', __listener);
                    var listeners = conn.__listeners;
                    if (!listeners) {
                        listeners = conn.__listeners = {};
                    }
                    listeners[channel] = __listener;
                    var sql = self.__listenSql(channel);
                    return self.__logAndExecute(sql, function () {
                        return conn.query(sql);
                    }).chain(function () {
                        self.__listeners[channel] = conn;
                    });
                }
            });
            return ret;
        },

        listenOnce: function (channel, cb, opts) {
            var self = this;
            var ret = new Promise(), called = false;
            this.listen(channel, function (payload) {
                //ensure we are not called twice
                if (!called) {
                    called = true;
                    self.unListen(channel).chain(function () {
                        ret.callback(payload);
                        self = ret = null;
                    }).addErrback(ret);
                }
            }, opts).addErrback(ret);
            return ret.promise();
        },

        unListen: function (channel) {
            var ret = new Promise().callback(), conn;
            channel = this.__quoteSchemaTable(channel);
            if (channel in this.__listeners && (conn = this.__listeners[channel])) {
                var sql = this.__unListenSql(channel), self = this;
                return this.__logAndExecute(sql, sql, function () {
                    return conn.query(sql);
                }).chain(function () {
                    delete self.__listeners[channel];
                    conn.connection.removeListener('notification', conn.__listeners[channel]);
                    return self._returnConnection(conn);
                });
            }
            return ret.promise();
        },

        notify: function (channel, payload) {
            return this.executeDdl(this.__notifySql(this.__quoteSchemaTable(channel), payload));
        },

        // Use the pg_* system tables to determine indexes on a table
        indexes: function (table, opts) {
            opts = opts || {};
            var m = this.outputIdentifierFunc;
            var im = this.inputIdentifierFunc;
            var parts = this.__schemaAndTable(table), schema = parts[0];
            table = parts[1];
            return this.serverVersion().chain(function (version) {
                var attNums;
                if (version >= 80100) {
                    attNums = sql.ANY("ind__indkey");
                } else {
                    attNums = [];
                    for (var i = 0; i < 32; i++) {
                        attNums.push(new Subscript("ind__indkey", [i]));
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
                    .filter({"indc__relkind": 'i', "ind__indisprimary": false, indexprs: null, indpred: null})
                    .order("indc__relname", orderRange)
                    .select("indc__relname___name", "ind__indisunique___unique", "att__attname___column");

                if (schema) {
                    ds = ds.join("pg_namespace___nsp", {oid: identifier("tab__relnamespace"), nspname: schema.toString()});
                }
                if (version >= 80200) {
                    ds = ds.filter({indisvalid: true});
                }
                if (version >= 80300) {
                    ds = ds.filter({indisready: true, indcheckxmin: false});
                }
                var indexes = {};
                return ds.forEach(function (r) {
                    var ident = m(r.name), i = indexes[ident];
                    if (!i) {
                        i = indexes[ident] = {columns: [], unique: r.unique};
                    }
                    i.columns.push(r.column);
                }).chain(function () {
                    return indexes;
                });
            });
        },

        locks: function () {
            return this.dataset.from("pg_class").join("pg_locks", {relation: identifier("relfilenode")}).select("pg_class__relname", identifier("pg_locks").all());
        },

        // Get version of postgres server, used for determined capabilities.
        serverVersion: function () {
            if (!this.__serverVersion) {
                var self = this;
                this.__serverVersion = this.get(identifier("version").sqlFunction).chain(function (version) {
                    var m = version.match(/PostgreSQL (\d+)\.(\d+)(?:(?:rc\d+)|\.(\d+))?/);
                    version = (parseInt(m[1], 10) * 10000) + (parseInt(m[2], 10) * 100) + parseInt(m[3], 10);
                    self._serverVersion = version;
                    return version;
                });
            }
            return this.__serverVersion.promise();
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
        tables: function (opts, cb) {
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
        views: function (opts) {
            return this.__pgClassRelname('v', opts);
        },

        primaryKey: function (table, opts) {
            var ret, quotedTable = this.__quoteSchemaTable(table).toString(), pks = this.__primaryKeys;
            if (pks.hasOwnProperty(quotedTable.toString())) {
                ret = pks[quotedTable];
            } else {
                ret = (pks[quotedTable] = this.__primarykey(table));
            }
            return ret.promise();
        },

        createMaterializedView: function (name, query, opts) {
            opts = opts || {};
            opts.materialized = true;
            return this.createView(name, query, opts);
        },

        dropMaterializedView: function (names, opts) {
            var args = argsToArray(arguments);
            if (isHash(args[args.length - 1])) {
                opts = args.pop();
            } else {
                opts = {};
            }
            opts.materialized = true;
            return this.dropView(args, opts);
        },

        refreshMaterializedView: function (names, opts) {
            if (isArray(names)) {
                var self = this, withNoData = opts.noData;
                return asyncArray(names).forEach(function (name) {
                    var sql = "REFRESH MATERIALIZED VIEW %s";
                    withNoData && (sql += " WITH NO DATA");
                    return self.executeDdl(format(sql, self.__quoteSchemaTable(name)));
                }, null, 1);
            } else {
                var args = argsToArray(arguments);
                opts = isHash(args[args.length - 1]) ? args.pop() : {};
                return this.refreshMaterializedView(args, opts);
            }
        },

        __primarykey: function (table) {
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
                ds.filter({"pg_namespace__nspname": m2(schema)});
            }
            return ds.all();

        },

        _indKeySql: function (key, version) {
            var ret = sql.identifier(key);
            if (version < 90000) {
                ret = sql.literal("string_to_array(textin(int2vectorout(?)), ' ')", ret);
            }
            return ret;
        },


        schemaParseTable: function (tableName, opts) {
            var self = this,
                m = this.outputIdentifierFunc,
                m2 = this.inputIdentifierFunc;
            return this.serverVersion().chain(function (serverVersion) {
                var ds = self.metadataDataset
                    .select(
                    "pg_attribute__attname___name",
                    sql["format_type"]("pg_type__oid", "pg_attribute__atttypmod").as("dbtype"),
                    sql["pg_get_expr"]("pg_attrdef__adbin", "pg_class__oid").as(literal('"default"')),
                    sql.NOT("pg_attribute__attnotnull").as("allownull"),
                    sql.COALESCE(BooleanExpression.fromValuePairs({"pg_attribute__attnum": sql.ANY(self._indKeySql("pg_index__indkey", serverVersion))}), false).as("primarykey"),
                    "pg_namespace__nspname"
                ).from("pg_class")
                    .join("pg_attribute", {attrelid: identifier("oid")})
                    .join("pg_type", {oid: identifier("atttypid")})
                    .join("pg_namespace", {oid: identifier("pg_class__relnamespace")})
                    .leftOuterJoin("pg_attrdef", {adrelid: identifier("pg_class__oid"), adnum: identifier("pg_attribute__attnum")})
                    .leftOuterJoin("pg_index", {indrelid: identifier("pg_class__oid"), indisprimary: true})
                    .filter({"pg_attribute__attisdropped": false})
                    .filter({"pg_attribute__attnum": {gt: 0}})
                    .filter({"pg_class__relname": m2(tableName)})
                    .order("pg_attribute__attnum");
                ds = self.__filterSchema(ds, opts);
                var currentSchema = null;
                return ds.map(function (row) {
                    row.allowNull = row.allownull;
                    delete row.allownull;
                    row.primaryKey = row.primarykey;
                    delete row.primarykey;
                    row.dbType = row.dbtype;
                    delete row.dbtype;
                    var sch = row.nspname;
                    delete row.nspname;
                    if (currentSchema) {
                        if (sch !== currentSchema) {
                            var error = new Error("columns from two tables were returned please specify a schema");
                            self.logError(error);
                        }
                    } else {
                        currentSchema = sch;
                    }
                    if (isBlank(row["default"])) {
                        row["default"] = null;
                    }
                    row.type = self.schemaColumnType(row.dbType);
                    var fieldName = m(row.name);
                    delete row.name;
                    return [fieldName, row];
                });
            });

        },

        __commitTransaction: function (conn, opts) {
            opts = opts || {};
            var s = opts.prepare;
            if (s && this.__transactionDepth <= 1) {
                return this.__logConnectionExecute(conn, ["PREPARE TRANSACTION ", this.literal(s)].join(""));
            } else {
                return this._super(arguments);
            }
        },

        //Backbone of the tables and views support.
        __pgClassRelname: function (type, opts, cb) {
            var ret;
            var ds = this.metadataDataset.from("pg_class")
                .filter({relkind: type}).select("relname")
                .exclude({relname: {like: this.SYSTEM_TABLE_REGEXP}})
                .join("pg_namespace", {oid: identifier("relnamespace")});
            ds = this.__filterSchema(ds, opts);
            var m = this.outputIdentifierFunc;
            if (cb) {
                ret = when(cb(ds));
            } else {
                ret = ds.map(function (r) {
                    return m(r.relname);
                });
            }
            return ret.promise();
        },

        //If opts includes a :schema option, or a default schema is used, restrict the dataset to
        // that schema.  Otherwise, just exclude the default PostgreSQL schemas except for public.
        __filterSchema: function (ds, opts) {
            opts = opts || {};
            var schema = opts.schema, ret = ds;
            if (schema) {
                ds = ds.filter({"pg_namespace__nspname": schema});
            } else {
                ds = ds.exclude({"pg_namespace__nspname": this.EXCLUDE_SCHEMAS});
            }
            return ds;
        },

        __notifySql: function (channel, payload) {
            return format("NOTIFY %s %s", channel, payload ? ", " + this.literal(JSON.stringify(payload)) : "");
        },

        __listenSql: function (channel) {
            return format("LISTEN %s", channel);
        },

        __unListenSql: function (channel) {
            return format("UNLISTEN %s", channel);
        },

        __dropViewSql: function (name, opts) {
            var sql = "DROP";
            if (opts.materialized) {
                sql += " MATERIALIZED";
            }
            sql += " VIEW";
            if (opts.ifExists) {
                sql += " IF EXISTS";
            }
            sql += " %s";
            if (opts.cascade) {
                sql += " CASCADE";
            }
            return format(sql, this.__quoteSchemaTable(name));
        },

        __createViewSql: function (name, source, opts) {
            var sql = "CREATE";
            opts = opts || {};
            if (opts.replace) {
                sql += " OR REPLACE";
            }
            if (opts.materialized) {
                sql += " MATERIALIZED";
            } else if (opts.recursize) {
                sql += " RECURSIVE";
            } else if (opts.temporary || opts.temp) {
                sql += " TEMPORARY";
            }
            sql += " VIEW %s AS %s";
            return format(sql, this.__quoteSchemaTable(name), source);
        },

        __indexDefinitionSql: function (tableName, index) {
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
            return ["CREATE", unique, "INDEX", this.__quoteIdentifier(indexName), "ON", this.__quoteSchemaTable(tableName), indexType ? "USING " + indexType : "", expr, filter].join(" ");

        },
        /*
         todo might need this?
         __insertResult:function (conn, table, values) {
         },
         */

        __renameTableSql: function (name, newName) {
            return ["ALTER TABLE ", this.__quoteSchemaTable(name), " RENAME TO ", this.__quoteIdentifier(this.__schemaAndTable(newName).pop())].join("");
        },

        __schemaAutoincrementingPrimaryKey: function (schema) {
            return this._super(arguments) && schema.dbType.match(/^(?:integer|bigint)$/i) && schema["default"].match(/^nextval/i);
        },

        __typeLiteralGenericNumeric: function (column) {
            return column.size ? format("numeric(%s)", array.toArray(column.size).join(', ')) : column.isInt ? "integer" : column.isDouble ? "double precision" : "numeric";
        },

        __typeLiteralGenericDateTime: function (column) {
            return "timestamp";
        },

        //handle bigserial
        __typeLiteralGenericBigint: function (column) {
            return column.serial ? "bigserial" : this.__typeLiteralSpecific(column);
        },

        __typeLiteralGenericBlob: function (column) {
            return "bytea";
        },

        //handle serial type
        __typeLiteralGenericInteger: function (column) {
            return column.serial ? "serial" : this.__typeLiteralSpecific(column);
        },

        // PostgreSQL prefers the text datatype.  If a fixed size is requested,
        // the char type is used.  If the text type is specifically
        // disallowed or there is a size specified, use the varchar type.
        // Otherwise use the type type.
        __typeLiteralGenericString: function (column) {
            if (column.fixed) {
                return ["char(", column.size || 255, ")"].join("");
            } else if (column.text === false || column.size) {
                return ["varchar(", column.size || 255, ")"].join("");
            } else {
                return 'text';
            }

        },

        // Allow __createTableSql to be passed options for utilizing
        // Postgres' table inheritance
        __createTableSql: function (name, generator, options) {
            options = options || {};
            var inherits = options.inherits,
                inheritsStr = !isUndefined(inherits) ? " INHERITS (" + inherits + ")" : "";
            return format(" %s%s", this._super(arguments), inheritsStr);
        },


        getters: {

            connectionExecuteMethod: function () {
                return "query";
            },

            dataset: function () {
                return new DS(this);
            },

            serialPrimaryKeyOptions: function () {
                return {
                  primaryKey: true,
                  serial: true,
                  type: this.defaultPrimaryKeyType
                };
            },

            supportsSavepoints: function () {
                return true;
            },

            supportsTransactionIsolationLevels: function () {
                return true;
            },

            identifierInputMethodDefault: function () {
                return null;
            },

            identifierOutputMethodDefault: function () {
                return null;
            }
        }
    },

    "static": {

        init: function () {
            this.setAdapterType("pg");
        }

    }
}).as(exports, "PostgresDatabase");
