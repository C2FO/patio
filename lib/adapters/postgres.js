var pg = require("pg"),
    comb = require("comb"),
    string = comb.string,
    format = string.format,
    hitch = comb.hitch,
    when = comb.when,
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
    instance:{},
    "static":{}
});

var DB = comb.define(Database, {
    instance:{

        EXCLUDE_SCHEMAS:/pg_*|information_schema/i,
        PREsPARED_ARG_PLACEHOLDER:new sql.LiteralString('$'),
        RE_CURRVAL_ERROR:/currval of sequence "(.*)" is not yet defined in this session|relation "(.*)" does not exist/,
        SYSTEM_TABLE_REGEXP:/^pg|sql/,

        type:"postgres",

// Use the pg_* system tables to determine indexes on a table
indexes : function(table, opts){
    opts = opts || {};
    var m = this.outputIdentifierFunc;
    var im = this.inputIdentifierFunc;
    var parts = this.__schemaAndTable(table), schema = parts[0], table = parts[1];
    var ret = new Promise();
    when(this.serverVersion()).then(function(version){
        if(version >= 80100){

        }else{

        }
    }, ret);
    return ret;
//range = 0...32
//attnums = server_version >= 80100 ? SQL::Function.new(:ANY, :ind__indkey) : range.map{|x| SQL::Subscript.new(:ind__indkey, [x])}
//ds = metadata_dataset.
//    from(:pg_class___tab).
//join(:pg_index___ind, :indrelid=>:oid, im.call(table)=>:relname).
//join(:pg_class___indc, :oid=>:indexrelid).
//join(:pg_attribute___att, :attrelid=>:tab__oid, :attnum=>attnums).
//filter(:indc__relkind=>'i', :ind__indisprimary=>false, :indexprs=>nil, :indpred=>nil).
//order(:indc__relname, range.map{|x| [SQL::Subscript.new(:ind__indkey, [x]), x]}.case(32, :att__attnum)).
//select(:indc__relname___name, :ind__indisunique___unique, :att__attname___column)
//
//ds.join!(:pg_namespace___nsp, :oid=>:tab__relnamespace, :nspname=>schema.to_s) if schema
//    ds.filter!(:indisvalid=>true) if server_version >= 80200
//    ds.filter!(:indisready=>true, :indcheckxmin=>false) if server_version >= 80300
//
//    indexes = {}
//ds.each do |r|
//    i = indexes[m.call(r[:name])] ||= {:columns=>[], :unique=>r[:unique]}
//i[:columns] << m.call(r[:column])
//end
//indexes
},

        getters:{
            dataset:function () {
                return new DS(this);
            }
        }
    },

    "static":{

        init:function () {
            this.setAdapterType("pg");
        }

    }
}).as(exports, "PostgresDatabase");
