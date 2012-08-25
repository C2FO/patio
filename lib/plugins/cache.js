var comb = require("comb"),
    hitch = comb.hitch,
    Promise = comb.Promise,
    PromiseList = comb.PromiseList,
    Hive = require("hive-cache");


var hive;
var i = 0;

var LOGGER = comb.logging.Logger.getLogger("patio.plugins.CachePlugin");
/**
 * @class Adds in memory caching support for models.
 *
 * @example
 *
 * var MyModel = patio.addModel("testTable", {
 *     plugins : [patio.plugins.CachePlugin];
 * });
 *
 * //NOW IT WILL CACHE
 *
 * @name CachePlugin
 * @memberOf patio.plugins
 */
exports.CachePlugin = comb.define(null, {
    instance:{

        constructor:function () {
            this._super(arguments);
            this.post("load", this._postLoad);
            this._static.initHive();
        },

        reload:function () {
            var ret = new Promise();
            this._super(arguments).then(hitch(this, function (m) {
                hive.replace((this.tableName.toString() + this.primaryKeyValue.toString()), m);
                ret.callback(m);
            }), ret);
            return ret.promise();
        },

        _postLoad:function (next) {
            hive.replace(this.tableName + this.primaryKeyValue, this);
            next();
        },

        update:function (options, errback) {
            var ret = new Promise();
            this._super(arguments).then(hitch(this, function (val) {
                hive.remove(this.tableName + this.primaryKeyValue, val);
                ret.callback(val);
            }), ret);
            return ret.promise();
        },

        remove:function (errback) {
            hive.remove(this.tableName + this.primaryKeyValue);
            return this._super(arguments);
        }
    },

    static:{
        initHive:function () {
            if (!hive) {
                hive = new Hive();
            }
            this.cache = hive;
        },

        findById:function (id) {
            var cached = hive.get(this.tableName + id);
            if (!cached) {
                return this._super(arguments);
            } else {
                var ret = new Promise();
                ret.callback(cached);
                return ret;
            }
        }
    }
});
