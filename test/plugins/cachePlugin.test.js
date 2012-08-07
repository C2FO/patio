var it = require("it"),
    assert = require("assert"),
    comb = require("comb"),
    patio = require("index"),
    mocks = require("../helpers/helper.js"),
    MockDB = mocks.MockDatabase;

it.describe("Model with cache plugin", function (it) {

    var mockDb, Model;
    it.beforeAll(function () {
        var MockDataset = comb.define(patio.Dataset, {
            instance:{
                insert:function () {
                    return this.db.execute(this.insertSql.apply(this, arguments));
                },

                update:function () {
                    return this.db.execute(this.updateSql.apply(this, arguments));
                },

                fetchRows:function (sql, cb) {
                    var ret = new comb.Promise();
                    this.db.execute(sql).then(function (res) {
                        var cbret = cb(res);
                        if (comb.isPromiseLike(cbret)) {
                            cbret.then(ret);
                        } else {
                            ret.callback(res);
                        }
                    }, ret);
                    return ret;
                },

                _quotedIdentifier:function (c) {
                    return '"' + c + '"';
                }

            }
        });
        mockDb = new (comb.define(MockDB, {
            instance:{
                schema:function () {
                    return new comb.Promise().callback({
                        id:{type:"integer", autoIncrement:true, allowNull:false, primaryKey:true, "default":null, jsDefault:null, dbType:"integer"},
                        b:{type:"boolean", autoIncrement:false, allowNull:true, primaryKey:false, "default":null, jsDefault:null, dbType:"tinyint(1)"},
                        i:{type:"integer", autoIncrement:false, allowNull:true, primaryKey:false, "default":null, jsDefault:null, dbType:"tinyint(4)"}
                    });
                },

                execute:function (sql, opts) {
                    var ret = new comb.Promise();
                    this.sqls.push(sql);
                    if (sql.match(/select/i)) {
                        ret.callback({id:1, b:true, i:1});
                    } else {
                        ret.callback();
                    }
                    return ret;
                },

                getters:{
                    dataset:function () {
                        return new MockDataset(this);
                    }
                }

            }
        }))();
        Model = patio.addModel(mockDb.from("cache"), {
            plugins:[patio.plugins.CachePlugin]
        });
        
        return patio.syncModels();
    });

    it.beforeEach(function () {
        mockDb.reset();
    });
    
    it.afterEach(function() {
        // Flush after each test to let the hive initialize during the first test
        Model.cache.flushAll();
    });

    it.should("cache on load of model", function (next) {
        Model.one().then(function (res) {
            var cachedModel = Model.cache.get(res.tableName + res.primaryKeyValue);
            assert.isNotNull(cachedModel);
            assert.strictEqual(res, cachedModel);
            assert.deepEqual(mockDb.sqls, ["SELECT * FROM cache LIMIT 1"]);
            next();
        }, next);
    });

    it.should("cache the model when using #findById", function (next) {
        Model.findById(1).then(function (res) {
            var cachedModel = Model.cache.get(res.tableName + res.primaryKeyValue);
            assert.deepEqual(mockDb.sqls, ["SELECT * FROM cache WHERE (id = 1) LIMIT 1"]);
            mockDb.reset();
            Model.findById(res.primaryKeyValue).then(function (res) {
                assert.strictEqual(res, cachedModel);
                assert.deepEqual(mockDb.sqls, []);
                next();
            }, next);
        }, next);
    });

    it.should("return cached value on when using #findById", function (next) {
        Model.one().then(function (res) {
            var cachedModel = Model.cache.get(res.tableName + res.primaryKeyValue);
            assert.deepEqual(mockDb.sqls, ["SELECT * FROM cache LIMIT 1"]);
            mockDb.reset();
            Model.findById(res.primaryKeyValue).then(function (res) {
                assert.strictEqual(res, cachedModel);
                assert.deepEqual(mockDb.sqls, []);
                next();
            }, next);
        }, next);
    });

    it.should("cache new model on reload", function (next) {
        Model.one().then(function (res) {
            var cachedModel = Model.cache.get(res.tableName + res.primaryKeyValue);
            assert.deepEqual(mockDb.sqls, ["SELECT * FROM cache LIMIT 1"]);
            mockDb.reset();
            res.reload().then(function (res) {
                assert.strictEqual(res, cachedModel);
                assert.deepEqual(mockDb.sqls, ["SELECT * FROM cache WHERE (id = 1) LIMIT 1"]);
                next();
            }, next);
        }, next);
    });

    it.should("cache new model on update", function (next) {
        Model.one().then(function (res) {
            var cachedModel = Model.cache.get(res.tableName + res.primaryKeyValue);
            assert.deepEqual(mockDb.sqls, ["SELECT * FROM cache LIMIT 1"]);
            mockDb.reset();
            res.update({b:false}).then(function (res, res2) {
                assert.strictEqual(res, cachedModel);
                assert.deepEqual(mockDb.sqls, [
                    "UPDATE cache SET b = 'f' WHERE (id = 1)",
                    "SELECT * FROM cache WHERE (id = 1) LIMIT 1"
                ]);
                next();
            }, next);
        }, next);
    });

    it.should("remove model from cache on remove", function (next) {
        Model.one().then(function (res) {
            var cachedModel = Model.cache.get(res.tableName + res.primaryKeyValue);
            assert.deepEqual(mockDb.sqls, ["SELECT * FROM cache LIMIT 1"]);
            mockDb.reset();
            res.remove().then(function (res) {
                assert.isNull(Model.cache.get(res.tableName + res.primaryKeyValue));
                assert.deepEqual(mockDb.sqls, ["DELETE FROM cache WHERE (id = 1)"]);
                next();
            }, next);
        }, next);
    });

    it.afterAll(function () {
        Model.cache.kill();
    });

});
