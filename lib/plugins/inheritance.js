var comb = require("comb");


comb.define(null, {
    instance:{},
    static:{

        configure:function (model) {

        }
    }
}).as(exports, "SingleTableInheritance");


comb.define(null, {

    instance:{

        // Delete the row from all backing tables, starting from the
        // most recent table and going through all superclasses.
        _remove:function () {
            var q = this._getPrimaryKeyQuery();
            return new comb.PromiseList(this._static.__ctiTables.reverse().map(function (table) {
                return this.db.from(table).filter(q).remove();
            }, this), true);
        },

        // Save each column according to the columns in each table
        _save:function () {
            var q = this._getPrimaryKeyQuery();
            var ret = new comb.Promise();
            new comb.PromiseList(this._static.__ctiTables.reverse().map(function (table) {
                var cols = this.__ctiColumns[table], insert = {};
                cols.forEach(function (c) {
                    insert[c] = this[c];
                }, this);
                return this.db.from(table).filter(q).insert(insert);
            }, this), true)
                .chain(comb.hitch(this, "_saveReload"), comb.hitch(ret, "errback"))
                .then(comb.hitch(ret, "callback"), comb.hitch(ret, "errback"));
            return ret;
        },

        // Save each column according to the columns in each table
        _update:function () {
            var q = this._getPrimaryKeyQuery(), changed = this.__changed;
            var ret = new comb.Promise();
            new comb.PromiseList(this._static.__ctiTables.reverse().map(function (table) {
                var cols = this.__ctiColumns[table], update = {};
                cols.forEach(function (c) {
                    if(!comb.isUndefined(changed[c])){
                         update[c] = changed[c];
                    }
                }, this);
                return comb.isEmpty(update) ? new comb.Promise().callback() : this.db.from(table).filter(q).update(update);
            }, this), true)
                .chain(comb.hitch(this, "_updateReload"), comb.hitch(ret, "errback"))
                .then(comb.hitch(ret, "callback"), comb.hitch(ret, "errback"));
            return ret;
        },


    },

    static:{

        configure:function (opts) {
            this.__ctiBaseModel = this;
            var key = this.__ctiKey = opts.key;
            this.__ctiModels = [this];
            this.__ctiTables = [this.tableName];
            var cols = this.__ctiColumns = {};
            cols[this.tableName] = this.columns;
            this.__ctiTableMap = opts.tableMap || {};
            this.dataset.rowCb = comb.hitch(this, function (r) {
                if (key) {
                    var model = patio.getModel(r[key]);
                    return (model || this).load(r);
                } else {
                    return this.load(r);
                }
            });
        },

        inherits:function (model) {
            var ctiKey = this.__ctiKey = model.__ctiKey;
            this.__ctiTables = model.__ctiTables.slice();
            this.__ctiModels = model.__ctiModels.slice();
            this.__ctiModels.push(this);
            this.__ctiTables.push(this.tableName);
            this.__ctiColumns = comb.merge({}, model.__ctiTableMap);
            this.__ctiColumns[this.tableName] = this.columns;
            this.__ctiBaseModel = model.__ctiBaseModel;
            this._setDataset(this.dataset.join(model.tableName, model.primaryKey));
            this._setColumns(comb.array.union(this.columns, model.columns));
            var schemas = model.__ctiModels.map(
                function (m) {
                    return m.schema;
                }).reverse();
            schemas.forEach(function (s) {
                for (var i in s) {
                    this.__schema[i] = s[i];
                }
            }, this);
            this.__primaryKey = this.__ctiBaseModel.primaryKey;
            this.__tableNme = this.__ctiTables[this.__ctiTables.length - 1];
            this.pre("save", function (next) {
                if (ctiKey) {
                    this[ctiKey] = model.tableName.toString();
                }
                next();
            });
            this.mixin(model);
        }
    }
}).as(exports, "ClassTableInheritance");