var comb = require("comb");


comb.define(null, {
    instance:{},
    static:{

        configure:function(model){

        }
    }
}).as(exports, "SingleTableInheritance");


comb.define(null, {

    instance:{

        // Delete the row from all backing tables, starting from the
        // most recent table and going through all superclasses.
        remove:function(){
            var q = this._getPrimaryKeyQuery();
            return new comb.PromiseList(this._static.__ctiTables.reverse().map(function(table){
                return this.db.from(table).filter(q).remove();
            }, this), true);
        }


    },

    static:{

        configure:function(opts){
            this.__ctiBaseModel = this;
            var key = this.__ctiKey = opts.key;
            this.__ctiModels = [this];
            this.__ctiTables = [this.tableName];
            var cols = this.__ctiColumns = {};
            cols[this.tableName] = this.columns;
            this.__ctiTableMap = opts.tableMap || {};
            this.dataset.rowCb = comb.hitch(this, function(r){
                if (key) {
                    var model = patio.getModel(r[key]);
                    return (model || this).load(r);
                } else {
                    return this.load(r);
                }
            });
        },

        inherits:function(model){
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
                function(m){return m.schema;}).reverse();
            schemas.forEach(function(s){
                for (var i in s) {
                    this.__schema[i] = s[i];
                }
            }, this);
            this.__primaryKey = this.__ctiBaseModel.primaryKey;
            this.__tableNme = this.__ctiTables[this.__ctiTables.length - 1];
            this.pre("save", function(next){
                if (ctiKey) {
                    this[ctiKey] = model.tableName.toString();
                }
                next();
            })
        }
    }
}).as(exports, "ClassTableInheritance");