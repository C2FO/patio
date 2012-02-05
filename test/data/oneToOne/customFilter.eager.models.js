var patio = require("index"),
    helper = require("./helper"),
    comb = require("comb");

exports.loadModels = function () {
    var ret = new comb.Promise();
    return comb.executeInOrder(helper, patio, function (helper, patio) {
        var DB = helper.createTables();
        var Works = patio.addModel(DB.from("works"), {
            static:{
                init:function () {
                    this._super(arguments);
                    this.manyToOne("employee", {fetchType:this.fetchType.EAGER});
                }
            }
        });
        var Employee = patio.addModel(DB.from("employee"), {
            static:{
                init:function () {
                    this._super(arguments);
                    this.oneToOne("works", {fetchType:this.fetchType.EAGER}, function (ds) {
                        return ds.filter(function () {
                            return this.salary.gte(100000.00);
                        });
                    });
                }
            }
        });
    });
};


exports.dropModels = function () {
    return helper.dropTableAndDisconnect();
};