var patio = require("index"),
    helper = require("./helper"),
    comb = require("comb");

exports.loadModels = function () {
    var ret = new comb.Promise();
    return comb.executeInOrder(helper, patio, function (helper, patio) {
        var DB = helper.createTables(true);
        var Works = patio.addModel(DB.from("works"), {
            static:{

                camelize:true,

                init:function () {
                    this._super(arguments);
                    this.manyToOne("employee");
                }
            }
        });
        var Employee = patio.addModel(DB.from("employee"), {
            static:{

                camelize:true,

                init:function () {
                    this._super(arguments);
                    this.oneToOne("works");
                }
            }
        });
        patio.syncModels();
    });
};


exports.dropModels = function () {
    return helper.dropTableAndDisconnect();
};
