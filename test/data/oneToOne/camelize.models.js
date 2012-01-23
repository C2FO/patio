var patio = require("index"),
    helper = require("./helper"),
    comb = require("comb");

exports.loadModels = function () {
    var ret = new comb.Promise();
    return comb.executeInOrder(helper, patio, function (helper, patio) {
        helper.createTables(true);
        var Works = patio.addModel("works", {
            static:{

                camelize:true,

                init:function () {
                    this.manyToOne("employee");
                }
            }
        });
        var Employee = patio.addModel("employee", {
            static:{

                camelize:true,

                init:function () {
                    this.oneToOne("works");
                }
            }
        });
    });
};


exports.dropModels = function () {
    return helper.dropTableAndDisconnect();
};
