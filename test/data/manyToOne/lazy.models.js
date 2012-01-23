var patio = require("index"),
    helper = require("./helper"),
    comb = require("comb");

exports.loadModels = function () {
    var ret = new comb.Promise();
    return comb.executeInOrder(helper, patio, function (helper, patio) {
        helper.createTables();
        var Company = patio.addModel("company", {
            static:{
                init:function () {
                    this.oneToMany("employees");
                }
            }
        });
        var Employee = patio.addModel("employee", {
            static:{
                init:function () {
                    this.manyToOne("company");
                }
            }
        });
    });
};


exports.dropModels = function () {
    return helper.dropTableAndDisconnect();
};
