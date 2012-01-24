var patio = require("index"),
    helper = require("./helper"),
    comb = require("comb");

exports.loadModels = function () {
    var ret = new comb.Promise();
    return comb.executeInOrder(helper, patio, function (helper, patio) {
        var DB = helper.createTables();
        var Company = patio.addModel(DB.from("company"), {
            static:{
                init:function () {
                    this.oneToMany("employees");
                }
            }
        });
        var Employee = patio.addModel(DB.from("employee"), {
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
