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
                    this.oneToMany("employees", {key:{id:"companyId"}});
                }
            }
        });
        var Employee = patio.addModel("employee", {
            static:{
                init:function () {
                    this.manyToOne("company", {key:{companyId:"id"}});
                }
            }
        });

    });
};


exports.dropModels = function () {
    return helper.dropTableAndDisconnect();
};
