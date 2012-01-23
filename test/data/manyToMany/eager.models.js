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
                    this.manyToMany("employees", {fetchType:this.fetchType.EAGER});
                }
            }
        });
        var Employee = patio.addModel("employee", {
            static:{
                init:function () {
                    this.manyToMany("companies", {fetchType:this.fetchType.EAGER});
                }
            }
        });

    });
};


exports.dropModels = function () {
    return helper.dropTableAndDisconnect();
};
