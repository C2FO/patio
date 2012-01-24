var patio = require("index"),
    helper = require("./helper"),
    comb = require("comb");

exports.loadModels = function () {
    var ret = new comb.Promise();
    return comb.executeInOrder(helper, patio, function (helper, patio) {
        var DB = helper.createTables(true);
        var Employee = patio.addModel(DB.from("employee"), {
            plugins:[patio.plugins.TimeStampPlugin]
        });
        Employee.timestamp({updated : "updatedAt", created : "createdAt"});
    });
};


exports.dropModels = function () {
    return helper.dropTableAndDisconnect();
};
