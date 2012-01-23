var patio = require("../../../lib"),
    helper = require("./helper"),
    comb = require("comb");

exports.loadModels = function () {
    var ret = new comb.Promise();
    return comb.executeInOrder(helper, patio, function (helper, patio) {
        helper.createTables(true);
        var Employee = patio.addModel("employee", {
            plugins:[patio.plugins.TimeStampPlugin]
        });
        Employee.timestamp({updated : "updatedAt", created : "createdAt"});
    });
};


exports.dropModels = function () {
    return helper.dropTableAndDisconnect();
};
