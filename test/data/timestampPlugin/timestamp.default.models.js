var patio = require("../../../lib"),
    helper = require("./helper"),
    comb = require("comb");

exports.loadModels = function () {
    var ret = new comb.Promise();
    return comb.executeInOrder(helper, patio, function (helper, patio) {
        helper.createTables();
        var Employee = patio.addModel("employee", {
            plugins:[patio.plugins.TimeStampPlugin]
        });
        Employee.timestamp();
    });
};


exports.dropModels = function () {
    return helper.dropTableAndDisconnect();
};
