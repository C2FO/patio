var patio = require("../../index"),
    data = require("./data"),
    helper = require("./schemas"),
    comb = require("comb"),
    models = require("./models");
exports.loadData = function () {
    patio.camelize = true;
    var ret = new comb.Promise();
    helper.createTables().then(function () {
        //sync our models
        return comb.when(
            models.Airport.save(data.airports),
            models.AirplaneType.save(data.airplaneTypes),
            models.Flight.save(data.flights)
        ).then(ret);
    }, comb.hitch(ret, "errback"));
    return ret;
};

exports.dropModels = function () {
    return helper.dropTableAndDisconnect();
};