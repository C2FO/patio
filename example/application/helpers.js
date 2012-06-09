var patio = require("../../index"),
    data = require("./data"),
    helper = require("./schemas"),
    comb = require("comb");
exports.loadData = function () {
    models = require("./models");
    patio.camelize = true;
    var ret = new comb.Promise();
    helper.createTables().then(function () {
        //sync our models
        patio.syncModels().chain(function () {
            return comb.when(
                models.Airport.save(data.airports),
                models.AirplaneType.save(data.airplaneTypes),
                models.Flight.save(data.flights)
            );
        }, comb.hitch(ret, "errback"))
            .then(comb.hitch(ret, "callback"), comb.hitch(ret, "errback"));
    }, comb.hitch(ret, "errback"));
    return ret;
};

exports.dropModels = function () {
    return helper.dropTableAndDisconnect();
};