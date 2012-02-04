var patio = require("../../index"),
    data = require("./data"),
    helper = require("./schemas"),
    comb = require("comb");


exports.loadData = function () {
    patio.camelize = true;
    var ret = new comb.Promise();
    helper.createTables().then(function () {
        patio.import(__dirname + "/models").chain(function () {
            var Airport = patio.getModel("airport"), AirplaneType = patio.getModel("airplaneType"), Flight = patio.getModel("flight");
            return comb.when(
                Airport.save(data.airports),
                AirplaneType.save(data.airplaneTypes),
                Flight.save(data.flights)
            );
        }, comb.hitch(ret, "errback"))
            .then(comb.hitch(ret, "callback"), comb.hitch(ret, "errback"));
    }, comb.hitch(ret, "errback"));
    return ret;
};

exports.dropModels = function () {
    return helper.dropTableAndDisconnect();
};