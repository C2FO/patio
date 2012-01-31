var patio = require("../../index"),
    data = require("./data"),
    helper = require("./schemas"),
    comb = require("comb");


exports.loadData = function () {
    patio.camelize = true;
    return comb.executeInOrder(helper, patio,
        function (helper, patio) {
            helper.createTables();
            patio.import(__dirname + "/models");
            var Airport = patio.getModel("airport"), AirplaneType = patio.getModel("airplaneType"), Flight = patio.getModel("flight");
            Airport.save(data.airports);
            AirplaneType.save(data.airplaneTypes);
            Flight.save(data.flights);
        });
};

exports.dropModels = function () {
    return helper.dropTableAndDisconnect();
};