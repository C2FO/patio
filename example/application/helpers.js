var patio = require("../../index"),
    data = require("./data"),
    helper = require("./schemas"),
    comb = require("comb"),
    models = require("./models");
exports.loadData = function () {
    patio.camelize = true;
    return helper.createTables().chain(function () {
        //sync our models
        return comb.when(
            models.Airport.save(data.airports),
            models.AirplaneType.save(data.airplaneTypes),
            models.Flight.save(data.flights)
        );
    });
};

exports.dropModels = function () {
    return helper.dropTableAndDisconnect();
};