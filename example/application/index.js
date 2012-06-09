var patio = require("../../index"),
    helpers = require("./helpers"),
    express = require("express");
//connect
patio.camelize = true;
patio.connect("mysql://test:testpass@localhost:3306/sandbox?maxConnections=50&minConnections=10");
patio.configureLogging();
patio.LOGGER.level = "ERROR";

var models = require("./models"),
    Flight = models.Flight,
    Airport = models.Airport;

helpers.loadData().then(function () {
    var app = express.createServer();
    Flight.route(app);
    Airport.route(app);
    app.listen(8080, "127.0.0.1");
    console.log("Ready for connections...");
}, function (err) {
    console.error(err);
    patio.disconnect();
});

process.on("uncaughtException", function (err) {
    console.trace(err);
});