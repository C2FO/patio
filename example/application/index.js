var patio = require("../../index"),
    helpers = require("./helpers"),
    express = require("express");

patio.configureLogging();
patio.LOGGER.level = "ERROR";
helpers.loadData().then(function () {
    var app = express.createServer();
    patio.getModel("flight").route(app);
    patio.getModel("airport").route(app);
    app.listen(8080, "127.0.0.1");
    console.log("Ready for connections...");
}, function (err) {
    console.error(err);
    patio.disconnect();
});

process.on("uncaughtException", function (err) {
    console.trace(err);
});