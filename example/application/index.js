var patio = require("../../index"),
    helpers = require("./helpers"),
    express = require("express");


helpers.loadData().then(function () {
    var app = express.createServer();
    patio.getModel("flight").route(app);
    patio.getModel("airport").route(app);
    app.listen(8080, "127.0.0.1");
}, function (err) {
    err.forEach(function (err) {
        console.log(err[1]);
    });
    throw err;
});