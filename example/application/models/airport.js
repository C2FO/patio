var patio = require("../../lib"),
    expressPlugin = require("../plugins/ExpressPlugin");


patio.addModel("airport", {
    plugins:[patio.plugins.CachePlugin, expressPlugin],

    static:{
        init:function () {
            this.manyToMany("supportedAirplaneTypes", {
                joinTable:"canLand",
                model:"airplaneType"
            });
        }
    }
});

