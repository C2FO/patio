var patio = require("../../../index"),
    expressPlugin = require("../plugins/ExpressPlugin");


patio.addModel("airport", {
    plugins:[patio.plugins.CachePlugin, expressPlugin],

    static:{
        init:function () {
            this._super(arguments);
            this.manyToMany("supportedAirplaneTypes", {
                joinTable:"canLand",
                model:"airplaneType"
            });
        }
    }
});

