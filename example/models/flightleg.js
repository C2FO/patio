var patio = require("../../lib"),
    comb = require("comb");


patio.addModel("flightLeg", {
    plugins:[patio.plugins.CachePlugin],
    instance:{
        toObject:function () {
            var obj = this._super(arguments);
            delete obj.departureCode;
            delete obj.arrivalCode;
            obj.departs = !comb.isUndefinedOrNull(this.departs) ? this.departs.toObject() : null;
            obj.arrives = !comb.isUndefinedOrNull(this.arrives) ? this.arrives.toObject() : null;
            return obj;
        }
    },

    static:{
        init:function () {
            //FlightLeg.oneToMany("legInstances", {model:"legInstance", key:"flightLegId"});
            var eager = this.fetchType.EAGER;
            this.manyToOne("flight", {fetchType:eager})
                .manyToOne("departs", {model:"airport", fetchType:eager, key:{departureCode:"airportCode"}})
                .manyToOne("arrives", {model:"airport", fetchType:eager, key:{arrivalCode:"airportCode"}});
        }
    }
});




