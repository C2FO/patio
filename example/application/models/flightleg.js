var patio = require("../../../index"),
    comb = require("comb");


patio.addModel("flightLeg", {

    static:{
        init:function () {
            this._super(arguments);
            var eager = this.fetchType.EAGER;
            this.manyToOne("flight", {fetchType:eager})
                .manyToOne("departs", {model:"airport", key:{departureCode:"airportCode"}})
                .manyToOne("arrives", {model:"airport", key:{arrivalCode:"airportCode"}});
        }
    }
}).as(module);




