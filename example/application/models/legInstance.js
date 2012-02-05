var patio = require("../../../index");

patio.addModel("legInstance", {
    static:{
        init:function () {
            this._super(arguments);
            var eager = this.fetchType.EAGER;
            this.manyToOne("airplane", {fetchType:eager})
                .manyToOne("flightLeg", {fetchType:eager, key:"flightLegId"});
        }
    }
});



