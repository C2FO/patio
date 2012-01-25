var patio = require("../../lib");

patio.addModel("airplane", {
    static:{
        init:function () {
            this.manyToOne("airplaneType", {fetchType:this.fetchType.EAGER})
                .oneToMany("legs", {model:"legInstance"});
        }
    }
});


