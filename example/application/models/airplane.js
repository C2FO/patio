var patio = require("../../../index");

patio.addModel("airplane", {
    static:{
        init:function () {
            this.manyToOne("airplaneType", {fetchType:this.fetchType.EAGER})
                .oneToMany("legs", {model:"legInstance"});
        }
    }
});


