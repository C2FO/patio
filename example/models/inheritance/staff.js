var patio = require("../../../index"),
    Employee = require("./employee.js");

patio.addModel("staff", Employee, {
    static:{
        init:function () {
            this._super(arguments);
            this.manyToOne("manager", {key:"managerId", fetchType:this.fetchType.EAGER});
        }
    }
}).as(module);

