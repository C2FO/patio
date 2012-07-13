var patio = require("../../../index"),
    Employee = require("./employee");


patio.addModel("manager", Employee, {
    static:{
        init:function () {
            this._super(arguments);
            this.oneToMany("staff", {key:"managerId", fetchType:this.fetchType.EAGER});
        }
    }
}).as(module);

 