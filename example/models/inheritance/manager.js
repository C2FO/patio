var patio = require("../../../index");
require("./employee");


/**
 * NOTE:
 * we required the employee model above so it might or might not be loaded
 * but patio will check if that model is currently in the process of being loaded if it
 * is then it will deffer until employee is done loading then load staff
 */
patio.addModel("manager", "employee", {
    static:{
        init:function () {
            this._super(arguments);
            this.oneToMany("staff", {key:"managerId", fetchType:this.fetchType.EAGER});
        }
    }
});

 