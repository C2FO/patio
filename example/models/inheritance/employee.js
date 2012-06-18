var patio = require("../../../index");

patio.addModel("employee", {
    plugins : [patio.plugins.ClassTableInheritancePlugin],
    static:{
        init:function () {
            this._super(arguments);
            this.configure({key : "kind"});
        }
    }
}).as(module);