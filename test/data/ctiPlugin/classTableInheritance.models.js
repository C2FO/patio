var patio = require("index"),
    helper = require("./helper"),
    comb = require("comb"),
    ClassTableInheritance = patio.plugins.ClassTableInheritancePlugin;

exports.loadModels = function () {
    var ret = new comb.Promise();
    helper.createTables(true).then(function () {
        var Employee = patio.addModel("employee", {
            plugins:[ClassTableInheritance],
            static:{

                init:function () {
                    this._super(arguments);
                    this.configure({key:"kind"});
                }
            }
        });
        var Staff = patio.addModel("staff", Employee, {

            static:{

                init:function () {
                    this._super(arguments);
                    this.manyToOne("manager", {key:"managerId", fetchType:this.fetchType.EAGER});
                }
            }
        });
        var Manager = patio.addModel("manager", Employee, {
            static:{
                init:function () {
                    this._super(arguments);
                    this.oneToMany("staff", {key:"managerId", fetchType:this.fetchType.EAGER});
                }
            }
        });

        patio.addModel("executive", Manager);
        patio.syncModels().then(ret);
    }, ret);
    return ret;
};


exports.dropModels = function () {
    return helper.dropTableAndDisconnect();
};
