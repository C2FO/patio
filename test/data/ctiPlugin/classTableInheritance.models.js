var patio = require("index"),
    helper = require("./helper"),
    comb = require("comb"),
    ClassTableInheritance = patio.plugins.ClassTableInheritancePlugin;

exports.loadModels = function () {
    var ret = new comb.Promise();
    return comb.executeInOrder(helper, patio, function (helper, patio) {
        helper.createTables(true);
        var Employee = patio.addModel("employee", {
            plugins : [ClassTableInheritance],
            static:{

                init:function () {
                    this._super(arguments);
                    this.configure({key : "kind"});
                }
            }
        });
        var Staff = patio.addModel("staff", Employee, {

            static:{

                init:function () {
                    this._super(arguments);
                    this.manyToOne("manager", {key : "managerId", fetchType : this.fetchType.EAGER});
                }
            }
        });
        var Manager = patio.addModel("manager", Employee, {
            static:{

                init:function () {
                    this._super(arguments);
                    this.oneToMany("staff", {key : "managerId", fetchType : this.fetchType.EAGER});
                }
            }
        });

        patio.addModel("executive",  Manager);

    });
};


exports.dropModels = function () {
    return helper.dropTableAndDisconnect();
};
