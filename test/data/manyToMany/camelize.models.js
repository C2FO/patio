var patio = require("index"),
    helper = require("./helper"),
    comb = require("comb");

exports.loadModels = function () {
    var ret = new comb.Promise();
    return comb.executeInOrder(helper, patio, function (helper, patio) {
        var DB = helper.createTables(true);
        var Company = patio.addModel(DB.from("company"), {
            static:{

                identifierOutputMethod:"camelize",

                identifierInputMethod:"underscore",

                init:function () {
                    this.manyToMany("employees");
                }
            }
        });
        var Employee = patio.addModel(DB.from("employee"), {
            static:{

                identifierOutputMethod:"camelize",

                identifierInputMethod:"underscore",

                init:function () {
                    this.manyToMany("companies");
                }
            }
        });


    });
};


exports.dropModels = function () {
    return helper.dropTableAndDisconnect();
};
