var patio = require("index"),
    sql = patio.SQL,
    helper = require("./helper"),
    comb = require("comb");

exports.loadModels = function () {
    var ret = new comb.Promise()
    return comb.executeInOrder(helper, patio, function (helper, patio) {
        helper.createTables();
        var Company = patio.addModel("company", {
            static:{
                init:function () {
                    this.oneToMany("employees", {fetchType:this.fetchType.EAGER});
                    this.oneToMany("omahaEmployees", {model:"employee", fetchType:this.fetchType.EAGER}, function (ds) {
                        return ds.filter(sql.identifier("city").ilike("omaha"));
                    });
                    this.oneToMany("lincolnEmployees", {model:"employee", fetchType:this.fetchType.EAGER}, function (ds) {
                        return ds.filter(sql.identifier("city").ilike("lincoln"));
                    });
                }
            }
        });
        var Employee = patio.addModel("employee", {
            static:{
                init:function () {
                    this.manyToOne("company", {fetchType:this.fetchType.EAGER});
                }
            }
        });
        //define associations


    });

};


exports.dropModels = function () {
    return helper.dropTableAndDisconnect();
};
