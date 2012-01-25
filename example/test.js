"use strict";
var patio = require("../index.js"),
    sql = patio.sql,
    comb = require("comb");

patio.camelize = true;

var oneToManyExample = function () {
    return patio.connectAndExecute("mysql://test:testpass@localhost:3306/sandbox",
        function (db, patio) {
            db.forceCreateTable("biologicalFather", function () {
                this.primaryKey("id");
                this.name(String);
            });
            db.forceCreateTable("child", function () {
                this.primaryKey("id");
                this.name(String);
                this.foreignKey("biologicalFatherId", "biologicalFather", {key:"id"});
            });


            //define the BiologicalFather model
            patio.addModel("biologicalFather", {
                static:{
                    init:function () {
                        this.oneToMany("children");
                    }
                }
            });


            //define Child  model
            patio.addModel("child", {
                static:{
                    init:function () {
                        this.manyToOne("biologicalFather");
                    }
                }
            });

            var BiologicalFather = patio.getModel("biologicalFather");
            var Child = patio.getModel("child");
            BiologicalFather.save([
                {name:"Fred", children:[
                    {name:"Bobby"},
                    {name:"Alice"},
                    {name:"Susan"}
                ]},
                {name:"Ben"},
                {name:"Bob"},
                {name:"Scott", children:[
                    {name:"Brad"}
                ]}
            ]);
            BiologicalFather.forEach(function(father){
                //you use a promise now because this is not an
                //executeInOrderBlock
                return father.children.then(function(children){
                    console.log(father.name + " has " + children.length + " children");
                    if(children.length){
                        console.log("The children's names are " + children.map(function(child){
                            return child.name;
                        }))
                    }
                });
            });
            var father =
            patio.logInfo(Child.findById(1).biologicalFather.name);
            patio.logInfo(father.name);
            patio.logInfo(father.children.map(function (child) {
                return child.name
            }));
        });

};

var oneToOneExample = function () {
    return patio.connectAndExecute("mysql://test:testpass@localhost:3306/sandbox",
        function (db, patio) {
            db.forceDropTable(["capital", "state"]);
            db.createTable("state", function () {
                this.primaryKey("id");
                this.name(String)
                this.population("integer");
                this.founded(Date);
                this.climate(String);
                this.description("text");
            });
            db.createTable("capital", function () {
                this.primaryKey("id");
                this.population("integer");
                this.name(String);
                this.founded(Date);
                this.foreignKey("stateId", "state", {key:"id"});
            });
            patio.addModel("state", {
                static:{
                    init:function () {
                        this.oneToOne("capital");
                    }
                }
            });
            patio.addModel("capital", {
                static:{
                    init:function () {
                        this.manyToOne("state");
                    }
                }
            });
            var State = patio.getModel("state");
            State.save([
                {
                    name:"Nebraska",
                    population:1796619,
                    founded:new Date(1867, 2, 4),
                    climate:"continental",
                    capital:{
                        name:"Lincoln",
                        founded:new Date(1856, 0, 1),
                        population:258379

                    }
                },
                {
                    name:"Texas",
                    population:25674681,
                    founded:new Date(1845, 11, 29),
                    capital:{
                        name:"Austin",
                        founded:new Date(1835, 0, 1),
                        population:790390

                    }
                }
            ]);
            State.forEach(function (state) {
                return state.capital.then(function (capital) {
                    console.log(state.name + "'s capital is " + capital.name);
                    console.log(capital.name + " was founded in " + capital.founded);
                });
            });

        });
};


oneToManyExample()
    .chainBoth(oneToOneExample
)
    .both(comb.hitch(patio, "disconnect"));
