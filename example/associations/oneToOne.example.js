"use strict";
var patio = require("../../index"),
    sql = patio.sql,
    comb = require("comb"),
    format = comb.string.format;

patio.camelize = true;

comb.logging.Logger.getRootLogger().level = comb.logging.Level.ERROR;

patio.connectAndExecute("mysql://test:testpass@localhost:3306/sandbox",
    function(db, patio){
        db.forceDropTable(["capital", "state"]);
        db.createTable("state", function(){
            this.primaryKey("id");
            this.name(String)
            this.population("integer");
            this.founded(Date);
            this.climate(String);
            this.description("text");
        });
        db.createTable("capital", function(){
            this.primaryKey("id");
            this.population("integer");
            this.name(String);
            this.founded(Date);
            this.foreignKey("stateId", "state", {key:"id"});
        });
        patio.addModel("state", {
            static:{
                init:function(){
                    this.oneToOne("capital");
                }
            }
        });
        patio.addModel("capital", {
            static:{
                init:function(){
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
        State.forEach(function(state){
            return state.capital.then(function(capital){
                console.log(state.name + "'s capital is " + capital.name);
                console.log(capital.name + " was founded in " + capital.founded);
            });
        });

    }).both(comb.hitch(patio, "disconnect"));

