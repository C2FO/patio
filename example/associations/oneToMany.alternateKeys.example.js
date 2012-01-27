"use strict";
var patio = require("../../index"),
    sql = patio.sql,
    comb = require("comb"),
    format = comb.string.format;

patio.camelize = true;

comb.logging.Logger.getRootLogger().level = comb.logging.Level.ERROR;
patio.connectAndExecute("mysql://test:testpass@localhost:3306/sandbox",
    function(db, patio){
        db.forceDropTable("child", "stepFather", "biologicalFather");
        db.createTable("biologicalFather", function(){
            this.primaryKey("id");
            this.name(String);
        });
        db.createTable("stepFather", function(){
            this.primaryKey("id");
            this.name(String, {unique : true});
        });

        db.createTable("child", function(){
            this.primaryKey("id");
            this.name(String);
            this.foreignKey("biologicalFatherKey", "biologicalFather", {key:"id"});
            this.foreignKey("stepFatherKey", "stepFather", {key:"name", type : String});
        });


        //define the BiologicalFather model
        patio.addModel("biologicalFather", {
            static:{
                init:function(){
                    this.oneToMany("children", {key : "biologicalFatherKey"});
                }
            }
        });

        //define the StepFather model
        patio.addModel("stepFather", {
            static:{
                init:function(){
                    this.oneToMany("children", {key : "stepFatherKey", primaryKey : "name"});
                }
            }
        });

        //define Child  model
        patio.addModel("child", {
            static:{
                init:function(){
                    this.manyToOne("biologicalFather", {key : "biologicalFatherKey"});
                    this.manyToOne("stepFather", {key : "stepFatherKey", primaryKey : "name"});
                }
            }
        });

        var BiologicalFather = patio.getModel("biologicalFather");
        var StepFather = patio.getModel("stepFather");
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
        //you could associate the children directly but we wont for this example
        StepFather.save([
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
                console.log("Father " + father.name + " has " + children.length + " children");
                if (children.length) {
                    console.log("The children's names are " + children.map(function(child){
                        return child.name;
                    }))
                }
            });
        });
        StepFather.forEach(function(father){
            //you use a promise now because this is not an
            //executeInOrderBlock
            return father.children.then(function(children){
                console.log("Step father " + father.name + " has " + children.length + " children");
                if (children.length) {
                    console.log("The children's names are " + children.map(function(child){
                        return child.name;
                    }))
                }
            });
        });
        var father =
            patio.logInfo(Child.findById(1).biologicalFather.name);
        patio.logInfo(father.name);
        patio.logInfo(father.children.map(function(child){
            return child.name
        }));
    }).both(comb.hitch(patio, "disconnect"));