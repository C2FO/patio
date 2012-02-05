var patio = require("../../index"),
    sql = patio.sql,
    comb = require("comb"),
    format = comb.string.format;

patio.camelize = true;

patio.configureLogging({"patio":{level:"ERROR"}});
patio.connectAndExecute("mysql://test:testpass@localhost:3306/sandbox",
    function (db, patio) {
        db.forceDropTable("child", "biologicalFather");
        db.createTable("biologicalFather", function () {
            this.primaryKey("id");
            this.name(String);
        });
        db.createTable("child", function () {
            this.primaryKey("id");
            this.name(String);
            this.foreignKey("biologicalFatherId", "biologicalFather", {key:"id"});
        });


        //define the BiologicalFather model
        patio.addModel("biologicalFather", {
            static:{
                init:function () {
                    this._super(arguments);
                    this.oneToMany("children");
                    this.oneToMany("letterBChildren", {model:"child", fetchType:this.fetchType.EAGER, dataset:function () {
                        return this.db.from("child").filter({name:{like:"B%"}, biologicalFatherId:this.id});
                    }});
                }
            }
        });


        //define Child  model
        patio.addModel("child", {
            static:{
                init:function () {
                    this._super(arguments);
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
        BiologicalFather.forEach(function (father) {
            //you use a promise now because this is not an
            //executeInOrderBlock
            if (father.letterBChildren.length > 0) {
                console.log(father.name + " has " + father.letterBChildren.length + " B children");
                console.log("The B letter children's names are " + father.letterBChildren.map(function (child) {
                    return child.name;
                }));
            }
            return father.children.then(function (children) {
                console.log(father.name + " has " + children.length + " children");
                if (children.length) {
                    console.log("The children's names are " + children.map(function (child) {
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
    }).both(comb.hitch(patio, "disconnect"));