var patio = require("../../index"),
    sql = patio.sql,
    comb = require("comb"),
    format = comb.string.format;

patio.camelize = true;
patio.configureLogging();
patio.LOGGER.level = "ERROR";
var DB = patio.connect("mysql://test:testpass@localhost:3306/sandbox");

var disconnect = comb.hitch(patio, "disconnect");
var disconnectError = function (err) {
    patio.logError(err);
    patio.disconnect();
};

//define the BiologicalFather model
var BiologicalFather = patio.addModel("biologicalFather", {
    static:{
        init:function () {
            this._super(arguments);
            this.oneToMany("children", {key:"biologicalFatherKey"});
        }
    }
});

//define the StepFather model
var StepFather = patio.addModel("stepFather", {
    static:{
        init:function () {
            this._super(arguments);
            this.oneToMany("children", {key:"stepFatherKey", primaryKey:"name"});
        }
    }
});

//define Child  model
var Child = patio.addModel("child", {
    static:{
        init:function () {
            this._super(arguments);
            this.manyToOne("biologicalFather", {key:"biologicalFatherKey"});
            this.manyToOne("stepFather", {key:"stepFatherKey", primaryKey:"name"});
        }
    }
});

var createTables = function () {
    return comb.serial([
        function () {
            return DB.forceDropTable("child", "stepFather", "biologicalFather");
        },
        function () {
            return comb.when(
                DB.createTable("biologicalFather", function () {
                    this.primaryKey("id");
                    this.name(String);
                }),

                DB.createTable("stepFather", function () {
                    this.primaryKey("id");
                    this.name(String, {unique:true});
                })
            );
        },
        function () {
            return DB.createTable("child", function () {
                this.primaryKey("id");
                this.name(String);
                this.foreignKey("biologicalFatherKey", "biologicalFather", {key:"id"});
                this.foreignKey("stepFatherKey", "stepFather", {key:"name", type:String});
            });
        },
        comb.hitch(patio, "syncModels")
    ]);

};

var createData = function () {
    return comb.when(
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
        ]),
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
        ])
    );
}


createTables().chain(createData, disconnectError).then(function () {
    comb.serial([
        function () {
            return BiologicalFather.forEach(function (father) {
                //you use a promise now because this is not an
                //executeInOrderBlock
                return father.children.then(function (children) {
                    console.log("Father " + father.name + " has " + children.length + " children");
                    if (children.length) {
                        console.log("The children's names are " + children.map(function (child) {
                            return child.name;
                        }));
                    }
                });
            });
        },
        function () {
            return StepFather.forEach(function (father) {
                //you use a promise now because this is not an
                //executeInOrderBlock
                return father.children.then(function (children) {
                    console.log("Step father " + father.name + " has " + children.length + " children");
                    if (children.length) {
                        console.log("The children's names are " + children.map(function (child) {
                            return child.name;
                        }));
                    }
                });
            });
        },
        function () {
            var ret = new comb.Promise();
            Child.findById(1).then(function (child) {
                child.biologicalFather.then(function (father) {
                    console.log("%s biological father is %s", child.name, father.name);
                    ret.callback();
                }, ret);
            }, ret);
            return ret;
        }
    ]).then(disconnect, disconnectError);
}, disconnectError);
