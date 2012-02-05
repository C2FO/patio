var patio = require("../../index"),
    sql = patio.sql,
    comb = require("comb"),
    format = comb.string.format;

patio.camelize = true;

comb.logging.Logger.getRootLogger().level = comb.logging.Level.ERROR;

patio.connectAndExecute("mysql://test:testpass@localhost:3306/sandbox",
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
                    this._super(arguments);
                    this.oneToOne("capital");
                }
            }
        });
        patio.addModel("capital", {
            static:{
                init:function () {
                    this._super(arguments);
                    this.manyToOne("state");
                }
            }
        });
        patio.getModel("state").save({
            name:"Nebraska",
            population:1796619,
            founded:new Date(1867, 2, 4),
            climate:"continental",
            capital:{
                name:"Lincoln",
                founded:new Date(1856, 0, 1),
                population:258379

            }
        });
        patio.getModel("capital").save({
            name:"Austin",
            founded:new Date(1835, 0, 1),
            population:790390,
            state:{
                name:"Texas",
                population:25674681,
                founded:new Date(1845, 11, 29)
            }
        });
        var State = patio.getModel("state"), Capital = patio.getModel("capital");
        State.order("name").forEach(function (state) {
            //if you return a promise here it will prevent the foreach from
            //resolving until all inner processing has finished.
            return state.capital.then(function (capital) {
                console.log(comb.string.format("%s's capital is %s.", state.name, capital.name));
            })
        });

        Capital.order("name").forEach(function (capital) {
            //if you return a promise here it will prevent the foreach from
            //resolving until all inner processing has finished.
            return capital.state.then(function (state) {
                console.log(comb.string.format("%s is the capital of %s.", capital.name, state.name));
            })
        });

    }).both(comb.hitch(patio, "disconnect"));

