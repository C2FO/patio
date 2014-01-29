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

var State = patio.addModel("state", {
    static: {
        init: function () {
            this._super(arguments);
            this.oneToOne("capital");
        }
    }
});
var Capital = patio.addModel("capital", {
    static: {
        init: function () {
            this._super(arguments);
            this.manyToOne("state");
        }
    }
});

var createTables = function () {
    return comb.serial([
        function () {
            return DB.forceDropTable(["capital", "state"]);
        },
        function () {
            return DB.createTable("state", function () {
                this.primaryKey("id");
                this.name(String)
                this.population("integer");
                this.founded(Date);
                this.climate(String);
                this.description("text");
            });
        },
        function () {
            return DB.createTable("capital", function () {
                this.primaryKey("id");
                this.population("integer");
                this.name(String);
                this.founded(Date);
                this.foreignKey("stateId", "state", {key: "id"});
            });
        },
        comb.hitch(patio, "syncModels")
    ]);
};

var createData = function () {
    return comb.when(
        State.save({
            name: "Nebraska",
            population: 1796619,
            founded: new Date(1867, 2, 4),
            climate: "continental",
            capital: {
                name: "Lincoln",
                founded: new Date(1856, 0, 1),
                population: 258379

            }
        }),
        Capital.save({
            name: "Austin",
            founded: new Date(1835, 0, 1),
            population: 790390,
            state: {
                name: "Texas",
                population: 25674681,
                founded: new Date(1845, 11, 29)
            }
        })
    );
};


createTables()
    .chain(createData)
    .chain(function () {
        return State.order("name").forEach(function (state) {
            //if you return a promise here it will prevent the foreach from
            //resolving until all inner processing has finished.
            return state.capital.chain(function (capital) {
                console.log(comb.string.format("%s's capital is %s.", state.name, capital.name));
            });
        })
    })
    .chain(function () {
        return Capital.order("name").forEach(function (capital) {
            //if you return a promise here it will prevent the foreach from
            //resolving until all inner processing has finished.
            return capital.state.chain(function (state) {
                console.log(comb.string.format("%s is the capital of %s.", capital.name, state.name));
            });
        });
    })
    .chain(disconnect, disconnectError);


