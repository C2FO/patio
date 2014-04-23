var patio = require("../../");

patio.camelize = true;
patio.connect("pg://postgres@127.0.0.1:5432/readme_example");

var State = patio.addModel("state").oneToOne("capital");
var Capital = patio.addModel("capital").manyToOne("state");

//save a state
State
    .save({
        name: "Nebraska",
        population: 1796619,
        founded: new Date(1867, 2, 4),
        climate: "continental",
        capital: {
            name: "Lincoln",
            founded: new Date(1856, 0, 1),
            population: 258379
        }
    })
    .chain(function () {
        //save a Capital
        return Capital.save({
            name: "Austin",
            founded: new Date(1835, 0, 1),
            population: 790390,
            state: {
                name: "Texas",
                population: 25674681,
                founded: new Date(1845, 11, 29)
            }
        });
    })
    .chain(function () {
        return State.order("name").forEach(function (state) {
            //if you return a promise here it will prevent the foreach from
            //resolving until all inner processing has finished.
            return state.capital.chain(function (capital) {
                console.log("%s's capital is %s.", state.name, capital.name);
            });
        }, 1);
    })
    .chain(function () {
        return State.remove();
    })
    .chain(function () {
        return Capital.remove();
    })
    .chain(process.exit, function (err) {
        console.log(err)
        process.exit(1);
    });