var server = "pg://postgres@127.0.0.1:5432/sandbox?maxConnections=10",
    patio = require("../index"),
    TIMES = parseInt(process.env.TIMES || 2),
    LIMIT = parseInt(process.env.LIMIT || 1000),
    comb = require("comb"),
    format = comb.string.format,
    noTransactions = require("./benchmark.noTransacitons"),
    defaults = require("./benchmark.defaults");

patio.camelize = true;


var Entry;
var loop = function (async, cb, limit) {
    var saves = [];
    limit = limit || LIMIT;
    for (var i = 0; i < limit; i++) {
        saves.push(async ? cb(i) : comb.partial(cb, i));
    }
    if (async) {
        saves = new comb.PromiseList(saves, true);
    }
    return async ? saves : comb.serial(saves);
};

var testInserts = function (async, limit) {
    var start = +new Date();
    return loop(async,function () {
        return new Entry({
            number: Math.floor(Math.random() * 99999),
            string: 'asdasd'
        }).save();
    }, limit).chain(function () {
            return +(new Date()) - start;
        });
};

var testUpdates = function (async, limit) {
    return Entry.all().chain(function (entries) {
        var start = +new Date;
        return loop(async,function (index) {
            return entries[index].update({number: Math.floor(Math.random() * 99999)});
        }, limit).chain(function () {
                return +(new Date()) - start;
            });

    });
};

var testRead = function () {
    var start = +new Date;
    return Entry.all().chain(function (entries) {
        return (+new Date) - start;
    });
};

var testDelete = function (async, limit) {
    return Entry.all().chain(function (entries) {
        var start = +new Date();
        return loop(async,function (index) {
            return entries[index].remove();
        }, limit).chain(function () {
                return +(new Date()) - start;
            });
    });
};

var addResult = function (obj, key, res) {
    !obj[key] && (obj[key] = 0);
    obj[key] += res;
}

var bench = function (module, header, times, limit) {
    return module.createTableAndModel(server).chain(function () {
        Entry = patio.getModel("patioEntry");
        var res = {};
        var runTestsOnce = function (index) {
            console.log("%s RUN %d...", header, index + 1);
            addResult(res, "total", 1);
            return testInserts(false, limit)
                .chain(function (result) {
                    addResult(res, "Serial Insert", result);
                    return testInserts(true, limit);
                })
                .chain(function (result) {
                    addResult(res, "Async Insert", result);
                    return testUpdates(false, limit);
                })
                .chain(function (result) {
                    addResult(res, "Serial Update", result);
                    return testUpdates(true, limit);
                })
                .chain(function (result) {
                    addResult(res, "Async Update", result);
                    return testRead(false, limit);
                })
                .chain(function (result) {
                    addResult(res, "Serial Read", result);
                    return testRead(true, limit);
                })
                .chain(function (result) {
                    addResult(res, "Async Read", result);
                    return testDelete(false, limit);
                })
                .chain(function (result) {
                    addResult(res, "Serial Delete", result);
                    return testDelete(true, limit);
                })
                .chain(function (result) {
                    addResult(res, "Async Delete", result);
                });
        };
        return loop(false, runTestsOnce, times).chain(function () {
            return res;
        });
    });
};


var printDurations = function (header, module, limit, durations) {
    console.log(header);
    var msg = "%-15s (%02s runs): Average duration % 8dms for %d items";
    for (var testName in durations) {
        if (testName !== "total") {
            console.log(format(msg, testName, durations.total, durations[testName] / durations.total), limit);
        }
    }
    module.disconnect();
};

console.log("Starting Benchmark...");
bench(noTransactions, "NO TRANSACTIONS MODEL", TIMES, LIMIT)
    .chain(comb.partial(printDurations, "NO TRANSACTIONS MODEL", noTransactions, LIMIT))
    .chain(comb.partial(bench, defaults, "DEFAULT MODEL", TIMES, LIMIT))
    .chain(comb.partial(printDurations, "DEFAULT MODEL", defaults, LIMIT))
    .chain(function () {
        console.log("DONE");
    }, defaults.disconnectErr);