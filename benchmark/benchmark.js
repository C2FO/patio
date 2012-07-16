var server = "mysql://test:testpass@localhost:3306/sandbox",
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
    var ret = new comb.Promise();
    var start = +new Date();
    loop(async,function () {
        return new Entry({
            number:Math.floor(Math.random() * 99999),
            string:'asdasd'
        }).save();
    }, limit).then(function () {
            ret.callback(+(new Date()) - start);
        }, ret);
    return ret;
};

var testUpdates = function (async, limit) {
    var ret = new comb.Promise();
    Entry.all().then(function (entries) {
        var start = +new Date;
        loop(async,function (index) {
            return entries[index].update({number:Math.floor(Math.random() * 99999)});
        }, limit).then(function () {
                ret.callback(+(new Date()) - start);
            }, ret);

    }, comb.hitch(ret, "errback"));
    return ret;
};

var testRead = function () {
    var ret = new comb.Promise();
    var start = +new Date;
    Entry.all().then(function (entries) {
        ret.callback((+new Date) - start);
    }, ret);
    return ret;
};

var testDelete = function (async, limit) {
    var ret = new comb.Promise();
    Entry.all().then(function (entries) {
        var start = +new Date();
        loop(async,function (index) {
            return entries[index].remove();
        }, limit).then(function () {
                ret.callback(+(new Date()) - start);
            }, ret);
    });
    return ret;
};

var addResult = function (obj, key, res) {
    !obj[key] && (obj[key] = 0);
    obj[key] += res;
}

var bench = function (module, header, times, limit) {
    var ret = new comb.Promise();
    module.createTableAndModel(server).then(function () {
        Entry = patio.getModel("patioEntry");
        var res = {};
        var runTestsOnce = function (index) {
            console.log("%s RUN %d...", header, index + 1);
            addResult(res, "total", 1);
            var ret = new comb.Promise();
            comb.serial([
                testInserts.bind(null, false, limit),
                testInserts.bind(null, true, limit),
                testUpdates.bind(null, false, limit),
                testUpdates.bind(null, true, limit),
                testRead.bind(null, false, limit),
                testRead.bind(null, true, limit),
                testDelete.bind(null, false, limit),
                testDelete.bind(null, true, limit)
            ]).then(function (results) {
                    addResult(res, "Serial Insert", results[0]);
                    addResult(res, "Async Insert", results[1]);
                    addResult(res, "Serial Update", results[2]);
                    addResult(res, "Async Update", results[3]);
                    addResult(res, "Serial Read", results[4]);
                    addResult(res, "Async Read", results[5]);
                    addResult(res, "Serial Delete", results[6]);
                    addResult(res, "Async Delete", results[7]);
                    ret.callback();
                }, ret);
            return ret;
        };
        loop(false, runTestsOnce, times).then(ret.callback.bind(ret, res), ret);
    });
    return ret;
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
}
console.log("Starting Benchmark...");
bench(noTransactions, "NO TRANSACTIONS MODEL", TIMES, LIMIT)
    .then(comb.partial(printDurations, "NO TRANSACTIONS MODEL", noTransactions, LIMIT), noTransactions.disconnectErr)
    .chain(comb.partial(bench, defaults, "DEFAULT MODEL", TIMES, LIMIT))
    .then(comb.partial(printDurations, "DEFAULT MODEL", defaults, LIMIT), defaults.disconnectErr)