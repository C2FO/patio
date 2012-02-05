var server = "mysql://test:testpass@localhost:3306/sandbox"
    , patio = require("../index")
    , TIMES = parseInt(process.env.TIMES || 2)
    , LIMIT = parseInt(process.env.LIMIT || 5000)
    , comb = require("comb")
    , format = comb.string.format
    , noTransactions = require("./benchmark.noTransacitons")
    , defaults = require("./benchmark.defaults");

patio.camelize = true;


var Entry;
var loop = function (async, cb, limit) {
    var saves = async ? [] : new comb.Promise().callback();
    limit = limit || LIMIT;
    for (var i = 0; i < limit; i++) {
        if (async) {
            saves.push(cb(i));
        } else {
            saves = saves.chain(comb.partial(cb, i));
        }
    }
    if (async) {
        saves = new comb.PromiseList(saves, true);
    }
    return saves;
};

var testInserts = function (async, limit) {
    var ret = new comb.Promise();
    var start = +new Date();
    loop(async,
        function () {
            return new Entry({
                number:Math.floor(Math.random() * 99999),
                string:'asdasd'
            }).save()
        }, limit).then(function () {
            ret.callback((+new Date) - start);
        }, comb.hitch(ret, "errback"));
    return ret;
};

var testUpdates = function (async, limit) {
    var ret = new comb.Promise();
    Entry.all().then(function (entries) {
        var start = +new Date;
        loop(async,
            function (index) {
                return entries[index].update({number:Math.floor(Math.random() * 99999)});
            }, limit).then(function () {
                ret.callback((+new Date) - start);
            }, comb.hitch(ret, "errback"));

    }, comb.hitch(ret, "errback"));
    return ret;
};

var testRead = function () {
    var ret = new comb.Promise();
    var start = +new Date;
    Entry.all().then(function (entries) {
        ret.callback((+new Date) - start);
    }, comb.hitch(ret, "errback"));
    return ret;
};

var testDelete = function (async, limit) {
    var ret = new comb.Promise();
    Entry.all().then(function (entries) {
        var start = +new Date();
        loop(async,
            function (index) {
                return entries[index].remove();
            }, limit).then(function () {
                ret.callback((+new Date) - start);
            }, comb.hitch(ret, "errback"));
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
            var ret = new comb.Promise();
            console.log("%s RUN %d...", header, index + 1);
            addResult(res, "total", 1);
            testInserts(false, limit)
                .addCallback(comb.partial(addResult, res, "Serial Insert"))
                .chain(comb.partial(testInserts, true, limit), comb.hitch(ret, "errback"))
                .addCallback(comb.partial(addResult, res, "Async Insert"))
                .chain(comb.partial(testUpdates, false, limit), comb.hitch(ret, "errback"))
                .addCallback(comb.partial(addResult, res, "Serial Update"))
                .chain(comb.partial(testUpdates, true, limit), comb.hitch(ret, "errback"))
                .addCallback(comb.partial(addResult, res, "Async Update"))
                .chain(comb.partial(testRead), comb.hitch(ret, "errback"))
                .addCallback(comb.partial(addResult, res, "Read"))
                .chain(comb.partial(testDelete, false, limit), comb.hitch(ret, "errback"))
                .addCallback(comb.partial(addResult, res, "Serial Delete"))
                .chain(comb.partial(testDelete, true, limit), comb.hitch(ret, "errback"))
                .addCallback(comb.partial(addResult, res, "Async Delete"))
                .then(comb.hitch(ret, "callback", res), comb.hitch(ret, "errback"));
            return ret;
        };
        loop(false, runTestsOnce, times).then(comb.hitch(ret, "callback"), comb.hitch(ret, "errback"));
    });
    return ret;
};


var printDurations = function (header, module, limit, durations) {
    console.log(header);
    var msg = "%-15s (%02s runs): Average duration % 8dms for %d items";
    for (var testName in durations) {
        if (testName != "total") {
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