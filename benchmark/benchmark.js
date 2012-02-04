var server = "mysql://root@localhost:3306/performance_analysis_sequelize"
    , patio = require("../index")
    , TIMES = parseInt(process.env.TIMES || 100)
    , LIMIT = parseInt(process.env.LIMIT || 5000)
    , comb = require("comb");

patio.camelize = true;
var DB = patio.connect(server);

var createTableAndModel = function () {
    var ret = new comb.Promise();
    DB.forceCreateTable("patioEntry",
        function () {
            this.primaryKey("id");
            this.column("number", "integer");
            this.column("string", String);
        }).then(function () {
            patio.addModel("patioEntry").then(function () {
                ret.callback();
            });
        });
    return ret;
};

var bench = function (times, limit, runCallback) {
    var durations = []
        , done = 0

    LIMIT = limit;


    createTableAndModel().then(function () {
        var runTestsOnce = function (callback) {

            var Entry = patio.getModel("patioEntry");
            Entry.useTransactions = false;
            Entry.reloadOnSave = false;
            Entry.reloadOnUpdate = false;
            Entry.typecastOnAssignment = false;
            Entry.typecastOnLoad = false;
            var testInserts = function (async, testInsertsCallback, disableLogging) {
                var done = 0
                    , start = +new Date()
                    , duration = null
                var createEntry = function () {
                    return new Entry({
                        number:Math.floor(Math.random() * 99999),
                        string:'asdasd'
                    }).save()
                }
                var saves = async ? [] : new comb.Promise().callback();
                for (var i = 0; i < LIMIT; i++) {
                    if (async) {
                        saves.push(createEntry());
                    } else {
                        saves = saves.chain(createEntry);
                    }
                }
                if (async) {
                    saves = new comb.PromiseList(saves);
                }
                saves.then(function () {
                    duration = (+new Date) - start
                    !disableLogging && console.log('Adding ' + LIMIT + ' database entries ' + (async ? 'async' : 'serially') + ' took ' + duration + 'ms')
                    testInsertsCallback(duration)
                });
            }

            var testUpdates = function (async, testUpdatesCallback) {
                Entry.all().then(function (entries) {
                    var done = 0
                        , start = +new Date()
                        , duration = null

                    var updateEntry = function (index) {
                        var entry = entries[index]
                        return entry.update({number:Math.floor(Math.random() * 99999)});
                    }

                    var updates = async ? [] : new comb.Promise().callback();
                    for (var i = 0; i < LIMIT; i++) {
                        if (async) {
                            updates.push(updateEntry(i));
                        } else {
                            updates = updates.chain(comb.partial(updateEntry, i));
                        }
                    }
                    if (async) {
                        updates = new comb.PromiseList(updates);
                    }
                    updates.then(function () {
                        duration = (+new Date) - start
                        console.log('Updating ' + LIMIT + ' database entries ' + (async ? 'async' : 'serially') + ' took ' + duration + 'ms')
                        testUpdatesCallback && testUpdatesCallback(duration)
                    });

                })
            }

            var testRead = function (testReadCallback) {
                var start = +new Date
                    , duration = null

                Entry.all().then(function (entries) {
                    duration = (+new Date) - start
                    console.log('Reading ' + entries.length + ' database entries took ' + duration + 'ms')
                    testReadCallback && testReadCallback(duration)
                })
            }

            var testDelete = function (async, testDeleteCallback) {

                Entry.all().then(function (entries) {
                    var start = +new Date()
                        , duration = null
                    var deleteEntry = function (index) {
                        return entries[index].remove();
                    }

                    var deletes = async ? [] : new comb.Promise().callback();
                    for (var i = 0; i < LIMIT; i++) {
                        if (async) {
                            deletes.push(deleteEntry(i));
                        } else {
                            deletes = deletes.chain(comb.partial(deleteEntry, i));
                        }
                    }
                    if (async) {
                        deletes = new comb.PromiseList(deletes);
                    }
                    deletes.then(function () {
                        duration = (+new Date) - start
                        console.log('Deleting ' + LIMIT + ' database entries ' + (async ? 'async' : 'serially') + ' took ' + duration + 'ms')
                        testDeleteCallback && testDeleteCallback(duration)
                    });
                })
            }

            console.log('\nRunning patio tests #' + (done + 1))

            var results = {}

            testInserts(false, function (duration) {
                results.insertSerially = duration

                testInserts(true, function (duration) {
                    results.insertAsync = duration

                    testUpdates(false, function (duration) {
                        results.updateSerially = duration

                        testUpdates(true, function (duration) {
                            results.updateAsync = duration

                            testRead(function (duration) {
                                results.read = duration

                                testDelete(false, function (duration) {
                                    results.deleteSerially = duration

                                    testDelete(true, function (duration) {
                                        results.deleteAsync = duration

                                        durations.push(results)
                                        callback && callback()
                                    })
                                })
                            })
                        })
                    })
                })
            })
        }

        var runTestsOnceCallback = function () {
            if (++done == times)
                runCallback && runCallback(durations)
            else
                runTestsOnce(runTestsOnceCallback)
        }

        runTestsOnce(runTestsOnceCallback)
    });
};


var printDurations = function (lib, durations) {
    console.log()

    for (var testName in durations[0]) {
        var sum = 0
            , msg = "{{lib}}#{{testName}} ({{times}} runs): {{duration}}ms"

        durations.forEach(function (res) {
            sum += res[testName]
        })

        msg = msg
            .replace('{{lib}}', lib)
            .replace('{{testName}}', testName)
            .replace('{{times}}', durations.length)
            .replace('{{duration}}', sum / durations.length)

        console.log(msg)
    }
}

bench(TIMES, LIMIT, function (patioDurations) {
    printDurations('patio', patioDurations)
});