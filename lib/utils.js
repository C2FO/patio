var stream = require("stream"),
    comb = require("comb"),
    isPromiseLike = comb.isPromiseLike,
    Promise = comb.Promise,
    hitch = comb.hitch;

function pipeAll(source, dest) {
    source.on("error", function (err) {
        dest.emit("error", err);
    });
    source.pipe(dest);
}

function resolveOrPromisfyFunction(cb, scope, args) {
    args = comb.argsToArray(arguments, 2);
    var promise = new Promise(), ret;
    var cbRet = cb.apply(scope, args.concat([hitch(promise, promise.resolve)]));
    if (cbRet && isPromiseLike(cbRet, Promise)) {
        ret = cbRet;
    } else {
        ret = promise;
    }
    return ret;
}

exports.pipeAll = pipeAll;
exports.resolveOrPromisfyFunction = resolveOrPromisfyFunction;
