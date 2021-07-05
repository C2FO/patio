const stream = require("stream");
const comb = require("comb");
const isPromiseLike = comb.isPromiseLike;
const Promise = comb.Promise;
const hitch = comb.hitch;

export function pipeAll(source, dest) {
    source.on("error", function (err) {
        dest.emit("error", err);
    });
    source.pipe(dest);
}

export function resolveOrPromisfyFunction(cb, scope, ...args) {
    const promise = new Promise();
    let ret;
    const cbRet = cb.apply(scope, args.concat([hitch(promise, promise.resolve)]));
    if (cbRet && isPromiseLike(cbRet, Promise)) {
        ret = cbRet;
    } else {
        ret = promise;
    }
    return ret;
}

