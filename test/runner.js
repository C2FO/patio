var fs = require("fs"), path = require("path"), exec = require("child_process").exec, sys = require("sys"), comb = require("comb"), string = comb.string;

/**
 * Coverage reporting based on https://github.com/visionmedia/expresso
 *
 * Copyright (c) 2010 TJ Holowaychuk <tj@vision-media.ca>
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the 'Software'), to deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 */

var COVERAGE_HEADER = string.format('\n %s \n', string.style("Test Coverage", "bold"));
var TABLE_SEP = string.format("+%--42s+%--11s+%--6s+%--7s+%--7s+", "-", "-", "-", "-", "-");
TABLE_SEP = comb.hitch(string, "format", TABLE_SEP + "\n%s\n" + TABLE_SEP)
var PRINT_FORMAT = comb.hitch(string, "format", "| %-40s | %9s | %4s | %5s | %6s|");
/**
 * Report test coverage in tabular format
 *
 * @param  {Object} cov
 */

var reportCoverage = process.argv[2];
var fileMatcher = /.js$/;

var printFile = function(file) {
    sys.error(PRINT_FORMAT(file.name, "" + file.coverage, "" + file.LOC, "" + file.SLOC, "" + file.totalMisses));
};

var printFileSource = function(file) {
    if (file.coverage < 100 && file.name == "index.js") {
        sys.error(string.format('\n %s \n %s \n' + string.style(file.name, "bold"), file.source));
    }
}

function reportCoverageTable(cov) {
    // Stats
    var print = sys.error;
    print(COVERAGE_HEADER);
    print(TABLE_SEP(PRINT_FORMAT('filename', 'coverage', 'LOC', 'SLOC', 'missed')));
    cov.files.forEach(printFile);
    print(TABLE_SEP(PRINT_FORMAT("Total", "" + cov.coverage, "" + cov.LOC, "" + cov.SLOC, "" + cov.totalMisses)));
    // Source
    cov.files.forEach(printFileSource);
}

function coverage(data, val) {
    var n = 0;
    for (var i = 0, len = data.length; i < len; ++i) {
        if (data[i] !== undefined && Boolean(data[i]) == val) ++n;
    }
    return n;
}

function populateCoverage(cov) {
    cov.LOC = cov.SLOC = cov.totalHits = cov.totalMisses = cov.coverage = 0;
    var files = [];
    for (var name in cov) {
        var file = cov[name];
        if (comb.isArray(file)) {
            // Stats
            files.push(file);
            delete cov[name];
            cov.totalHits += file.totalHits = coverage(file, true);
            cov.totalMisses += file.totalMisses = coverage(file, false);
            cov.SLOC += file.SLOC = file.totalHits + file.totalMisses;
            !file.source && (file.source = []);
            cov.LOC += file.LOC = file.source.length;
            file.coverage = ((file.totalHits / file.SLOC) * 100).toFixed(2);
            // Source
            file.name = name;
            if (file.coverage < 100) {
                var width = file.source.length.toString().length;
                file.source = file.source.map(
                        function(line, i) {
                            ++i;
                            var hits = file[i] === 0 ? 0 : (file[i] || ' ');
                            if (hits === 0) {
                                hits = string.style(string.pad(hits, 5, null, true), ["bold", "red"]);
                                line = string.style(line, "redBackground");
                            } else {
                                hits = string.style(string.pad(hits, 5, null, true), "bold");
                            }
                            return string.format('\n %-' + width + 's | %s | %s', "" + i, "" + hits, "" + line);
                        }).join('');
            }
        }
    }
    cov.coverage = ((cov.totalHits / cov.SLOC) * 100).toFixed(2);
    cov.files = files;
}

var showCoverage = function() {
    if (typeof _$jscoverage === 'object') {
        populateCoverage(_$jscoverage);
        if (_$jscoverage.coverage) {
            reportCoverageTable(_$jscoverage);
        }
    }
}


var runTests = function(files) {
    var ret = new comb.Promise();
    (function run(files) {
        var f = files.shift();
        if (f) {
            console.log("RUNNING %s", f);
            require(f).then(comb.partial(run, files));
        } else {
            ret.callback();
        }
    })(files);
    return ret;
}

var startTests = function() {
    var ret = new comb.Promise();
    exec("find " + __dirname + " -name *.test.js", function(err, stdout) {
        if (err) ret.errback();
        var files = stdout.split("\n");
        if (files.length) {

            if (reportCoverage && reportCoverage == "coverage") {
                exec('rm -fr ' + __dirname + "/../lib-cov && node-jscoverage " + __dirname + "/../lib " + __dirname + "/../lib-cov", function(err) {
                    if (err) {
                        ret.errback(err);
                    } else {
                        require.paths.unshift(__dirname + "/../lib-cov");
                        runTests(files).then(comb.hitch(ret, "callback"), comb.hitch(ret, "errback"));
                    }
                });
            } else {
                require.paths.unshift(__dirname + "/../lib");
                runTests(files).then(comb.hitch(ret, "callback"), comb.hitch(ret, "errback"));
            }
        } else {
            ret.callback();
        }
    });
    return ret;
};

startTests().addErrback(function(error) {
    console.log(error.stack);
});

var orig = process.emit;
process.emit = function(event) {
    orig.apply(this, arguments);
    if (event === 'exit' && reportCoverage) {
        showCoverage();
    }
};
