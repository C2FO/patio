var comb = require("comb"),
    merge = comb.merge;

var types = {
    BOOL:"bool",
    BOOLEAN:"boolean",
    DATE:"date",
    TIME:"time",
    DATETIME:"datetime",
    TIMESTAMP:"timestamp",
    CHARACTER:"character",
    CHAR:"char",
    VARCHAR:"varchar",
    INTEGER:"int",
    SMALL_INT:"smallint",
    FLOAT:"float",
    REAL:"REAL",
    MEDIUM_INT:"mediumint",
    TINYINT:"tinyint",
    NUMERIC:"numeric",
    DOUBLE_PRECISION:"double precision"
};

for (var i in types) {
    var newI = i.replace("_", "");
    if (!types[newI]) {
        types[newI] = types[i];
    }
}
merge(exports, types);