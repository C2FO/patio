var comb = require("comb"),
    merge = comb.merge;
/**
 * @ignore
 * @name patio.associations
 * @namespace
 * */
comb(exports).merge({
    oneToMany:require("./oneToMany"),
    manyToOne:require("./manyToOne"),
    oneToOne:require("./oneToOne"),
    manyToMany:require("./manyToMany"),
    fetch:{
        LAZY:"lazy",
        EAGER:"eager"
    }
});

