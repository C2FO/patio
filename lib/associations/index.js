var comb = require("comb");
/**
 * @name patio.associations
 * @namespace
 * */
comb.merge(exports, {
    oneToMany:require("./oneToMany"),
    manyToOne:require("./manyToOne"),
    oneToOne:require("./oneToOne"),
    manyToMany:require("./manyToMany"),
    fetch:{
        LAZY:"lazy",
        EAGER:"eager"
    }
});

