const comb = require("comb");
const merge = comb.merge;
/**
 * @ignore
 * @name patio.associations
 * @namespace
 * */

export default comb.merge({
    oneToMany:require("./oneToMany"),
    manyToOne:require("./manyToOne"),
    oneToOne:require("./oneToOne"),
    manyToMany:require("./manyToMany"),
    fetch:{
        LAZY:"lazy",
        EAGER:"eager"
    }
});

