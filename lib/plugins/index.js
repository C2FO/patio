var comb = require("comb"), inheritance = require("./inheritance");
/**
 * @ignore
 * @namespace
 * @name patio.plugins
 */
comb.merge(exports, {
    QueryPlugin:require("./query").QueryPlugin,
    CachePlugin:require("./cache").CachePlugin,
    AssociationPlugin:require("./association").AssociationPlugin,
    TimeStampPlugin:require("./timestamp"),
    ClassTableInheritancePlugin:inheritance.ClassTableInheritance,
    ColumnMapper:require("./columnMapper.js"),
    ValidatorPlugin:require("./validation.js")
});

