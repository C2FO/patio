var comb = require("comb"),
    hitch = comb.hitch,
    logging = comb.logging,
    Logger = logging.Logger,
    DatasetError = require("../errors").DatasetError,
    util = require('util'),
    Promise = comb.Promise,
    PromiseList = comb.PromiseList,
    graph = require("./graph"),
    actions = require("./actions"),
    features = require("./features"),
    query = require("./query"),
    sql = require("./sql");

var moose, adapter;

/**
 * @class Wrapper for {@link SQL} adpaters to allow execution functions such as:
 * <ul>
 *     <li>forEach</li>
 *     <li>one</li>
 *     <li>all</li>
 *     <li>first</li>
 *     <li>last</li>
 *     <li>all</li>
 *     <li>save</li>
 * </ul>
 *
 * This class should be used insead of SQL directly, becuase:
 * <ul>
 *     <li>Allows for Model creation if needed</li>
 *     <li>Handles the massaging of data to make the use of results easier.</li>
 *     <li>Closing of database connections</li>
 * </ul>
 * @name Dataset
 * @augments SQL
 *
 *
 */

var LOGGER = Logger.getLogger("moose.Dataset");
console.log("++++++++++++++++CALLED+++++++++++++++++");
comb.define([actions, graph, features, query, sql], {
    instance : {
        constructor : function(db, opts) {
            this.super(arguments);
            this.db = db;
            this.__opts = {};
            this.__rowCb = null;
            if (db) {
                this.__quoteIdentifiers = db.quoteIdentifiers;
                this.__identifierInputMethod = db.identifierInputMethod;
                this.__identifierOutputMethod = db.identifierOutputMethod;
            }
        },

        getters : {
            rowCb : function() {
                return this.__rowCb;
            }
        },

        setters : {
            rowCb : function(cb) {
                if (comb.isFunction(cb) || comb.isNull(cb)) {
                    this.__rowCb = cb;
                } else {
                    throw new DatasetError("rowCb mus be a function");
                }
            }
        }
    }
}).export(module);


/*//returns a dataset for a particular type
 exports.getDataSet = function(table, db, type, model) {
 if(!moose){
 moose = require("../index.js"), adapter = moose.adapter;
 }
 var dataset = comb.define([adapter, Dataset], {});
 return new dataset(table, db, type, model);
 }; */

