var define = require("comb").define, DateTime;

/**
 * @class Time stamp plugin to support creating timestamp
 *
 * @example
 *
 * //initialize default timestamp functionality
 * var MyModel = patio.addModel("testTable", {
 *     plugins : [patio.plugins.TimeStampPlugin],
 *
 *     static : {
 *         init : function(){
 *             this._super("arguments");
 *             this.timestamp();
 *         }
 *     }
 * });
 *
 *
 *
 * //custom updated column
 * var MyModel = patio.addModel("testTable", {
 *     plugins : [patio.plugins.TimeStampPlugin],
 *
 *     static : {
 *         init : function(){
 *             this._super("arguments");
 *             this.timestamp({updated : "myUpdatedColumn"});
 *         }
 *     }
 * });
 *
 * //custom created column
 * var MyModel = patio.addModel("testTable", {
 *     plugins : [patio.plugins.TimeStampPlugin],
 *
 *     static : {
 *         init : function(){
 *             this._super("arguments");
 *             this.timestamp({created : "customCreatedColumn"});
 *         }
 *     }
 * });
 *
 * //set both custom columns
 * var MyModel = patio.addModel("testTable", {
 *     plugins : [patio.plugins.TimeStampPlugin],
 *
 *     static : {
 *         init : function(){
 *             this._super("arguments");
 *             this.timestamp({created : "customCreatedColumn", updated : "myUpdatedColumn"});
 *         }
 *     }
 * });
 *
 * //Set to update the updated column when row is created
 * var MyModel = patio.addModel("testTable", {
 *     plugins : [patio.plugins.TimeStampPlugin],
 *
 *     static : {
 *         init : function(){
 *             this._super("arguments");
 *             this.timestamp({updateOnCreate : true});
 *         }
 *     }
 * });
 *
 * //Set all three options
 * var MyModel = patio.addModel("testTable", {
 *     plugins : [patio.plugins.TimeStampPlugin],
 *
 *     static : {
 *         init : function(){
 *             this._super("arguments");
 *             this.timestamp({created : "customCreatedColumn", updated : "myUpdatedColumn", updateOnCreate : true});
 *         }
 *     }
 * });
 *
 *
 *
 *
 * @name TimeStampPlugin
 * @memberOf patio.plugins
 */
module.exports = exports = define(null, {

    instance:{

        constructor:function () {
            this._super(arguments);
            var options = (this._timestampOptions = this._static._timestampOptions);
            this._updateColumn = options.updated || "updated";
            this._createdColumn = options.created || "created";
            this._updateOnCreate = options.updateOnCreate || false;
        },

        getInsertSql:function () {
            this[this._createdColumn] = new Date();
            if (this._updateOnCreate) {
                this[this._updateColumn] = new Date();
            }
            return this._super(arguments);
        },

        getUpdateSql:function () {
            this[this._updateColumn] = new Date();
            return this._super(arguments);
        }
    },

    static:{
        /**@lends patio.plugins.TimeStampPlugin*/

        /**
         * Adds timestamp functionality to a table.
         * @param {Object} [options]
         * @param {String} [options.updated="updated"] the name of the column to set the updated timestamp on.
         * @param {String} [options.created="created"] the name of the column to set the created timestamp on
         * @param {Boolean} [options.updateOnCreate=false] Set to true to set the updated column on creation
         **/
        timestamp:function (options) {
            options = options || {};
            this._timestampOptions = options;
            var updateColumn = options.updated || "updated";
            var createdColumn = options.created || "created";
            var updateOnCreate = options.updateOnCreate || false;
            this.pre("save", function (next) {
                this[createdColumn] = new Date();
                if (updateOnCreate) {
                    this[updateColumn] = new Date();
                }
                next();
            });
            this.pre("update", function (next) {
                this[updateColumn] = new Date();
                next();
            });
            return this;
        }
    }

});