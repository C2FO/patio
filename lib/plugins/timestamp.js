var define = require("comb").define, DateTime;

/**
 * @class Time stamp plugin to support creating timestamp
 *
 * @example
 *
 * //create your model and register the plugin.
 * var MyModel = patio.addModel("testTable", {
 *     plugins : [patio.plugins.TimeStampPlugin];
 * });
 *
 * //initialize default timestamp functionality
 * MyModel.timestamp();
 *
 * //Or
 *
 * //initialize custom update column
 * MyModel.timestamp({updated : "myUpdateColumn"});
 *
 * //Or
 *
 * //initialize custom created column
 * MyModel.timestamp({created : "myCreatedColumn"});
 *
 * //Or
 *
 * //Set to update the updated column when row is created
 * MyModel.timestamp({updateOnCreate : true});
 *
 * //Or
 *
 * //Set both custom columns
 * MyModel.timestamp({updated : "myUpdateColumn", created : "myCreatedColumn"});
 *
 * //Or
 *
 * //Use all three options!
 * MyModel.timestamp({
 *          updated : "myUpdateColumn",
 *          created : "myCreatedColumn",
 *          updateOnCreate : true
 * });
 *
 *
 * @name TimeStampPlugin
 * @memberOf patio.plugins
 */
module.exports = exports = define(null, {

  static:{
    /**@lends patio.plugins.TimeStampPlugin*/

    /**
     * Adds timestamp functionality to a table.
     * @param {Object} [options]
     * @param {String} [options.updated="updated"] the name of the column to set the updated timestamp on.
     * @param {String} [options.created="created"] the name of the column to set the created timestamp on
     * @param {Boolean} [options.updateOnCreate=false] Set to true to set the updated column on creation
     **/
    timestamp:function(options) {
      options = options || {};
      var updateColumn = options.updated || "updated";
      var createdColumn = options.created || "created";
      var updateOnCreate = options.updateOnCreate || false;
      this.pre("save", function(next) {
        this[createdColumn] = new Date();
        if (updateOnCreate) {
          this[updateColumn] = new Date();
        }
        next();
      });
      this.pre("update", function(next) {
        this[updateColumn] = new Date();
        next();
      });
    }
  }

});