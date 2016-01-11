var pg = require("./postgres"),
    PostgresDatabase = pg.PostgresDatabase,
    PostgresDataset = pg.PostgresDataset;

var Dataset = PostgresDataset.extend({
    instance: {
        //redshift does not support returning
        _insertReturningSql: function (sql) {
            return "";
        }
    }
});

PostgresDatabase.extend({
    instance: {
        //handle serial type
        __typeLiteralGenericInteger: function (column) {
            return column.serial ? "bigint identity(0, 1)" : this.__typeLiteralSpecific(column);
        },

        __columnDefinitionSql: function (column) {
            var ret = this._super(arguments);
            if (column.distKey) {
                ret += " distkey";
            }
            if (column.sortKey) {
                ret += " sortkey";
            }
            return ret;
        },

        //Use MySQL specific syntax for engine type and character encoding
        __createTableSql: function (name, generator, options) {
            options = options || {};
            var distStyle = options.distStyle;
            distStyle = distStyle ? " diststyle " + distStyle : "";
            return [this._super(arguments), distStyle].join("");
        },

        getters: {

            dataset: function () {
                return new Dataset(this);
            }
        }
    },

    "static": {

        PRIMARY_KEY: " primary key",

        init: function () {
            this.setAdapterType("redshift");
        }

    }
}).as(exports, "RedshiftDatabase");