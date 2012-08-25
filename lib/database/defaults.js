var comb = require("comb"),
    isUndefined = comb.isUndefined,
    isUndefinedOrNull = comb.isUndefinedOrNull,
    define = comb.define;

define(null, {
    instance:{

        __supportsTransactionIsolationLevels:false,

        __supportsSavePoints:false,

        __supportsPreparedTransactions:false,

        constructor:function (opts) {
            this._super(arguments);
            var statics = this._static;
            this.__identifierInputMethod = isUndefined(opts.identifierInputMethod) ? isUndefined(statics.identifierInputMethod) ? this.identifierInputMethodDefault : statics.identifierInputMethod : opts.identifierInputMethod;
            this.__identifierOutputMethod = isUndefined(opts.identifierOutputMethod) ? isUndefined(statics.identifierOutputMethod) ? this.identifierOutputMethodDefault : statics.identifierOutputMethod : opts.identifierOutputMethod;
            this.__quoteIdentifiers = isUndefined(opts.quoteIdentifiers) ? isUndefinedOrNull(statics.quoteIdentifiers) ? this.quoteIdentifiersDefault : statics.quoteIdentifiers : opts.quoteIdentifiers;
        },

        getters:{
            /**@lends patio.Database.prototype*/

            /**
             * The default options for the connection pool.
             * @field
             * @type Object
             */
            connectionPoolDefaultOptions:function () {
                return {};
            },

            /**
             * Default schema to use. This is generally null but may be overridden by an adapter.
             * @field
             * @type {String|patio.sql.Identifier}
             */
            defaultSchemaDefault:function () {
                return null;
            },

            /**
             * The default String or comb method to use transform identifiers with when
             * sending identifiers to the database.
             * @field
             * @type String
             * @default toUpperCase
             */
            identifierInputMethodDefault:function () {
                return "toUpperCase";
            },

            /**
             * The default String or comb method to use transform identifiers with when
             * they are retrieved from the database.
             * @field
             * @type String
             * @default toLowerCase
             */
            identifierOutputMethodDefault:function () {
                return "toLowerCase";
            },

            /**
             * Default boolean of whether or not to quote identifiers before sending
             * then to the database.
             * @field
             * @type Boolean
             * @default true
             */
            quoteIdentifiersDefault:function () {
                return true;
            },

            /**
             * Default serial primary key options, used by the table creation
             * code.
             * @field
             * @type Object
             * @default {primaryKey : true, type : "integer", autoIncrement : true}
             * */
            serialPrimaryKeyOptions:function () {
                return {primaryKey:true, type:"integer", autoIncrement:true};
            },

            /**
             * Whether the database and adapter support prepared transactions
             * (two-phase commit)
             * @field
             * @type Boolean
             * @default false
             */
            supportsPreparedTransactions:function () {
                return this.__supportsPreparedTransactions;
            },

            /**
             * Whether the database and adapter support savepoints.
             * @field
             * @type Boolean
             * @default false
             */
            supportsSavepoints:function () {
                return this.__supportsSavePoints;
            },

            /**
             * Whether the database and adapter support transaction isolation levels.
             *
             * @field
             * @type Boolean
             * @default false
             * */
            supportsTransactionIsolationLevels:function () {
                return this.__supportsTransactionIsolationLevels;
            },

            /**
             * The String or comb method to use transform identifiers with when
             * sending identifiers to the database. If this property is undefined then
             * {@link patio.Database#identifierInputMethodDefault} will be used.
             *
             * @field
             * @type String
             */
            identifierInputMethod:function () {
                return this.__identifierInputMethod;
            },

            /**
             * The String or comb method to use transform identifiers with when
             * they are retrieved from database. If this property is undefined then
             * {@link patio.Database#identifierOutputMethodDefault} will be used.
             *
             * @field
             * @type String
             */
            identifierOutputMethod:function () {
                return this.__identifierOutputMethod;
            },

            /**
             * Boolean of whether or not to quote identifiers before sending
             * then to the database. If this property is undefined then
             * then {@link patio.Database#quoteIdentifiersDefault} will be used.
             *
             * @field
             * @type Boolean
             * @default true
             */
            quoteIdentifiers:function () {
                return this.__quoteIdentifiers;
            }
        },

        setters:{
            identifierInputMethod:function (identifierInputMethod) {
                this.__identifierInputMethod = identifierInputMethod;
            },

            identifierOutputMethod:function (identifierOutputMethod) {
                this.__identifierOutputMethod = identifierOutputMethod;
            },

            quoteIdentifiers:function (quoteIdentifiers) {
                this.__quoteIdentifiers = quoteIdentifiers;
            },

            supportsTransactionIsolationLevels:function (supports) {
                this.__supportsTransactionIsolationLevels = supports;
            },

            supportsPreparedTransactions:function (supports) {
                this.__supportsPreparedTransactions = supports;
            },

            supportsSavepoints:function (supports) {
                this.__supportsSavePoints = supports;
            }
        }
    },

    "static":{

        __identifierInputMethod:undefined,

        __identifierOutputMethod:undefined,

        __quoteIdentifiers:null,

        getters:{
            /**@lends patio.Database*/

            /**
             * The String or comb method to use transform identifiers with when
             * they are sent to database. See {@link patio#identifierInputMethod}
             *
             * @field
             * @type String
             */
            identifierInputMethod:function () {
                return this.__identifierInputMethod;
            },

            /**
             * The String or comb method to use transform identifiers with when
             * they are retrieved from database. See {@link patio#identifierOutputMethod}
             *
             * @field
             * @type String
             */
            identifierOutputMethod:function () {
                return this.__identifierOutputMethod;
            },

            /**
             * Boolean of whether or not to quote identifiers before sending
             * then to the database. See {@link patio#quoteIdentifiers}
             *
             * @field
             * @type Boolean
             */
            quoteIdentifiers:function () {
                return this.__quoteIdentifiers;
            }
        },

        setters:{
            identifierInputMethod:function (identifierInputMethod) {
                this.__identifierInputMethod = identifierInputMethod;
            },

            identifierOutputMethod:function (identifierOutputMethod) {
                this.__identifierOutputMethod = identifierOutputMethod;
            },

            quoteIdentifiers:function (quoteIdentifiers) {
                this.__quoteIdentifiers = quoteIdentifiers;
            }
        }
    }
}).as(module);
