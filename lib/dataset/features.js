var comb = require("comb"),
    define = comb.define;

define({
    /**@ignore*/
    instance: {
        __providesAccurateRowsMatched: true,
        __requiresSqlStandardDateTimes: false,
        __supportsCte: true,
        __supportsDistinctOn: false,
        __supportsIntersectExcept: true,
        __supportsIntersectExceptAll: true,
        __supportsIsTrue: true,
        __supportsJoinUsing: true,
        __supportsModifyingJoins: false,
        __supportsMultipleColumnIn: true,
        __supportsTimestampTimezones: false,
        __supportsTimestampUsecs: true,
        __supportsWindowFunctions: false,

        /**@ignore*/
        getters: {
            /**@lends patio.Dataset.prototype*/
            // Whether this dataset quotes identifiers.
            /**@ignore*/
            quoteIdentifiers: function () {
                return this.__quoteIdentifiers;
            },

            // Whether this dataset will provide accurate number of rows matched for
            // delete and update statements.  Accurate in this case is the number of
            // rows matched by the dataset's filter.
            /**@ignore*/
            providesAccurateRowsMatched: function () {
                return this.__providesAccurateRowsMatched;
            },

            //Whether the dataset requires SQL standard datetimes (false by default,
            // as most allow strings with ISO 8601 format).
            /**@ignore*/
            requiresSqlStandardDateTimes: function () {
                return this.__requiresSqlStandardDateTimes;
            },

            // Whether the dataset supports common table expressions (the WITH clause).
            /**@ignore*/
            supportsCte: function () {
                return this.__supportsCte;
            },

            // Whether the dataset supports the DISTINCT ON clause, false by default.
            /**@ignore*/
            supportsDistinctOn: function () {
                return this.__supportsDistinctOn;
            },

            //Whether the dataset supports the INTERSECT and EXCEPT compound operations, true by default.
            /**@ignore*/
            supportsIntersectExcept: function () {
                return this.__supportsIntersectExcept;
            },

            //Whether the dataset supports the INTERSECT ALL and EXCEPT ALL compound operations, true by default.
            /**@ignore*/
            supportsIntersectExceptAll: function () {
                return this.__supportsIntersectExceptAll;
            },

            //Whether the dataset supports the IS TRUE syntax.
            /**@ignore*/
            supportsIsTrue: function () {
                return this.__supportsIsTrue;
            },

            //Whether the dataset supports the JOIN table USING (column1, ...) syntax.
            /**@ignore*/
            supportsJoinUsing: function () {
                return this.__supportsJoinUsing;
            },

            //Whether modifying joined datasets is supported.
            /**@ignore*/
            supportsModifyingJoins: function () {
                return this.__supportsModifyingJoins;
            },

            //Whether the IN/NOT IN operators support multiple columns when an
            /**@ignore*/
            supportsMultipleColumnIn: function () {
                return this.__supportsMultipleColumnIn;
            },

            //Whether the dataset supports timezones in literal timestamps
            /**@ignore*/
            supportsTimestampTimezones: function () {
                return this.__supportsTimestampTimezones;
            },

            //Whether the dataset supports fractional seconds in literal timestamps
            /**@ignore*/
            supportsTimestampUsecs: function () {
                return this.__supportsTimestampUsecs;
            },

            //Whether the dataset supports window functions.
            /**@ignore*/
            supportsWindowFunctions: function () {
                return this.__supportsWindowFunctions;
            }

        },

        /**@ignore*/
        setters: {
            /**@lends patio.Dataset.prototype*/
            // Whether this dataset quotes identifiers.
            /**@ignore*/
            quoteIdentifiers: function (val) {
                this.__quoteIdentifiers = val;
            },

            // Whether this dataset will provide accurate number of rows matched for
            // delete and update statements.  Accurate in this case is the number of
            // rows matched by the dataset's filter.
            /**@ignore*/
            providesAccurateRowsMatched: function (val) {
                this.__providesAccurateRowsMatched = val;
            },

            //Whether the dataset requires SQL standard datetimes (false by default,
            // as most allow strings with ISO 8601 format).
            /**@ignore*/
            requiresSqlStandardDateTimes: function (val) {
                this.__requiresSqlStandardDateTimes = val;
            },

            // Whether the dataset supports common table expressions (the WITH clause).
            /**@ignore*/
            supportsCte: function (val) {
                this.__supportsCte = val;
            },

            // Whether the dataset supports the DISTINCT ON clause, false by default.
            /**@ignore*/
            supportsDistinctOn: function (val) {
                this.__supportsDistinctOn = val;
            },

            //Whether the dataset supports the INTERSECT and EXCEPT compound operations, true by default.
            /**@ignore*/
            supportsIntersectExcept: function (val) {
                this.__supportsIntersectExcept = val;
            },

            //Whether the dataset supports the INTERSECT ALL and EXCEPT ALL compound operations, true by default.
            /**@ignore*/
            supportsIntersectExceptAll: function (val) {
                this.__supportsIntersectExceptAll = val;
            },

            //Whether the dataset supports the IS TRUE syntax.
            /**@ignore*/
            supportsIsTrue: function (val) {
                this.__supportsIsTrue = val;
            },

            //Whether the dataset supports the JOIN table USING (column1, ...) syntax.
            /**@ignore*/
            supportsJoinUsing: function (val) {
                this.__supportsJoinUsing = val;
            },

            //Whether modifying joined datasets is supported.
            /**@ignore*/
            supportsModifyingJoins: function (val) {
                this.__supportsModifyingJoins = val;
            },

            //Whether the IN/NOT IN operators support multiple columns when an
            /**@ignore*/
            supportsMultipleColumnIn: function (val) {
                this.__supportsMultipleColumnIn = val;
            },

            //Whether the dataset supports timezones in literal timestamps
            /**@ignore*/
            supportsTimestampTimezones: function (val) {
                this.__supportsTimestampTimezones = val;
            },

            //Whether the dataset supports fractional seconds in literal timestamps
            /**@ignore*/
            supportsTimestampUsecs: function (val) {
                this.__supportsTimestampUsecs = val;
            },

            //Whether the dataset supports window functions.
            /**@ignore*/
            supportsWindowFunctions: function (val) {
                this.__supportsWindowFunctions = val;
            }

        }

    },

    static: {
        /**@lends patio.Dataset*/

        /**
         * @property {String[]}
         * @default ["quoteIdentifiers","providesAccurateRowsMatched","requiresSqlStandardDateTimes","supportsCte",
         * "supportsDistinctOn","supportsIntersectExcept","supportsIntersectExceptAll","supportsIsTrue","supportsJoinUsing",
         * "supportsModifyingJoins","supportsMultipleColumnIn","supportsTimestampTimezones","supportsTimestampUsecs",
         * "supportsWindowFunctions"]
         * Array of features.
         */
        FEATURES: ["quoteIdentifiers", "providesAccurateRowsMatched", "requiresSqlStandardDateTimes", "supportsCte",
            "supportsDistinctOn", "supportsIntersectExcept", "supportsIntersectExceptAll", "supportsIsTrue", "supportsJoinUsing",
            "supportsModifyingJoins", "supportsMultipleColumnIn", "supportsTimestampTimezones", "supportsTimestampUsecs",
            "supportsWindowFunctions"]
    }
}).as(module);
