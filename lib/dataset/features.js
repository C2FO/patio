var comb = require("comb");

comb.define(null, {
  /**@ignore*/
  instance:{
    __providesAccurateRowsMatched:true,
    __requiresSqlStandardDateTimes:false,
    __supportsCte:true,
    __supportsDistinctOn:false,
    __supportsIntersectExcept:true,
    __supportsIntersectExceptAll:true,
    __supportsIsTrue:true,
    __supportsJoinUsing:true,
    __supportsModifyingJoins:false,
    __supportsMultipleColumnIn:true,
    __supportsTimestampTimezones:false,
    __supportsTimestampUsecs:true,
    __supportsWindowFunctions:false,

    /**@ignore*/
    getters:{

      // Whether this dataset quotes identifiers.
      quoteIdentifiers:function() {
        return this.__quoteIdentifiers;
      },

      // Whether this dataset will provide accurate number of rows matched for
      // delete and update statements.  Accurate in this case is the number of
      // rows matched by the dataset's filter.
      providesAccurateRowsMatched:function() {
        return this.__providesAccurateRowsMatched;
      },

      //Whether the dataset requires SQL standard datetimes (false by default,
      // as most allow strings with ISO 8601 format).
      requiresSqlStandardDateTimes:function() {
        return this.__requiresSqlStandardDateTimes;
      },

      // Whether the dataset supports common table expressions (the WITH clause).
      supportsCte:function() {
        return this.__supportsCte;
      },

      // Whether the dataset supports the DISTINCT ON clause, false by default.
      supportsDistinctOn:function() {
        return this.__supportsDistinctOn;
      },

      //Whether the dataset supports the INTERSECT and EXCEPT compound operations, true by default.
      supportsIntersectExcept:function() {
        return this.__supportsIntersectExcept;
      },

      //Whether the dataset supports the INTERSECT ALL and EXCEPT ALL compound operations, true by default.
      supportsIntersectExceptAll:function() {
        return this.__supportsIntersectExceptAll;
      },

      //Whether the dataset supports the IS TRUE syntax.
      supportsIsTrue:function() {
        return this.__supportsIsTrue;
      },

      //Whether the dataset supports the JOIN table USING (column1, ...) syntax.
      supportsJoinUsing:function() {
        return this.__supportsJoinUsing;
      },

      //Whether modifying joined datasets is supported.
      supportsModifyingJoins:function() {
        return this.__supportsModifyingJoins;
      },

      //Whether the IN/NOT IN operators support multiple columns when an
      supportsMultipleColumnIn:function() {
        return this.__supportsMultipleColumnIn;
      },

      //Whether the dataset supports timezones in literal timestamps
      supportsTimestampTimezones:function() {
        return this.__supportsTimestampTimezones;
      },

      //Whether the dataset supports fractional seconds in literal timestamps
      supportsTimestampUsecs:function() {
        return this.__supportsTimestampUsecs;
      },

      //Whether the dataset supports window functions.
      supportsWindowFunctions:function() {
        return this.__supportsWindowFunctions;
      }

    },

    /**@ignore*/
    setters:{

      // Whether this dataset quotes identifiers.
      quoteIdentifiers:function(val) {
        this.__quoteIdentifiers = val;
      },

      // Whether this dataset will provide accurate number of rows matched for
      // delete and update statements.  Accurate in this case is the number of
      // rows matched by the dataset's filter.
      providesAccurateRowsMatched:function(val) {
        this.__providesAccurateRowsMatched = val;
      },

      //Whether the dataset requires SQL standard datetimes (false by default,
      // as most allow strings with ISO 8601 format).
      requiresSqlStandardDateTimes:function(val) {
        this.__requiresSqlStandardDateTimes = val;
      },

      // Whether the dataset supports common table expressions (the WITH clause).
      supportsCte:function(val) {
        this.__supportsCte = val;
      },

      // Whether the dataset supports the DISTINCT ON clause, false by default.
      supportsDistinctOn:function(val) {
        this.__supportsDistinctOn = val;
      },

      //Whether the dataset supports the INTERSECT and EXCEPT compound operations, true by default.
      supportsIntersectExcept:function(val) {
        this.__supportsIntersectExcept = val;
      },

      //Whether the dataset supports the INTERSECT ALL and EXCEPT ALL compound operations, true by default.
      supportsIntersectExceptAll:function(val) {
        this.__supportsIntersectExceptAll = val;
      },

      //Whether the dataset supports the IS TRUE syntax.
      supportsIsTrue:function(val) {
        this.__supportsIsTrue = val;
      },

      //Whether the dataset supports the JOIN table USING (column1, ...) syntax.
      supportsJoinUsing:function(val) {
        this.__supportsJoinUsing = val;
      },

      //Whether modifying joined datasets is supported.
      supportsModifyingJoins:function(val) {
        this.__supportsModifyingJoins = val;
      },

      //Whether the IN/NOT IN operators support multiple columns when an
      supportsMultipleColumnIn:function(val) {
        this.__supportsMultipleColumnIn = val;
      },

      //Whether the dataset supports timezones in literal timestamps
      supportsTimestampTimezones:function(val) {
        this.__supportsTimestampTimezones = val;
      },

      //Whether the dataset supports fractional seconds in literal timestamps
      supportsTimestampUsecs:function(val) {
        this.__supportsTimestampUsecs = val;
      },

      //Whether the dataset supports window functions.
      supportsWindowFunctions:function(val) {
        this.__supportsWindowFunctions = val;
      }

    }

  },

  static:{
    /**@lends patio.Dataset*/

    /**
     * @property {String[]}
     * @default ["quoteIdentifiers","providesAccurateRowsMatched","requiresSqlStandardDateTimes","supportsCte",
     * "supportsDistinctOn","supportsIntersectExcept","supportsIntersectExceptAll","supportsIsTrue","supportsJoinUsing",
     * "supportsModifyingJoins","supportsMultipleColumnIn","supportsTimestampTimezones","supportsTimestampUsecs",
     * "supportsWindowFunctions"]
     * Array of features.
     */
    FEATURES:["quoteIdentifiers", "providesAccurateRowsMatched", "requiresSqlStandardDateTimes", "supportsCte",
      "supportsDistinctOn", "supportsIntersectExcept", "supportsIntersectExceptAll", "supportsIsTrue", "supportsJoinUsing",
      "supportsModifyingJoins", "supportsMultipleColumnIn", "supportsTimestampTimezones", "supportsTimestampUsecs",
      "supportsWindowFunctions"]
  }
}).as(module);
