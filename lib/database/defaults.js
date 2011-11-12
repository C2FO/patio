var comb = require("comb");

comb.define(null, {
    instance : {

        __supportsTransactionIsolationLevels : false,

        __supportsSavePoints : false,

        __supportsPreparedTransactions : false,

        constructor : function(opts) {
            this.super(arguments);
            var static  = this.static;
            this.__identifierInputMethod = comb.isUndefined(opts.identifierInputMethod) ? comb.isUndefinedOrNull(static.identifierInputMethod) ? this.identifierInputMethodDefault : static.identifierInputMethod : opts.identifierInputMethod;
            this.__identifierOutputMethod = comb.isUndefined(opts.identifierOutputMethod) ? comb.isUndefinedOrNull(static.identifierOutputMethod) ? this.identifierOutputMethodDefault : static.identifierOutputMethod : opts.identifierOutputMethod;
            this.__quoteIdentifiers = comb.isUndefined(opts.quoteIdentifiers) ? comb.isUndefinedOrNull(static.quoteIdentifiers) ? this.quoteIdentifiersDefault : static.quoteIdentifiers : opts.quoteIdentifiers;
        },

        getters : {
            // The default options for the connection pool.
            connectionPoolDefaultOptions : function() {
                return {}
            },

            defaultSchemaDefault : function() {
                return null;
            },

            identifierInputMethodDefault : function() {
                return "toUpperCase";
            },

            identifierOutputMethodDefault : function() {
                return "toLowerCase";
            },

            quoteIdentifiersDefault : function() {
                return true;
            },

            /*
             * Default serial primary key options, used by the table creation
             * code.
             * */
            serialPrimaryKeyOptions : function() {
                return {primaryKey : true, type : "integer", autoIncrement : true};
            },

            /**
             * Whether the database and adapter support prepared transactions
             * (two-phase commit), false by default.
             */
            supportsPreparedTransactions : function() {
                return this.__supportsPreparedTransactions;
            },

            /*
             *Whether the database and adapter support savepoints, false by default.
             */
            supportsSavepoints : function() {
                return this.__supportsSavePoints;
            },

            /*
             * Whether the database and adapter support transaction isolation levels, false by default.
             * */
            supportsTransactionIsolationLevels : function() {
                return this.__supportsTransactionIsolationLevels;
            },

            identifierInputMethod : function() {
                return this.__identifierInputMethod;
            },

            identifierOutputMethod : function() {
                return this.__identifierOutputMethod;
            },

            quoteIdentifiers : function() {
                return this.__quoteIdentifiers;
            }
        },

        setters : {
            identifierInputMethod : function(identifierInputMethod) {
                this.__identifierInputMethod = identifierInputMethod;
            },

            identifierOutputMethod : function(identifierOutputMethod) {
                this.__identifierOutputMethod = identifierOutputMethod;
            },

            quoteIdentifiers : function(quoteIdentifiers) {
                this.__quoteIdentifiers = quoteIdentifiers;
            },

             /*
             * Whether the database and adapter support transaction isolation levels, false by default.
             * */
            supportsTransactionIsolationLevels : function(supports) {
                this.__supportsTransactionIsolationLevels = supports;
            },

            supportsPreparedTransactions : function(supports) {
                this.__supportsPreparedTransactions = supports;
            },

            supportsSavepoints : function(supports){
                this.__supportsSavePoints = supports;
            }
        }
    },

    static : {

        __identifierInputMethod : null,

        __identifierOutputMethod : null,

        __quoteIdentifiers : null,

        getters : {
            identifierInputMethod : function() {
                return this.__identifierInputMethod;
            },

            identifierOutputMethod : function() {
                return this.__identifierOutputMethod;
            },

            quoteIdentifiers : function() {
                return this.__quoteIdentifiers;
            }
        },

        setters : {
            identifierInputMethod : function(identifierInputMethod) {
                this.__identifierInputMethod = identifierInputMethod;
            },

            identifierOutputMethod : function(identifierOutputMethod) {
                this.__identifierOutputMethod = identifierOutputMethod;
            },

            quoteIdentifiers : function(quoteIdentifiers) {
                this.__quoteIdentifiers = quoteIdentifiers;
            }
        }
    }
}).export(module);

/**


 # The default schema to use, generally should be nil.
 attr_accessor :default_schema

 # The method to call on identifiers going into the database
 def identifier_input_method
 case @identifier_input_method
 when nil
 @identifier_input_method = @opts.fetch(:identifier_input_method, (@@identifier_input_method.nil? ? identifier_input_method_default : @@identifier_input_method))
 @identifier_input_method == "" ? nil : @identifier_input_method
 when ""
 nil
 else
 @identifier_input_method
 end
 end

 # Set the method to call on identifiers going into the database:
 #
 #   DB[:items] # SELECT * FROM items
 #   DB.identifier_input_method = :upcase
 #   DB[:items] # SELECT * FROM ITEMS
 def identifier_input_method=(v)
 reset_schema_utility_dataset
 @identifier_input_method = v || ""
 end

 # The method to call on identifiers coming from the database
 def identifier_output_method
 case @identifier_output_method
 when nil
 @identifier_output_method = @opts.fetch(:identifier_output_method, (@@identifier_output_method.nil? ? identifier_output_method_default : @@identifier_output_method))
 @identifier_output_method == "" ? nil : @identifier_output_method
 when ""
 nil
 else
 @identifier_output_method
 end
 end

 # Set the method to call on identifiers coming from the database:
 #
 #   DB[:items].first # {:id=>1, :name=>'foo'}
 #   DB.identifier_output_method = :upcase
 #   DB[:items].first # {:ID=>1, :NAME=>'foo'}
 def identifier_output_method=(v)
 reset_schema_utility_dataset
 @identifier_output_method = v || ""
 end

 # Set whether to quote identifiers (columns and tables) for this database:
 #
 #   DB[:items] # SELECT * FROM items
 #   DB.quote_identifiers = true
 #   DB[:items] # SELECT * FROM "items"
 def quote_identifiers=(v)
 reset_schema_utility_dataset
 @quote_identifiers = v
 end

 # Returns true if the database quotes identifiers.
 def quote_identifiers?
 return @quote_identifiers unless @quote_identifiers.nil?
 @quote_identifiers = @opts.fetch(:quote_identifiers, (@@quote_identifiers.nil? ? quote_identifiers_default : @@quote_identifiers))
 end

 private

 # The default value for default_schema.
 def default_schema_default
 nil
 end

 # The method to apply to identifiers going into the database by default.
 # Should be overridden in subclasses for databases that fold unquoted
 # identifiers to lower case instead of uppercase, such as
 # MySQL, PostgreSQL, and SQLite.
 def identifier_input_method_default
 :upcase
 end

 # The method to apply to identifiers coming the database by default.
 # Should be overridden in subclasses for databases that fold unquoted
 # identifiers to lower case instead of uppercase, such as
 # MySQL, PostgreSQL, and SQLite.
 def identifier_output_method_default
 :downcase
 end

 # Whether to quote identifiers by default for this database, true
 # by default.
 def quote_identifiers_default
 true
 end
 end
 */