var comb = require("comb"),
    array = comb.array,
    ExpressionError = require("./errors").ExpressionError,
    Dataset, moose;


var virtualRow = function (name) {
    var WILDCARD = new LiteralString('*');
    var QUESTION_MARK = new LiteralString('?');
    var COMMA_SEPARATOR = new LiteralString(', ');
    var DOUBLE_UNDERSCORE = '__';

    var parts = name.split(DOUBLE_UNDERSCORE);
    var table = parts[0], column = parts[1];
    var ident = column ? QualifiedIdentifier.fromArgs([table, column]) : Identifier.fromArgs([name]);
    var prox = comb.methodMissing(ident, function (m) {
        return function () {
            var args = comb.argsToArray(arguments);
            return SQLFunction.fromArgs([m, name].concat(args));
        }
    }, column ? QualifiedIdentifier : Identifier);
    var ret = comb.createFunctionWrapper(prox, function (m) {
        var args = comb.argsToArray(arguments);
        if (args.length) {
            return SQLFunction.fromArgs([name].concat(args));
        } else {
            return prox;
        }
    }, function () {
        return SQLFunction.fromArgs(arguments);
    });
    ret.__proto__ = ident;
    return ret;
};

var DATE_METHODS = ["getDate", "getDay", "getFullYear", "getHours", "getMilliseconds", "getMinutes", "getMonth", "getSeconds",
    "getTime", "getTimezoneOffset", "getUTCDate", "getUTCDay", "getUTCFullYear", "getUTCHours", "getUTCMilliseconds",
    "getUTCMinutes", "getUTCMonth", "getUTCSeconds", "getYear", "parse", "setDate", "setFullYear", "setHours", "setMilliseconds",
    "setMinutes", "setMonth", "setSeconds", "setTime", "setUTCDate", "setUTCFullYear", "setUTCHours", "setUTCMilliseconds",
    "setUTCMinutes", "setUTCMonth", "setUTCSeconds", "setYear", "toDateString", "toGMTString", "toLocaleDateString",
    "toLocaleTimeString", "toLocaleString", "toTimeString", "toUTCString", "UTC", "valueOf"];

var addDateMethod = function (op) {
    return function () {
        return this.date[op].apply(this.date, arguments);
    }
};

var Year = function (y) {
    this.date = comb.isDate(y) ? y : new Date(y, 0, 1, 0, 0, 0);
};

Year.prototype.toJSON = function () {
    return comb.isUndefined(this.date) ? this.date : sql.moose.dateToString(this);
};

Year.prototype.toString = function () {
    return comb.isUndefined(this.date) ? this.date : sql.moose.dateToString(this);
};
DATE_METHODS.forEach(function (op) {
    Year.prototype[op] = addDateMethod(op);
}, this);


var Time = function (h, min, s, ms) {
    if (comb.isDate(h)) {
        this.date = h;
    } else {
        var date = new Date(1970,0,1,0,0,0);
        comb.isNumber(h) && date.setHours(h);
        comb.isNumber(min) && date.setMinutes(min);
        comb.isNumber(s) && date.setSeconds(s);
        comb.isNumber(ms) && date.setMilliseconds(ms);
        this.date = date;
    }

};

Time.prototype.toJSON = function () {
    return comb.isUndefined(this.date) ? this.date : sql.moose.dateToString(this);
};

Time.prototype.toString = function () {
    return comb.isUndefined(this.date) ? this.date : sql.moose.dateToString(this);
};

DATE_METHODS.forEach(function (op) {
    Time.prototype[op] = addDateMethod(op);
}, this);


var TimeStamp = function (y, m, d, h, min, s, ms) {
    if (comb.isDate(y)) {
        this.date = y;
    } else {
        var date = new Date(1970,0,1,0,0,0);
        comb.isNumber(y) && date.setYear(y);
        comb.isNumber(m) && date.setMonth(m);
        comb.isNumber(d) && date.setDate(d);
        comb.isNumber(h) && date.setHours(h);
        comb.isNumber(min) && date.setMinutes(min);
        comb.isNumber(s) && date.setSeconds(s);
        comb.isNumber(ms) && date.setMilliseconds(ms);
        this.date = date;
    }
};

TimeStamp.prototype.toJSON = function () {
    return comb.isUndefined(this.date) ? this.date : sql.moose.dateToString(this);
};

TimeStamp.prototype.toString = function () {
    return comb.isUndefined(this.date) ? this.date : sql.moose.dateToString(this);
};

DATE_METHODS.forEach(function (op) {
    TimeStamp.prototype[op] = addDateMethod(op);
}, this);


var DateTime = function (y, m, d, h, min, s, ms) {
    if (comb.isDate(y)) {
        this.date = y;
    } else {
        var date = new Date(1970,0,1,0,0,0);
        comb.isNumber(y) && date.setYear(y);
        comb.isNumber(m) && date.setMonth(m);
        comb.isNumber(d) && date.setDate(d);
        comb.isNumber(h) && date.setHours(h);
        comb.isNumber(min) && date.setMinutes(min);
        comb.isNumber(s) && date.setSeconds(s);
        comb.isNumber(ms) && date.setMilliseconds(ms);
        this.date = date;
    }
};

DateTime.prototype.toJSON = function () {
    return comb.isUndefined(this.date) ? this.date : sql.moose.dateToString(this);
};

DateTime.prototype.toString = function () {
    return comb.isUndefined(this.date) ? this.date : sql.moose.dateToString(this);
};

DATE_METHODS.forEach(function (op) {
    DateTime.prototype[op] = addDateMethod(op);
}, this);

var Float = function (number) {
    this.number = number;
};

Float.prototype.toJSON = function () {
    return this.number;
};

var Decimal = function (number) {
    this.number = number;
};

Decimal.prototype.toJSON = function () {
    return this.number;
};

var hashToArray = function (hash) {
    var ret = [];
    if (comb.isHash(hash)) {
        for (var i in hash) {
            var k = sql.stringToIdentifier(i), v = hash[i];
            v = comb.isHash(v) ? hashToArray(v) : v;
            ret.push([k , v]);
        }
    }
    return ret;
};

var sql = {

    stringToIdentifier:function (name, isIdentifier) {
        !Dataset && (Dataset = require("./dataset"));
        return new Dataset().stringToIdentifier(name);
    },

    identifier:function (s) {
        return sql.stringToIdentifier(s);
    },

    literal:function (s) {
        var args = comb.argsToArray(arguments);
        return args.length > 1 ? PlaceHolderLiteralString.fromArgs(args) : new LiteralString(s);
    },

    "case":function (hash, /*args**/opts) {
        var args = comb.argsToArray(arguments).slice(1);
        return CaseExpression.fromArgs([hashToArray(hash)].concat(args));
    },

    sqlStringJoin:function (arr, joiner) {
        joiner = joiner || null;
        var args;
        arr = arr.map(function (a) {
            return comb.isInstanceOf(a, Expression, LiteralString, Boolean) || comb.isNull(a) ? a : sql.stringToIdentifier(a)
        });
        if (joiner) {
            var newJoiner = [];
            for (var i = 0; i < arr.length; i++) {
                newJoiner.push(joiner);
            }
            args = array.flatten(array.zip(arr, newJoiner));
            args.pop();
        } else {
            args = arr;
        }
        args = args.map(function (a) {
            return comb.isInstanceOf(a, Expression, LiteralString, Boolean) || comb.isNull(a) ? a : "" + a;
        });
        return StringExpression.fromArgs(["||"].concat(args));
    },

    Year:Year,
    TimeStamp:TimeStamp,
    Time:Time,
    DateTime:DateTime,
    Float:Float,
    Decimal:Decimal

};

sql.__defineGetter__("moose", function () {
    !moose && (moose = require("index.js"));
    return moose;
});

exports.sql = comb.methodMissing(sql, function (name) {
    return virtualRow(name);
});

var OPERTATOR_INVERSIONS = {
    AND:"OR",
    OR:"AND",
    GT:"lte",
    GTE:"lt",
    LT:"gte",
    LTE:"gt",
    EQ:"neq",
    NEQ:"eq",
    LIKE:'NOT LIKE',
    "NOT LIKE":"LIKE",
    '!~*':'~*',
    '~*':'!~*',
    "~":'!~',
    "IN":'NOTIN',
    "NOTIN":"IN",
    "IS":'IS NOT',
    "ISNOT":"IS",
    NOT:"NOOP",
    NOOP:"NOT",
    ILIKE:'NOT ILIKE',
    NOTILIKE:"ILIKE"
};

// Standard mathematical operators used in +NumericMethods+
var MATHEMATICAL_OPERATORS = {PLUS:"+", MINUS:"-", DIVIDE:"/", MULTIPLY:"*"};

// Bitwise mathematical operators used in +NumericMethods+
var BITWISE_OPERATORS = {bitWiseAnd:"&", bitWiseOr:"|", exclusiveOr:"^", leftShift:"<<", rightShift:">>"};


var INEQUALITY_OPERATORS = {
    GT:">",
    GTE:">=",
    LT:"<",
    LTE:"<="
};

//Hash of ruby operator symbols to SQL operators, used in +BooleanMethods+
var BOOLEAN_OPERATORS = {
    AND:"AND",
    OR:"OR"
};

//Operators that use IN/NOT IN for inclusion/exclusion
var IN_OPERATORS = {
    IN:"IN",
    NOTIN:'NOT IN'
};

//Operators that use IS, used for special casing to override literal true/false values
var IS_OPERATORS = {
    IS:"IS",
    ISNOT:'IS NOT'
};

//Operator symbols that take exactly two arguments
var TWO_ARITY_OPERATORS = comb.merge({
    EQ:'=',
    NEQ:'!=',
    LIKE:"LIKE",
    "NOT LIKE":'NOT LIKE',
    ILIKE:"ILIKE",
    "NOT ILIKE":'NOT ILIKE',
    "~":"~",
    '!~':"!~",
    '~*':"~*",
    '!~*':"!~*"}, INEQUALITY_OPERATORS, BITWISE_OPERATORS, IS_OPERATORS, IN_OPERATORS);

//Operator symbols that take one or more arguments
var N_ARITY_OPERATORS = comb.merge({"||":"||"}, BOOLEAN_OPERATORS, MATHEMATICAL_OPERATORS);

//Operator symbols that take only a single argument
var ONE_ARITY_OPERATORS = {
    "NOT":"NOT",
    "NOOP":"NOOP"
};

var AliasMethods = comb.define(null, {
    instance:{
        /**
         *  Create an SQL alias (+AliasedExpression+) of the receiving column or expression to the given alias.
         *
         *   :column.as(:alias) # "column" AS "alias"
         * @param alias
         */
        as:function (alias) {
            return new AliasedExpression(this, alias);
        }

    }
}).as(sql, "AliasMethods");

var bitWiseMethod = function (op) {
    return function (expression) {
        if (comb.isInstanceOf(expression, StringExpression) || comb.isInstanceOf(expression, BooleanExpression))
            throw new ExpressionError("Cannot apply " + op + " to a non numeric expression");
        else {
            return new BooleanExpression(op, this, expression);
        }
    }
};

/**
 *  This defines the bitwise methods: &, |, ^, ~, <<, and >>.  Because these
 * methods overlap with the standard +BooleanMethods methods+, and they only
 * make sense for integers, they are only included in +NumericExpression+.
 *
 *   :a.sql_number & :b # "a" & "b"
 *   :a.sql_number | :b # "a" | "b"
 *   :a.sql_number ^ :b # "a" ^ "b"
 *   :a.sql_number << :b # "a" << "b"
 *   :a.sql_number >> :b # "a" >> "b"
 *   ~:a.sql_number # ~"a"
 */
var BitWiseMethods = comb.define(null, {
    instance:{
        bitWiseAnd:bitWiseMethod("bitWiseAnd"),
        bitWiseOr:bitWiseMethod("bitWiseOr"),
        exclusiveOr:bitWiseMethod("exclusiveOr"),
        leftShift:bitWiseMethod("leftShift"),
        rightShift:bitWiseMethod("rightShift")
    }
}).as(sql, "BitWiseMethods");

var booleanMethod = function (op) {
    return function (expression) {
        if (comb.isInstanceOf(expression, StringExpression) || comb.isInstanceOf(expression, NumericExpression))
            throw new ExpressionError("Cannot apply " + op + " to a non boolean expression");
        else {
            return new BooleanExpression(op, this, expression);
        }
    }
};

/**
 * This module includes the boolean/logical AND (&), OR (|) and NOT (~) operators
 * that are defined on objects that can be used in a boolean context in SQL
 * (+Symbol+, +LiteralString+, and <tt>SQL::GenericExpression</tt>).
 *
 *   :a & :b # "a" AND "b"
 *   :a | :b # "a" OR "b"
 *   ~:a # NOT "a"
 */
var BooleanMethods = comb.define(null, {
    instance:{
        and:booleanMethod("and"),
        or:booleanMethod("or"),
        not:function () {
            return BooleanExpression.invert(this);
        }

    }
}).as(sql, "BooleanMethods");

var CastMethods = comb.define(null, {
    instance:{

        /**
         * Cast the reciever to the given SQL type.  You can specify a ruby class as a type,
         * and it is handled similarly to using a database independent type in the schema methods.
         *
         *   :a.cast(:integer) # CAST(a AS integer)
         *   :a.cast(String) # CAST(a AS varchar(255))
         */

        cast:function (type) {
            return new Cast(this, type);
        },

        /**
         *   Cast the reciever to the given SQL type (or the database's default Integer type if none given),
         * and return the result as a +NumericExpression+, so you can use the bitwise operators
         * on the result.
         *
         *   :a.cast_numeric # CAST(a AS integer)
         *   :a.cast_numeric(Float) # CAST(a AS double precision)
         * @param type
         */
        castNumeric:function (type) {
            return this.cast(type || Number).sqlNumber;
        },

        /**
         *  Cast the reciever to the given SQL type (or the database's default String type if none given),
         * and return the result as a +StringExpression+, so you can use +
         * directly on the result for SQL string concatenation.
         *
         *   :a.cast_string # CAST(a AS varchar(255))
         *   :a.cast_string(:text) # CAST(a AS text)
         * @param type
         */
        castString:function (type) {
            return this.cast(type || moose.adapter.type.VARCHAR).sqlString;
        }
    }
}).as(sql, "CastMethods");


var ComplexExpressionMethods = comb.define(null, {
    instance:{
        getters:{
            /** Return a BooleanExpression representation of +self+.*/
            sqlBoolean:function () {
                return new BooleanExpression("noop", this);
            },

            sqlFunction:function () {
                return new SQLFunction(this);
            },

            /**
             *  Return a NumericExpression representation of +self+.
             *
             *   ~:a # NOT "a"
             *   ~:a.sql_number # ~"a"
             */
            sqlNumber:function () {
                return new NumericExpression("noop", this);
            },

            /**
             *    Return a StringExpression representation of this expression.
             *
             *   :a + :b # "a" + "b"
             *   :a.sql_string + :b # "a" || "b"
             */
            sqlString:function () {
                return new StringExpression("noop", this);
            }
        }
    }
}).as(sql, "ComplexExpressionMethods");

var inequalityMethod = function (op) {
    return function (expression) {
        if (comb.isInstanceOf(expression, BooleanExpression)
            || comb.isBoolean(expression)
            || comb.isNull(expression)
            || (comb.isObject(expression) && !comb.isInstanceOf(expression, Expression) && !comb.isInstanceOf(expression, Dataset))
            || comb.isArray(expression)) {
            throw new ExpressionError("Cannot apply " + op + " to a boolean expression");
        } else {
            return new BooleanExpression(op, this, expression);
        }
    }
};

/**
 * This module includes the inequality methods (>, <, >=, <=) that are defined on objects that can be
 * used in a numeric or string context in SQL (+Symbol+ (except on ruby 1.9), +LiteralString+,
 * <tt>SQL::GenericExpression</tt>).
 *
 *   'a'.lit > :b # a > "b"
 *   'a'.lit < :b # a > "b"
 *   'a'.lit >= :b # a >= "b"
 *   'a'.lit <= :b # a <= "b"
 */
var InequalityMethods = comb.define(null, {
    instance:{
        gt:inequalityMethod("gt"),
        gte:inequalityMethod("gte"),
        lt:inequalityMethod("lt"),
        lte:inequalityMethod("lte"),
        eq:inequalityMethod("eq")
    }
}).as(sql, "InequalityMethods");

/**
 *   This module augments the default initalize method for the
 *   +ComplexExpression+ subclass it is included in, so that
 * attempting to use boolean input when initializing a +NumericExpression+
 * or +StringExpression+ results in an error.  It is not expected to be
 * used directly.
 */
var NoBooleanInputMethods = comb.define(null, {
    instance:{
        constructor:function (op) {
            var args = comb.argsToArray(arguments).slice(1);
            args.forEach(function (expression) {
                if ((comb.isInstanceOf(expression, BooleanExpression))
                    || comb.isBoolean(expression)
                    || comb.isNull(expression)
                    || (comb.isObject(expression) && !comb.isInstanceOf(expression, Expression, Dataset, LiteralString))
                    || comb.isArray(expression)) {
                    throw new ExpressionError("Cannot apply " + op + " to a boolean expression");
                }
            });
            this._super(arguments);
        }
    }
}).as(sql, "NoBooleanInputMethods");

var numericMethod = function (op) {
    return function (expression) {
        if (comb.isInstanceOf(expression, BooleanExpression) || comb.isInstanceOf(expression, StringExpression)) {
            throw new ExpressionError("Cannot apply " + op + " to a non numeric expression");
        } else {
            return new NumericExpression(op, this, expression);
        }
    }
};


/**
 *  This module includes the standard mathematical methods (+, -, *, and /)
 * that are defined on objects that can be used in a numeric context in SQL
 * (+Symbol+, +LiteralString+, and +SQL::GenericExpression+).
 *
 *   :a + :b # "a" + "b"
 *   :a - :b # "a" - "b"
 *   :a * :b # "a" * "b"
 *   :a / :b # "a" / "b"
 */
var NumericMethods = comb.define(null, {
    instance:{
        plus:numericMethod("plus"),
        minus:numericMethod("minus"),
        divide:numericMethod("divide"),
        multiply:numericMethod("multiply")
    }
}).as(sql, "NumericMethods");


var OrderedMethods = comb.define(null, {
    instance:{

        /**
         *  * Mark the receiving SQL column as sorting in an ascending fashion (generally a no-op).
         * Options:
         *
         * :nulls :: Set to :first to use NULLS FIRST (so NULL values are ordered
         *           before other values), or :last to use NULLS LAST (so NULL values
         *           are ordered after other values).
         * @param options
         */
        asc:function (options) {
            return new OrderedExpression(this, false, options);
        },

        /**
         *       Mark the receiving SQL column as sorting in a descending fashion.
         * Options:
         *
         * :nulls :: Set to :first to use NULLS FIRST (so NULL values are ordered
         *           before other values), or :last to use NULLS LAST (so NULL values
         *           are ordered after other values).
         * @param options
         */
        desc:function (options) {
            return new OrderedExpression(this, true, options);
        }
    }
}).as(sql, "OrderedMethods");


var QualifyingMethods = comb.define(null, {
    instance:{
        /**
         *    Qualify the receiver with the given +qualifier+ (table for column/schema for table).
         *
         *   :column.qualify(:table) # "table"."column"
         *   :table.qualify(:schema) # "schema"."table"
         *   :column.qualify(:table).qualify(:schema) # "schema"."table"."column"
         * @param qualifier
         */
        qualify:function (qualifier) {
            return new QualifiedIdentifier(qualifier, this);
        }
    }
}).as(sql, "QualifyingMethods");


var StringMethods = comb.define(null, {
    instance:{


        /**
         *  Create a +BooleanExpression+ case insensitive pattern match of the receiver
         * with the given patterns.  See <tt>StringExpression.like</tt>.
         *
         *   :a.ilike('A%') # "a" ILIKE 'A%'
         */
        ilike:function (expression) {
            expression = comb.argsToArray(arguments);
            return StringExpression.like.apply(StringExpression, [this].concat(expression).concat([
                {caseInsensitive:true}
            ]));
        },

        /**
         *     * Create a +BooleanExpression+ case sensitive (if the database supports it) pattern match of the receiver with
         * the given patterns.  See <tt>StringExpression.like</tt>.
         *
         *   :a.like('A%') # "a" LIKE 'A%'
         * @param expression
         */
        like:function (expression) {
            expression = comb.argsToArray(arguments);
            return StringExpression.like.apply(StringExpression, [this].concat(expression));
        }
    }
}).as(sql, "StringMethods");

var StringConcatenationMethods = comb.define(null, {
    instance:{

        /**
         *  Return a +StringExpression+ representing the concatenation of the receiver
         * with the given argument.
         *
         *   :x.sql_string + :y => # "x" || "y"
         * @param expression
         */
        concat:function (expression) {
            return new StringExpression("||", this, expression);
        }
    }
}).as(sql, "StringConcatenationMethods");

var SubscriptMethods = comb.define(null, {
    instance:{

        /**
         * Return a <tt>Subscript</tt> with the given arguments, representing an
         * SQL array access.
         *
         *   array.sql_subscript(1) => array[1]
         *   array.sql_subscript(1, 2) => array[1, 2]
         *   array.sql_subscript([1, 2]) => array[1, 2]
         * @param subscript
         */
        sqlSubscript:function (subscript) {
            var args = comb.argsToArray(arguments);
            return new SubScript(this, comb.array.flatten(args));
        }
    }
}).as(sql, "SubScriptMethods");


var Expression = comb.define(null, {

    instance:{

        sqlLiteral:function (ds) {
            return this.toString(ds);
        }

    },

    static:{

        fromArgs:function (args) {
            var ret;
            try {
                ret = new this();
            } catch (ignore) {
            }
            this.apply(ret, args);
            return ret;
        },

        isConditionSpecifier:function (obj) {
            return (comb.isHash(obj)) || (comb.isArray(obj) && obj.length && obj.every(function (i) {
                return comb.isArray(i) && i.length == 2
            }));
        }
    }

}).as(sql, "Expression");

var GenericExpression = comb.define([Expression, AliasMethods, BooleanMethods, CastMethods, ComplexExpressionMethods, InequalityMethods, NumericMethods, OrderedMethods, StringMethods, SubscriptMethods]).as(sql, "GenericExpression");

var AliasedExpression = comb.define(Expression, {
        instance:{
            constructor:function (expression, alias) {
                this.expression = expression;
                this.alias = alias;
            },

            toString:function (ds) {
                return ds.aliasedExpressionSql(this);
            }
        }
    }
).as(sql, "AliasedExpression");

var Blob = comb.define(null, {
    instance:{
        constructor:function (str) {
            this.data = str;
        },

        toString:function (ds) {
            return this.data;
        },

        toSqlBlob:function () {
            return this.data;
        }
    }
}).as(sql, "Blob");


var CaseExpression = comb.define(GenericExpression, {
    instance:{

        /**
         * Create an object with the given conditions and
         * default value.  An expression can be provided to
         * test each condition against, instead of having
         * all conditions represent their own boolean expression.
         * @param conditions
         * @param def
         * @param expression
         */
        constructor:function (conditions, def, expression) {
            if (Expression.isConditionSpecifier(conditions)) {
                this.conditions = comb.array.toArray(conditions);
                this.def = def;
                this.expression = expression;
                this.noExpression = comb.isUndefined(expression);
            }
        },

        toString:function (ds) {
            return ds.caseExpressionSql(this);
        },

        getters:{
            hasExpression:function () {
                return !this.noExpression;
            }
        }
    }
}).as(sql, "CaseExpression");


//Represents a cast of an SQL expression to a specific type.
var Cast = comb.define(GenericExpression, {
    instance:{
        constructor:function (expr, type) {
            this.expr = expr;
            this.type = type;
        },

        toString:function (ds) {
            return ds.castSql(this.expr, this.type);
        }
    }
}).as(sql, "Cast");


//Represents all columns in a given table, table.* in SQL
var ColumnAll = comb.define(Expression, {
    instance:{
        constructor:function (table) {
            this.table = table;
        },

        toString:function (ds) {
            return ds.columnAllSql(this);
        }
    }
}).as(sql, "ColumnAll");

var ComplexExpression = comb.define([Expression, AliasMethods, CastMethods, OrderedMethods, SubscriptMethods], {
    instance:{
        constructor:function (op) {
            if (op) {
                var args = comb.argsToArray(arguments).slice(1);
                //make a copy of the args
                var origArgs = args.slice(0);
                args.forEach(function (a, i) {
                    if (Expression.isConditionSpecifier(a)) {
                        args[i] = BooleanExpression.fromValuePairs(a);
                    }
                });
                op = op.toUpperCase();

                if (N_ARITY_OPERATORS.hasOwnProperty(op)) {
                    if (args.length < 1)
                        throw new ExpressionError("The " + op + " operator requires at least 1 argument")
                    var oldArgs = args.slice(0);
                    args = [];
                    oldArgs.forEach(function (a) {
                        a instanceof ComplexExpression && a.op == op ? args = args.concat(a.args) : args.push(a);
                    });

                } else if (TWO_ARITY_OPERATORS.hasOwnProperty(op)) {
                    if (args.length != 2)
                        throw new ExpressionError("The " + op + " operator requires precisely 2 arguments");
                    //With IN/NOT IN, even if the second argument is an array of two element arrays,
                    //don't convert it into a boolean expression, since it's definitely being used
                    //as a value list.
                    if (IN_OPERATORS[op]) {
                        args[1] = origArgs[1]
                    }
                } else if (ONE_ARITY_OPERATORS.hasOwnProperty(op)) {
                    if (args.length != 1) {
                        throw new ExpressionError("The " + op + " operator requires only one argument");
                    }
                } else {
                    throw new ExpressionError("Invalid operator " + op);
                }
                this.op = op;
                this.args = args;
            }
        },

        toString:function (ds) {
            return ds.complexExpressionSql(this.op, this.args);
        }
    },

    static:{
        OPERATOR_INVERSIONS:OPERTATOR_INVERSIONS,
        MATHEMATICAL_OPERATORS:MATHEMATICAL_OPERATORS,
        BITWISE_OPERATORS:BITWISE_OPERATORS,
        INEQUALITY_OPERATORS:INEQUALITY_OPERATORS,
        BOOLEAN_OPERATORS:BOOLEAN_OPERATORS,
        IN_OPERATORS:IN_OPERATORS,
        IS_OPERATORS:IS_OPERATORS,
        TWO_ARITY_OPERATORS:TWO_ARITY_OPERATORS,
        N_ARITY_OPERATORS:N_ARITY_OPERATORS,
        ONE_ARITY_OPERATORS:ONE_ARITY_OPERATORS
    }
}).as(sql, "ComplexExpression");


var BooleanExpression = comb.define([ComplexExpression, BooleanMethods], {
    static:{

        /** Invert the expression, if possible.  If the expression cannot
         * be inverted, raise an error.  An inverted expression should match everything that the
         * uninverted expression did not match, and vice-versa, except for possible issues with
         * SQL NULL (i.e. 1 == NULL is NULL and 1 != NULL is also NULL).
         *
         *   BooleanExpression.invert(:a) # NOT "a"
         */
        invert:function (expression) {
            if (comb.isInstanceOf(expression, BooleanExpression)) {
                var op = expression.op, newArgs;
                if (op == "AND" || op == "OR") {
                    newArgs = [OPERTATOR_INVERSIONS[op]].concat(expression.args.map(function (arg) {
                        return BooleanExpression.invert(arg);
                    }));
                    return BooleanExpression.fromArgs(newArgs);
                } else {
                    newArgs = [OPERTATOR_INVERSIONS[op]].concat(expression.args);
                    return BooleanExpression.fromArgs(newArgs);
                }
            } else if (comb.isInstanceOf(expression, StringExpression) || comb.isInstanceOf(expression, NumericExpression)) {
                throw new ExpressionError(comb.string.format("Cannot invert %4j", [expression]));
            } else {
                return new BooleanExpression("NOT", expression);
            }
        },
        /**
         *  Take pairs of values (e.g. a hash or array of two element arrays)
         * and converts it to a +BooleanExpression+.  The operator and args
         * used depends on the case of the right (2nd) argument:
         *
         * 0..10 - left >= 0 AND left <= 10
         * [1,2] - left IN (1,2)
         * nil - left IS NULL
         * true - left IS TRUE
         * false - left IS FALSE
         * /as/ - left ~ 'as'
         * :blah - left = blah
         * 'blah' - left = 'blah'
         *
         * If multiple arguments are given, they are joined with the op given (AND
         * by default, OR possible).  If negate is set to true,
         * all subexpressions are inverted before used.  Therefore, the following
         * expressions are equivalent:
         *
         *   ~from_value_pairs(hash)
         *   from_value_pairs(hash, :OR, true)
         * @param a
         * @param op
         * @param negate
         */
        fromValuePairs:function (a, op, negate) {
            !Dataset && (Dataset = require("./dataset"));
            op = op || "AND", negate = negate || false;
            var pairArr = [];
            var isArr = comb.isArray(a) && Expression.isConditionSpecifier(a);
            for (var k in a) {
                var v = isArr ? a[k][1] : a[k], ret;
                k = isArr ? a[k][0] : k;
                if (comb.isArray(v) || comb.isInstanceOf(v, Dataset)) {
                    k = comb.isArray(k) ? k.map(function (i) {
                        return comb.isString(i) ? new Identifier(i) : i
                    }) : k;
                    ret = new BooleanExpression("IN", k, v);
                } else if (comb.isInstanceOf(v, NegativeBooleanConstant)) {
                    ret = new BooleanExpression("ISNOT", k, v.constant);
                } else if (comb.isInstanceOf(v, BooleanConstant)) {
                    ret = new BooleanExpression("IS", k, v.constant);
                } else if (comb.isNull(v) || comb.isBoolean(v)) {
                    ret = new BooleanExpression("IS", k, v);
                } else if (comb.isHash(v)) {
                    ret = BooleanExpression.fromValuePairs(v);
                } else if (comb.isRexExp(v)) {
                    ret = StringExpression.like(k, v);
                } else {
                    ret = new BooleanExpression("EQ", sql.stringToIdentifier(k), v);
                }
                negate && (ret = BooleanExpression.invert(ret));
                pairArr.push(ret);
            }
            //if We just have one then return the first otherwise create a new Boolean expression
            return pairArr.length == 1 ? pairArr[0] : BooleanExpression.fromArgs([op].concat(pairArr));
        }
    }
}).as(sql, "BooleanExpression");


//Represents constants or psuedo-constants (e.g. +CURRENT_DATE+) in SQL.
var Constant = comb.define(GenericExpression, {
    instance:{
        constructor:function (constant) {
            this.__constant = constant;
        },

        toString:function (ds) {
            return ds.constantSql(this.__constant);
        },

        getters:{
            constant:function () {
                return this.__constant;
            }
        }
    }
}).as(sql, "Constant");

//Represents boolean constants such as +NULL+, +NOTNULL+, +TRUE+, and +FALSE+.
var BooleanConstant = comb.define(Constant, {
    instance:{
        toString:function (ds) {
            return ds.booleanConstantSql(this.__constant);
        }
    }
}).as(sql, "BooleanConstant");

//Represents inverse boolean constants (currently only +NOTNULL+). A
//special class to allow for special behavior.
var NegativeBooleanConstant = comb.define(BooleanConstant, {
    instance:{
        toString:function (ds) {
            return ds.negativeBooleanConstantSql(this.__constant);
        }
    }
}).as(sql, "NegativeBooleanConstant");

//Holds default generic constants that can be referenced.  These
// are included in the Sequel top level module and are also available
// in this module which can be required at the top level to get
// direct access to the constants.
var Constants = sql.Constants = {
    CURRENT_DATE:new Constant("CURRENT_DATE"),
    CURRENT_TIME:new Constant("CURRENT_TIME"),
    CURRENT_TIMESTAMP:new Constant("CURRENT_TIMESTAMP"),
    SQLTRUE:new BooleanConstant(1),
    TRUE:new BooleanConstant(1),
    SQLFALSE:new BooleanConstant(0),
    FALSE:new BooleanConstant(0),
    NULL:new BooleanConstant(null),
    NOTNULL:new NegativeBooleanConstant(null)

};


//Represents an identifier (column or table). Can be used
// to specify a +Symbol+ with multiple underscores should not be
// split, or for creating an identifier without using a symbol.
var Identifier = comb.define([GenericExpression, QualifyingMethods], {
    instance:{
        constructor:function (value) {
            this.__value = value;
        },

        toString:function (ds) {
            return ds ? ds.quoteIdentifier(this) : this.__value;
        },

        getters:{
            value:function () {
                return this.__value;
            }
        }
    }
}).as(sql, "Identifier");

//Represents an SQL JOIN clause, used for joining tables.
var JoinClause = comb.define(Expression, {
    instance:{
        constructor:function (joinType, table, tableAlias) {
            this.__joinType = joinType;
            this.__table = table;
            this.__tableAlias = tableAlias || null;
        },

        toString:function (ds) {
            return ds.joinClauseSql(this);
        },

        getters:{
            joinType:function () {
                return this.__joinType;
            },

            table:function () {
                return this.__table;
            },

            tableAlias:function () {
                return this.__tableAlias;
            }
        }
    }
}).as(sql, "JoinClause");

//Represents an SQL JOIN clause with ON conditions.
var JoinOnClause = comb.define(JoinClause, {
    instance:{
        constructor:function (on, joinType, table, tableAlias) {
            this.__on = on;
            this._super(arguments, [joinType, table, tableAlias]);
        },

        toString:function (ds) {
            return ds.joinOnClauseSql(this);
        },

        getters:{
            on:function () {
                return this.__on;
            }
        }
    }
}).as(sql, "JoinOnClause");


//Represents an SQL JOIN clause with USING conditions.
var JoinUsingClause = comb.define(JoinClause, {
    instance:{
        constructor:function (using, joinType, table, tableAlias) {
            this.__using = using.map(function (u) {
                return comb.isString(u) ? new Identifier(u) : u;
            });
            this._super(arguments, [joinType, table, tableAlias]);
        },

        toString:function (ds) {
            return ds.joinUsingClauseSql(this);
        },

        getters:{
            using:function () {
                return this.__using;
            }
        }
    }
}).as(sql, "JoinUsingClause");

// Represents a literal string with placeholders and arguments.
// This is necessary to ensure delayed literalization of the arguments
// required for the prepared statement support and for database-specific
// literalization.
var PlaceHolderLiteralString = comb.define(GenericExpression, {
    instance:{
        constructor:function (str, args, parens) {
            parens = parens || false;
            var v;
            this.__str = str;
            this.__args = comb.isArray(args) && args.length == 1 && comb.isHash((v = args[0])) ? v : args;
            this.__parens = parens;
        },

        toString:function (ds) {
            return ds.placeholderLiteralStringSql(this);
        },

        getters:{
            str:function () {
                return this.__str;
            },
            args:function () {
                return this.__args;
            },

            parens:function () {
                return this.__parens;
            }

        }
    }
}).as(sql, "PlaceHolderLiteralString");

// Represents an SQL function call.
var SQLFunction = comb.define(GenericExpression, {
    instance:{
        constructor:function (f) {
            var args = comb.argsToArray(arguments).slice(1);
            this.__f = comb.isInstanceOf(f, Identifier) ? f.value : f, this.__args = args.map(function (a) {
                return comb.isString(a) ? sql.stringToIdentifier(a) : a;
            });
        },

        toString:function (ds) {
            return ds.functionSql(this);
        },

        getters:{
            f:function () {
                return this.__f;
            },

            args:function () {
                return this.__args;
            }
        }
    }
}).as(sql, "SQLFunction");

// Subclass of +ComplexExpression+ where the expression results
// in a numeric value in SQL.
var NumericExpression = comb.define([ComplexExpression, BitWiseMethods, NumericMethods, InequalityMethods]).as(sql, "NumericExpression");

var INVERT_NULLS = {first:"last", last:"first"};
var OrderedExpression = comb.define(Expression, {
    instance:{
        constructor:function (expression, descending, opts) {
            descending = comb.isBoolean(descending) ? descending : true;
            opts = opts || {};
            this.__expression = expression;
            this.__descending = descending;
            this.__nulls = opts.nulls;

        },

        //Return a copy that is ordered ASC
        asc:function () {
            return new OrderedExpression(this.__expression, false, {nulls:this.__nulls});
        },

        //Return a copy that is ordered DESC
        desc:function () {
            return new OrderedExpression(this.__expression, true, {nulls:this.__nulls});
        },

        //Return an inverted expression, changing ASC to DESC and NULLS FIRST to NULLS LAST.
        invert:function () {
            return new OrderedExpression(this.__expression, !this.__descending, {nulls:INVERT_NULLS[this.__nulls] || this.__nulls});
        },

        toString:function (ds) {
            return ds.orderedExpressionSql(this);
        },

        getters:{
            expression:function () {
                return this.__expression;
            },
            descending:function () {
                return this.__descending;
            },
            nulls:function () {
                return this.__nulls;
            }
        }
    },
    static:{
        INVERT_NULLS:INVERT_NULLS
    }
}).as(sql, "OrderedExpression");


//Represents a qualified identifier (column with table or table with schema).
var QualifiedIdentifier = comb.define([GenericExpression, QualifyingMethods], {
    instance:{
        constructor:function (table, column) {
            this.__table = table;
            this.__column = column;
        },

        toString:function (ds) {
            return ds.qualifiedIdentifierSql(this);
        },

        getters:{
            table:function () {
                return this.__table;
            },

            column:function () {
                return this.__column;
            }
        }
    }
}).as(sql, "QualifiedIdentifier");

var LIKE_MAP = {"truetrue":'~*', "truefalse":"~", "falsetrue":"ILIKE", "falsefalse":"LIKE"};

var likeElement = function (re) {
    var ret;
    if (comb.isRexExp(re)) {
        ret = [("" + re).replace(/^\/|\/$|\/[i|m|g]*$/g, ""), true, re.ignoreCase]
    } else {
        ret = [re, false, false];
    }
    return ret;
};
// Subclass of +ComplexExpression+ where the expression results
// in a text/string/varchar value in SQL.
var StringExpression = comb.define([ComplexExpression, StringMethods, StringConcatenationMethods, InequalityMethods, NoBooleanInputMethods], {
    static:{

        /**
         *  Creates a SQL pattern match expression. left (l) is the SQL string we
         * are matching against, and ces are the patterns we are matching.
         * The match succeeds if any of the patterns match (SQL OR).
         *
         * If a regular expression is used as a pattern, an SQL regular expression will be
         * used, which is currently only supported on MySQL and PostgreSQL.  Be aware
         * that MySQL and PostgreSQL regular expression syntax is similar to ruby
         * regular expression syntax, but it not exactly the same, especially for
         * advanced regular expression features.  Sequel just uses the source of the
         * ruby regular expression verbatim as the SQL regular expression string.
         *
         * If any other object is used as a regular expression, the SQL LIKE operator will
         * be used, and should be supported by most databases.
         *
         * The pattern match will be case insensitive if the last argument is a hash
         * with a key of :case_insensitive that is not false or nil. Also,
         * if a case insensitive regular expression is used (//i), that particular
         * pattern which will always be case insensitive.
         *
         *   StringExpression.like(:a, 'a%') * "a" LIKE 'a%'
         *   StringExpression.like(:a, 'a%', :case_insensitive=>true) * "a" ILIKE 'a%'
         *   StringExpression.like(:a, 'a%', /^a/i) # "a" LIKE 'a%' OR "a" ~* '^a'
         * @param lh
         */
        like:function (l) {
            var args = comb.argsToArray(arguments).slice(1);
            var params = likeElement(l);
            var lh = params[0], lre = params[1], lci = params[2];
            var last = args[args.length - 1];
            lci = (comb.isHash(last) ? args.pop() : {})["caseInsensitive"] ? true : lci;
            args = args.map(function (ce) {
                var r, rre, rci;
                var ceArr = likeElement(ce);
                r = ceArr[0], rre = ceArr[1], rci = ceArr[2];
                return new BooleanExpression(LIKE_MAP["" + (lre || rre) + (lci || rci)], l, r)
            }, this);
            return args.length == 1 ? args[0] : BooleanExpression.fromArgs(["OR"].concat(args));
        },

        likeMap:LIKE_MAP


    }
}).as(sql, "StringExpression");

//Represents an SQL array access, with multiple possible arguments.
var SubScript = comb.define(GenericExpression, {
    instance:{
        constructor:function (arrCol, sub) {
            //The SQL array column
            this.__arrCol = arrCol;
            //The array of subscripts to use (should be an array of numbers)
            this.__sub = sub;
        },

        //Create a new +Subscript+ appending the given subscript(s)
        // the the current array of subscripts.
        addSub:function (sub) {
            return new SubScript(this.__arrCol, this.__sub.concat(sub));
        },

        toString:function (ds) {
            return ds.subscriptSql(this);
        },

        getters:{
            f:function () {
                return this.__arrCol;
            },

            sub:function () {
                return this.__sub;
            }
        }
    }
}).as(sql, "SubScript");

var STRING_METHODS = ["charAt", "charCodeAt", "concat", "indexOf", "lastIndexOf", "localeCompare", "match", "quote",
    "replace", "search", "slice", "split", "substr", "substring", "toLocaleLowerCase", "toLocaleUpperCase", "toLowerCase",
    "toSource", "toString", "toUpperCase", "trim", "trimLeft", "trimRight", "valueOf"];


var addStringMethod = function (op) {
    return function () {
        return this.__str[op].apply(this.__str, arguments);
    }
};
var LiteralString = comb.define([OrderedMethods, ComplexExpressionMethods, BooleanMethods, NumericMethods, StringMethods, InequalityMethods, AliasMethods], {
    instance:{
        constructor:function (str) {
            this.__str = str;
        }
    }
}).as(sql, "LiteralString");

STRING_METHODS.forEach(function (op) {
    LiteralString.prototype[op] = addStringMethod(op);
}, this);















