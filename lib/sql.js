var comb = require("comb-proxy"),
    array = comb.array,
    flatten = array.flatten,
    ExpressionError = require("./errors").ExpressionError,
    methodMissing = comb.methodMissing,
    createFunctionWrapper = comb.createFunctionWrapper,
    isUndefined = comb.isUndefined,
    isUndefinedOrNull = comb.isUndefinedOrNull,
    isNull = comb.isNull,
    isInstanceOf = comb.isInstanceOf,
    argsToArray = comb.argsToArray,
    isDate = comb.isDate,
    isHash = comb.isHash,
    merge = comb.merge,
    isArray = comb.isArray,
    toArray = array.toArray,
    format = comb.string.format,
    isBoolean = comb.isBoolean,
    isNumber = comb.isNumber,
    isObject = comb.isObject,
    isString = comb.isString,
    define = comb.define,
    isRegExp = comb.isRegExp,
    Dataset, patio, sql, Expression, AliasedExpression, CaseExpression, Cast, ColumnAll, BooleanExpression, JsonArray,
    BooleanConstant, NegativeBooleanConstant, Identifier, PlaceHolderLiteralString, SQLFunction, OrderedExpression,
    NumericExpression, QualifiedIdentifier, StringExpression, SubScript, LiteralString, Json;


var virtualRow = function (name) {
    var DOUBLE_UNDERSCORE = '__';

    var parts = name.split(DOUBLE_UNDERSCORE);
    var table = parts[0], column = parts[1];
    var ident = column ? QualifiedIdentifier.fromArgs([table, column]) : Identifier.fromArgs([name]);
    var prox = methodMissing(ident, function (m) {
        return function () {
            var args = argsToArray(arguments);
            return SQLFunction.fromArgs([m, name].concat(args));
        };
    }, column ? QualifiedIdentifier : Identifier);
    var ret = createFunctionWrapper(prox, function (m) {
        var args = argsToArray(arguments);
        if (args.length) {
            return SQLFunction.fromArgs([name].concat(args));
        } else {
            return prox;
        }
    }, function () {
        return SQLFunction.fromArgs(arguments);
    });
    Object.setPrototypeOf(ret, ident);
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
    };
};

/**
 * @constructor
 * Creates a Year type to be used in queries that require a SQL year datatype.
 * All <i>Date</i> methods ar included in the prototype of the Year type. toString and toJSON
 * are overridden to return a year format instead of the default <i>Date</i> formatting.
 * See {@link patioTime#yearToString} for formatting information.
 *
 * @example
 *
 * var year = patio.Year(2009); //=> 2009
 * JSON.stringify(year)l //=> 2009
 *
 * @memberOf patio.sql
 * @param {Number} y the year this year represents.
 */
var Year = function (y) {
    this.date = isUndefined(y) ? new Date() : isDate(y) ? y : new Date(y, 0, 1, 0, 0, 0);
};

Year.prototype.toJSON = function () {
    return isUndefined(this.date) ? this.date : sql.patio.dateToString(this);
};

Year.prototype.toString = function () {
    return isUndefined(this.date) ? this.date : sql.patio.dateToString(this);
};
DATE_METHODS.forEach(function (op) {
    Year.prototype[op] = addDateMethod(op);
}, this);


/**
 * @constructor
 * Creates a Time type to be used in queries that require a SQL time datatype.
 * All <i>Date</i> methods ar included in the prototype of the Time type. toString and toJSON
 * are overridden to return a time format instead of the default <i>Date</i> formatting.
 * See {@link patioTime#timeToString} for formatting information.
 *
 * @example
 *
 * var time = patio.Time(12, 12, 12); //=> 12:12:12
 * JSON.stringify(time); //=> 12:12:12
 *
 * @memberOf patio.sql
 * @param {Number} [h=0] the hour
 * @param {Number} [min=0] the minute/s
 * @param {Number} [s=0] the second/s
 * @param {Number} [ms=0] the millisecond/s, this paramater is not be used, but may depending on the adapter.
 */
var Time = function (h, min, s, ms) {
    var args = argsToArray(arguments);
    if (args.length === 0) {
        this.date = new Date();
    } else if (isDate(h)) {
        this.date = h;
    } else {
        var date = new Date(1970, 0, 1, 0, 0, 0);
        isNumber(h) && date.setHours(h);
        isNumber(min) && date.setMinutes(min);
        isNumber(s) && date.setSeconds(s);
        isNumber(ms) && date.setMilliseconds(ms);
        this.date = date;
    }

};

Time.prototype.toJSON = function () {
    return isUndefined(this.date) ? this.date : sql.patio.dateToString(this);
};

Time.prototype.toString = function () {
    return isUndefined(this.date) ? this.date : sql.patio.dateToString(this);
};

DATE_METHODS.forEach(function (op) {
    Time.prototype[op] = addDateMethod(op);
}, this);

/**
 * @constructor
 * Creates a TimeStamp type to be used in queries that require a SQL timestamp datatype.
 * All <i>Date</i> methods ar included in the prototype of the TimeStamp type. toString and toJSON
 * are overridden to return a ISO8601 format instead of the default <i>Date</i> formatting.
 * See {@link patioTime#timeStampToString} for formatting information.
 *
 * @example
 *
 * var timeStamp = patio.TimeStamp(2009, 10, 10, 10, 10, 10); //=> '2009-11-10 10:10:10'
 * JSON.stringify(timeStamp); //=> '2009-11-10 10:10:10'
 *
 * @memberOf patio.sql
 * @param {Number} [y=1970] the year
 * @param {Number} [m=0] the month
 * @param {Number} [d=1] the day
 * @param {Number} [h=0] the hour
 * @param {Number} [min=0] the minute/s
 * @param {Number} [s=0] the second/s
 * @param {Number} [ms=0] the millisecond/s, this paramater is not be used, but may depending on the adapter.
 */
var TimeStamp = function (y, m, d, h, min, s, ms) {
    var args = argsToArray(arguments);
    if (args.length === 0) {
        this.date = new Date();
    } else if (isDate(y)) {
        this.date = y;
    } else {
        var date = new Date(1970, 0, 1, 0, 0, 0);
        isNumber(y) && date.setYear(y);
        isNumber(m) && date.setMonth(m);
        isNumber(d) && date.setDate(d);
        isNumber(h) && date.setHours(h);
        isNumber(min) && date.setMinutes(min);
        isNumber(s) && date.setSeconds(s);
        isNumber(ms) && date.setMilliseconds(ms);
        this.date = date;
    }
};

TimeStamp.prototype.toJSON = function () {
    return isUndefined(this.date) ? this.date : sql.patio.dateToString(this);
};

TimeStamp.prototype.toString = function () {
    return isUndefined(this.date) ? this.date : sql.patio.dateToString(this);
};

DATE_METHODS.forEach(function (op) {
    TimeStamp.prototype[op] = addDateMethod(op);
}, this);


/**
 * @constructor
 * Creates a DateTime type to be used in queries that require a SQL datetime datatype.
 * All <i>Date</i> methods ar included in the prototype of the DateTime type. toString and toJSON
 * are overridden to return a ISO8601 formatted date string instead of the default <i>Date</i> formatting.
 * See {@link patioTime#dateTimeToString} for formatting information.
 *
 * @example
 *
 * var dateTime = patio.DateTime(2009, 10, 10, 10, 10, 10); //=> '2009-11-10 10:10:10'
 * JSON.stringify(dateTime); //=> '2009-11-10 10:10:10'
 *
 * @memberOf patio.sql
 * @param {Number} [y=1970] the year
 * @param {Number} [m=0] the month
 * @param {Number} [d=1] the day
 * @param {Number} [h=0] the hour
 * @param {Number} [min=0] the minute/s
 * @param {Number} [s=0] the second/s
 * @param {Number} [ms=0] the millisecond/s, this paramater is not be used, but may depending on the adapter.
 */
var DateTime = function (y, m, d, h, min, s, ms) {
    var args = argsToArray(arguments);
    if (args.length === 0) {
        this.date = new Date();
    } else if (isDate(y)) {
        this.date = y;
    } else {
        var date = new Date(1970, 0, 1, 0, 0, 0);
        isNumber(y) && date.setYear(y);
        isNumber(m) && date.setMonth(m);
        isNumber(d) && date.setDate(d);
        isNumber(h) && date.setHours(h);
        isNumber(min) && date.setMinutes(min);
        isNumber(s) && date.setSeconds(s);
        isNumber(ms) && date.setMilliseconds(ms);
        this.date = date;
    }
};

DateTime.prototype.toJSON = function () {
    return isUndefined(this.date) ? this.date : sql.patio.dateToString(this);
};

DateTime.prototype.toString = function () {
    return isUndefined(this.date) ? this.date : sql.patio.dateToString(this);
};

DATE_METHODS.forEach(function (op) {
    DateTime.prototype[op] = addDateMethod(op);
}, this);

/**
 * @class Represents a SQL Float type, by default is converted to double precision
 * @param {Number} number the number to be represented as a float
 * @memberOf patio.sql
 */
var Float = function (number) {
    this.number = number;
};

Float.prototype.toJSON = function () {
    return this.number;
};

/**
 * @class
 * Represents a SQL Decimal type, by default is converted to double precision
 * @param {Number} number the number to be represented as a decimal
 * @memberOf patio.sql
 */
var Decimal = function (number) {
    this.number = number;
};

Decimal.prototype.toJSON = function () {
    return this.number;
};

var hashToArray = function (hash) {
    var ret = [];
    if (isHash(hash)) {
        for (var i in hash) {
            var k = sql.stringToIdentifier(i), v = hash[i];
            v = isHash(v) ? hashToArray(v) : v;
            ret.push([k, v]);
        }
    }
    return ret;
};

/**
 * @namespace  Collection of SQL related types, and expressions.
 *
 * <p>
 *  The {@link patio.sql} object
 *  can be used directly to create {@link patio.sql.Expression}s, {@link patio.sql.Identifier}s, {@link patio.sql.SQLFunction}s,
 *  and {@link patio.sql.QualifiedIdentifier}s.
 * <pre class='code'>
 *  var sql = patio.sql;
 *  //creating an identifier
 *  sql.a; //=> a;
 *
 *  //creating a qualified identifier
 *  sql.table__column; //table.column;
 *
 *  //BooleanExpression
 *  sql.a.lt(sql.b); //=> a < 'b';
 *
 *  //SQL Functions
 *  sql.sum(sql.a); //=> sum(a)
 *  sql.avg(sql.b); //=> avg(b)
 *  sql.a("b", 1); //=> a(b, 1)
 *  sql.myDatabasesObjectFunction(sql.a, sql.b, sql.c); //=> myDatabasesObjectFunction(a, b, c);
 *
 *  //combined
 *  sql.a.cast("boolean"); //=> 'CAST(a AS boolean)'
 *  sql.a.plus(sql.b).lt(sql.c.minus(3) //=> ((a + b) < (c - 3))
 *
 * </pre>
 *
 * This is useful when combined with dataset filtering
 *
 * <pre class="code">
 *  var ds = DB.from("t");
 *
 *  ds.filter({a:[sql.b, sql.c]}).sql;
 *      //=> SELECT * FROM t WHERE (a IN (b, c))
 *
 *  ds.select(sql["case"]({b:{c:1}}, false)).sql;
 *      //=> SELECT (CASE WHEN b THEN (c = 1) ELSE 'f' END) FROM t
 *
 *  ds.select(sql.a).qualifyToFirstSource().sql;
 *      //=>  SELECT a FROM t
 *
 *  ds.order(sql.a.desc(), sql.b.asc()).sql;
 *      //=>  SELECT * FROM t ORDER BY a DESC, b ASC
 *
 *  ds.select(sql.a.as("b")).sql;
 *      //=> SELECT a AS b FROM t
 *
 *  ds.filter(sql["case"]({a:sql.b}, sql.c, sql.d)).sql
 *      //=> SELECT * FROM t WHERE (CASE d WHEN a THEN b ELSE c END)
 *
 *  ds.filter(sql.a.cast("boolean")).sql;
 *      //=> SELECT * FROM t WHERE CAST(a AS boolean)
 *
 *  ds.filter(sql.a("b", 1)).sql
 *      //=> SELECT * FROM t WHERE a(b, 1)
 *  ds.filter(sql.a.plus(sql.b).lt(sql.c.minus(3)).sql;
 *      //=> SELECT * FROM t WHERE ((a + b) < (c - 3))
 *
 *  ds.filter(sql.a.sqlSubscript(sql.b, 3)).sql;
 *      //=> SELECT * FROM t WHERE a[b, 3]
 *
 *  ds.filter('? > ?', sql.a, 1).sql;
 *     //=> SELECT * FROM t WHERE (a > 1);
 *
 *  ds.filter('{a} > {b}', {a:sql.c, b:1}).sql;
 *      //=>SELECT * FROM t WHERE (c > 1)
 *
 *  ds.select(sql.literal("'a'"))
 *     .filter(sql.a(3))
 *     .filter('blah')
 *     .order(sql.literal(true))
 *     .group(sql.literal('a > ?', [1]))
 *     .having(false).sql;
 *      //=>"SELECT 'a' FROM t WHERE (a(3) AND (blah)) GROUP BY a > 1 HAVING 'f' ORDER BY true");
 </pre>
 *
 * </p>
 * @name sql
 * @memberOf patio
 */
sql = {
    /**@lends patio.sql*/

    /**
     * Returns a {@link patio.sql.Identifier}, {@link patio.sql.QualifiedIdentifier},
     * or {@link patio.sql.ALiasedExpression} depending on the format of the string
     * passed in.
     *
     * <ul>
     *      <li>For columns : table__column___alias.</li>
     *      <li>For tables : schema__table___alias.</li>
     * </ul>
     * each portion of the identifier is optional. See example below
     *
     * @example
     *
     * patio.sql.identifier("a") //= > new patio.sql.Identifier("a");
     * patio.sql.identifier("table__column"); //=> new patio.sql.QualifiedIdentifier(table, column);
     * patio.sql.identifier("table__column___alias");
     *      //=> new patio.sql.AliasedExpression(new patio.sql.QualifiedIdentifier(table, column), alias);
     *
     * @param {String} name the name to covert to an an {@link patio.sql.Identifier}, {@link patio.sql.QualifiedIdentifier},
     * or {@link patio.sql.AliasedExpression}.
     *
     * @return  {patio.sql.Identifier|patio.sql.QualifiedIdentifier|patio.sql.AliasedExpression} an identifier generated based on the name string.
     */
    identifier: function (s) {
        return sql.stringToIdentifier(s);
    },

    /**
     * @see patio.sql.identifier
     */
    stringToIdentifier: function (name) {
        !Dataset && (Dataset = require("./dataset"));
        return new Dataset().stringToIdentifier(name);
    },

    /**
     * Creates a {@link patio.sql.LiteralString} or {@link patio.sql.PlaceHolderLiteralString}
     * depending on the arguments passed in. If a single string is passed in then
     * it is assumed to be a {@link patio.sql.LiteralString}. If more than one argument is
     * passed in then it is assumed to be a {@link patio.sql.PlaceHolderLiteralString}.
     *
     * @example
     *
     * //a literal string that will be placed in an SQL query with out quoting.
     * patio.sql.literal("a"); //=> new patio.sql.LiteralString('a');
     *
     * //a placeholder string that will have ? replaced with the {@link patio.Dataset#literal} version of
     * //the arugment and replaced in the string.
     * patio.sql.literal("a = ?", 1)  //=> a = 1
     * patio.sql.literal("a = ?", "b"); //=> a = 'b'
     * patio.sql.literal("a = {a} AND b = {b}", {a : 1, b : 2}); //=> a = 1 AND b = 2
     *
     * @param {String ...} s variable number of arguments where the first argument
     * is a string. If multiple arguments are passed it is a assumed to be a {@link patio.sql.PlaceHolderLiteralString}
     *
     * @return {patio.sql.LiteralString|patio.sql.PlaceHolderLiteralString} an expression that can be used as an argument
     * for {@link patio.Dataset} query methods.
     */
    literal: function (s) {
        var args = argsToArray(arguments);
        return args.length > 1 ? PlaceHolderLiteralString.fromArgs(args) : new LiteralString(s);
    },

    /**
     * Creates a {@link patio.sql.Json}
     * depending on the arguments passed in. If a single string is passed in then
     * it is assumed that it's a valid json string. If an objects passed in it will stringify
     * it.

     *
     * @param {String or Object ...} An object or string.
     *
     * @return {patio.sql.Json} an expression that can be used as an argument
     * for {@link patio.Dataset} query methods.
     */
    json: function (json) {
        var ret = json;
        if (!(isInstanceOf(ret, Json, JsonArray))) {
            if (isString(ret)) {
                ret = JSON.parse(ret);
            }
            if (isUndefinedOrNull(ret)) {
                ret = null;
            } else if (isArray(ret)) {
                ret = new JsonArray(ret);
            } else if (isObject(ret)) {
                ret = new Json(ret);
            } else {
                throw new ExpressionError("Invalid value for json " + ret);
            }
        }
        return ret;
    },

    /**
     * Returns a {@link patio.sql.CaseExpression}. See {@link patio.sql.CaseExpression} for argument types.
     *
     * @example
     *
     * sql["case"]({a:sql.b}, sql.c, sql.d); //=> (CASE t.d WHEN t.a THEN t.b ELSE t.c END)
     *
     */
    "case": function (hash, /*args**/opts) {
        var args = argsToArray(arguments, 1);
        return CaseExpression.fromArgs([hashToArray(hash)].concat(args));
    },

    /**
     * Creates a {@link patio.sql.StringExpression}
     *
     * Return a {@link patio.sql.StringExpression} representing an SQL string made up of the
     * concatenation of this array's elements.  If an joiner is passed
     * it is used in between each element of the array in the SQL
     * concatenation.
     *
     * @example
     *   patio.sql.sqlStringJoin(["a"]); //=> a
     *   //you can use sql.* as a shortcut to get an identifier
     *   patio.sql.sqlStringJoin([sql.identifier("a"), sql.b]);//=> a || b
     *   patio.sql.sqlStringJoin([sql.a, 'b']) # SQL: a || 'b'
     *   patio.sql.sqlStringJoin(['a', sql.b], ' '); //=> 'a' || ' ' || b
     */
    sqlStringJoin: function (arr, joiner) {
        joiner = joiner || null;
        var args;
        arr = arr.map(function (a) {
            return isInstanceOf(a, Expression, LiteralString, Boolean) || isNull(a) ? a : sql.stringToIdentifier(a);
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
            return isInstanceOf(a, Expression, LiteralString, Boolean) || isNull(a) ? a : "" + a;
        });
        return StringExpression.fromArgs(["||"].concat(args));
    },

    Year: Year,
    TimeStamp: TimeStamp,
    Time: Time,
    DateTime: DateTime,
    Float: Float,
    Decimal: Decimal

};

sql["__defineGetter__"]("patio", function () {
    !patio && (patio = require("./index"));
    return patio;
});

exports.sql = methodMissing(sql, function (name) {
    return virtualRow(name);
});

var OPERTATOR_INVERSIONS = {
    AND: "OR",
    OR: "AND",
    GT: "lte",
    GTE: "lt",
    LT: "gte",
    LTE: "gt",
    EQ: "neq",
    NEQ: "eq",
    LIKE: 'NOT LIKE',
    "NOT LIKE": "LIKE",
    '!~*': '~*',
    '~*': '!~*',
    "~": '!~',
    "IN": 'NOTIN',
    "NOTIN": "IN",
    "IS": 'IS NOT',
    "ISNOT": "IS",
    NOT: "NOOP",
    NOOP: "NOT",
    ILIKE: 'NOT ILIKE',
    NOTILIKE: "ILIKE"
};

// Standard mathematical operators used in +NumericMethods+
var MATHEMATICAL_OPERATORS = {PLUS: "+", MINUS: "-", DIVIDE: "/", MULTIPLY: "*"};

// Bitwise mathematical operators used in +NumericMethods+
var BITWISE_OPERATORS = {bitWiseAnd: "&", bitWiseOr: "|", exclusiveOr: "^", leftShift: "<<", rightShift: ">>"};


var INEQUALITY_OPERATORS = {GT: ">", GTE: ">=", LT: "<", LTE: "<="};

//Hash of ruby operator symbols to SQL operators, used in +BooleanMethods+
var BOOLEAN_OPERATORS = {AND: "AND", OR: "OR"};

//Operators that use IN/NOT IN for inclusion/exclusion
var IN_OPERATORS = {IN: "IN", NOTIN: 'NOT IN'};

//Operators that use IS, used for special casing to override literal true/false values
var IS_OPERATORS = {IS: "IS", ISNOT: 'IS NOT'};

//Operator symbols that take exactly two arguments
var TWO_ARITY_OPERATORS = merge({
    EQ: '=',
    NEQ: '!=',
    LIKE: "LIKE",
    "NOT LIKE": 'NOT LIKE',
    ILIKE: "ILIKE",
    "NOT ILIKE": 'NOT ILIKE',
    "~": "~",
    '!~': "!~",
    '~*': "~*",
    '!~*': "!~*"
}, INEQUALITY_OPERATORS, BITWISE_OPERATORS, IS_OPERATORS, IN_OPERATORS);

//Operator symbols that take one or more arguments
var N_ARITY_OPERATORS = merge({"||": "||"}, BOOLEAN_OPERATORS, MATHEMATICAL_OPERATORS);

//Operator symbols that take only a single argument
var ONE_ARITY_OPERATORS = {"NOT": "NOT", "NOOP": "NOOP"};

/**
 * @class Mixin to provide alias methods to an expression.
 *
 * @name AliasMethods
 * @memberOf patio.sql
 */
var AliasMethods = define(null, {
    instance: {
        /**@lends patio.sql.AliasMethods.prototype*/

        /**
         *  Create an SQL alias {@link patio.sql.AliasedExpression} of the receiving column or expression
         *  to the given alias.
         *
         * @example
         *
         *  sql.identifier("column").as("alias");
         *      //=> "column" AS "alias"
         *
         * @param {String} alias the alias to assign to the expression.
         *
         * @return {patio.sql.AliasedExpression} the aliased expression.
         */
        as: function (alias) {
            return new AliasedExpression(this, alias);
        }

    }
}).as(sql, "AliasMethods");

var bitWiseMethod = function (op) {
    return function (expression) {
        if (isInstanceOf(expression, StringExpression) || isInstanceOf(expression, BooleanExpression)) {
            throw new ExpressionError("Cannot apply " + op + " to a non numeric expression");
        }
        else {
            return new BooleanExpression(op, this, expression);
        }
    };
};

/**
 * @class Defines the bitwise methods: bitWiseAnd, bitWiseOr, exclusiveOr, leftShift, and rightShift.  These
 * methods are only on {@link patio.sql.NumericExpression}
 *
 * @example
 *   sql.a.sqlNumber.bitWiseAnd("b"); //=> "a" & "b"
 *   sql.a.sqlNumber.bitWiseOr("b") //=> "a" | "b"
 *   sql.a.sqlNumber.exclusiveOr("b") //=> "a" ^ "b"
 *   sql.a.sqlNumber.leftShift("b") // "a" << "b"
 *   sql.a.sqlNumber.rightShift("b") //=> "a" >> "b"
 *
 * @name BitWiseMethods
 * @memberOf patio.sql
 */
var BitWiseMethods = define(null, {
    instance: {
        /**@lends patio.sql.BitWiseMethods.prototype*/

        /**
         * Bitwise and
         *
         * @example
         * sql.a.sqlNumber.bitWiseAnd("b"); //=> "a" & "b"
         */
        bitWiseAnd: bitWiseMethod("bitWiseAnd"),

        /**
         * Bitwise or
         *
         * @example
         * sql.a.sqlNumber.bitWiseOr("b") //=> "a" | "b"
         */
        bitWiseOr: bitWiseMethod("bitWiseOr"),

        /**
         * Exclusive Or
         *
         * @example
         *
         * sql.a.sqlNumber.exclusiveOr("b") //=> "a" ^ "b"
         */
        exclusiveOr: bitWiseMethod("exclusiveOr"),

        /**
         *  Bitwise shift left
         *
         *  @example
         *
         *  sql.a.sqlNumber.leftShift("b") // "a" << "b"
         */
        leftShift: bitWiseMethod("leftShift"),

        /**
         * Bitwise shift right
         *
         * @example
         *
         * sql.a.sqlNumber.rightShift("b") //=> "a" >> "b"
         */
        rightShift: bitWiseMethod("rightShift")
    }
}).as(sql, "BitWiseMethods");

var booleanMethod = function (op) {
    return function (expression) {
        if (isInstanceOf(expression, StringExpression) || isInstanceOf(expression, NumericExpression)) {
            throw new ExpressionError("Cannot apply " + op + " to a non boolean expression");
        }
        else {
            return new BooleanExpression(op, this, expression);
        }
    };
};

/**
 * @class Defines boolean/logical AND (&), OR (|) and NOT (~) operators
 * that are defined on objects that can be used in a boolean context in SQL
 * ({@link patio.sql.LiteralString}, and {@link patio.sql.GenericExpression}).
 *
 * @example
 * sql.a.and(sql.b) //=> "a" AND "b"
 * sql.a.or(sql.b) //=> "a" OR "b"
 * sql.a.not() //=> NOT "a"
 *
 * @name BooleanMethods
 * @memberOf patio.sql
 */
var BooleanMethods = define({
    instance: {
        /**@lends patio.sql.BooleanMethods.prototype*/

        /**
         *
         * @function
         * Logical AND
         *
         * @example
         *
         * sql.a.and(sql.b) //=> "a" AND "b"
         *
         * @return {patio.sql.BooleanExpression} a ANDed boolean expression.
         */
        and: booleanMethod("and"),

        /**
         * @function
         * Logical OR
         *
         * @example
         *
         * sql.a.or(sql.b) //=> "a" OR "b"
         *
         * @return {patio.sql.BooleanExpression} a ORed boolean expression
         */
        or: booleanMethod("or"),

        /**
         * Logical NOT
         *
         * @example
         *
         * sql.a.not() //=> NOT "a"
         *
         * @return {patio.sql.BooleanExpression} a inverted boolean expression.
         */
        not: function () {
            return BooleanExpression.invert(this);
        }

    }
}).as(sql, "BooleanMethods");

/**
 * @class Defines case methods
 *
 * @name CastMethods
 * @memberOf patio.sql
 */
var CastMethods = define({
    instance: {
        /**@lends patio.sql.CastMethods.prototype*/
        /**
         * Cast the reciever to the given SQL type.
         *
         * @example
         *
         * sql.a.cast("integer") //=> CAST(a AS integer)
         * sql.a.cast(String) //=> CAST(a AS varchar(255))
         *
         * @return {patio.sql.Cast} the casted expression
         */
        cast: function (type) {
            return new Cast(this, type);
        },

        /**
         * Cast the reciever to the given SQL type (or the database's default Number type if none given.
         *
         * @example
         *
         * sql.a.castNumeric() //=> CAST(a AS integer)
         * sql.a.castNumeric("double") //=> CAST(a AS double precision)
         *
         * @param type the numeric type to cast to
         *
         * @return {patio.sql.NumericExpression} a casted numberic expression
         */
        castNumeric: function (type) {
            return this.cast(type || "integer").sqlNumber;
        },

        /**
         * Cast the reciever to the given SQL type (or the database's default String type if none given),
         * and return the result as a {@link patio.sql.StringExpression}.
         *
         * @example
         *
         *  sql.a.castString() //=> CAST(a AS varchar(255))
         *  sql.a.castString("text") //=> CAST(a AS text)
         * @param type the string type to cast to
         *
         * @return {patio.sql.StringExpression} the casted string expression
         */
        castString: function (type) {
            return this.cast(type || String).sqlString;
        }
    }
}).as(sql, "CastMethods");


/**
 * @class Provides methods to assist in assigning a SQL type to
 * particular types, i.e. Boolean, Function, Number or String.
 *
 * @name ComplexExpressionMethods
 * @memberOf patio.sql
 * @property {patio.sql.BooleanExpression} sqlBoolean Return a {@link patio.sql.BooleanExpression} representation of this expression type.
 * @property {patio.sql.BooleanExpression} sqlFunction Return a {@link patio.sql.SQLFunction} representation of this expression type.
 * @property {patio.sql.BooleanExpression} sqlNumber Return a {@link patio.sql.NumericExpression} representation of this expression type.
 * <pre class="code">
 * sql.a.not("a") //=> NOT "a"
 * sql.a.sqlNumber.not() //=> ~"a"
 * </pre>
 * @property {patio.sql.BooleanExpression} sqlString  Return a {@link patio.sql.StringExpression} representation of this expression type.
 * <pre class="code">
 * sql.a.plus(sql.b); //=> "a" + "b"
 * sql.a.sqlString.concat(sql.b) //=> "a" || "b"
 * </pre>
 */
var ComplexExpressionMethods = define({
    instance: {
        /**@ignore*/
        getters: {

            /**
             * @ignore
             */
            sqlBoolean: function () {
                return new BooleanExpression("noop", this);
            },


            /**
             * @ignore
             */
            sqlFunction: function () {
                return new SQLFunction(this);
            },


            /**
             * @ignore
             */
            sqlNumber: function () {
                return new NumericExpression("noop", this);
            },

            /**
             * @ignore
             */
            sqlString: function () {
                return new StringExpression("noop", this);
            }
        }
    }
}).as(sql, "ComplexExpressionMethods");

var inequalityMethod = function (op) {
    return function (expression) {
        if (isInstanceOf(expression, BooleanExpression) ||
            isBoolean(expression) ||
            isNull(expression) ||
            (isHash(expression)) ||
            isArray(expression)) {
            throw new ExpressionError("Cannot apply " + op + " to a boolean expression");
        } else {
            return new BooleanExpression(op, this, expression);
        }
    };
};

/**
 * @class This mixin includes the inequality methods (>, <, >=, <=) that are defined on objects that can be
 * used in a numeric or string context in SQL.
 *
 * @example
 * sql.a.gt("b")  //=> a > "b"
 * sql.a.lt("b")  //=> a > "b"
 * sql.a.gte("b") //=> a >= "b"
 * sql.a.lte("b") //=> a <= "b"
 * sql.a.eq("b") //=> a = "b"
 *
 * @name InequalityMethods
 * @memberOf patio.sql
 */
var InequalityMethods = define({
    instance: {
        /**@lends patio.sql.InequalityMethods.prototype*/

        /**
         * @function Creates a gt {@link patio.sql.BooleanExpression} compared to this expression.
         * @example
         *
         * sql.a.gt("b")  //=> a > "b"
         *
         * @return {patio.sql.BooleanExpression}
         */
        gt: inequalityMethod("gt"),
        /**
         * @function Creates a gte {@link patio.sql.BooleanExpression} compared to this expression.
         *
         * @example
         *
         * sql.a.gte("b")  //=> a >= "b"
         *
         * @return {patio.sql.BooleanExpression}
         */
        gte: inequalityMethod("gte"),
        /**
         * @function Creates a lt {@link patio.sql.BooleanExpression} compared to this expression.
         *
         * @example
         *
         * sql.a.lt("b")  //=> a < "b"
         *
         * @return {patio.sql.BooleanExpression}
         */
        lt: inequalityMethod("lt"),
        /**
         * @function  Creates a lte {@link patio.sql.BooleanExpression} compared to this expression.
         *
         * @example
         *
         * sql.a.lte("b")  //=> a <= "b"
         *
         * @return {patio.sql.BooleanExpression}
         */
        lte: inequalityMethod("lte"),
        /**
         * @function  Creates a eq {@link patio.sql.BooleanExpression} compared to this expression.
         *
         * @example
         *
         * sql.a.eq("b")  //=> a = "b"
         *
         * @return {patio.sql.BooleanExpression}
         */
        eq: inequalityMethod("eq"),

        neq: inequalityMethod("neq"),

        /**
         * @private
         *
         * Creates a boolean expression where the key is '>=' value 1 and '<=' value two.
         *
         * @example
         *
         * sql.x.between([1,2]) => //=> WHERE ((x >= 1) AND (x <= 10))
         * sql.x.between([1,2]).invert() => //=> WHERE ((x < 1) OR (x > 10))
         *
         * @param {Object} items a two element array where the first element it the item to be gte and the second item lte.
         *
         * @return {patio.sql.BooleanExpression} a boolean expression containing the between expression.
         */
        between: function (items) {
            return new BooleanExpression("AND", new BooleanExpression("gte", this, items[0]), new BooleanExpression("lte", this, items[1]));
        }
    }
}).as(sql, "InequalityMethods");

/**
 * @class This mixin augments the default constructor for {@link patio.sql.ComplexExpression},
 * so that attempting to use boolean input when initializing a {@link patio.sql.NumericExpression}
 * or {@link patio.sql.StringExpression} results in an error. <b>It is not expected to be used directly.</b>
 *
 * @name NoBooleanInputMethods
 * @memberOf patio.sql
 */
var NoBooleanInputMethods = define({
    instance: {
        constructor: function (op) {
            var args = argsToArray(arguments, 1);
            args.forEach(function (expression) {
                if ((isInstanceOf(expression, BooleanExpression)) ||
                    isBoolean(expression) ||
                    isNull(expression) ||
                    (isObject(expression) && !isInstanceOf(expression, Expression, Dataset, LiteralString)) ||
                    isArray(expression)) {
                    throw new ExpressionError("Cannot apply " + op + " to a boolean expression");
                }
            });
            this._super(arguments);
        }
    }
}).as(sql, "NoBooleanInputMethods");

var numericMethod = function (op) {
    return function (expression) {
        if (isInstanceOf(expression, BooleanExpression, StringExpression)) {
            throw new ExpressionError("Cannot apply " + op + " to a non numeric expression");
        } else {
            return new NumericExpression(op, this, expression);
        }
    };
};


/**
 * @class This mixin includes the standard mathematical methods (+, -, *, and /)
 * that are defined on objects that can be used in a numeric context in SQL.
 *
 * @example
 * sql.a.plus(sql.b) //=> "a" + "b"
 * sql.a.minus(sql.b) //=> "a" - "b"
 * sql.a.multiply(sql.b) //=> "a" * "b"
 * sql.a.divide(sql.b) //=> "a" / "b"
 *
 * @name NumericMethods
 * @memberOf patio.sql
 */
var NumericMethods = define({
    instance: {
        /**@lends patio.sql.NumericMethods.prototype*/


        /**
         * @function  Adds the provided expression to this expression and returns a {@link patio.sql.NumericExpression}.
         *
         * @example
         *
         * sql.a.plus(sql.b)  //=> "a" + "b"
         *
         * @return {patio.sql.NumericExpression}
         */
        plus: numericMethod("plus"),

        /**
         * @function  Subtracts the provided expression from this expression and returns a {@link patio.sql.NumericExpression}.
         *
         * @example
         *
         * sql.a.minus(sql.b)  //=> "a" - "b"
         *
         * @return {patio.sql.NumericExpression}
         */
        minus: numericMethod("minus"),

        /**
         * @function  Divides this expression by the  provided expression and returns a {@link patio.sql.NumericExpression}.
         *
         * @example
         *
         * sql.a.divide(sql.b)  //=> "a" / "b"
         *
         * @return {patio.sql.NumericExpression}
         */
        divide: numericMethod("divide"),

        /**
         * @function  Divides this expression by the  provided expression and returns a {@link patio.sql.NumericExpression}.
         *
         * @example
         *
         * sql.a.multiply(sql.b)  //=> "a" * "b"
         *
         * @return {patio.sql.NumericExpression}
         */
        multiply: numericMethod("multiply")
    }
}).as(sql, "NumericMethods");


/**
 * @class This mixin provides ordering methods ("asc", "desc") to expression.
 *
 * @example
 *
 * sql.name.asc(); //=> name ASC
 * sql.price.desc(); //=> price DESC
 * sql.name.asc({nulls:"last"}); //=> name ASC NULLS LAST
 * sql.price.desc({nulls:"first"}); //=> price DESC NULLS FIRST
 *
 * @name OrderedMethods
 * @memberOf patio.sql
 */
var OrderedMethods = define({
    instance: {
        /**@lends patio.sql.OrderedMethods.prototype*/

        /**
         * Mark the receiving SQL column as sorting in an ascending fashion (generally a no-op).
         *
         * @example
         * sql.name.asc(); //=> name ASC
         * sql.name.asc({nulls:"last"}); //=> name ASC NULLS LAST
         *
         * @param {Object} [options] options to use when sorting
         * @param {String} [options.nulls = null] Set to "first" to use NULLS FIRST (so NULL values are ordered
         *           before other values), or "last" to use NULLS LAST (so NULL values are ordered after other values).
         * @return {patio.sql.OrderedExpression}
         */
        asc: function (options) {
            return new OrderedExpression(this, false, options);
        },

        /**
         * Mark the receiving SQL column as sorting in a descending fashion.
         * @example
         *
         * sql.price.desc(); //=> price DESC
         * sql.price.desc({nulls:"first"}); //=> price DESC NULLS FIRST
         *
         * @param {Object} [options] options to use when sorting
         * @param {String} [options.nulls = null] Set to "first" to use NULLS FIRST (so NULL values are ordered
         *           before other values), or "last" to use NULLS LAST (so NULL values are ordered after other values).
         * @return {patio.sql.OrderedExpression}
         */
        desc: function (options) {
            return new OrderedExpression(this, true, options);
        }
    }
}).as(sql, "OrderedMethods");


/**
 * @class This mixin provides methods related to qualifying expression.
 *
 * @example
 *
 * sql.column.qualify("table") //=> "table"."column"
 * sql.table.qualify("schema") //=> "schema"."table"
 * sql.column.qualify("table").qualify("schema") //=> "schema"."table"."column"
 *
 * @name QualifyingMethods
 * @memberOf patio.sql
 */
var QualifyingMethods = define({
    instance: {
        /**@lends patio.sql.QualifyingMethods.prototype*/

        /**
         * Qualify the receiver with the given qualifier (table for column/schema for table).
         *
         * @example
         * sql.column.qualify("table") //=> "table"."column"
         * sql.table.qualify("schema") //=> "schema"."table"
         * sql.column.qualify("table").qualify("schema") //=> "schema"."table"."column"
         *
         * @param {String|patio.sql.Identifier} qualifier table/schema to qualify this expression to.
         *
         * @return {patio.sql.QualifiedIdentifier}
         */
        qualify: function (qualifier) {
            return new QualifiedIdentifier(qualifier, this);
        },

        /**
         * Use to create a .* expression.
         *
         * @example
         * sql.table.all() //=> "table".*
         * sql.table.qualify("schema").all() //=> "schema"."table".*
         *
         *
         * @return {patio.sql.ColumnAll}
         */
        all: function () {
            return new ColumnAll(this);
        }


    }
}).as(sql, "QualifyingMethods");


/**
 * @class This mixin provides SQL string methods such as (like and iLike).
 *
 * @example
 *
 * sql.a.like("A%"); //=> "a" LIKE 'A%'
 * sql.a.iLike("A%"); //=> "a" LIKE 'A%'
 * sql.a.like(/^a/); //=>  "a" ~* '^a'
 *
 * @name StringMethods
 * @memberOf patio.sql
 */
var StringMethods = define({
    instance: {
        /**@lends patio.sql.StringMethods.prototype*/

        /**
         * Create a {@link patio.sql.BooleanExpression} case insensitive pattern match of the receiver
         * with the given patterns.  See {@link patio.sql.StringExpression#like}.
         *
         * @example
         * sql.a.iLike("A%"); //=> "a" LIKE 'A%'
         *
         * @return {patio.sql.BooleanExpression}
         */
        ilike: function (expression) {
            expression = argsToArray(arguments);
            return StringExpression.like.apply(StringExpression, [this].concat(expression).concat([
                {caseInsensitive: true}
            ]));
        },

        /**
         * Create a {@link patio.sql.BooleanExpression} case sensitive (if the database supports it) pattern match of the receiver with
         * the given patterns.  See {@link patio.sql.StringExpression#like}.
         *
         * @example
         * sql.a.like(/^a/); //=>  "a" ~* '^a'
         * sql.a.like("A%"); //=> "a" LIKE 'A%'
         *
         * @param expression
         */
        like: function (expression) {
            expression = argsToArray(arguments);
            return StringExpression.like.apply(StringExpression, [this].concat(expression));
        }
    }
}).as(sql, "StringMethods");

/**
 * @class This mixin provides string concatenation methods ("concat");
 *
 * @example
 *
 * sql.x.sqlString.concat("y"); //=> "x" || "y"
 *
 * @name StringConcatenationMethods
 * @memberOf patio.sql
 */
var StringConcatenationMethods = define({
    instance: {
        /**@lends patio.sql.StringConcatenationMethods.prototype*/

        /**
         * Return a {@link patio.sql.StringExpression} representing the concatenation of this expression
         * with the given argument.
         *
         * @example
         *
         * sql.x.sqlString.concat("y"); //=> "x" || "y"
         *
         * @param expression expression to concatenate this expression with.
         */
        concat: function (expression) {
            return new StringExpression("||", this, expression);
        }
    }
}).as(sql, "StringConcatenationMethods");

/**
 * @class This mixin provides the ability to access elements within a SQL array.
 *
 * @example
 * sql.array.sqlSubscript(1) //=> array[1]
 * sql.array.sqlSubscript(1, 2) //=> array[1, 2]
 * sql.array.sqlSubscript([1, 2]) //=> array[1, 2]
 *
 * @name SubscriptMethods
 * @memberOf patio.sql
 */
var SubscriptMethods = define({
    instance: {

        /**
         * Return a {@link patio.sql.Subscript} with the given arguments, representing an
         * SQL array access.
         *
         * @example
         * sql.array.sqlSubscript(1) //=> array[1]
         * sql.array.sqlSubscript(1, 2) //=> array[1, 2]
         * sql.array.sqlSubscript([1, 2]) //=> array[1, 2]
         *
         * @param subscript
         */
        sqlSubscript: function (subscript) {
            var args = argsToArray(arguments);
            return new SubScript(this, flatten(args));
        }
    }
}).as(sql, "SubScriptMethods");


/**
 * @class This is the parent of all expressions.
 *
 * @name Expression
 * @memberOf patio.sql
 */
Expression = define({

    instance: {
        /**@lends patio.sql.Expression.prototype*/

        /**
         * Returns the string representation of this expression
         *
         * @param {patio.Dataset} ds the dataset that will be used to SQL-ify this expression.
         * @return {String} a string literal version of this expression.
         */
        sqlLiteral: function (ds) {
            return this.toString(ds);
        }

    },

    static: {
        /**@lends patio.sql.Expression*/

        /**
         * This is a helper method that will take in an array of arguments and return an expression.
         *
         * @example
         *
         * QualifiedIdentifier.fromArgs(["table", "column"]);
         *
         * @param {*[]} args array of arguments to pass into the constructor of the function.
         *
         * @return {patio.sql.Expression} an expression.
         */
        fromArgs: function (args) {
            var ret, Self = this;
            try {
                ret = new Self();
            } catch (ignore) {
            }
            this.apply(ret, args);
            return ret;
        },

        /**
         * Helper to determine if something is a condition specifier. Returns true if the object
         * is a Hash or is an array of two element arrays.
         *
         * @example
         * Expression.isConditionSpecifier({a : "b"}); //=> true
         * Expression.isConditionSpecifier("a"); //=> false
         * Expression.isConditionSpecifier([["a", "b"], ["c", "d"]]); //=> true
         * Expression.isConditionSpecifier([["a", "b", "e"], ["c", "d"]]); //=> false
         *
         * @param {*} obj object to test if it is a condition specifier
         * @return {Boolean} true if the object is a Hash or is an array of two element arrays.
         */
        isConditionSpecifier: function (obj) {
            return isHash(obj) || (isArray(obj) && obj.length && obj.every(function (i) {
                    return isArray(i) && i.length === 2;
                }));
        }
    }

}).as(sql, "Expression");

/**
 * @class Base class for all GenericExpressions
 *
 * @augments patio.sql.Expression
 * @augments patio.sql.AliasMethods
 * @augments patio.sql.BooleanMethods
 * @augments patio.sql.CastMethods
 * @augments patio.sql.ComplexExpressionMethods
 * @augments patio.sql.InequalityMethods
 * @augments patio.sql.NumericMethods
 * @augments patio.sql.OrderedMethods
 * @augments patio.sql.StringMethods
 * @augments patio.sql.SubscriptMethods
 *
 * @name GenericExpression
 * @memberOf patio.sql
 */
var GenericExpression = define([Expression, AliasMethods, BooleanMethods, CastMethods, ComplexExpressionMethods, InequalityMethods, NumericMethods, OrderedMethods, StringMethods, SubscriptMethods]).as(sql, "GenericExpression");


AliasedExpression = Expression.extend({
        instance: {
            /**@lends patio.sql.AliasedExpression.prototype*/

            /**
             * This class reperesents an Aliased Expression
             *
             * @constructs
             * @augments patio.sql.Expression
             *
             * @param expression  the expression to alias.
             * @param alias the alias to alias the expression to.
             *
             * @property expression the expression being aliased
             * @property alias the alias of the expression
             *
             */
            constructor: function (expression, alias) {
                this.expression = expression;
                this.alias = alias;
            },

            /**
             * Converts the aliased expression to a string
             * @param {patio.Dataset} [ds] dataset used to created the SQL fragment, if
             * the dataset is ommited then the default {@link patio.Dataset} implementation is used.
             *
             * @return String the SQL alias fragment.
             */
            toString: function (ds) {
                !Dataset && (Dataset = require("./dataset"));
                ds = ds || new Dataset();
                return ds.aliasedExpressionSql(this);
            }
        }
    }
).as(sql, "AliasedExpression");


CaseExpression = GenericExpression.extend({
    instance: {
        /**@lends patio.sql.CaseExpression.prototype*/

        /**
         * Create an object with the given conditions and
         * default value.  An expression can be provided to
         * test each condition against, instead of having
         * all conditions represent their own boolean expression.
         *
         * @constructs
         * @augments patio.sql.GenericExpression
         * @param {Array|Object} conditions conditions to create the case expression from
         * @param def default value
         * @param expression expression to create the CASE expression from
         *
         * @property {Boolean} hasExpression returns true if this case expression has a expression
         * @property conditions the conditions of the {@link patio.sql.CaseExpression}.
         * @property def the default value of the {@link patio.sql.CaseExpression}.
         * @property expression the expression of the {@link patio.sql.CaseExpression}.
         * @property {Boolean} noExpression true if this {@link patio.sql.CaseExpression}'s expression is undefined.
         */
        constructor: function (conditions, def, expression) {
            if (Expression.isConditionSpecifier(conditions)) {
                this.conditions = toArray(conditions);
                this.def = def;
                this.expression = expression;
                this.noExpression = isUndefined(expression);
            }
        },

        /**
         * Converts the case expression to a string
         *
         * @param {patio.Dataset} [ds] dataset used to created the SQL fragment, if
         * the dataset is ommited then the default {@link patio.Dataset} implementation is used.
         *
         * @return String the SQL case expression fragment.
         */
        toString: function (ds) {
            !Dataset && (Dataset = require("./dataset"));
            ds = ds || new Dataset();
            return ds.caseExpressionSql(this);
        },

        /**@ignore*/
        getters: {
            /**@ignore*/
            hasExpression: function () {
                return !this.noExpression;
            }
        }
    }
}).as(sql, "CaseExpression");


Cast = GenericExpression.extend({
    instance: {
        /**@lends patio.sql.Cast*/

        /**
         * Represents a cast of an SQL expression to a specific type.
         * @constructs
         * @augments patio.sql.GenericExpression
         *
         * @param expr the expression to CAST.
         * @param type the  type to CAST the expression to.
         *
         * @property expr the expression to CAST.
         * @property type the  type to CAST the expression to.
         */
        constructor: function (expr, type) {
            this.expr = expr;
            this.type = type;
        },

        /**
         * Converts the cast expression to a string
         *
         * @param {patio.Dataset} [ds] dataset used to created the SQL fragment, if
         * the dataset is ommited then the default {@link patio.Dataset} implementation is used.
         *
         * @return String the SQL cast expression fragment.
         */
        toString: function (ds) {
            !Dataset && (Dataset = require("./dataset"));
            ds = ds || new Dataset();
            return ds.castSql(this.expr, this.type);
        }
    }
}).as(sql, "Cast");


ColumnAll = Expression.extend({
    instance: {
        /**@lends patio.sql.ColumnAll.prototype*/

        /**
         * Represents all columns in a given table, table.* in SQL
         * @constructs
         *
         * @augments patio.sql.Expression
         *
         * @param table the table this expression is for.
         *
         * @property table the table this all column expression represents.
         */
        constructor: function (table) {
            this.table = table;
        },

        /**
         * Converts the ColumnAll expression to a string
         *
         * @param {patio.Dataset} [ds] dataset used to created the SQL fragment, if
         * the dataset is ommited then the default {@link patio.Dataset} implementation is used.
         *
         * @return String the SQL columnAll expression fragment.
         */
        toString: function (ds) {
            !Dataset && (Dataset = require("./dataset"));
            ds = ds || new Dataset();
            return ds.columnAllSql(this);
        }
    }
}).as(sql, "ColumnAll");

var ComplexExpression = define([Expression, AliasMethods, CastMethods, OrderedMethods, SubscriptMethods], {
    instance: {
        /**@lends patio.sql.ComplexExpression.prototype*/

        /**
         * Represents a complex SQL expression, with a given operator and one
         * or more attributes (which may also be ComplexExpressions, forming
         * a tree).
         *
         * This is an abstract class that is not that useful by itself.  The
         * subclasses @link patio.sql.BooleanExpression},
         * {@link patio.sql.NumericExpression} and {@link patio.sql.StringExpression} should
         * be used instead of this class directly.
         *
         * @constructs
         * @augments patio.sql.Expression
         * @augments patio.sql.AliasMethods
         * @augments patio.sql.CastMethods
         * @augments patio.sql.OrderedMethods
         * @augments patio.sql.SubscriptMethods
         *
         * @throws {patio.sql.ExpressionError} if the operator doesn't allow boolean input and a boolean argument is given.
         * @throws {patio.sql.ExpressionError} if the wrong number of arguments for a given operator is used.
         *
         * @param {...} op The operator and arguments for this object to the ones given.
         * <p>
         *     Convert all args that are hashes or arrays of two element arrays to {@link patio.sql.BooleanExpression}s,
         *          other than the second arg for an IN/NOT IN operator.</li>
         * </p>
         */
        constructor: function (op) {
            if (op) {
                var args = argsToArray(arguments, 1);
                //make a copy of the args
                var origArgs = args.slice(0);
                args.forEach(function (a, i) {
                    if (Expression.isConditionSpecifier(a)) {
                        args[i] = BooleanExpression.fromValuePairs(a);
                    }
                });
                op = op.toUpperCase();

                if (N_ARITY_OPERATORS.hasOwnProperty(op)) {
                    if (args.length < 1) {
                        throw new ExpressionError("The " + op + " operator requires at least 1 argument");
                    }
                    var oldArgs = args.slice(0);
                    args = [];
                    oldArgs.forEach(function (a) {
                        a instanceof ComplexExpression && a.op === op ? args = args.concat(a.args) : args.push(a);
                    });

                } else if (TWO_ARITY_OPERATORS.hasOwnProperty(op)) {
                    if (args.length !== 2) {
                        throw new ExpressionError("The " + op + " operator requires precisely 2 arguments");
                    }
                    //With IN/NOT IN, even if the second argument is an array of two element arrays,
                    //don't convert it into a boolean expression, since it's definitely being used
                    //as a value list.
                    if (IN_OPERATORS[op]) {
                        args[1] = origArgs[1];
                    }
                } else if (ONE_ARITY_OPERATORS.hasOwnProperty(op)) {
                    if (args.length !== 1) {
                        throw new ExpressionError("The " + op + " operator requires only one argument");
                    }
                } else {
                    throw new ExpressionError("Invalid operator " + op);
                }
                this.op = op;
                this.args = args;
            }
        },

        /**
         * Converts the ComplexExpression to a string.
         *
         * @param {patio.Dataset} [ds] dataset used to created the SQL fragment, if
         * the dataset is ommited then the default {@link patio.Dataset} implementation is used.
         *
         * @return String the SQL version of the {@link patio.sql.ComplexExpression}.
         */
        toString: function (ds) {
            !Dataset && (Dataset = require("./dataset"));
            ds = ds || new Dataset();
            return ds.complexExpressionSql(this.op, this.args);
        }
    },

    static: {
        /**@lends patio.sql.ComplexExpression*/

        /**
         * Hash of operator inversions
         * @type Object
         * @default {
         *      AND:"OR",
         *      OR:"AND",
         *      GT:"lte",
         *      GTE:"lt",
         *      LT:"gte",
         *      LTE:"gt",
         *      EQ:"neq",
         *      NEQ:"eq",
         *      LIKE:'NOT LIKE',
         *      "NOT LIKE":"LIKE",
         *      '!~*':'~*',
         *      '~*':'!~*',
         *      "~":'!~',
         *      "IN":'NOTIN',
         *      "NOTIN":"IN",
         *      "IS":'IS NOT',
         *      "ISNOT":"IS",
         *      NOT:"NOOP",
         *      NOOP:"NOT",
         *      ILIKE:'NOT ILIKE',
         *      NOTILIKE:"ILIKE"
         * }
         */
        OPERATOR_INVERSIONS: OPERTATOR_INVERSIONS,

        /**
         * Default mathematical operators.
         *
         * @type Object
         * @default {PLUS:"+", MINUS:"-", DIVIDE:"/", MULTIPLY:"*"}
         */
        MATHEMATICAL_OPERATORS: MATHEMATICAL_OPERATORS,

        /**
         * Default bitwise operators.
         *
         * @type Object
         * @default {bitWiseAnd:"&", bitWiseOr:"|", exclusiveOr:"^", leftShift:"<<", rightShift:">>"}
         */
        BITWISE_OPERATORS: BITWISE_OPERATORS,
        /**
         * Default inequality operators.
         *
         * @type Object
         * @default {GT:">",GTE:">=",LT:"<",LTE:"<="}
         */
        INEQUALITY_OPERATORS: INEQUALITY_OPERATORS,

        /**
         * Default boolean operators.
         *
         * @type Object
         * @default {AND:"AND",OR:"OR"}
         */
        BOOLEAN_OPERATORS: BOOLEAN_OPERATORS,

        /**
         * Default IN operators.
         *
         * @type Object
         * @default {IN:"IN",NOTIN:'NOT IN'}
         */
        IN_OPERATORS: IN_OPERATORS,
        /**
         * Default IS operators.
         *
         * @type Object
         * @default {IS:"IS", ISNOT:'IS NOT'}
         */
        IS_OPERATORS: IS_OPERATORS,
        /**
         * Default two arity operators.
         *
         * @type Object
         * @default {
         *      EQ:'=',
         *      NEQ:'!=', LIKE:"LIKE",
         *      "NOT LIKE":'NOT LIKE',
         *      ILIKE:"ILIKE",
         *      "NOT ILIKE":'NOT ILIKE',
         *      "~":"~",
         *      '!~':"!~",
         *      '~*':"~*",
         *      '!~*':"!~*",
         *      GT:">",
         *      GTE:">=",
         *      LT:"<",
         *      LTE:"<=",
         *      bitWiseAnd:"&",
         *      bitWiseOr:"|",
         *      exclusiveOr:"^",
         *      leftShift:"<<",
         *      rightShift:">>",
         *      IS:"IS",
         *      ISNOT:'IS NOT',
         *      IN:"IN",
         *      NOTIN:'NOT IN'
         *  }
         */
        TWO_ARITY_OPERATORS: TWO_ARITY_OPERATORS,

        /**
         * Default N(multi) arity operators.
         *
         * @type Object
         * @default {
         *      "||":"||",
         *      AND:"AND",
         *      OR:"OR",
         *      PLUS:"+",
         *      MINUS:"-",
         *      DIVIDE:"/", MULTIPLY:"*"
         * }
         */
        N_ARITY_OPERATORS: N_ARITY_OPERATORS,

        /**
         * Default ONE operators.
         *
         * @type Object
         * @default {
         *      "NOT":"NOT",
         *      "NOOP":"NOOP"
         *  }
         */
        ONE_ARITY_OPERATORS: ONE_ARITY_OPERATORS
    }
}).as(sql, "ComplexExpression");


/**
 * @class Subclass of {@link patio.sql.ComplexExpression} where the expression results
 * in a boolean value in SQL.
 *
 * @augments patio.sql.ComplexExpression
 * @augments patio.sql.BooleanMethods
 * @name BooleanExpression
 * @memberOf patio.sql
 */
BooleanExpression = define([ComplexExpression, BooleanMethods], {
    static: {
        /**@lends patio.sql.BooleanExpression*/

        /**
         * Invert the expression, if possible.  If the expression cannot
         * be inverted, it throws an {@link patio.error.ExpressionError}.  An inverted expression should match
         * everything that the uninverted expression did not match, and vice-versa, except for possible issues with
         * SQL NULL (i.e. 1 == NULL is NULL and 1 != NULL is also NULL).
         *
         * @example
         *   BooleanExpression.invert(sql.a) //=> NOT "a"
         *
         * @param {patio.sql.BooleanExpression} expression
         * the expression to invert.
         *
         * @return {patio.sql.BooleanExpression} the inverted expression.
         */
        invert: function (expression) {
            if (isInstanceOf(expression, BooleanExpression)) {
                var op = expression.op, newArgs;
                if (op === "AND" || op === "OR") {
                    newArgs = [OPERTATOR_INVERSIONS[op]].concat(expression.args.map(function (arg) {
                        return BooleanExpression.invert(arg);
                    }));
                    return BooleanExpression.fromArgs(newArgs);
                } else {
                    newArgs = [OPERTATOR_INVERSIONS[op]].concat(expression.args);
                    return BooleanExpression.fromArgs(newArgs);
                }
            } else if (isInstanceOf(expression, StringExpression) || isInstanceOf(expression, NumericExpression)) {
                throw new ExpressionError(format("Cannot invert %4j", [expression]));
            } else {
                return new BooleanExpression("NOT", expression);
            }
        },
        /**
         * Take pairs of values (e.g. a hash or array of two element arrays)
         * and converts it to a {@link patio.sql.BooleanExpression}.  The operator and args
         * used depends on the case of the right (2nd) argument:
         *
         * <pre class='code'>
         * BooleanExpression.fromValuePairs({a : [1,2,3]}) //=> a IN (1,2,3)
         * BooleanExpression.fromValuePairs({a : true}); // a IS TRUE;
         * BooleanExpression.fromValuePairs({a : /^A/i}); // a *~ '^A'
         * </pre>
         *
         * If multiple arguments are given, they are joined with the op given (AND
         * by default, OR possible).  If negate is set to true,
         * all subexpressions are inverted before used.
         * <pre class='code'>
         * BooleanExpression.fromValuePairs({a : [1,2,3], b : true}) //=> a IN (1,2,3) AND b IS TRUE
         * BooleanExpression.fromValuePairs({a : [1,2,3], b : true}, "OR") //=> a IN (1,2,3) OR b IS TRUE
         * BooleanExpression.fromValuePairs({a : [1,2,3], b : true}, "OR", true) //=> a NOT IN (1,2,3) AND b IS NOT TRUE
         * </pre>
         * @param {Object} a object to convert to a {@link patio.sql.BooleanExpression}
         * @param {String} [op="AND"] Boolean operator to join each subexpression with.
         * <pre class="code">
         *     BooleanExpression.fromValuePairs({a : [1,2,3], b : true}, "OR") //=> a IN (1,2,3) OR b IS TRUE
         * </pre>
         * @param {Boolean} [negate=false] set to try to invert the {@link patio.sql.BooleanExpression}.
         * <pre class="code">
         * BooleanExpression.fromValuePairs({a : [1,2,3], b : true}, "OR", true) //=> a NOT IN (1,2,3) AND b IS NOT TRUE
         * </pre>
         * @return {patio.sql.BooleanExpression} expression composed of sub expressions built from the hash.
         */
        fromValuePairs: function (a, op, negate) {
            !Dataset && (Dataset = require("./dataset"));
            op = op || "AND", negate = negate || false;
            var pairArr = [];
            var isArr = isArray(a) && Expression.isConditionSpecifier(a);
            if (isHash(a)) {
                pairArr.push(this.__filterObject(a, null, op));
            } else {
                for (var k in a) {
                    var v = isArr ? a[k][1] : a[k], ret;
                    k = isArr ? a[k][0] : k;
                    if (isArray(v) || isInstanceOf(v, Dataset)) {
                        k = isArray(k) ? k.map(sql.stringToIdentifier) : sql.stringToIdentifier(k);
                        ret = new BooleanExpression("IN", k, v);
                    } else if (isInstanceOf(v, NegativeBooleanConstant)) {
                        ret = new BooleanExpression("ISNOT", k, v.constant);
                    } else if (isInstanceOf(v, BooleanConstant)) {
                        ret = new BooleanExpression("IS", k, v.constant);
                    } else if (isNull(v) || isBoolean(v)) {
                        ret = new BooleanExpression("IS", k, v);
                    } else if (isHash(v)) {
                        ret = BooleanExpression.__filterObject(v, k, op);
                    } else if (isRegExp(v)) {
                        ret = StringExpression.like(sql.stringToIdentifier(k), v);
                    } else {
                        ret = new BooleanExpression("EQ", sql.stringToIdentifier(k), v);
                    }
                    negate && (ret = BooleanExpression.invert(ret));
                    pairArr.push(ret);
                }
            }
            //if We just have one then return the first otherwise create a new Boolean expression
            return pairArr.length === 1 ? pairArr[0] : BooleanExpression.fromArgs([op].concat(pairArr));
        },

        /**
         * @private
         *
         * This builds an expression from a hash
         *
         * @example
         *
         *  Dataset._filterObject({a : 1}) //=> WHERE (a = 1)
         *  Dataset._filterObject({x : {gt : 1}}) //=> WHERE (x > 1)
         *  Dataset._filterObject({x : {gt : 1}, a : 1}) //=> WHERE ((x > 1) AND (a = 1))
         *  Dataset._filterObject({x : {like : "name"}}) //=> WHERE (x LIKE 'name')
         *  Dataset._filterObject({x : {iLike : "name"}}) //=> WHERE (x LIKE 'name')
         *  Dataset._filterObject({x : {between : [1,10]}}) //=> WHERE ((x >= 1) AND (x <= 10))
         *  Dataset._filterObject({x : {notBetween : [1,10]}}) //=> WHERE ((x < 1) OR (x > 10))
         *  Dataset._filterObject({x : {neq : 1}}) //=> WHERE (x != 1)
         *
         * @param {Object} expr the expression we need to create an expression out of
         * @param {String} [key=null] the key that the hash corresponds to
         *
         * @return {patio.sql.Expression} an expression to use in the filter
         */
        __filterObject: function (expr, key, op) {
            /*jshint loopfunc:true*/
            var pairs = [], opts, newKey;
            var twoArityOperators = this.TWO_ARITY_OPERATORS;
            for (var k in expr) {
                var v = expr[k];
                if (isHash(v)) { //its a hash too filter it too!
                    pairs.push(this.__filterObject(v, k, op));
                } else if (key && (twoArityOperators[k.toUpperCase()] || k.match(/between/i))) {
                    //its a two arrity operator (e.g. '=', '>')
                    newKey = isString(key) ? key.split(",") : [key];
                    if (newKey.length > 1) {
                        //this represents a hash where the key represents two columns
                        //(e.g. {"col1,col2" : 1}) => WHERE (col1 = 1 AND col2 = 1)
                        pairs = pairs.concat(newKey.map(function (k) {
                            //filter each column with the expression
                            return this.__filterObject(expr, k, op);
                        }, this));
                    } else {
                        newKey = [sql.stringToIdentifier(newKey[0])];
                        if (k.match(/^like$/)) {
                            //its a like clause {col : {like : "hello"}}

                            pairs.push(StringExpression.like.apply(StringExpression, (newKey.concat(isArray(v) ? v : [v]))));
                        } else if (k.match(/^iLike$/)) {
                            //its a like clause {col : {iLike : "hello"}}
                            pairs.push(StringExpression.like.apply(StringExpression, (newKey.concat(isArray(v) ? v : [v]).concat({caseInsensitive: true}))));
                        } else if (k.match(/between/i)) {
                            //its a like clause {col : {between : [1,10]}}
                            var between = sql.stringToIdentifier(newKey[0]).between(v);
                            k === "notBetween" && (between = between.not());
                            pairs.push(between);
                        } else {
                            //otherwise is just a boolean expressio
                            //it its not a valid operator then we
                            //BooleanExpression with throw an error
                            pairs.push(new BooleanExpression(k, newKey[0], v));
                        }
                    }
                } else {
                    //we're not a twoarity operator
                    //so we create a boolean expression out of it
                    newKey = k.split(",");
                    if (newKey.length === 1) {
                        newKey = sql.stringToIdentifier(newKey[0]);
                    }
                    opts = [
                        [newKey, v]
                    ];
                    pairs.push(BooleanExpression.fromValuePairs(opts));
                }
            }
            //if the total of pairs is one then we just return the first element
            //otherwise we join them all with an AND
            return pairs.length === 1 ? pairs[0] : BooleanExpression.fromArgs([op || "AND"].concat(pairs));
        }
    }
}).as(sql, "BooleanExpression");


var Constant = GenericExpression.extend({
    instance: {
        /**@lends patio.sql.Constant.prototype*/
        /**
         * Represents constants or psuedo-constants (e.g.'CURRENT_DATE) in SQL.
         *
         * @constructs
         * @augments patio.sql.GenericExpression
         * @property {String} constant <b>READ ONLY</b> the contant.
         */
        constructor: function (constant) {
            this.__constant = constant;
        },

        /**
         * Converts the {@link patio.sql.Constant} to a string.
         *
         * @param {patio.Dataset} [ds] dataset used to created the SQL fragment, if
         * the dataset is ommited then the default {@link patio.Dataset} implementation is used.
         *
         * @return String the SQL version of the {@link patio.sql.Constant}.
         */
        toString: function (ds) {
            !Dataset && (Dataset = require("./dataset"));
            ds = ds || new Dataset();
            return ds.constantSql(this.__constant);
        },

        getters: {
            constant: function () {
                return this.__constant;
            }
        }
    }
}).as(sql, "Constant");

/**
 * @class Represents boolean constants such as NULL, NOTNULL, TRUE, and FALSE.
 * @augments patio.sql.Constant
 * @name BooleanConstant
 * @memberOf patio.sql
 */
BooleanConstant = Constant.extend({
    instance: {
        /**@lends patio.sql.BooleanConstant.prototype*/

        /**
         * Converts the {@link patio.sql.BooleanConstant} to a string.
         *
         * @param {patio.Dataset} [ds] dataset used to created the SQL fragment, if
         * the dataset is ommited then the default {@link patio.Dataset} implementation is used.
         *
         * @return String the SQL version of the {@link patio.sql.BooleanConstant}.
         */
        toString: function (ds) {
            !Dataset && (Dataset = require("./dataset"));
            ds = ds || new Dataset();
            return ds.booleanConstantSql(this.__constant);
        }
    }
}).as(sql, "BooleanConstant");

/**
 * Represents inverse boolean constants (currently only NOTNULL). A
 * special class to allow for special behavior.
 *
 * @augments patio.sql.BooleanConstant
 * @name NegativeBooleanConstant
 * @memberOf patio.sql
 */
NegativeBooleanConstant = BooleanConstant.extend({
    instance: {
        /**@lends patio.sql.NegativeBooleanConstant.prototype*/

        /**
         * Converts the {@link patio.sql.NegativeBooleanConstant} to a string.
         *
         * @param {patio.Dataset} [ds] dataset used to created the SQL fragment, if
         * the dataset is ommited then the default {@link patio.Dataset} implementation is used.
         *
         * @return String the SQL version of the {@link patio.sql.NegativeBooleanConstant}.
         */
        toString: function (ds) {
            !Dataset && (Dataset = require("./dataset"));
            ds = ds || new Dataset();
            return ds.negativeBooleanConstantSql(this.__constant);
        }
    }
}).as(sql, "NegativeBooleanConstant");

/**
 * @namespace Holds default generic constants that can be referenced.  These
 * are included in {@link patio}
 * @name Constants
 * @memberOf patio.sql
 */
sql.Constants = {
    /**@lends patio.sql.Constants*/

    /**
     * Constant for CURRENT DATE
     * @type patio.sql.Constant
     */
    CURRENT_DATE: new Constant("CURRENT_DATE"),

    /**
     * Constant for CURRENT TIME
     * @type patio.sql.Constant
     */
    CURRENT_TIME: new Constant("CURRENT_TIME"),

    /**
     * Constant for CURRENT TIMESTAMP
     * @type patio.sql.Constant
     */
    CURRENT_TIMESTAMP: new Constant("CURRENT_TIMESTAMP"),

    /**
     * Constant for TRUE
     * @type patio.sql.BooleanConstant
     */
    SQLTRUE: new BooleanConstant(1),

    /**
     * Constant for TRUE
     * @type patio.sql.BooleanConstant
     */
    TRUE: new BooleanConstant(1),

    /**
     * Constant for FALSE.
     * @type patio.sql.BooleanConstant
     */
    SQLFALSE: new BooleanConstant(0),

    /**
     * Constant for FALSE
     * @type patio.sql.BooleanConstant
     */
    FALSE: new BooleanConstant(0),
    /**
     * Constant for NULL
     * @type patio.sql.BooleanConstant
     */
    NULL: new BooleanConstant(null),

    /**
     * Constant for NOT NULL
     * @type patio.sql.NegativeBooleanConstant
     */
    NOTNULL: new NegativeBooleanConstant(null)

};

var Constants = sql.Constants;

Identifier = define([GenericExpression, QualifyingMethods], {
    instance: {
        /**@lends patio.sql.Identifier.prototype*/

        /**
         * Represents an identifier (column or table). Can be used
         * to specify a String with multiple underscores that should not be
         * split, or for creating an implicit identifier without using a String.
         *
         * @constructs
         * @augments patio.sql.GenericExpression
         * @augments patio.sql.QualifyingMethods
         *
         * @param {String}value the identifier.
         *
         * @property {String} value <b>READ ONLY</b> the column or table this identifier represents.
         */
        constructor: function (value) {
            this.__value = value;
        },

        /**
         * Converts the {@link patio.sql.Identifier} to a string.
         *
         * @param {patio.Dataset} [ds] dataset used to created the SQL fragment, if
         * the dataset is ommited then the default {@link patio.Dataset} implementation is used.
         *
         * @return String the SQL version of the {@link patio.sql.Identifier}.
         */
        toString: function (ds) {
            !Dataset && (Dataset = require("./dataset"));
            ds = ds || new Dataset();
            return ds.quoteIdentifier(this);
        },

        /**@ignore*/
        getters: {
            value: function () {
                return this.__value;
            }
        }
    }
}).as(sql, "Identifier");

var JoinClause = Expression.extend({
    instance: {
        /**@lends patio.sql.JoinClause.prototype*/

        /**
         * Represents an SQL JOIN clause, used for joining tables.
         * Created by {@link patio.Dataset} join methods.
         * @constructs
         * @augments patio.sql.Expression
         *
         * @param {String} joinType the type of join this JoinClause should use
         * @param table the table to join with
         * @param tableAlias the alias to use for this join clause
         *
         * @property {String} joinType <b>READ ONLY</b> the type of join this JoinClause should use
         * @property table <b>READ ONLY</b> the table to join with
         * @property joinType <b>READ ONLY</b> the alias to use for this join clause
         * */
        constructor: function (joinType, table, tableAlias) {
            this.__joinType = joinType;
            this.__table = table;
            this.__tableAlias = tableAlias || null;
        },

        /**
         * Converts the {@link patio.sql.JoinClause} to a string.
         *
         * @param {patio.Dataset} [ds] dataset used to created the SQL fragment, if
         * the dataset is ommited then the default {@link patio.Dataset} implementation is used.
         *
         * @return String the SQL version of the {@link patio.sql.JoinClause}.
         */
        toString: function (ds) {
            !Dataset && (Dataset = require("./dataset"));
            ds = ds || new Dataset();
            return ds.joinClauseSql(this);
        },

        /**@ignore*/
        getters: {
            joinType: function () {
                return this.__joinType;
            },

            table: function () {
                return this.__table;
            },

            tableAlias: function () {
                return this.__tableAlias;
            }
        }
    }
}).as(sql, "JoinClause");


var JoinOnClause = JoinClause.extend({
    instance: {
        /**@lends patio.sql.JoinOnClause.prototype*/
        /**
         * Represents an SQL JOIN clause with ON conditions. Created by {@link patio.Dataset} join methods.
         * See {@link patio.sql.JoinClause} for other argument parameters.
         * @constructs
         * @augments patio.sql.JoinClause
         *
         * @param on the expression to filter with. See {@link patio.Dataset#filter}
         * @property on <b>READ ONLY</b> the filter to use with joining the datasets.
         */
        constructor: function (on, joinType, table, tableAlias) {
            this.__on = on;
            this._super(arguments, [joinType, table, tableAlias]);
        },

        /**
         * Converts the {@link patio.sql.JoinOnClause} to a string.
         *
         * @param {patio.Dataset} [ds] dataset used to created the SQL fragment, if
         * the dataset is ommited then the default {@link patio.Dataset} implementation is used.
         *
         * @return String the SQL version of the {@link patio.sql.JoinOnClause}.
         */
        toString: function (ds) {
            !Dataset && (Dataset = require("./dataset"));
            ds = ds || new Dataset();
            return ds.joinOnClauseSql(this);
        },

        /**@ignore*/
        getters: {
            on: function () {
                return this.__on;
            }
        }
    }
}).as(sql, "JoinOnClause");


var JoinUsingClause = JoinClause.extend({
    instance: {
        /**@lends patio.sql.JoinUsingClause.prototype*/

        /**
         * Represents an SQL JOIN clause with USING conditions.
         * Created by {@link patio.Dataset} join methods.
         * See {@link patio.sql.JoinClause} for other argument parameters.
         *
         * @constructs
         * @augments patio.sql.JoinClause
         *
         * @param using the column/s to use when joining.
         * @property using <b>READ ONLY</b> the column/s to use when joining.
         */
        constructor: function (using, joinType, table, tableAlias) {
            this.__using = using.map(function (u) {
                return isString(u) ? new Identifier(u) : u;
            });
            this._super(arguments, [joinType, table, tableAlias]);
        },

        /**
         * Converts the {@link patio.sql.JoinUsingClause} to a string.
         *
         * @param {patio.Dataset} [ds] dataset used to created the SQL fragment, if
         * the dataset is ommited then the default {@link patio.Dataset} implementation is used.
         *
         * @return String the SQL version of the {@link patio.sql.JoinUsingClause}.
         */
        toString: function (ds) {
            !Dataset && (Dataset = require("./dataset"));
            ds = ds || new Dataset();
            return ds.joinUsingClauseSql(this);
        },

        /**@ignore*/
        getters: {
            using: function () {
                return this.__using;
            }
        }
    }
}).as(sql, "JoinUsingClause");


PlaceHolderLiteralString = GenericExpression.extend({
    instance: {
        /**@lends patio.sql.PlaceHolderLiteralString.prototype*/

        /**
         * Represents a literal string with placeholders and arguments.
         * This is necessary to ensure delayed literalization of the arguments
         * required for the prepared statement support and for database-specific
         * literalization.
         *
         * @constructs
         * @augments patio.sql.GenericExpression
         *
         * @param {String} str the string that contains placeholders.
         * @param {Array} args array of arguments that will be literalized using {@link patio.Dataset#literal}, and
         * replaced in the string.
         * @param {Boolean} [parens=false] set to true to wrap the string in parens.
         *
         * @property {String} str <b>READ ONLY</b> the string that contains placeholders.
         * @property {Array} args <b>READ ONLY</b> array of arguments that will be literalized using {@link patio.Dataset#literal}, and
         * replaced in the string.
         * @property {String} parens <b>READ ONLY</b> set to true to wrap the string in parens.
         */
        constructor: function (str, args, parens) {
            parens = parens || false;
            var v;
            this.__str = str;
            this.__args = isArray(args) && args.length === 1 && isHash((v = args[0])) ? v : args;
            this.__parens = parens;
        },

        /**
         * Converts the {@link patio.sql.PlaceHolderLiteralString} to a string.
         *
         * @param {patio.Dataset} [ds] dataset used to created the SQL fragment, if
         * the dataset is ommited then the default {@link patio.Dataset} implementation is used.
         *
         * @return String the SQL version of the {@link patio.sql.PlaceHolderLiteralString}.
         */
        toString: function (ds) {
            !Dataset && (Dataset = require("./dataset"));
            ds = ds || new Dataset();
            return ds.placeholderLiteralStringSql(this);
        },

        /**@ignore*/
        getters: {
            str: function () {
                return this.__str;
            },
            args: function () {
                return this.__args;
            },

            parens: function () {
                return this.__parens;
            }

        }
    }
}).as(sql, "PlaceHolderLiteralString");


SQLFunction = GenericExpression.extend({
    instance: {
        /**@lends patio.sql.SQLFunction.prototype*/

        /**
         * Represents an SQL function call.
         *
         * @constructs
         * @augments patio.sql.GenericExpression
         *
         * @param {...} f variable number of arguments where the first argument is the name
         * of the SQL function to invoke. The rest of the arguments will be literalized through
         * {@link patio.Dataset#literal} and placed into the SQL function call.
         *
         * @property {String} f <b>READ ONLY</b> the SQL function to call.
         * @property {Array} args <b>READ ONLY</b> args  arguments will be literalized through
         * {@link patio.Dataset#literal} and placed into the SQL function call.
         * */
        constructor: function (f) {
            var args = argsToArray(arguments).slice(1);
            this.__f = isInstanceOf(f, Identifier) ? f.value : f, this.__args = args.map(function (a) {
                return isString(a) ? sql.stringToIdentifier(a) : a;
            });
        },

        /**
         * Converts the {@link patio.sql.SQLFunction} to a string.
         *
         * @param {patio.Dataset} [ds] dataset used to created the SQL fragment, if
         * the dataset is ommited then the default {@link patio.Dataset} implementation is used.
         *
         * @return String the SQL version of the {@link patio.sql.SQLFunction}.
         */
        toString: function (ds) {
            !Dataset && (Dataset = require("./dataset"));
            ds = ds || new Dataset();
            return ds.functionSql(this);
        },

        /**@ignore*/
        getters: {
            f: function () {
                return this.__f;
            },

            args: function () {
                return this.__args;
            }
        }
    }
}).as(sql, "SQLFunction");

/**
 * @class Subclass of {@link patio.sql.ComplexExpression} where the expression results
 * in a numeric value in SQL.
 *
 * @name NumericExpression
 * @memberOf patio.sql
 * @augments patio.sql.ComplexExpression
 * @augments patio.sql.BitWiseMethods
 * @augments patio.sql.NumericMethods
 * @augments patio.sql.InequalityMethods
 */
NumericExpression = define([ComplexExpression, BitWiseMethods, NumericMethods, InequalityMethods]).as(sql, "NumericExpression");


OrderedExpression = Expression.extend({
    instance: {
        /**@lends patio.sql.OrderedExpression.prototype*/

        /**
         * Represents a column/expression to order the result set by.
         * @constructs
         * @augments patio.sql.Expression
         *
         * @param expression the expression to order
         * @param {Boolean}[descending=true] set to false to order ASC
         * @param {String|Object} [opts=null] additional options
         * <ul>
         *     <li>String: if value is "first" the null values will be first, if "last" then null values
         *     will be last</li>
         *     <li>Object: will pull the nulls property off of the object use use the same rules as if it
         *     were a string</li>
         * </ul>
         * @property expression <b>READ ONLY</b> the expression to order.
         * @property {Boolean} [descending=true] <b>READ ONLY</b> true if decending, false otherwise.
         * @property {String} [nulls=null] if value is "first" the null values will be first, if "last" then null values
         *     will be last
         */
        constructor: function (expression, descending, opts) {
            descending = isBoolean(descending) ? descending : true;
            opts = opts || {};
            this.__expression = expression;
            this.__descending = descending;
            var nulls = isString(opts) ? opts : opts.nulls;
            this.__nulls = isString(nulls) ? nulls.toLowerCase() : null;
        },

        /**
         * @return {patio.sql.OrderedExpression} a copy that is ordered ASC
         */
        asc: function () {
            return new OrderedExpression(this.__expression, false, {nulls: this.__nulls});
        },

        /**
         * @return {patio.sql.OrderedExpression} Return a copy that is ordered DESC
         */
        desc: function () {
            return new OrderedExpression(this.__expression, true, {nulls: this.__nulls});
        },

        /**
         * * @return {patio.sql.OrderedExpression} an inverted expression, changing ASC to DESC and NULLS FIRST to NULLS LAST.
         * */
        invert: function () {
            return new OrderedExpression(this.__expression, !this.__descending, {nulls: this._static.INVERT_NULLS[this.__nulls] || this.__nulls});
        },

        /**
         * Converts the {@link patio.sql.OrderedExpression} to a string.
         *
         * @param {patio.Dataset} [ds] dataset used to created the SQL fragment, if
         * the dataset is ommited then the default {@link patio.Dataset} implementation is used.
         *
         * @return String the SQL version of the {@link patio.sql.OrderedExpression}.
         */
        toString: function (ds) {
            !Dataset && (Dataset = require("./dataset"));
            ds = ds || new Dataset();
            return ds.orderedExpressionSql(this);
        },

        /**@ignore*/
        getters: {
            expression: function () {
                return this.__expression;
            },
            descending: function () {
                return this.__descending;
            },
            nulls: function () {
                return this.__nulls;
            }
        }
    },
    static: {
        /**@lends patio.sql.OrderedExpression*/
        /**
         * Hash that contains the inversions for "first" and "last".
         * @type Object
         * @default {first:"last", last:"first"}
         */
        INVERT_NULLS: {first: "last", last: "first"}
    }
}).as(sql, "OrderedExpression");

QualifiedIdentifier = define([GenericExpression, QualifyingMethods], {
    instance: {
        /**@lends patio.sql.QualifiedIdentifier.prototype*/

        /**
         * Represents a qualified identifier (column with table or table with schema).
         *
         * @constructs
         * @augments patio.sql.GenericExpression
         * @augments patio.sql.QualifyingMethods
         *
         * @param table the table or schema to qualify the column or table to.
         * @param column the column or table to qualify.
         *
         * @property table <b>READ ONLY</b> the table or schema to qualify the column or table to.
         * @property column <b>READ ONLY</b> he column or table to qualify.
         */
        constructor: function (table, column) {
            this.__table = table;
            this.__column = column;
        },

        /**
         * Converts the {@link patio.sql.QualifiedIdentifier} to a string.
         *
         * @param {patio.Dataset} [ds] dataset used to created the SQL fragment, if
         * the dataset is ommited then the default {@link patio.Dataset} implementation is used.
         *
         * @return String the SQL version of the {@link patio.sql.QualifiedIdentifier}.
         */
        toString: function (ds) {
            !Dataset && (Dataset = require("./dataset"));
            ds = ds || new Dataset();
            return ds.qualifiedIdentifierSql(this);
        },

        /**@ignore*/
        getters: {
            table: function () {
                return this.__table;
            },

            column: function () {
                return this.__column;
            }
        }
    }
}).as(sql, "QualifiedIdentifier");


var likeElement = function (re) {
    var ret;
    if (isRegExp(re)) {
        ret = [("" + re).replace(/^\/|\/$|\/[i|m|g]*$/g, ""), true, re.ignoreCase];
    } else {
        ret = [re, false, false];
    }
    return ret;
};
/**
 * @class Subclass of {@link patio.sql.ComplexExpression} where the expression results
 * in a text/string/varchar value in SQL.
 *
 * @augments patio.sql.ComplexExpression
 * @augments patio.sql.StringMethods
 * @augments patio.sql.StringConcatenationMethods
 * @augments patio.sql.InequalityMethods
 * @augments patio.sql.NoBooleanInputMethods
 * @name StringExpression
 * @memberOf patio.sql
 */
StringExpression = define([ComplexExpression, StringMethods, StringConcatenationMethods, InequalityMethods, NoBooleanInputMethods], {
    static: {
        /**@lends patio.sql.StringExpression*/

        /**
         * <p>Creates a SQL pattern match expression. left (l) is the SQL string we
         * are matching against, and ces are the patterns we are matching.
         * The match succeeds if any of the patterns match (SQL OR).</p>
         *
         * <p>If a regular expression is used as a pattern, an SQL regular expression will be
         * used, which is currently only supported on MySQL and PostgreSQL.  Be aware
         * that MySQL and PostgreSQL regular expression syntax is similar to javascript
         * regular expression syntax, but it not exactly the same, especially for
         * advanced regular expression features.  Patio just uses the source of the
         * regular expression verbatim as the SQL regular expression string.</p>
         *
         * <p>If any other object is used as a regular expression, the SQL LIKE operator will
         * be used, and should be supported by most databases.</p>
         *
         * <p>The pattern match will be case insensitive if the last argument is a hash
         * with a key of caseInsensitive that is not false or null. Also,
         * if a case insensitive regular expression is used (//i), that particular
         * pattern which will always be case insensitive.</p>
         *
         * @example
         *   StringExpression.like(sql.a, 'a%') //=> "a" LIKE 'a%'
         *   StringExpression.like(sql.a, 'a%', {caseInsensitive : true}) //=> "a" ILIKE 'a%'
         *   StringExpression.like(sql.a, 'a%', /^a/i) //=> "a" LIKE 'a%' OR "a" ~* '^a'
         */
        like: function (l) {
            var args = argsToArray(arguments, 1);
            var params = likeElement(l);
            var likeMap = this.likeMap;
            var lh = params[0], lre = params[1], lci = params[2];
            var last = args[args.length - 1];
            lci = (isHash(last) ? args.pop() : {})["caseInsensitive"] ? true : lci;
            args = args.map(function (ce) {
                var r, rre, rci;
                var ceArr = likeElement(ce);
                r = ceArr[0], rre = ceArr[1], rci = ceArr[2];
                return new BooleanExpression(likeMap["" + (lre || rre) + (lci || rci)], l, r);
            }, this);
            return args.length === 1 ? args[0] : BooleanExpression.fromArgs(["OR"].concat(args));
        },

        /**
         * Like map used to by {@link patio.sql.StringExpression.like} to create the
         * LIKE expression.
         * @type Object
         */
        likeMap: {"truetrue": '~*', "truefalse": "~", "falsetrue": "ILIKE", "falsefalse": "LIKE"}


    }
}).as(sql, "StringExpression");

SubScript = GenericExpression.extend({
    instance: {
        /**@lends patio.sql.SubScript.prototype*/

        /**
         * Represents an SQL array access, with multiple possible arguments.
         * @constructs
         * @augments patio.sql.GenericExpression
         *
         * @param arrCol the SQL array column
         * @param sub The array of subscripts to use (should be an array of numbers)
         */
        constructor: function (arrCol, sub) {
            //The SQL array column
            this.__arrCol = arrCol;
            //The array of subscripts to use (should be an array of numbers)
            this.__sub = sub;
        },

        /**
         * Create a new {@link patio.sql.Subscript} appending the given subscript(s)
         * the the current array of subscripts.
         */
        addSub: function (sub) {
            return new SubScript(this.__arrCol, this.__sub.concat(sub));
        },

        /**
         * Converts the {@link patio.sql.SubScript} to a string.
         *
         * @param {patio.Dataset} [ds] dataset used to created the SQL fragment, if
         * the dataset is ommited then the default {@link patio.Dataset} implementation is used.
         *
         * @return String the SQL version of the {@link patio.sql.SubScript}.
         */
        toString: function (ds) {
            !Dataset && (Dataset = require("./dataset"));
            ds = ds || new Dataset();
            return ds.subscriptSql(this);
        },

        /**@ignore*/
        getters: {
            f: function () {
                return this.__arrCol;
            },

            sub: function () {
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
    };
};

LiteralString = define([OrderedMethods, ComplexExpressionMethods, BooleanMethods, NumericMethods, StringMethods, InequalityMethods, AliasMethods], {
    instance: {
        /**@lends patio.sql.LiteralString*/

        /**
         * Represents a string that should be placed into a SQL query literally.
         * <b>This class has all methods that a normal javascript String has.</b>
         * @constructs
         * @augments patio.sql.OrderedMethods
         * @augments patio.sql.ComplexExpressionMethods
         * @augments patio.sql.BooleanMethods
         * @augments patio.sql.NumericMethods
         * @augments patio.sql.StringMethods
         * @augments patio.sql.InequalityMethods
         * @augments patio.sql.AliasMethods
         *
         * @param {String} str the literal string.
         */
        constructor: function (str) {
            this.__str = str;
        }
    }
}).as(sql, "LiteralString");

STRING_METHODS.forEach(function (op) {
    LiteralString.prototype[op] = addStringMethod(op);
}, this);


/**
 * Represents a json object that should be placed into a SQL query literally.
 * @constructs
 *
 * @name Json
 * @memberOf patio.sql
 */
Json = define({
    instance: {
        constructor: function (obj) {
            merge(this, obj);
        }
    }
}).as(sql, "Json");


/**
 * Represents a json array that should be placed into a SQL query literally.
 * @constructs
 *
 * @name JsonArray
 * @memberOf patio.sql
 */
function JsonArray(arr) {
    if (!(this instanceof JsonArray)) {
        return new JsonArray(arr);
    }
    Array.call(this);
    var i = -1, l = arr.length;
    while (++i < l) {
        this.push(arr[i]);
    }
}
/*jshint supernew:false*/
JsonArray.prototype = [];
JsonArray.prototype.toJSON = function () {
    return this.slice();
};

JsonArray.prototype.valueOf = function () {
    return this.slice();
};
//require("util").inherits(JsonArray, Array);
sql.JsonArray = JsonArray;
