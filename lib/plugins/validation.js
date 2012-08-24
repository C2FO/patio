var comb = require("comb"),
    array = comb.array,
    compact = array.compact,
    flatten = array.flatten,
    toArray = array.toArray,
    net = require("net"),
    isIP = net.isIP,
    isIPv4 = net.isIPv4,
    isIPv6 = net.isIPv6,
    validator = require("validator"),
    validatorCheck = validator.check,
    dateCmp = comb.date.compare,
    isArray = comb.isArray,
    combDeepEqual = comb.deepEqual,
    combIsBoolean = comb.isBoolean,
    combIsDefined = comb.isDefined,
    combIsNull = comb.isNull,
    format = comb.string.format,
    Promise = comb.Promise,
    serial = comb.serial,
    when = comb.when,
    merge = comb.merge,
    define = comb.define;


var Validator = define(null, {
    instance:{
        constructor:function validator(col) {
            this.col = col;
            this.__actions = [];
        },

        __addAction:function __addAction(action, opts) {
            this.__actions.push({
                action:action,
                opts:merge({onlyDefined:true, onlyNotNull:false}, opts)
            });
            return this;
        },

        isAfter:function (date, opts) {
            opts = opts || {};
            var cmpDate = combIsBoolean(opts.date) ? opts.date : true;
            return this.__addAction(function (col) {
                return dateCmp(col, date, cmpDate ? "date" : "datetime") > 0;
            }, merge({message:"{col} must be after " + date + " got {val}."}, opts));
        },

        isBefore:function (date, opts) {
            opts = opts || {};
            var cmpDate = combIsBoolean(opts.date) ? opts.date : true;
            return this.__addAction(function (col) {
                return dateCmp(col, date, cmpDate ? "date" : "datetime") === -1;
            }, merge({message:"{col} must be before " + date + " got {val}."}, opts));
        },

        isDefined:function isDefined(opts) {
            return this.__addAction(function (col) {
                return combIsDefined(col);
            }, merge({message:"{col} must be defined.", onlyDefined:false, onlyNotNull:false}, opts));
        },

        isNotDefined:function isDefined(opts) {
            return this.__addAction(function (col) {
                return !combIsDefined(col);
            }, merge({message:"{col} cannot be defined.", onlyDefined:false, onlyNotNull:false}, opts));
        },

        isNotNull:function isNotNull(opts) {
            return this.__addAction(function (col) {
                return !combIsNull(col);
            }, merge({message:"{col} cannot be null.", onlyDefined:false, onlyNotNull:false}, opts));
        },

        isNull:function isNull(opts) {
            return this.__addAction(function (col) {
                return combIsNull(col);
            }, merge({message:"{col} must be null got {val}.", onlyDefined:false, onlyNotNull:false}, opts));
        },

        isEq:function eq(val, opts) {
            return this.__addAction(function (col) {
                return combDeepEqual(col, val);
            }, merge({message:"{col} must === " + val + " got {val}."}, opts));
        },

        isNeq:function neq(val, opts) {
            return this.__addAction(function (col) {
                return !combDeepEqual(col, val);
            }, merge({message:"{col} must !== " + val + "."}, opts));
        },

        isLike:function like(val, opts) {
            return this.__addAction(function (col) {
                return !!col.match(val);
            }, merge({message:"{col} must be like " + val + " got {val}."}, opts));
        },

        isNotLike:function notLike(val, opts) {
            return this.__addAction(function (col) {
                return !(!!col.match(val));
            }, merge({message:"{col} must not be like " + val + "."}, opts));
        },

        isLt:function lt(num, opts) {
            return this.__addAction(function (col) {
                return col < num;
            }, merge({message:"{col} must be < " + num + " got {val}."}, opts));
        },

        isGt:function gt(num, opts) {
            return this.__addAction(function (col) {
                return col > num;
            }, merge({message:"{col} must be > " + num + " got {val}."}, opts));
        },

        isLte:function lte(num, opts) {
            return this.__addAction(function (col) {
                return col <= num;
            }, merge({message:"{col} must be <= " + num + " got {val}."}, opts));
        },

        isGte:function gte(num, opts) {
            return this.__addAction(function (col) {
                return col >= num;
            }, merge({message:"{col} must be >= " + num + " got {val}."}, opts));
        },

        isIn:function isIn(arr, opts) {
            if (!isArray(arr)) {
                throw new Error("isIn requires an array of values");
            }
            return this.__addAction(function (col) {
                return arr.indexOf(col) !== -1;
            }, merge({message:"{col} must be in " + arr.join(",") + " got {val}."}, opts));
        },

        isNotIn:function notIn(arr, opts) {
            if (!isArray(arr)) {
                throw new Error("notIn requires an array of values");
            }
            return this.__addAction(function (col) {
                return arr.indexOf(col) === -1;
            }, merge({message:"{col} cannot be in " + arr.join(",") + " got {val}."}, opts));
        },

        isMacAddress:function isMaxAddress(opts) {
            return this.__addAction(function (col) {
                return !!col.match(/^([0-9A-F]{2}[:\-]){5}([0-9A-F]{2})$/);
            }, merge({message:"{col} must be a valid MAC address got {val}."}, opts));
        },

        isIPAddress:function isIpAddress(opts) {
            return this.__addAction(function (col) {
                return !!isIP(col);
            }, merge({message:"{col} must be a valid IPv4 or IPv6 address got {val}."}, opts));
        },

        isIPv4Address:function isIpAddress(opts) {
            return this.__addAction(function (col) {
                return isIPv4(col);
            }, merge({message:"{col} must be a valid IPv4 address got {val}."}, opts));
        },

        isIPv6Address:function isIpAddress(opts) {
            return this.__addAction(function (col) {
                return isIPv6(col);
            }, merge({message:"{col} must be a valid IPv6 address got {val}."}, opts));
        },

        isUUID:function isUUID(opts) {
            return this.__addAction(function (col) {
                return !!col.match(/^(\{{0,1}([0-9a-fA-F]){8}-([0-9a-fA-F]){4}-([0-9a-fA-F]){4}-([0-9a-fA-F]){4}-([0-9a-fA-F]){12}\}{0,1})$/);
            }, merge({message:"{col} must be a valid UUID got {val}"}, opts));
        },

        isEmail:function isEmail(opts) {
            return this.__addAction(function (col) {
                return validatorCheck(col).isEmail();
            }, merge({message:"{col} must be a valid Email Address got {val}"}, opts));
        },

        isUrl:function isUrl(opts) {
            return this.__addAction(function (col) {
                return validatorCheck(col).isUrl();
            }, merge({message:"{col} must be a valid url got {val}"}, opts));
        },

        isAlpha:function isAlpha(opts) {
            return this.__addAction(function (col) {
                return validatorCheck(col).isAlpha();
            }, merge({message:"{col} must be a only letters got {val}"}, opts));
        },

        isAlphaNumeric:function isAlphaNumeric(opts) {
            return this.__addAction(function (col) {
                return validatorCheck(col).isAlphanumeric();
            }, merge({message:"{col} must be a alphanumeric got {val}"}, opts));
        },

        hasLength:function hasLength(min, max, opts) {
            return this.__addAction(function (col) {
                return validatorCheck(col).len(min, max);
            }, merge({message:"{col} must have a length between " + min + (max ? " and " + max : "") + "."}, opts));
        },

        isLowercase:function isLowercase(opts) {
            return this.__addAction(function (col) {
                return validatorCheck(col).isLowercase();
            }, merge({message:"{col} must be lowercase got {val}."}, opts));
        },

        isUppercase:function isUppercase(opts) {
            return this.__addAction(function (col) {
                return validatorCheck(col).isUppercase();
            }, merge({message:"{col} must be uppercase got {val}."}, opts));
        },

        isEmpty:function isEmpty(opts) {
            return this.__addAction(function (col) {
                try {
                    validatorCheck(col).notEmpty();
                    return false;
                } catch (e) {
                    return true;
                }
            }, merge({message:"{col} must be empty got {val}."}, opts));
        },

        isNotEmpty:function isNotEmpty(opts) {
            return this.__addAction(function (col) {
                return validatorCheck(col).notEmpty();
            }, merge({message:"{col} must not be empty."}, opts));
        },

        isCreditCard:function isCreditCard(opts) {
            return this.__addAction(function (col) {
                return validatorCheck(col).isCreditCard();
            }, merge({message:"{col} is an invalid credit card"}, opts));
        },

        check:function (fun, opts) {
            return this.__addAction(fun, opts);
        },

        validate:function validate(value) {
            var errOpts = {col:this.col, val:value};
            return compact(this.__actions.map(function (action) {
                var actionOpts = action.opts;
                if ((!actionOpts.onlyDefined || combIsDefined(value)) ||
                    (combIsNull(value) && actionOpts.onlyNotNull)) {
                    var ret = null;
                    try {
                        if (!action.action(value)) {
                            ret = format(actionOpts.message, errOpts);
                        }
                    } catch (e) {
                        ret = format(actionOpts.message, errOpts);
                    }
                    return ret;
                }
            }, this));
        }

    }
});

function shouldValidate(opts, def) {
    opts = opts || {};
    return combIsBoolean(opts.validate) ? opts.validate : def;
}

function validateHook(prop, next, opts) {
    if (shouldValidate(opts, prop) && !this.isValid()) {
        next(flatten(toArray(this.errors).map(function (entry) {
            return entry[1].map(function (err) {
                return new Error(err);
            });
        })));
    } else {
        next();
    }
}

define(null, {

    instance:{

        constructor:function () {
            this._super(arguments);
            this.errors = {};
        },

        validate:function () {
            this.errors = {};
            return flatten(this._static.validators.map(function runValidator(validator) {
                var col = validator.col, val = this.__values[validator.col], ret = validator.validate(val);
                this.errors[col] = ret;
                return ret;
            }, this));
        },

        isValid:function () {
            return this.validate().length === 0;
        }
    },

    "static":{

        validateOnSave:true,
        validateOnUpdate:true,

        init:function () {
            this._super(arguments);
        },

        __initValidation:function () {
            if (!this.__isValidationInited) {
                this.validators = [];
                this.pre("save", function preSaveValidate(next, opts) {
                    validateHook.call(this, this._static.validateOnSave, next, opts);
                });
                this.pre("update", function preUpdateValidate(next, opts) {
                    validateHook.call(this, this._static.validateOnSave, next, opts);
                });
                this.__isValidationInited = true;
            }
        },

        validate:function (name) {
            this.__initValidation();
            var ret = new Validator(name);
            this.validators.push(ret);
            return ret;
        }
    }

}).as(module);