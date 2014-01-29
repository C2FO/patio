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
    isString = comb.isString,
    combIsDefined = comb.isDefined,
    combIsNull = comb.isNull,
    ModelError = require("../errors.js").ModelError,
    isFunction = comb.isFunction,
    format = comb.string.format,
    Promise = comb.Promise,
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

        isNotDefined:function isNotDefined(opts) {
            return this.__addAction(function (col) {
                return !combIsDefined(col);
            }, merge({message:"{col} cannot be defined.", onlyDefined:false, onlyNotNull:false}, opts));
        },

        isNotNull:function isNotNull(opts) {
            return this.__addAction(function (col) {
                return combIsDefined(col) && !combIsNull(col);
            }, merge({message:"{col} cannot be null.", onlyDefined:false, onlyNotNull:false}, opts));
        },

        isNull:function isNull(opts) {
            return this.__addAction(function (col) {
                return !combIsDefined(col) || combIsNull(col);
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
                if (!actionOpts.onlyDefined || (combIsDefined(value) &&
                    (!actionOpts.onlyNotNull || !combIsNull(value)) )) {
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

        /**
         * A validation plugin for patio models. This plugin adds a <code>validate</code> method to each {@link patio.Model}
         * class that adds it as a plugin. This plugin does not include most typecast checks as <code>patio</code> already checks
         * types upon column assignment.
         *
         * To do single col validation
         * {@code
         *
         * var Model = patio.addModel("validator", {
         *      plugins:[ValidatorPlugin]
         * });
         * //this ensures column assignment
         * Model.validate("col1").isNotNull().isAlphaNumeric().hasLength(1, 10);
         * //col2 does not have to be assigned but if it is it must match /hello/ig.
         * Model.validate("col2").like(/hello/ig);
         * //Ensures that the emailAddress column is a valid email address.
         * Model.validate("emailAddress").isEmailAddress();
         * }
         *
         * Or you can do a mass validation through a callback.
         * {@code
         *
         * var Model = patio.addModel("validator", {
         *      plugins:[ValidatorPlugin]
         * });
         * Model.validate(function(validate){
         *      //this ensures column assignment
         *      validate("col1").isNotNull().isAlphaNumeric().hasLength(1, 10);
         *      //col2 does not have to be assigned but if it is it must match /hello/ig.
         *      validate("col2").isLike(/hello/ig);
         *      //Ensures that the emailAddress column is a valid email address.
         *      validate("emailAddress").isEmail();
         * });
         * }
         *
         * To check if a {@link patio.Model} is valid you can run the <code>isValid</code> method.
         *
         * {@code
         * var model1 = new Model({col2 : 'grape', emailAddress : "test"}),
         *     model2 = new Model({col1 : "grape", col2 : "hello", emailAddress : "test@test.com"});
         *
         * model1.isValid() //false
         * model2.isValid() //true
         * }
         *
         * To get the errors associated with an invalid model you can access the errors property
         *
         * {@code
         * model1.errors; //{ col1: [ 'col1 must be defined.' ],
         *                //  col2: [ 'col2 must be like /hello/gi got grape.' ],
         *                //  emailAddress: [ 'emailAddress must be a valid Email Address got test' ] }
         * }
         *
         * Validation is also run pre save and pre update. To prevent this you can specify the <code>validate</code> option
         *
         * {@code
         * model1.save(null, {validate : false});
         * model2.save(null, {validate : false});
         * }
         *
         * Or you can specify the class level properties <code>validateOnSave</code> and <code>validateOnUpdate</code>
         * to false respectively
         * {@code
         * Model.validateOnSave = false;
         * Model.validateOnUpdate = false;
         * }
         *
         * Avaiable validation methods are.
         *
         * <ul>
         *  <li><code>isAfter</code> : check that a date is after a specified date</li>
         *  <li><code>isBefore</code> : check that a date is after before a specified date </li>
         *  <li><code>isDefined</code> : ensure that a column is defined</li>
         *  <li><code>isNotDefined</code> : ensure that a column is not defined</li>
         *  <li><code>isNotNull</code> : ensure that a column is defined and not null</li>
         *  <li><code>isNull</code> : ensure that a column is not defined or null</li>
         *  <li><code>isEq</code> : ensure that a column equals a value <b>this uses deep equal</b></li>
         *  <li><code>isNeq</code> : ensure that a column does not equal a value <b>this uses deep equal</b></li>
         *  <li><code>isLike</code> : ensure that a column is like a value, can be a regexp or string</li>
         *  <li><code>isNotLike</code> : ensure that a column is not like a value, can be a regexp or string</li>
         *  <li><code>isLt</code> : ensure that a column is less than a value</li>
         *  <li><code>isGt</code> : ensure that a column is greater than a value</li>
         *  <li><code>isLte</code> : ensure that a column is less than or equal to a value</li>
         *  <li><code>isGte</code> : ensure that a column is greater than or equal to a value</li>
         *  <li><code>isIn</code> : ensure that a column is contained in an array of values</li>
         *  <li><code>isNotIn</code> : ensure that a column is not contained in an array of values</li>
         *  <li><code>isMacAddress</code> : ensure that a column is a valid MAC address</li>
         *  <li><code>isIPAddress</code> : ensure that a column is a valid IPv4 or IPv6 address</li>
         *  <li><code>isIPv4Address</code> : ensure that a column is a valid IPv4 address</li>
         *  <li><code>isIPv6Address</code> : ensure that a column is a valid IPv6 address</li>
         *  <li><code>isUUID</code> : ensure that a column is a valid UUID</li>
         *  <li><code>isEmail</code> : ensure that a column is a valid email address</li>
         *  <li><code>isUrl</code> : ensure than a column is a valid URL</li>
         *  <li><code>isAlpha</code> : ensure than a column is all letters</li>
         *  <li><code>isAlphaNumeric</code> : ensure than a column is all letters or numbers</li>
         *  <li><code>hasLength</code> : ensure than a column is fits within the specified length accepts a min and optional max value</li>
         *  <li><code>isLowercase</code> : ensure than a column is lowercase</li>
         *  <li><code>isUppercase</code> : ensure than a column is uppercase</li>
         *  <li><code>isEmpty</code> : ensure than a column empty (i.e. a blank string)</li>
         *  <li><code>isNotEmpty</code> : ensure than a column not empty (i.e. not a blank string)</li>
         *  <li><code>isCreditCard</code> : ensure than a is a valid credit card number</li>
         *  <li><code>check</code> : accepts a function to perform validation</li>
         * </ul>
         *
         * All of the validation methods are chainable, and accept an options argument.
         *
         * The options include
         * <ul>
         *     <li><code>message</code> : a message to return if a column fails validation. The message can include <code>{val}</code> and <code>{col}</code>
         *     replacements which will insert the invalid value and the column name.
         *     </li>
         *     <li><code>[onlyDefined=true]</code> : set to false to run the method even if the column value is not defined.</li>
         *     <li><code>[onlyNotNull=true]</code> : set to false to run the method even if the column value is null.</li>
         * </ul>
         *
         *
         * @constructs
         * @name ValidatorPlugin
         * @memberOf patio.plugins
         * @property {Object} [errors={}] the validation errors for this model.
         *
         */
        constructor:function () {
            this._super(arguments);
            this.errors = {};
        },

        /**
         * Validates a model, returning an array of error messages for each invalid property.
         * @return {String[]} an array of error messages for each invalid property.
         */
        validate:function () {
            this.errors = {};
            return flatten(this._static.validators.map(function runValidator(validator) {
                var col = validator.col, val = this.__values[validator.col], ret = validator.validate(val);
                this.errors[col] = ret;
                return ret;
            }, this));
        },

        /**
         * Returns if this model passes validation.
         *
         * @return {Boolean}
         */
        isValid:function () {
            return this.validate().length === 0;
        }
    },

    "static":{
        /**@lends patio.plugins.ValidatorPlugin*/

        /**
         * Set to false to prevent model validation when saving.
         * @default true
         */
        validateOnSave:true,

        /**
         * Set to false to prevent model validation when updating.
         * @default true
         */
        validateOnUpdate:true,

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

        __getValidator:function validator(name) {
            var ret = new Validator(name);
            this.validators.push(ret);
            return ret;
        },

        /**
         * Sets up validation for a model.
         *
         * To do single col validation
         * {@code
         *
         * var Model = patio.addModel("validator", {
         *      plugins:[ValidatorPlugin]
         * });
         * //this ensures column assignment
         * Model.validate("col1").isDefined().isAlphaNumeric().hasLength(1, 10);
         * //col2 does not have to be assigned but if it is it must match /hello/ig.
         * Model.validate("col2").like(/hello/ig);
         * //Ensures that the emailAddress column is a valid email address.
         * Model.validate("emailAddress").isEmailAddress();
         * }
         *
         * Or you can do a mass validation through a callback.
         * {@code
         *
         * var Model = patio.addModel("validator", {
         *      plugins:[ValidatorPlugin]
         * });
         * Model.validate(function(validate){
         *      //this ensures column assignment
         *      validate("col1").isDefined().isAlphaNumeric().hasLength(1, 10);
         *      //col2 does not have to be assigned but if it is it must match /hello/ig.
         *      validate("col2").isLike(/hello/ig);
         *      //Ensures that the emailAddress column is a valid email address.
         *      validate("emailAddress").isEmail();
         * });
         * }
         *
         *
         * @param {String|Function} name the name of the column, or a callback that accepts a function to create validators.
         *
         * @throws {patio.ModelError} if name is not a function or string.
         * @return {patio.Model|Validator} returns a validator if name is a string, other wise returns this for chaining.
         */
        validate:function (name) {
            this.__initValidation();
            var ret;
            if (isFunction(name)) {
                name.call(this, this.__getValidator.bind(this));
                ret = this;
            } else if (isString(name)) {
                ret = this.__getValidator(name);
            } else {
                throw new ModelError("name is must be a string or function when validating");
            }
            return ret;
        }
    }

}).as(module);

