---
sidebar_position: 5
---

### [patio.plugins.ValidatorPlugin](./patio_plugins_ValidatorPlugin.html)

A validation plugin for patio models. This plugin adds a `validate` method to each [Model](./patio_Model.html)
class that adds it as a plugin. This plugin does not include most typecast checks as `patio` already checks
types upon column assignment.

To do single col validation

```
var Model = patio.addModel("validator", {
     plugins:[patio.plugins.ValidatorPlugin]
});
//this ensures column assignment
Model.validate("col1").isNotNull().isAlphaNumeric().hasLength(1, 10);
//col2 does not have to be assigned but if it is it must match /hello/ig.
Model.validate("col2").like(/hello/ig);
//Ensures that the emailAddress column is a valid email address.
Model.validate("emailAddress").isEmailAddress();
```

Or you can do a mass validation through a callback.

```
var Model = patio.addModel("validator", {
     plugins:[patio.plugins.ValidatorPlugin]
});
Model.validate(function(validate){
     //this ensures column assignment
     validate("col1").isNotNull().isAlphaNumeric().hasLength(1, 10);
     //col2 does not have to be assigned but if it is it must match /hello/ig.
     validate("col2").isLike(/hello/ig);
     //Ensures that the emailAddress column is a valid email address.
     validate("emailAddress").isEmail();
});
```

To check if a [Model](./patio_Model.html) is valid you can run the `isValid` method.

```
var model1 = new Model({col2 : 'grape', emailAddress : "test"}),
    model2 = new Model({col1 : "grape", col2 : "hello", emailAddress : "test@test.com"});

model1.isValid() //false
model2.isValid() //true
```

To get the errors associated with an invalid model you can access the errors property

```
model1.errors; //{ col1: [ 'col1 must be defined.' ],
               //  col2: [ 'col2 must be like /hello/gi got grape.' ],
               //  emailAddress: [ 'emailAddress must be a valid Email Address got test' ] }
```

Validation is also run pre save and pre update. To prevent this you can specify the `validate` option

```
model1.save(null, {validate : false});
model2.save(null, {validate : false});
```

Or you can specify the class level properties `validateOnSave` and `validateOnUpdate`
to false respectively

```
Model.validateOnSave = false;
Model.validateOnUpdate = false;
```

Available validation methods are.

- `isAfter` : check that a date is after a specified date
- `isBefore` : check that a date is after before a specified date 
- `isDefined` : ensure that a column is defined
- `isNotDefined` : ensure that a column is not defined
- `isNotNull` : ensure that a column is defined and not null
- `isNull` : ensure that a column is not defined or null
- `isEq` : ensure that a column equals a value <b>this uses deep equal</b>
- `isNeq` : ensure that a column does not equal a value <b>this uses deep equal</b>
- `isLike` : ensure that a column is like a value, can be a regexp or string
- `isNotLike` : ensure that a column is not like a value, can be a regexp or string
- `isLt` : ensure that a column is less than a value
- `isGt` : ensure that a column is greater than a value
- `isLte` : ensure that a column is less than or equal to a value
- `isGte` : ensure that a column is greater than or equal to a value
- `isIn` : ensure that a column is contained in an array of values
- `isNotIn` : ensure that a column is not contained in an array of values
- `isMacAddress` : ensure that a column is a valid MAC address
- `isIPAddress` : ensure that a column is a valid IPv4 or IPv6 address
- `isIPv4Address` : ensure that a column is a valid IPv4 address
- `isIPv6Address` : ensure that a column is a valid IPv6 address
- `isUUID` : ensure that a column is a valid UUID
- `isEmail` : ensure that a column is a valid email address
- `isUrl` : ensure than a column is a valid URL
- `isAlpha` : ensure than a column is all letters
- `isAlphaNumeric` : ensure than a column is all letters or numbers
- `hasLength` : ensure than a column is fits within the specified length accepts a min and optional max value
- `isLowercase` : ensure than a column is lowercase
- `isUppercase` : ensure than a column is uppercase
- `isEmpty` : ensure than a column empty (i.e. a blank string)
- `isNotEmpty` : ensure than a column not empty (i.e. not a blank string)
- `isCreditCard` : ensure than a is a valid credit card number
- `check` : accepts a function to perform validation

All of the validation methods are chainable, and accept an options argument.

The options include

- `message` : a message to return if a column fails validation. The message can include `{val}` and `{col}`
    replacements which will insert the invalid value and the column name.                                                                                  
- `onlyDefined`=`true` : set to false to run the method even if the column value is not defined.
- `onlyNotNull`=`true` : set to false to run the method even if the column value is null.