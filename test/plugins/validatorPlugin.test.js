var it = require('it'),
    assert = require('assert'),
    helper = require("../data/validator.helper.js"),
    patio = require("index"),
    ValidatorPlugin = patio.plugins.ValidatorPlugin,
    sql = patio.sql,
    comb = require("comb"),
    Promise = comb.Promise,
    hitch = comb.hitch;

it.describe("patio.plugins.ValidatorPlugin", function (it) {
    var Model;
    it.beforeAll(function () {
        return helper.createSchemaAndSync(true);
    });

    it.afterEach(function () {
        return comb.when(patio.defaultDatabase.from("validator").remove());
    });

    it.describe("#isAfter", function (it) {

        it.beforeAll(function () {
            Model = patio.addModel("validator", {
                plugins: [ValidatorPlugin]
            });
            Model.validate("date").isAfter(new Date(2006, 1, 1));

            return  Model.sync();
        });

        it.should("throw an error if invalid", function () {
            var m = new Model({date: new Date(2005, 1, 1)});
            assert.isFalse(m.isValid());
            return m.save().chain(assert.fail, function (err) {
                assert.isTrue(!!err[0].message.match(/date must be after/));
            });
        });

        it.should("not throw an error if valid", function () {
            return new Model({date: new Date(2007, 1, 1)}).save();
        });

        it.should("not throw an error if not defined", function () {
            return new Model().save();
        });
    });

    it.describe("#isBefore", function (it) {


        it.beforeAll(function () {
            Model = patio.addModel("validator", {
                plugins: [ValidatorPlugin]
            });
            Model.validate("date").isBefore(new Date(2006, 1, 1));
            return  Model.sync();
        });

        it.should("throw an error if invalid", function () {
            var m = new Model({date: new Date(2007, 1, 1)});
            assert.isFalse(m.isValid());
            return m.save().chain(assert.fail, function (err) {
                assert.isTrue(!!err[0].message.match(/date must be before/));
            });
        });

        it.should("not throw an error if valid", function () {
            return new Model({date: new Date(2005, 1, 1)}).save();
        });

        it.should("not throw an error if not defined", function () {
            return new Model().save();
        });
    });

    it.describe("#isDefined", function (it) {

        it.beforeAll(function () {
            Model = patio.addModel("validator", {
                plugins: [ValidatorPlugin]
            });
            Model.validate("str").isDefined();
            return  Model.sync();
        });

        it.should("throw an error if invalid", function () {
            var m = new Model();
            assert.isFalse(m.isValid());
            return m.save().chain(assert.fail, function (err) {
                assert.equal(err[0].message, "str must be defined.");
            });
        });
        it.should("not throw an error if valid", function () {
            return new Model({str: "HELLO"}).save();
        });

    });

    it.describe("#isNotDefined", function (it) {

        it.beforeAll(function () {
            Model = patio.addModel("validator", {
                plugins: [ValidatorPlugin]
            });
            Model.validate("str").isNotDefined();
            return  Model.sync();
        });

        it.should("throw an error if invalid", function () {
            var m = new Model({str: "HELLO"})
            assert.isFalse(m.isValid());
            return m.save().chain(assert.fail, function (err) {
                assert.equal(err[0].message, "str cannot be defined.");
            });
        });
        it.should("not throw an error if valid", function () {
            return new Model().save();
        });

    });

    it.describe("#isNotNull", function (it) {
        it.beforeAll(function () {
            Model = patio.addModel("validator", {
                plugins: [ValidatorPlugin]
            });
            Model.validate("str").isNotNull();

            return  Model.sync();
        });

        it.should("throw an error if invalid", function () {
            var m = new Model({str: null});
            assert.isFalse(m.isValid());
            return m.save().chain(assert.fail, function (err) {
                assert.equal(err[0].message, "str cannot be null.");
            });
        });
        it.should("not throw an error if valid", function () {
            return new Model({str: "HELLO"}).save();
        });
    });
    it.describe("#isNull", function (it) {

        it.beforeAll(function () {
            Model = patio.addModel("validator", {
                plugins: [ValidatorPlugin]
            });
            Model.validate("str").isNull();
            return  Model.sync();
        });

        it.should("throw an error if invalid", function () {
            var m = new Model({str: "HELLO"});
            assert.isFalse(m.isValid());
            return m.save().chain(assert.fail, function (err) {
                assert.equal(err[0].message, "str must be null got HELLO.");
            });
        });
        it.should("not throw an error if valid", function () {
            return new Model({str: null}).save();
        });
    });
    it.describe("#isEq", function (it) {

        it.beforeAll(function () {
            Model = patio.addModel("validator", {
                plugins: [ValidatorPlugin]
            });
            Model.validate("str").isEq("HELLO");
            Model.validate("date").isEq(new Date(2006, 1, 1));
            return  Model.sync();
        });

        it.should("throw an error if invalid", function () {
            var m = new Model({str: "HELL"});
            assert.isFalse(m.isValid());
            return m.save().chain(assert.fail, function (err) {
                assert.equal(err[0].message, "str must === HELLO got HELL.");
            });
        });

        it.should("throw an error if invalid with objects", function () {
            var m = new Model({str: "HELLO", date: new Date(2005, 1, 1)});
            assert.isFalse(m.isValid());
            return m.save().chain(assert.fail, function (err) {
                assert.isTrue(err[0].message.match(/date must ===/) !== null);
            });
        });

        it.should("not throw an error if valid", function () {
            return new Model({str: "HELLO", date: new Date(2006, 1, 1)}).save();
        });
    });
    it.describe("#isNeq", function (it) {

        it.beforeAll(function () {
            Model = patio.addModel("validator", {
                plugins: [ValidatorPlugin]
            });
            Model.validate("str").isNeq("HELLO");
            Model.validate("date").isNeq(new Date(2006, 1, 1));
            return  Model.sync();
        });

        it.should("throw an error if invalid", function () {
            var m = new Model({str: "HELLO"});
            assert.isFalse(m.isValid());
            return m.save().chain(assert.fail, function (err) {
                assert.equal(err[0].message, "str must !== HELLO.");
            });
        });

        it.should("throw an error if invalid with objects", function () {
            var m = new Model({str: "HELL", date: new Date(2006, 1, 1)})
            assert.isFalse(m.isValid());
            return m.save().chain(assert.fail, function (err) {
                assert.isTrue(err[0].message.match(/date must !==/) !== null);
            });
        });

        it.should("not throw an error if valid", function () {
            return new Model({str: "HELL", date: new Date(2005, 1, 1)}).save();
        });
    });
    it.describe("#isLike", function (it) {

        it.beforeAll(function () {
            Model = patio.addModel("validator", {
                plugins: [ValidatorPlugin]
            });
            Model.validate("str").isLike("HELLO");
            Model.validate("str2").isLike(/HELLO/i);
            return  Model.sync();
        });

        it.should("throw an error if invalid with string", function () {
            var m = new Model({str: "HELL"});
            assert.isFalse(m.isValid());
            return m.save().chain(assert.fail, function (err) {
                assert.equal(err[0].message, "str must be like HELLO got HELL.");
            });
        });

        it.should("throw an error if invalid with regexp", function () {
            var m = new Model({str2: "hell"});
            assert.isFalse(m.isValid());
            return m.save().chain(assert.fail, function (err) {
                assert.equal(err[0].message, "str2 must be like /HELLO/i got hell.");
            });
        });

        it.should("not throw an error if valid", function () {
            return new Model({str: "HELLO", str2: "hello"}).save();
        });

        it.should("not throw an error if values are undefined", function () {
            return new Model().save();
        });
    });
    it.describe("#isNotLike", function (it) {

        it.beforeAll(function () {
            Model = patio.addModel("validator", {
                plugins: [ValidatorPlugin]
            });
            Model.validate("str").isNotLike("HELLO");
            Model.validate("str2").isNotLike(/HELLO/i);
            return  Model.sync();
        });

        it.should("throw an error if invalid with string", function () {
            var m = new Model({str: "HELLO"});
            assert.isFalse(m.isValid());
            return m.save().chain(assert.fail, function (err) {
                assert.equal(err[0].message, "str must not be like HELLO.");
            });
        });

        it.should("throw an error if invalid with regexp", function () {
            var m = new Model({str2: "hello"});
            assert.isFalse(m.isValid());
            return m.save().chain(assert.fail, function (err) {
                assert.equal(err[0].message, "str2 must not be like /HELLO/i.");
            });
        });

        it.should("not throw an error if valid", function () {
            return new Model({str: "HELL", str2: "hell"}).save();
        });

        it.should("not throw an error if values are undefined", function () {
            return new Model().save();
        });
    });
    it.describe("#isLt", function (it) {

        it.beforeAll(function () {
            Model = patio.addModel("validator", {
                plugins: [ValidatorPlugin]
            });
            Model.validate("num").isLt(10);
            return  Model.sync();
        });

        it.should("throw an error if invalid with string", function () {
            var m = new Model({num: 10});
            assert.isFalse(m.isValid());
            return m.save().chain(assert.fail, function (err) {
                assert.equal(err[0].message, "num must be < 10 got 10.");
            });
        });

        it.should("not throw an error if valid", function () {
            return new Model({num: 9}).save();
        });

        it.should("not throw an error if values are undefined", function () {
            return new Model().save();
        });

    });
    it.describe("#isGt", function (it) {

        it.beforeAll(function () {
            Model = patio.addModel("validator", {
                plugins: [ValidatorPlugin]
            });
            Model.validate("num").isGt(10);
            return  Model.sync();
        });

        it.should("throw an error if invalid with string", function () {
            var m = new Model({num: 10});
            assert.isFalse(m.isValid());
            return m.save().chain(assert.fail, function (err) {
                assert.equal(err[0].message, "num must be > 10 got 10.");
            });
        });

        it.should("not throw an error if valid", function () {
            return new Model({num: 11}).save();
        });

        it.should("not throw an error if values are undefined", function () {
            return new Model().save();
        });
    });
    it.describe("#isLte", function (it) {

        it.beforeAll(function () {
            Model = patio.addModel("validator", {
                plugins: [ValidatorPlugin]
            });
            Model.validate("num").isLte(10);
            return  Model.sync();
        });

        it.should("throw an error if invalid with string", function () {
            var m = new Model({num: 11});
            assert.isFalse(m.isValid());
            return m.save().chain(assert.fail, function (err) {
                assert.equal(err[0].message, "num must be <= 10 got 11.");
            });
        });

        it.should("not throw an error if valid", function () {
            return new Model({num: 9}).save();
        });

        it.should("not throw an error if values are undefined", function () {
            return new Model().save();
        });
    });
    it.describe("#isGte", function (it) {

        it.beforeAll(function () {
            Model = patio.addModel("validator", {
                plugins: [ValidatorPlugin]
            });
            Model.validate("num").isGte(10);
            return  Model.sync();
        });

        it.should("throw an error if invalid with string", function () {
            var m = new Model({num: 9});
            assert.isFalse(m.isValid());
            return m.save().chain(assert.fail, function (err) {
                assert.equal(err[0].message, "num must be >= 10 got 9.");
            });
        });

        it.should("not throw an error if valid", function () {
            return new Model({num: 10}).save();
        });

        it.should("not throw an error if values are undefined", function () {
            return new Model().save();
        });
    });
    it.describe("#isIn", function (it) {

        it.beforeAll(function () {
            Model = patio.addModel("validator", {
                plugins: [ValidatorPlugin]
            });
            Model.validate("str").isIn(["a", "b", "c"]);
            return  Model.sync();
        });

        it.should("throw an error if arr is not an array", function () {
            assert.throws(function () {
                Model.validate("str2").isIn("a");
            });
        });

        it.should("throw an error if invalid with string", function () {
            var m = new Model({str: "d"});
            assert.isFalse(m.isValid());
            return m.save().chain(assert.fail, function (err) {
                assert.equal(err[0].message, "str must be in a,b,c got d.");
            });
        });

        it.should("not throw an error if valid", function () {
            return new Model({str: "a"}).save();
        });

        it.should("not throw an error if values are undefined", function () {
            return new Model().save();
        });

    });
    it.describe("#isNotIn", function (it) {

        it.beforeAll(function () {
            Model = patio.addModel("validator", {
                plugins: [ValidatorPlugin]
            });
            Model.validate("str").isNotIn(["a", "b", "c"]);
            return  Model.sync();
        });

        it.should("throw an error if arr is not an array", function () {
            assert.throws(function () {
                Model.validate("str2").isNotIn("a");
            });
        });

        it.should("throw an error if invalid with string", function () {
            var m = new Model({str: "a"});
            assert.isFalse(m.isValid());
            return m.save().chain(assert.fail, function (err) {
                assert.equal(err[0].message, "str cannot be in a,b,c got a.");
            });
        });

        it.should("not throw an error if valid", function () {
            return new Model({str: "d"}).save();
        });

        it.should("not throw an error if values are undefined", function () {
            return new Model().save();
        });

    });
    it.describe("#isMacAddress", function (it) {

        it.beforeAll(function () {
            Model = patio.addModel("validator", {
                plugins: [ValidatorPlugin]
            });
            Model.validate("macAddress").isMacAddress();
            return  Model.sync();
        });

        it.should("throw an error if invalid with string", function () {
            var m = new Model({macAddress: "a"});
            assert.isFalse(m.isValid());
            return m.save().chain(assert.fail, function (err) {
                assert.equal(err[0].message, "macAddress must be a valid MAC address got a.");
            });
        });

        it.should("not throw an error if valid", function () {
            return comb.when(
                new Model({macAddress: "00-00-00-00-00-00"}).save(),
                new Model({macAddress: "00:00:00:00:00:00"}).save()
            );
        });

        it.should("not throw an error if values are undefined", function () {
            return new Model().save();
        });
    });
    it.describe("#isIpAddress", function (it) {

        it.beforeAll(function () {
            Model = patio.addModel("validator", {
                plugins: [ValidatorPlugin]
            });
            Model.validate("ipAddress").isIPAddress();
            return  Model.sync();
        });

        it.should("throw an error if invalid with string", function () {
            var m = new Model({ipAddress: "192.168.1.1.1.1.1"});
            assert.isFalse(m.isValid());
            return m.save().chain(assert.fail, function (err) {
                assert.equal(err[0].message, "ipAddress must be a valid IPv4 or IPv6 address got 192.168.1.1.1.1.1.");
            });
        });

        it.should("not throw an error if valid", function () {
            return comb.when(
                new Model({ipAddress: "192.168.1.1"}).save(),
                new Model({ipAddress: "2001:0db8:85a3:0000:0000:8a2e:0370:7334"}).save()
            );
        });

        it.should("not throw an error if values are undefined", function () {
            return new Model().save();
        });
    });
    it.describe("#isIpV4Address", function (it) {

        it.beforeAll(function () {
            Model = patio.addModel("validator", {
                plugins: [ValidatorPlugin]
            });
            Model.validate("ipAddress").isIPv4Address();
            return  Model.sync();
        });

        it.should("throw an error if invalid with string", function () {
            var m = new Model({ipAddress: "2001:0db8:85a3:0000:0000:8a2e:0370:7334"});
            assert.isFalse(m.isValid());
            return m.save().chain(assert.fail, function (err) {
                assert.equal(err[0].message, "ipAddress must be a valid IPv4 address got 2001:0db8:85a3:0000:0000:8a2e:0370:7334.");
            });
        });

        it.should("not throw an error if valid", function () {
            return new Model({ipAddress: "192.168.1.1"}).save()
        });

        it.should("not throw an error if values are undefined", function () {
            return new Model().save();
        });
    });
    it.describe("#isIpV6Address", function (it) {

        it.beforeAll(function () {
            Model = patio.addModel("validator", {
                plugins: [ValidatorPlugin]
            });
            Model.validate("ipAddress").isIPv6Address();
            return  Model.sync();
        });

        it.should("throw an error if invalid with string", function () {
            var m = new Model({ipAddress: "192.168.1.1"});
            assert.isFalse(m.isValid());
            return m.save().chain(assert.fail, function (err) {
                assert.equal(err[0].message, "ipAddress must be a valid IPv6 address got 192.168.1.1.");
            });
        });

        it.should("not throw an error if valid", function () {
            return new Model({ipAddress: "2001:0db8:85a3:0000:0000:8a2e:0370:7334"}).save();
        });

        it.should("not throw an error if values are undefined", function () {
            return new Model().save();
        });
    });
    it.describe("#isUUID", function (it) {

        it.beforeAll(function () {
            Model = patio.addModel("validator", {
                plugins: [ValidatorPlugin]
            });
            Model.validate("uuid").isUUID();
            return  Model.sync();
        });

        it.should("throw an error if invalid with string", function () {
            var m = new Model({uuid: "fa25a170-edb1-11e1-aff1-0800200c9a6"});
            assert.isFalse(m.isValid());
            return m.save().chain(assert.fail, function (err) {
                assert.equal(err[0].message, "uuid must be a valid UUID got fa25a170-edb1-11e1-aff1-0800200c9a6");
            });
        });

        it.should("not throw an error if valid", function () {
            return new Model({uuid: "fa25a170-edb1-11e1-aff1-0800200c9a66"}).save();
        });

        it.should("not throw an error if values are undefined", function () {
            return new Model().save();
        });
    });

    it.describe("#isEmail", function (it) {

        it.beforeAll(function () {
            Model = patio.addModel("validator", {
                plugins: [ValidatorPlugin]
            });
            Model.validate("str").isEmail();
            return  Model.sync();
        });

        it.should("throw an error if invalid with string", function () {
            var m = new Model({str: "me@me"});
            assert.isFalse(m.isValid());
            return m.save().chain(assert.fail, function (err) {
                assert.equal(err[0].message, "str must be a valid Email Address got me@me");
            });
        });

        it.should("not throw an error if valid", function () {
            return new Model({str: "me@me.com"}).save();
        });

        it.should("not throw an error if values are undefined", function () {
            return new Model().save();
        });

    });

    it.describe("#isUrl", function (it) {

        it.beforeAll(function () {
            Model = patio.addModel("validator", {
                plugins: [ValidatorPlugin]
            });
            Model.validate("str").isUrl();
            return  Model.sync();
        });

        it.should("throw an error if invalid with string", function () {
            var m = new Model({str: "http://test"});
            assert.isFalse(m.isValid());
            return m.save().chain(assert.fail, function (err) {
                assert.equal(err[0].message, "str must be a valid url got http://test");

            });
        });

        it.should("not throw an error if valid", function () {
            return new Model({str: "http://localhost"}).save();
        });

        it.should("not throw an error if values are undefined", function () {
            return new Model().save();
        });
    });

    it.describe("#isAlpha", function (it) {

        it.beforeAll(function () {
            Model = patio.addModel("validator", {
                plugins: [ValidatorPlugin]
            });
            Model.validate("str").isAlpha();
            return  Model.sync();
        });

        it.should("throw an error if invalid with string", function () {
            var m = new Model({str: "123Test"});
            assert.isFalse(m.isValid());
            return m.save().chain(assert.fail, function (err) {
                assert.equal(err[0].message, "str must be a only letters got 123Test");
            });
        });

        it.should("not throw an error if valid", function () {
            return new Model({str: "test"}).save();
        });

        it.should("not throw an error if values are undefined", function () {
            return new Model().save();
        });
    });

    it.describe("#isAlphaNumeric", function (it) {

        it.beforeAll(function () {
            Model = patio.addModel("validator", {
                plugins: [ValidatorPlugin]
            });
            Model.validate("str").isAlphaNumeric();
            return  Model.sync();
        });

        it.should("throw an error if invalid with string", function () {
            var m = new Model({str: "Test_"});
            assert.isFalse(m.isValid());
            return m.save().chain(assert.fail, function (err) {
                assert.equal(err[0].message, "str must be a alphanumeric got Test_");
            });
        });

        it.should("not throw an error if valid", function () {
            return new Model({str: "test123"}).save();
        });

        it.should("not throw an error if values are undefined", function () {
            return new Model().save();
        });
    });

    it.describe("#hasLength", function (it) {

        it.beforeAll(function () {
            Model = patio.addModel("validator", {
                plugins: [ValidatorPlugin]
            });
            Model.validate("str").hasLength(10);
            Model.validate("str2").hasLength(7, 10);
            return  Model.sync();
        });

        it.should("throw an error if invalid with string", function () {
            var m = new Model({str: "123456789"});
            assert.isFalse(m.isValid());
            return m.save().chain(assert.fail, function (err) {
                assert.equal(err[0].message, "str must have a length between 10.");
            });
        });

        it.should("throw an error if string not to long", function () {
            var m = new Model({str2: "1234567891111"});
            assert.isFalse(m.isValid());
            return m.save().chain(assert.fail, function (err) {
                assert.equal(err[0].message, "str2 must have a length between 7 and 10.");
            });
        });

        it.should("not throw an error if valid", function () {
            return new Model({str: "1234567891", str2: "11111111"}).save();
        });

        it.should("not throw an error if values are undefined", function () {
            return new Model().save();
        });

    });

    it.describe("#isLowercase", function (it) {

        it.beforeAll(function () {
            Model = patio.addModel("validator", {
                plugins: [ValidatorPlugin]
            });
            Model.validate("str").isLowercase();
            return  Model.sync();
        });

        it.should("throw an error if invalid with string", function () {
            var m = new Model({str: "A"});
            assert.isFalse(m.isValid());
            return m.save().chain(assert.fail, function (err) {
                assert.equal(err[0].message, "str must be lowercase got A.");
            });
        });

        it.should("not throw an error if valid", function () {
            return new Model({str: "b"}).save();
        });

        it.should("not throw an error if values are undefined", function () {
            return new Model().save();
        });
    });

    it.describe("#isUppercase", function (it) {

        it.beforeAll(function () {
            Model = patio.addModel("validator", {
                plugins: [ValidatorPlugin]
            });
            Model.validate("str").isUppercase();
            return  Model.sync();
        });

        it.should("throw an error if invalid with string", function () {
            var m = new Model({str: "a"});
            assert.isFalse(m.isValid());
            return m.save().chain(assert.fail, function (err) {
                assert.equal(err[0].message, "str must be uppercase got a.");
            });
        });

        it.should("not throw an error if valid", function () {
            return new Model({str: "B"}).save();
        });

        it.should("not throw an error if values are undefined", function () {
            return new Model().save();
        });
    });

    it.describe("#isEmpty", function (it) {

        it.beforeAll(function () {
            Model = patio.addModel("validator", {
                plugins: [ValidatorPlugin]
            });
            Model.validate("str").isEmpty();
            return  Model.sync();
        });

        it.should("throw an error if invalid with string", function () {
            var m = new Model({str: "A"});
            assert.isFalse(m.isValid());
            return m.save().chain(assert.fail, function (err) {
                assert.equal(err[0].message, "str must be empty got A.");
            });
        });

        it.should("not throw an error if valid", function () {
            return new Model({str: ""}).save();
        });

        it.should("not throw an error if values are undefined", function () {
            return new Model().save();
        });

    });
    it.describe("#isNotEmpty", function (it) {

        it.beforeAll(function () {
            Model = patio.addModel("validator", {
                plugins: [ValidatorPlugin]
            });
            Model.validate("str").isNotEmpty();
            return  Model.sync();
        });

        it.should("throw an error if invalid with string", function () {
            var m = new Model({str: ""});
            assert.isFalse(m.isValid());
            return m.save().chain(assert.fail, function (err) {
                assert.equal(err[0].message, "str must not be empty.");
            });
        });

        it.should("not throw an error if valid", function () {
            return new Model({str: "A"}).save();
        });

        it.should("not throw an error if values are undefined", function () {
            return new Model().save();
        });
    });

    it.context(function (it) {

        it.beforeAll(function () {
            Model = patio.addModel("validator", {
                plugins: [ValidatorPlugin]
            });
            Model.validate("str").isNotEmpty().isAlpha().isLike(/hello/);
            return  Model.sync();
        });
        it.should("throw an error if string is empty", function () {
            var m = new Model({str: ""});
            assert.isFalse(m.isValid());
            return m.save().chain(assert.fail, function (err) {
                assert.equal(err[0].message, "str must not be empty.");
            });
        });

        it.should("throw an error if the string is not alpha", function () {
            var m = new Model({str: "1"});
            assert.isFalse(m.isValid());
            return m.save().chain(assert.fail, function (err) {
                assert.equal(err[0].message, "str must be a only letters got 1");
            });
        });

        it.should("throw an error if the string is not like /hello/", function () {
            var m = new Model({str: "hell"});
            assert.isFalse(m.isValid());
            return m.save().chain(assert.fail, function (err) {
                assert.equal(err[0].message, "str must be like /hello/ got hell.");
            });
        });

        it.should("not throw an error if valid", function () {
            return new Model({str: "hello"}).save();
        });

        it.should("not throw an error if values are undefined", function () {
            return new Model().save();
        });

        it.should("not validate on save if validate is false", function () {
            return new Model({str: "hell"}).save(null, {validate: false});
        });

        it.should("not validate on update if validate is false", function () {
            return Model.save(null).chain(function (model) {
                model.str = "hell";
                assert.isFalse(model.isValid());
                return model.update(null, {validate: false});
            });
        });
    });

    it.describe("#check", function (it) {

        it.beforeAll(function () {
            Model = patio.addModel("validator", {
                plugins: [ValidatorPlugin]
            });
            Model.validate("num").check(function (val) {
                return val % 2 === 0;
            }, {message: "{col} must be even got {val}."});

            Model.validate("num2").isNotNull().check(function (val) {
                return val % 2 === 0;
            }, {message: "{col} must be even got {val}."});
            return  Model.sync();
        });

        it.should("allow adding a check function", function () {
            var m = new Model({num: 1, num2: 2});
            assert.isFalse(m.isValid());
            return m.save().chain(assert.fail, function (err) {
                assert.equal(err[0].message, "num must be even got 1.");
            });
        });

        it.should("allow adding a check function", function () {
            var m = new Model({num2: null});
            assert.isFalse(m.isValid());
            return m.save().chain(assert.fail, function (err) {
                assert.equal(err[0].message, "num2 cannot be null.");
            });
        });

        it.should("allow adding a check function", function () {
            var m = new Model({num2: 1});
            assert.isFalse(m.isValid());
            return m.save().chain(assert.fail, function (err) {
                assert.equal(err[0].message, "num2 must be even got 1.");
            });
        });

        it.should("not throw an error if valid", function () {
            return comb.when(
                new Model({num: null, num2: 2}).save(),
                new Model({num: 2, num2: 2}).save(),
                new Model({num2: 2}).save()
            );
        });
    });

    it.context(function (it) {

        it.beforeAll(function () {
            Model = patio.addModel("validator", {
                plugins: [ValidatorPlugin]
            });
            Model.validate(function (validate) {
                validate("num").check(function (val) {
                    return val % 2 === 0;
                }, {message: "{col} must be even got {val}."});
                validate("num2").isNotNull().check(function (val) {
                    return val % 2 === 0;
                }, {message: "{col} must be even got {val}."});
            });
            return  Model.sync();
        });

        it.should("allow mass validation", function () {
            return comb.serial([
                function () {
                    var m = new Model({num: 1, num2: 2});
                    assert.isFalse(m.isValid());
                    return m.save().chain(assert.fail, function (err) {
                        assert.equal(err[0].message, "num must be even got 1.");
                    });
                },
                function () {
                    var m = new Model({num2: null});
                    assert.isFalse(m.isValid());
                    return m.save().chain(assert.fail, function (err) {
                        assert.equal(err[0].message, "num2 cannot be null.");
                    });
                },
                function () {
                    var m = new Model({num2: 1});
                    assert.isFalse(m.isValid());
                    return m.save().chain(assert.fail, function (err) {
                        assert.equal(err[0].message, "num2 must be even got 1.");
                    });
                },
                function () {
                    return comb.when(
                        new Model({num: null, num2: 2}).save(),
                        new Model({num: 2, num2: 2}).save(),
                        new Model({num2: 2}).save()
                    );
                }
            ]);
        });

    });

    it.describe("#onlyNotNull", function (it) {

        it.beforeAll(function () {
            Model = patio.addModel("validator", {
                plugins: [ValidatorPlugin]
            });
            Model.validate("col1").isEq("HELLO", { onlyDefined: true, onlyNotNull: false });
            Model.validate("col2").isEq("HELLO", { onlyDefined: true, onlyNotNull: true  });
            return  Model.sync();
        });

        it.should("throw an error if required field is null", function () {
            var m = new Model({col1: null, col2: "HELLO"});
            assert.isFalse(m.isValid());
            return m.save().chain(assert.fail, function (err) {
                assert.equal(err[0].message, "col1 must === HELLO got null.");
            });
        });

        it.should("not throw an error if valid and not null", function () {
            return new Model({col1: "HELLO", col2: "HELLO"}).save();
        });

        it.should("not throw an error if valid and optional fields are null", function () {
            return new Model({col1: "HELLO", col2: null}).save();
        });

        it.should("not throw an error if values are undefined", function () {
            return new Model().save();
        });

    });

    it.afterAll(function () {
        return helper.dropModels();
    });
});