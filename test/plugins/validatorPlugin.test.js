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
    it.beforeAll(function () {
        return helper.createSchemaAndSync(true);
    });

    it.afterEach(function () {
        return comb.when(patio.defaultDatabase.from("validator").remove());
    });

    it.describe("#isAfter", function (it) {
        var Model = patio.addModel("validator", {
            plugins:[ValidatorPlugin]
        });
        Model.validate("date").isAfter(new Date(2006, 1, 1));

        it.beforeAll(function () {
            return  Model.sync();
        });
        it.should("throw an error if invalid", function (next) {
            var m = new Model({date:new Date(2005, 1, 1)});
            assert.isFalse(m.isValid());
            m.save().then(next, function (err) {
                assert.isTrue(!!err[0].message.match(/date must be after/));
                next();
            });
        });
        it.should("not throw an error if valid", function (next) {
            new Model({date:new Date(2007, 1, 1)}).save().classic(next);
        });

        it.should("not throw an error if not defined", function (next) {
            new Model().save().classic(next);
        });
    });

    it.describe("#isBefore", function (it) {
        var Model = patio.addModel("validator", {
            plugins:[ValidatorPlugin]
        });
        Model.validate("date").isBefore(new Date(2006, 1, 1));

        it.beforeAll(function () {
            return  Model.sync();
        });

        it.should("throw an error if invalid", function (next) {
            var m = new Model({date:new Date(2007, 1, 1)});
            assert.isFalse(m.isValid());
            m.save().then(next, function (err) {
                assert.isTrue(!!err[0].message.match(/date must be before/));
                next();
            });
        });
        it.should("not throw an error if valid", function (next) {
            new Model({date:new Date(2005, 1, 1)}).save().classic(next);
        });

        it.should("not throw an error if not defined", function (next) {
            new Model().save().classic(next);
        });
    });

    it.describe("#isDefined", function (it) {
        var Model = patio.addModel("validator", {
            plugins:[ValidatorPlugin]
        });
        Model.validate("str").isDefined();

        it.beforeAll(function () {
            return  Model.sync();
        });

        it.should("throw an error if invalid", function (next) {
            var m = new Model()
            assert.isFalse(m.isValid());
            m.save().then(next, function (err) {
                assert.equal(err[0].message, "str must be defined.");
                next();
            });
        });
        it.should("not throw an error if valid", function (next) {
            new Model({str:"HELLO"}).save().classic(next);
        });

    });

    it.describe("#isNotDefined", function (it) {
        var Model = patio.addModel("validator", {
            plugins:[ValidatorPlugin]
        });
        Model.validate("str").isNotDefined();

        it.beforeAll(function () {
            return  Model.sync();
        });

        it.should("throw an error if invalid", function (next) {
            var m = new Model({str:"HELLO"})
            assert.isFalse(m.isValid());
            m.save().then(next, function (err) {
                assert.equal(err[0].message, "str cannot be defined.");
                next();
            });
        });
        it.should("not throw an error if valid", function (next) {
            new Model().save().classic(next);
        });

    });

    it.describe("#isNotNull", function (it) {
        var Model = patio.addModel("validator", {
            plugins:[ValidatorPlugin]
        });
        Model.validate("str").isNotNull();

        it.beforeAll(function () {
            return  Model.sync();
        });

        it.should("throw an error if invalid", function (next) {
            var m = new Model({str:null});
            assert.isFalse(m.isValid());
            m.save().then(next, function (err) {
                assert.equal(err[0].message, "str cannot be null.");
                next();
            });
        });
        it.should("not throw an error if valid", function (next) {
            new Model({str:"HELLO"}).save().classic(next);
        });
    });
    it.describe("#isNull", function (it) {
        var Model = patio.addModel("validator", {
            plugins:[ValidatorPlugin]
        });
        Model.validate("str").isNull();

        it.beforeAll(function () {
            return  Model.sync();
        });

        it.should("throw an error if invalid", function (next) {
            var m = new Model({str:"HELLO"});
            assert.isFalse(m.isValid());
            m.save().then(next, function (err) {
                assert.equal(err[0].message, "str must be null got HELLO.");
                next();
            });
        });
        it.should("not throw an error if valid", function (next) {
            new Model({str:null}).save().classic(next);
        });
    });
    it.describe("#isEq", function (it) {
        var Model = patio.addModel("validator", {
            plugins:[ValidatorPlugin]
        });
        Model.validate("str").isEq("HELLO");
        Model.validate("date").isEq(new Date(2006, 1, 1));

        it.beforeAll(function () {
            return  Model.sync();
        });

        it.should("throw an error if invalid", function (next) {
            var m = new Model({str:"HELL"});
            assert.isFalse(m.isValid());
            m.save().then(next, function (err) {
                assert.equal(err[0].message, "str must === HELLO got HELL.");
                next();
            });
        });

        it.should("throw an error if invalid with objects", function (next) {
            var m = new Model({str:"HELLO", date:new Date(2005, 1, 1)});
            assert.isFalse(m.isValid());
            m.save().then(next, function (err) {
                assert.isTrue(err[0].message.match(/date must ===/) !== null);
                next();
            });
        });

        it.should("not throw an error if valid", function (next) {
            new Model({str:"HELLO", date:new Date(2006, 1, 1)}).save().classic(next);
        });
    });
    it.describe("#isNeq", function (it) {
        var Model = patio.addModel("validator", {
            plugins:[ValidatorPlugin]
        });
        Model.validate("str").isNeq("HELLO");
        Model.validate("date").isNeq(new Date(2006, 1, 1));

        it.beforeAll(function () {
            return  Model.sync();
        });

        it.should("throw an error if invalid", function (next) {
            var m = new Model({str:"HELLO"});
            assert.isFalse(m.isValid());
            m.save().then(next, function (err) {
                assert.equal(err[0].message, "str must !== HELLO.");
                next();
            });
        });

        it.should("throw an error if invalid with objects", function (next) {
            var m = new Model({str:"HELL", date:new Date(2006, 1, 1)})
            assert.isFalse(m.isValid());
            m.save().then(next, function (err) {
                assert.isTrue(err[0].message.match(/date must !==/) !== null);
                next();
            });
        });

        it.should("not throw an error if valid", function (next) {
            new Model({str:"HELL", date:new Date(2005, 1, 1)}).save().classic(next);
        });
    });
    it.describe("#isLike", function (it) {
        var Model = patio.addModel("validator", {
            plugins:[ValidatorPlugin]
        });
        Model.validate("str").isLike("HELLO");
        Model.validate("str2").isLike(/HELLO/i);

        it.beforeAll(function () {
            return  Model.sync();
        });

        it.should("throw an error if invalid with string", function (next) {
            var m = new Model({str:"HELL"});
            assert.isFalse(m.isValid());
            m.save().then(next, function (err) {
                assert.equal(err[0].message, "str must be like HELLO got HELL.");
                next();
            });
        });

        it.should("throw an error if invalid with regexp", function (next) {
            var m = new Model({str2:"hell"});
            assert.isFalse(m.isValid());
            m.save().then(next, function (err) {
                assert.equal(err[0].message, "str2 must be like /HELLO/i got hell.");
                next();
            });
        });

        it.should("not throw an error if valid", function (next) {
            new Model({str:"HELLO", str2:"hello"}).save().classic(next);
        });

        it.should("not throw an error if values are undefined", function (next) {
            new Model().save().classic(next);
        });
    });
    it.describe("#isNotLike", function (it) {
        var Model = patio.addModel("validator", {
            plugins:[ValidatorPlugin]
        });
        Model.validate("str").isNotLike("HELLO");
        Model.validate("str2").isNotLike(/HELLO/i);

        it.beforeAll(function () {
            return  Model.sync();
        });

        it.should("throw an error if invalid with string", function (next) {
            var m = new Model({str:"HELLO"});
            assert.isFalse(m.isValid());
            m.save().then(next, function (err) {
                assert.equal(err[0].message, "str must not be like HELLO.");
                next();
            });
        });

        it.should("throw an error if invalid with regexp", function (next) {
            var m = new Model({str2:"hello"});
            assert.isFalse(m.isValid());
            m.save().then(next, function (err) {
                assert.equal(err[0].message, "str2 must not be like /HELLO/i.");
                next();
            });
        });

        it.should("not throw an error if valid", function (next) {
            new Model({str:"HELL", str2:"hell"}).save().classic(next);
        });

        it.should("not throw an error if values are undefined", function (next) {
            new Model().save().classic(next);
        });
    });
    it.describe("#isLt", function (it) {
        var Model = patio.addModel("validator", {
            plugins:[ValidatorPlugin]
        });
        Model.validate("num").isLt(10);

        it.beforeAll(function () {
            return  Model.sync();
        });

        it.should("throw an error if invalid with string", function (next) {
            var m = new Model({num:10});
            assert.isFalse(m.isValid());
            m.save().then(next, function (err) {
                assert.equal(err[0].message, "num must be < 10 got 10.");
                next();
            });
        });

        it.should("not throw an error if valid", function (next) {
            new Model({num:9}).save().classic(next);
        });

        it.should("not throw an error if values are undefined", function (next) {
            new Model().save().classic(next);
        });

    });
    it.describe("#isGt", function (it) {
        var Model = patio.addModel("validator", {
            plugins:[ValidatorPlugin]
        });
        Model.validate("num").isGt(10);

        it.beforeAll(function () {
            return  Model.sync();
        });

        it.should("throw an error if invalid with string", function (next) {
            var m = new Model({num:10});
            assert.isFalse(m.isValid());
            m.save().then(next, function (err) {
                assert.equal(err[0].message, "num must be > 10 got 10.");
                next();
            });
        });

        it.should("not throw an error if valid", function (next) {
            new Model({num:11}).save().classic(next);
        });

        it.should("not throw an error if values are undefined", function (next) {
            new Model().save().classic(next);
        });
    });
    it.describe("#isLte", function (it) {
        var Model = patio.addModel("validator", {
            plugins:[ValidatorPlugin]
        });
        Model.validate("num").isLte(10);

        it.beforeAll(function () {
            return  Model.sync();
        });

        it.should("throw an error if invalid with string", function (next) {
            var m = new Model({num:11});
            assert.isFalse(m.isValid());
            m.save().then(next, function (err) {
                assert.equal(err[0].message, "num must be <= 10 got 11.");
                next();
            });
        });

        it.should("not throw an error if valid", function (next) {
            new Model({num:9}).save().classic(next);
        });

        it.should("not throw an error if values are undefined", function (next) {
            new Model().save().classic(next);
        });
    });
    it.describe("#isGte", function (it) {
        var Model = patio.addModel("validator", {
            plugins:[ValidatorPlugin]
        });
        Model.validate("num").isGte(10);

        it.beforeAll(function () {
            return  Model.sync();
        });

        it.should("throw an error if invalid with string", function (next) {
            var m = new Model({num:9});
            assert.isFalse(m.isValid());
            m.save().then(next, function (err) {
                assert.equal(err[0].message, "num must be >= 10 got 9.");
                next();
            });
        });

        it.should("not throw an error if valid", function (next) {
            new Model({num:10}).save().classic(next);
        });

        it.should("not throw an error if values are undefined", function (next) {
            new Model().save().classic(next);
        });
    });
    it.describe("#isIn", function (it) {

        var Model = patio.addModel("validator", {
            plugins:[ValidatorPlugin]
        });
        Model.validate("str").isIn(["a", "b", "c"]);

        it.beforeAll(function () {
            return  Model.sync();
        });

        it.should("throw an error if arr is not an array", function () {
            assert.throws(function () {
                Model.validate("str2").isIn("a");
            });
        });

        it.should("throw an error if invalid with string", function (next) {
            var m = new Model({str:"d"});
            assert.isFalse(m.isValid());
            m.save().then(next, function (err) {
                assert.equal(err[0].message, "str must be in a,b,c got d.");
                next();
            });
        });

        it.should("not throw an error if valid", function (next) {
            new Model({str:"a"}).save().classic(next);
        });

        it.should("not throw an error if values are undefined", function (next) {
            new Model().save().classic(next);
        });

    });
    it.describe("#isNotIn", function (it) {
        var Model = patio.addModel("validator", {
            plugins:[ValidatorPlugin]
        });
        Model.validate("str").isNotIn(["a", "b", "c"]);

        it.beforeAll(function () {
            return  Model.sync();
        });

        it.should("throw an error if arr is not an array", function () {
            assert.throws(function () {
                Model.validate("str2").isNotIn("a");
            });
        });

        it.should("throw an error if invalid with string", function (next) {
            var m = new Model({str:"a"});
            assert.isFalse(m.isValid());
            m.save().then(next, function (err) {
                assert.equal(err[0].message, "str cannot be in a,b,c got a.");
                next();
            });
        });

        it.should("not throw an error if valid", function (next) {
            new Model({str:"d"}).save().classic(next);
        });

        it.should("not throw an error if values are undefined", function (next) {
            new Model().save().classic(next);
        });

    });
    it.describe("#isMacAddress", function (it) {
        var Model = patio.addModel("validator", {
            plugins:[ValidatorPlugin]
        });
        Model.validate("macAddress").isMacAddress();

        it.beforeAll(function () {
            return  Model.sync();
        });

        it.should("throw an error if invalid with string", function (next) {
            var m = new Model({macAddress:"a"});
            assert.isFalse(m.isValid());
            m.save().then(next, function (err) {
                assert.equal(err[0].message, "macAddress must be a valid MAC address got a.");
                next();
            });
        });

        it.should("not throw an error if valid", function (next) {
            comb.when(
                new Model({macAddress:"00-00-00-00-00-00"}).save(),
                new Model({macAddress:"00:00:00:00:00:00"}).save()
            ).classic(next);
        });

        it.should("not throw an error if values are undefined", function (next) {
            new Model().save().classic(next);
        });
    });
    it.describe("#isIpAddress", function (it) {
        var Model = patio.addModel("validator", {
            plugins:[ValidatorPlugin]
        });
        Model.validate("ipAddress").isIPAddress();

        it.beforeAll(function () {
            return  Model.sync();
        });

        it.should("throw an error if invalid with string", function (next) {
            var m = new Model({ipAddress:"192.168.1.1.1.1.1"});
            assert.isFalse(m.isValid());
            m.save().then(next, function (err) {
                assert.equal(err[0].message, "ipAddress must be a valid IPv4 or IPv6 address got 192.168.1.1.1.1.1.");
                next();
            });
        });

        it.should("not throw an error if valid", function (next) {
            comb.when(
                new Model({ipAddress:"192.168.1.1"}).save(),
                new Model({ipAddress:"2001:0db8:85a3:0000:0000:8a2e:0370:7334"}).save()
            ).classic(next);
        });

        it.should("not throw an error if values are undefined", function (next) {
            new Model().save().classic(next);
        });
    });
    it.describe("#isIpV4Address", function (it) {
        var Model = patio.addModel("validator", {
            plugins:[ValidatorPlugin]
        });
        Model.validate("ipAddress").isIPv4Address();

        it.beforeAll(function () {
            return  Model.sync();
        });

        it.should("throw an error if invalid with string", function (next) {
            var m = new Model({ipAddress:"2001:0db8:85a3:0000:0000:8a2e:0370:7334"});
            assert.isFalse(m.isValid());
            m.save().then(next, function (err) {
                assert.equal(err[0].message, "ipAddress must be a valid IPv4 address got 2001:0db8:85a3:0000:0000:8a2e:0370:7334.");
                next();
            });
        });

        it.should("not throw an error if valid", function (next) {
            comb.when(
                new Model({ipAddress:"192.168.1.1"}).save()
            ).classic(next);
        });

        it.should("not throw an error if values are undefined", function (next) {
            new Model().save().classic(next);
        });
    });
    it.describe("#isIpV6Address", function (it) {
        var Model = patio.addModel("validator", {
            plugins:[ValidatorPlugin]
        });
        Model.validate("ipAddress").isIPv6Address();

        it.beforeAll(function () {
            return  Model.sync();
        });

        it.should("throw an error if invalid with string", function (next) {
            var m = new Model({ipAddress:"192.168.1.1"});
            assert.isFalse(m.isValid());
            m.save().then(next, function (err) {
                assert.equal(err[0].message, "ipAddress must be a valid IPv6 address got 192.168.1.1.");
                next();
            });
        });

        it.should("not throw an error if valid", function (next) {
            comb.when(
                new Model({ipAddress:"2001:0db8:85a3:0000:0000:8a2e:0370:7334"}).save()
            ).classic(next);
        });

        it.should("not throw an error if values are undefined", function (next) {
            new Model().save().classic(next);
        });
    });
    it.describe("#isUUID", function (it) {
        var Model = patio.addModel("validator", {
            plugins:[ValidatorPlugin]
        });
        Model.validate("uuid").isUUID();

        it.beforeAll(function () {
            return  Model.sync();
        });

        it.should("throw an error if invalid with string", function (next) {
            var m = new Model({uuid:"fa25a170-edb1-11e1-aff1-0800200c9a6"});
            assert.isFalse(m.isValid());
            m.save().then(next, function (err) {
                assert.equal(err[0].message, "uuid must be a valid UUID got fa25a170-edb1-11e1-aff1-0800200c9a6");
                next();
            });
        });

        it.should("not throw an error if valid", function (next) {
            comb.when(
                new Model({uuid:"fa25a170-edb1-11e1-aff1-0800200c9a66"}).save()
            ).classic(next);
        });

        it.should("not throw an error if values are undefined", function (next) {
            new Model().save().classic(next);
        });
    });

    it.describe("#isEmail", function (it) {
        var Model = patio.addModel("validator", {
            plugins:[ValidatorPlugin]
        });
        Model.validate("str").isEmail();

        it.beforeAll(function () {
            return  Model.sync();
        });

        it.should("throw an error if invalid with string", function (next) {
            var m = new Model({str:"me@me"});
            assert.isFalse(m.isValid());
            m.save().then(next, function (err) {
                assert.equal(err[0].message, "str must be a valid Email Address got me@me");
                next();
            });
        });

        it.should("not throw an error if valid", function (next) {
            comb.when(
                new Model({str:"me@me.com"}).save()
            ).classic(next);
        });

        it.should("not throw an error if values are undefined", function (next) {
            new Model().save().classic(next);
        });

    });

    it.describe("#isUrl", function (it) {
        var Model = patio.addModel("validator", {
            plugins:[ValidatorPlugin]
        });
        Model.validate("str").isUrl();

        it.beforeAll(function () {
            return  Model.sync();
        });

        it.should("throw an error if invalid with string", function (next) {
            var m = new Model({str:"http://test"});
            assert.isFalse(m.isValid());
            m.save().then(next, function (err) {
                assert.equal(err[0].message, "str must be a valid url got http://test");
                next();
            });
        });

        it.should("not throw an error if valid", function (next) {
            comb.when(
                new Model({str:"http://localhost"}).save()
            ).classic(next);
        });

        it.should("not throw an error if values are undefined", function (next) {
            new Model().save().classic(next);
        });
    });

    it.describe("#isAlpha", function (it) {

        var Model = patio.addModel("validator", {
            plugins:[ValidatorPlugin]
        });
        Model.validate("str").isAlpha();

        it.beforeAll(function () {
            return  Model.sync();
        });

        it.should("throw an error if invalid with string", function (next) {
            var m = new Model({str:"123Test"});
            assert.isFalse(m.isValid());
            m.save().then(next, function (err) {
                assert.equal(err[0].message, "str must be a only letters got 123Test");
                next();
            });
        });

        it.should("not throw an error if valid", function (next) {
            comb.when(
                new Model({str:"test"}).save()
            ).classic(next);
        });

        it.should("not throw an error if values are undefined", function (next) {
            new Model().save().classic(next);
        });
    });

    it.describe("#isAlphaNumeric", function (it) {

        var Model = patio.addModel("validator", {
            plugins:[ValidatorPlugin]
        });
        Model.validate("str").isAlphaNumeric();

        it.beforeAll(function () {
            return  Model.sync();
        });

        it.should("throw an error if invalid with string", function (next) {
            var m = new Model({str:"Test_"});
            assert.isFalse(m.isValid());
            m.save().then(next, function (err) {
                assert.equal(err[0].message, "str must be a alphanumeric got Test_");
                next();
            });
        });

        it.should("not throw an error if valid", function (next) {
            comb.when(
                new Model({str:"test123"}).save()
            ).classic(next);
        });

        it.should("not throw an error if values are undefined", function (next) {
            new Model().save().classic(next);
        });
    });

    it.describe("#hasLength", function (it) {
        var Model = patio.addModel("validator", {
            plugins:[ValidatorPlugin]
        });
        Model.validate("str").hasLength(10);
        Model.validate("str2").hasLength(7, 10);

        it.beforeAll(function () {
            return  Model.sync();
        });

        it.should("throw an error if invalid with string", function (next) {
            var m = new Model({str:"123456789"});
            assert.isFalse(m.isValid());
            m.save().then(next, function (err) {
                assert.equal(err[0].message, "str must have a length between 10.");
                next();
            });
        });

        it.should("throw an error if string not to long", function (next) {
            var m = new Model({str2:"1234567891111"});
            assert.isFalse(m.isValid());
            m.save().then(next, function (err) {
                assert.equal(err[0].message, "str2 must have a length between 7 and 10.");
                next();
            });
        });

        it.should("not throw an error if valid", function (next) {
            comb.when(
                new Model({str:"1234567891", str2:"11111111"}).save()
            ).classic(next);
        });

        it.should("not throw an error if values are undefined", function (next) {
            new Model().save().classic(next);
        });

    });

    it.describe("#isLowercase", function (it) {
        var Model = patio.addModel("validator", {
            plugins:[ValidatorPlugin]
        });
        Model.validate("str").isLowercase();

        it.beforeAll(function () {
            return  Model.sync();
        });

        it.should("throw an error if invalid with string", function (next) {
            var m = new Model({str:"A"});
            assert.isFalse(m.isValid());
            m.save().then(next, function (err) {
                assert.equal(err[0].message, "str must be lowercase got A.");
                next();
            });
        });

        it.should("not throw an error if valid", function (next) {
            comb.when(
                new Model({str:"b"}).save()
            ).classic(next);
        });

        it.should("not throw an error if values are undefined", function (next) {
            new Model().save().classic(next);
        });
    });

    it.describe("#isUppercase", function (it) {
        var Model = patio.addModel("validator", {
            plugins:[ValidatorPlugin]
        });
        Model.validate("str").isUppercase();

        it.beforeAll(function () {
            return  Model.sync();
        });

        it.should("throw an error if invalid with string", function (next) {
            var m = new Model({str:"a"});
            assert.isFalse(m.isValid());
            m.save().then(next, function (err) {
                assert.equal(err[0].message, "str must be uppercase got a.");
                next();
            });
        });

        it.should("not throw an error if valid", function (next) {
            comb.when(
                new Model({str:"B"}).save()
            ).classic(next);
        });

        it.should("not throw an error if values are undefined", function (next) {
            new Model().save().classic(next);
        });
    });

    it.describe("#isEmpty", function (it) {
        var Model = patio.addModel("validator", {
            plugins:[ValidatorPlugin]
        });
        Model.validate("str").isEmpty();

        it.beforeAll(function () {
            return  Model.sync();
        });

        it.should("throw an error if invalid with string", function (next) {
            var m = new Model({str:"A"});
            assert.isFalse(m.isValid());
            m.save().then(next, function (err) {
                assert.equal(err[0].message, "str must be empty got A.");
                next();
            });
        });

        it.should("not throw an error if valid", function (next) {
            comb.when(
                new Model({str:""}).save()
            ).classic(next);
        });

        it.should("not throw an error if values are undefined", function (next) {
            new Model().save().classic(next);
        });

    });
    it.describe("#isNotEmpty", function (it) {
        var Model = patio.addModel("validator", {
            plugins:[ValidatorPlugin]
        });
        Model.validate("str").isNotEmpty();

        it.beforeAll(function () {
            return  Model.sync();
        });

        it.should("throw an error if invalid with string", function (next) {
            var m = new Model({str:""});
            assert.isFalse(m.isValid());
            m.save().then(next, function (err) {
                assert.equal(err[0].message, "str must not be empty.");
                next();
            });
        });

        it.should("not throw an error if valid", function (next) {
            comb.when(
                new Model({str:"A"}).save()
            ).classic(next);
        });

        it.should("not throw an error if values are undefined", function (next) {
            new Model().save().classic(next);
        });
    });

    it.context(function (it) {
        var Model = patio.addModel("validator", {
            plugins:[ValidatorPlugin]
        });
        Model.validate("str").isNotEmpty().isAlpha().isLike(/hello/);

        it.beforeAll(function () {
            return  Model.sync();
        });
        it.should("throw an error if string is empty", function (next) {
            var m = new Model({str:""});
            assert.isFalse(m.isValid());
            m.save().then(next, function (err) {
                assert.equal(err[0].message, "str must not be empty.");
                next();
            });
        });

        it.should("throw an error if the string is not alpha", function (next) {
            var m = new Model({str:"1"});
            assert.isFalse(m.isValid());
            m.save().then(next, function (err) {
                assert.equal(err[0].message, "str must be a only letters got 1");
                next();
            });
        });

        it.should("throw an error if the string is not like /hello/", function (next) {
            var m = new Model({str:"hell"});
            assert.isFalse(m.isValid());
            m.save().then(next, function (err) {
                assert.equal(err[0].message, "str must be like /hello/ got hell.");
                next();
            });
        });

        it.should("not throw an error if valid", function (next) {
            comb.when(
                new Model({str:"hello"}).save()
            ).classic(next);
        });

        it.should("not throw an error if values are undefined", function (next) {
            new Model().save().classic(next);
        });

        it.should("not validate on save if validate is false", function (next) {
            comb.when(
                new Model({str:"hell"}).save(null, {validate:false})
            ).classic(next);
        });

        it.should("not validate on update if validate is false", function (next) {
            Model.save(null).chain(function (model) {
                model.str = "hell";
                assert.isFalse(model.isValid());
                return model.update(null, {validate:false});
            }).classic(next);
        });
    });

    it.describe("#check", function (it) {

        var Model = patio.addModel("validator", {
            plugins:[ValidatorPlugin]
        });
        Model.validate("num").check(function (val) {
            return val % 2 === 0;
        }, {message:"{col} must be even got {val}."});

        Model.validate("num2").isNotNull().check(function (val) {
            return val % 2 === 0;
        }, {message:"{col} must be even got {val}."});

        it.beforeAll(function () {
            return  Model.sync();
        });

        it.should("allow adding a check function", function (next) {
            var m = new Model({num:1});
            assert.isFalse(m.isValid());
            m.save().then(next, function (err) {
                assert.equal(err[0].message, "num must be even got 1.");
                next();
            });
        });

        it.should("allow adding a check function", function (next) {
            var m = new Model({num2:null});
            assert.isFalse(m.isValid());
            m.save().then(next, function (err) {
                assert.equal(err[0].message, "num2 cannot be null.");
                next();
            });
        });

        it.should("allow adding a check function", function (next) {
            var m = new Model({num2:1});
            assert.isFalse(m.isValid());
            m.save().then(next, function (err) {
                assert.equal(err[0].message, "num2 must be even got 1.");
                next();
            });
        });

        it.should("not throw an error if valid", function (next) {
            comb.when(
                new Model({num:null}).save(),
                new Model({num:2}).save(),
                new Model({num2:2}).save()
            ).classic(next);
        });
    });

    it.run();

});

