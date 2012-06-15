var it = require('it'),
    assert = require('assert'),
    patio = require("index"),
    events = require("events"),
    sql = patio.SQL,
    comb = require("comb"),
    hitch = comb.hitch;

var gender = ["M", "F"];

var DB1, DB2;
var createTablesAndSync = function () {
    var ret = new comb.Promise();
    comb.when(
        DB1.forceCreateTable("employee", function () {
            this.primaryKey("id");
            this.firstname("string", {length:20, allowNull:false});
            this.lastname("string", {length:20, allowNull:false});
            this.midinitial("char", {length:1});
            this.position("integer");
            this.gender("enum", {elements:["M", "F"]});
            this.street("string", {length:50, allowNull:false});
            this.city("string", {length:20, allowNull:false});
        }),
        DB2.forceCreateTable("employee", function () {
            this.primaryKey("id");
            this.firstname("string", {length:20, allowNull:false});
            this.lastname("string", {length:20, allowNull:false});
            this.midinitial("char", {length:1});
            this.position("integer");
            this.gender("enum", {elements:["M", "F"]});
            this.street("string", {length:50, allowNull:false});
            this.city("string", {length:20, allowNull:false});
        })
    ).chain(patio.syncModels, ret).then(ret);
    return ret;

};

var dropTableAndDisconnect = function () {
    return comb.executeInOrder(patio, DB1, DB2, function (patio, db1, db2) {
        db1.forceDropTable("employee");
        db2.forceDropTable("employee");
        patio.disconnect();
        patio.resetIdentifierMethods();
    });
};


it.describe("Models from mutliple databases", function (it) {

    var Employee, Employee2;
    it.beforeAll(function () {
        DB1 = patio.connect("mysql://test:testpass@localhost:3306/sandbox");
        DB2 = patio.connect("mysql://test:testpass@localhost:3306/sandbox2");

        Employee = patio.addModel(DB1.from("employee"), {
            "static":{
                //class methods
                findByGender:function (gender, callback, errback) {
                    return this.filter({gender:gender}).all();
                }
            }
        });
        Employee2 = patio.addModel(DB2.from("employee"), {
            "static":{
                //class methods
                findByGender:function (gender, callback, errback) {
                    return this.filter({gender:gender}).all();
                }
            }
        });
        return createTablesAndSync();
    });

    var emp1, emp2;
    it.beforeEach(function () {

        emp1 = new Employee({
            firstname:"doug",
            lastname:"martin",
            position:1,
            midinitial:null,
            gender:"M",
            street:"1 nowhere st.",
            city:"NOWHERE"});
        emp2 = new Employee2({
            firstname:"doug1",
            lastname:"martin1",
            position:1,
            midinitial:null,
            gender:"F",
            street:"2 nowhere st.",
            city:"NOWHERE2"});
        return comb.serial([
            function () {
                return comb.when(Employee.remove(), Employee2.remove());
            },
            function () {
                return comb.when(emp1.save(), emp2.save());
            }]);
    });

    it.describe("patio", function (it) {
        it.should("retrive models by database", function () {
            assert.strictEqual(patio.getModel("employee", DB1), Employee);
            assert.strictEqual(patio.getModel("employee", DB2), Employee2);
        });
    });


    it.should("save models to respective databases", function (next) {
        assert.instanceOf(emp1, Employee);
        assert.equal("doug", emp1.firstname);
        assert.equal("martin", emp1.lastname);
        assert.isNull(emp1.midinitial);
        assert.equal("M", emp1.gender);
        assert.equal("1 nowhere st.", emp1.street);
        assert.equal("NOWHERE", emp1.city);

        assert.instanceOf(emp2, Employee2);
        assert.equal("doug1", emp2.firstname);
        assert.equal("martin1", emp2.lastname);
        assert.isNull(emp2.midinitial);
        assert.equal("F", emp2.gender);
        assert.equal("2 nowhere st.", emp2.street);
        assert.equal("NOWHERE2", emp2.city);

        comb.when(Employee.count(), Employee2.count()).then(function (res) {
            assert.equal(res[0], 1);
            assert.equal(res[1], 1);
            next();
        }, next);
    });

    it.should("retrieve models from respective database", function (next) {
        comb.when(Employee.all(), Employee2.all()).then(function (res) {
            var emps1 = res[0], emps2 = res[1];
            assert.lengthOf(emps1, 1);
            assert.lengthOf(emps2, 1);
            next();
        }, next);
    });

    it.should("remove models from respective databases", function (next) {
        comb.when(Employee.all(), Employee2.all()).then(function (res) {
            var emps1 = res[0], emps2 = res[1];
            assert.lengthOf(emps1, 1);
            assert.lengthOf(emps2, 1);
            Employee.remove().then(function () {
                comb.when(Employee.all(), Employee2.all()).then(function (res) {
                    var emps1 = res[0], emps2 = res[1];
                    assert.lengthOf(emps1, 0);
                    assert.lengthOf(emps2, 1);
                    Employee2.remove().then(function () {
                        comb.when(Employee.all(), Employee2.all()).then(function (res) {
                            var emps1 = res[0], emps2 = res[1];
                            assert.lengthOf(emps1, 0);
                            assert.lengthOf(emps2, 0);
                            next();
                        }, next);
                    }, next);
                }, next);
            }, next);
        }, next);
    });

    it.afterAll(dropTableAndDisconnect);

});



