var vows = require('vows'),
    assert = require('assert'),
    patio = require("index"),
    comb = require("comb"),
    hitch = comb.hitch,
    helper = require("../data/ctiPlugin/classTableInheritance.models");

var ret = module.exports = exports = new comb.Promise();
patio.quoteIdentifiers = false;
helper.loadModels().then(function () {
    var Employee = patio.getModel("employee"),
        Staff = patio.getModel("staff"),
        Manager = patio.getModel("manager"),
        Executive = patio.getModel("executive")
    var suite = vows.describe("ClassTableInheritance custom columns");

    suite.addBatch({
        "The ClassTableInheritance should":{
            topic:{Employee:Employee, Staff:Staff, Manager:Manager, Executive:Executive},

            "should set up the proper sql":function () {
                assert.equal(Employee.dataset.sql, "SELECT * FROM employee");
                assert.equal(Staff.dataset.sql, "SELECT * FROM employee INNER JOIN staff USING (id)");
                assert.equal(Manager.dataset.sql, "SELECT * FROM employee INNER JOIN manager USING (id)");
                assert.equal(Executive.dataset.sql, "SELECT * FROM employee INNER JOIN manager USING (id) INNER JOIN executive USING (id)");
            }
        }
    });

    suite.addBatch({
        "The ClassTableInheritance should":{
            topic:{Employee:Employee, Staff:Staff, Manager:Manager, Executive:Executive},

            "should insert employees properly":{
                topic:function () {
                    comb.when(
                        new Employee({name:"Bob"}).save(),
                        new Staff({name:"Greg"}).save(),
                        new Manager({name:"Jane"}).save(),
                        new Executive({name:"Sue"}).save()
                    ).then(hitch(this, "callback", null), hitch(this, "callback"));
                },

                "and should maintin type":function (res) {
                    var bob = res[0], greg = res[1], jane = res[2], sue = res[3];
                    assert.instanceOf(bob, Employee);
                    assert.instanceOf(greg, Employee);
                    assert.instanceOf(greg, Staff);
                    assert.instanceOf(jane, Employee);
                    assert.instanceOf(jane, Manager);
                    assert.instanceOf(sue, Employee);
                    assert.instanceOf(sue, Manager);
                    assert.instanceOf(sue, Executive);
                }
            }
        }
    });

    suite.addBatch({
        "The ClassTableInheritance should":{
            topic:{Employee:Employee, Staff:Staff, Manager:Manager, Executive:Executive},

            "should retrieve employees properly":{
                topic:function () {
                    Employee.all().then(hitch(this, "callback", null), hitch(this, "callback"));
                },

                "and return proper model types":function (res) {
                    var bob = res[0], greg = res[1], jane = res[2], sue = res[3];
                    assert.instanceOf(bob, Employee);
                    assert.instanceOf(greg, Employee);
                    assert.instanceOf(greg, Staff);
                    assert.instanceOf(jane, Employee);
                    assert.instanceOf(jane, Manager);
                    assert.instanceOf(sue, Employee);
                    assert.instanceOf(sue, Manager);
                    assert.instanceOf(sue, Executive);
                }
            },

            "should retrieve subclasses properly":{
                topic:function () {
                    comb.when(
                        Manager.all(),
                        Staff.all(),
                        Executive.all()
                    ).then(hitch(this, "callback", null), hitch(this, "callback"));
                },

                "and return proper model types":function (res) {
                    var managers = res[0];
                    var staff = res[1];
                    var executives = res[2];
                    assert.lengthOf(managers, 2);
                    managers.forEach(function (model) {
                        assert.instanceOf(model, Manager);
                    });

                    assert.lengthOf(staff, 1);
                    staff.forEach(function (model) {
                        assert.instanceOf(model, Staff);
                    });

                    assert.lengthOf(executives, 1);
                    executives.forEach(function (model) {
                        assert.instanceOf(model, Executive);
                    })
                }
            }
        }
    });

    suite.addBatch({
        "The ClassTableInheritance should":{
            topic:{Employee:Employee, Staff:Staff, Manager:Manager, Executive:Executive},

            "should maintain associations of employees properly":{
                topic:function () {
                    var ret = new comb.Promise(), i = 0;
                    Manager.order('kind').forEach(
                        function (manager) {
                            return manager.addStaff(new Staff({name:"Staff" + i++}))

                        }).then(function () {
                            Employee.order('kind', "id").all().then(comb.hitch(ret, "callback"), comb.hitch(ret, "errback"));
                        }, comb.hitch(ret, "errback"));
                    ret.then(comb.hitch(this, "callback", null), comb.hitch(this, "callback"));
                },

                "and should maintin type":function (res) {
                    assert.lengthOf(res, 6);
                    var employee = res[0], executive = res[1], manager = res[2], staff1 = res[3], staff2 = res[4], staff3 = res[5]
                    assert.isFalse(employee.hasAssociations);
                    assert.isTrue(executive.hasAssociations);
                    assert.lengthOf(executive.staff, 1);
                    assert.isTrue(manager.hasAssociations);
                    assert.lengthOf(manager.staff, 1);
                    assert.isTrue(staff1.hasAssociations);
                    assert.isNull(staff1.manager);
                    assert.isTrue(staff2.hasAssociations);
                    assert.isNotNull(staff2.manager);
                    assert.instanceOf(staff2.manager, Manager);
                    assert.isTrue(staff3.hasAssociations);
                    assert.isNotNull(staff3.manager);
                    assert.instanceOf(staff2.manager, Manager);

                }
            }
        }
    });

    suite.addBatch({
        "The ClassTableInheritance should":{
            topic:{Employee:Employee, Staff:Staff, Manager:Manager, Executive:Executive},

            "should update employees properly":{
                topic:function () {
                    var ret = new comb.Promise(), i = 0;
                    Manager.order('kind', "id").forEach(function (emp) {
                            emp.name = "Manager " + i++;
                            emp.numStaff = emp.staff.length;
                            if(emp instanceof Executive){
                                emp.numManagers = 0;
                            }
                            return emp.update();
                        }).then(function () {
                            Manager.order('kind', "id").all().then(comb.hitch(ret, "callback"), comb.hitch(ret, "errback"));
                        }, comb.hitch(ret, "errback"));
                    ret.then(comb.hitch(this, "callback", null), comb.hitch(this, "callback"));
                },

                "and should maintin type":function (res) {
                   assert.lengthOf(res, 2);
                   res.forEach(function(manager, i){
                       assert.equal(manager.name, "Manager " + i);
                       assert.equal(manager.numStaff, manager.staff.length);
                       if(manager instanceof Executive){
                           assert.equal(manager.numManagers, 0);
                       }
                   })
                }
            }
        }
    });

    suite.addBatch({
        "The ClassTableInheritance should":{
            topic:{Employee:Employee, Staff:Staff, Manager:Manager, Executive:Executive},

            "should remove employees properly":{
                topic:function () {
                    var ret = new comb.Promise(), i = 0;
                    Employee.remove().then(function(){
                        var db = patio.defaultDatabase;
                        comb.when(
                            db.from("employee").count(),
                            db.from("staff").count(),
                            db.from("manager").count(),
                            db.from("executive").count()
                        ).then(comb.hitch(ret, "callback"), comb.hitch(ret, "errback"));
                    }, comb.hitch(ret, "errback"));
                    ret.then(comb.hitch(this, "callback", null), comb.hitch(this, "callback"));
                },

                "and should maintain type":function (res) {
                    assert.lengthOf(res, 4);
                    res.forEach(function(i){
                        assert.equal(i, 0);
                    });
                }
            }
        }
    });


    suite.run({reporter:require("vows").reporter.spec}, function () {
        helper.dropModels().then(comb.hitch(ret, "callback"), comb.hitch(ret, "errback"));
    });
});