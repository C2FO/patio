var it = require('it'),
    assert = require('assert'),
    patio = require("../../lib"),
    ClassTableInheritance = patio.plugins.ClassTableInheritancePlugin,
    comb = require("comb"),
    hitch = comb.hitch,
    helper = require("../data/classTableInheritance.helper.js"),
    Employee, Staff, Manager, Executive;


patio.quoteIdentifiers = false;

it.describe("ClassTableInheritancePlugin", function (it) {


    it.beforeAll(function () {
        Employee = patio.addModel("employee", {
            plugins: [ClassTableInheritance],
            "static": {

                init: function () {
                    this._super(arguments);
                    this.configure({key: "kind"});
                }
            }
        });
        Staff = patio.addModel("staff", Employee, {

            "static": {

                init: function () {
                    this._super(arguments);
                    this.manyToOne("manager", {key: "managerid", fetchType: this.fetchType.EAGER});
                }
            }
        });
        Manager = patio.addModel("manager", Employee, {
            "static": {
                init: function () {
                    this._super(arguments);
                    this.oneToMany("staff", {key: "managerid", fetchType: this.fetchType.EAGER});
                }
            }
        });

        Executive = patio.addModel("executive", Manager);

        return helper.createSchemaAndSync();
    });

    it.should("set up the proper sql", function () {
        assert.equal(Employee.dataset.sql, "SELECT * FROM employee");
        assert.equal(Staff.dataset.sql, "SELECT * FROM employee INNER JOIN staff USING (id)");
        assert.equal(Manager.dataset.sql, "SELECT * FROM employee INNER JOIN manager USING (id)");
        assert.equal(Executive.dataset.sql, "SELECT * FROM employee INNER JOIN manager USING (id) INNER JOIN executive USING (id)");
    });

    it.should("insert properly", function () {
        return comb.when(
                new Employee({name: "Bob"}).save(),
                new Staff({name: "Greg"}).save(),
                new Manager({name: "Jane"}).save(),
                new Executive({name: "Sue"}).save()
            ).chain(function (res) {
                var bob = res[0], greg = res[1], jane = res[2], sue = res[3];
                assert.instanceOf(bob, Employee);
                assert.instanceOf(greg, Employee);
                assert.instanceOf(greg, Staff);
                assert.instanceOf(jane, Employee);
                assert.instanceOf(jane, Manager);
                assert.instanceOf(sue, Employee);
                assert.instanceOf(sue, Manager);
                assert.instanceOf(sue, Executive);
            });
    });

    it.should("fetch properly", function () {
        return comb.when(
                Employee.all(),
                Manager.all(),
                Staff.all(),
                Executive.all()
            ).chain(function (res) {
                var emps = res[0], managers = res[1], staff = res[2], executives = res[3];
                assert.lengthOf(emps, 4);
                emps.forEach(function (model) {
                    assert.instanceOf(model, Employee);
                });
                assert.lengthOf(managers, 2);
                managers.forEach(function (model) {
                    assert.instanceOf(model, Employee);
                    assert.instanceOf(model, Manager);
                });

                assert.lengthOf(staff, 1);
                staff.forEach(function (model) {
                    assert.instanceOf(model, Employee);
                    assert.instanceOf(model, Staff);
                });

                assert.lengthOf(executives, 1);
                executives.forEach(function (model) {
                    assert.instanceOf(model, Employee);
                    assert.instanceOf(model, Executive);
                    assert.instanceOf(model, Manager);
                });
            });
    });


    it.should("maintain associations properly", function () {
        var i = 0;
        return Manager.order('kind').forEach(
            function (manager) {
                return manager.addStaff(new Staff({name: "Staff" + i++}));
            }).chain(function () {
                return Employee.order('kind', "id").all().chain(function (res) {
                    assert.lengthOf(res, 6);
                    var employee = res[0], executive = res[1], manager = res[2], staff1 = res[3], staff2 = res[4], staff3 = res[5];
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
                });
            });
    });


    it.should("update properly", function () {
        var i = 0;
        return Manager.order('kind', "id")
            .forEach(function (emp) {
                emp.name = "Manager " + i++;
                emp.numstaff = emp.staff.length;
                if (emp instanceof Executive) {
                    emp.nummanagers = 0;
                }
                return emp.update();
            })
            .chain(function () {
                return Manager.order('kind', "id").all();
            })
            .chain(function (res) {
                assert.lengthOf(res, 2);
                res.forEach(function (manager, i) {
                    assert.equal(manager.name, "Manager " + i);
                    assert.equal(manager.numstaff, manager.staff.length);
                    if (manager instanceof Executive) {
                        assert.equal(manager.nummanagers, 0);
                    }
                });
            });
    });


    it.should("remove properly", function () {
        return Employee.remove().chain(function () {
            var db = patio.defaultDatabase;
            return comb.when(
                    db.from("employee").count(),
                    db.from("staff").count(),
                    db.from("manager").count(),
                    db.from("executive").count()
                ).chain(function (res) {
                    assert.lengthOf(res, 4);
                    res.forEach(function (i) {
                        assert.equal(i, 0);
                    });
                });
        });
    });

    it.afterAll(function () {
        return helper.dropModels();
    });
});