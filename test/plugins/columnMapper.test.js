var it = require('it'),
    assert = require('assert'),
    helper = require("../data/mappedColumnPlugin.helper.js"),
    patio = require("index"),
    sql = patio.sql,
    comb = require("comb"),
    Promise = comb.Promise,
    hitch = comb.hitch;


var gender = ["M", "F"];

it.describe("patio.plugins.ColumnMapper", function (it) {
    var Works, Employee;
    it.beforeAll(function () {
        Works = patio.addModel("works", {
            "static":{

                camelize:true,

                init:function () {
                    this._super(arguments);
                    this.manyToOne("employee");
                }
            }
        });
        Employee = patio.addModel("employee", {
            plugins:[patio.plugins.ColumnMapper],
            "static":{

                camelize:true,

                init:function () {
                    this._super(arguments);
                    this.oneToOne("works", {fetchType:this.fetchType.EAGER});
                    this.mappedColumn("salary", "works", {employeeId:sql.identifier("id")});
                    this.mappedColumn("mySalary", "works", {employeeId:sql.identifier("id")}, {column:"salaryTwo"});
                    this.mappedColumn("salaryInner", "works", {employeeId:sql.identifier("id")}, {joinType:"inner", column:"salaryInner"});
                    this.mappedColumn("salaryOther", "works", {employeeId:sql.identifier("id")}, {joinType:"inner", column:"salaryThree"});
                }
            }
        });
        return helper.createSchemaAndSync(true);
    });

    it.afterEach(function () {
        return comb.when(Employee.remove(), Works.remove());
    });

    it.should("throw an error if table is not provided", function(){
       assert.throws(function(){
           Employee.mappedColumn("salaryOther", {id : "test"});
       });
    });

    it.should("throw an error if a join condition is not defined", function(){
        assert.throws(function(){
            Employee.mappedColumn("salaryOther", "test");
        });
    });


    it.should("retrieve properties on save", function (next) {
        var employee = new Employee({
            lastName:"last" + 1,
            firstName:"first" + 1,
            midInitial:"m",
            gender:gender[1 % 2],
            street:"Street " + 1,
            city:"City " + 1,
            works:{
                companyName:"Google",
                salary:100000,
                salaryTwo:90000,
                salaryInner:80000,
                salaryThree:70000
            }
        });
        employee.save().then(function () {
            assert.equal(employee.salary, 100000);
            assert.equal(employee.mySalary, 90000);
            assert.equal(employee.salaryInner, 80000);
            assert.equal(employee.salaryOther, 70000);
            next();
        }, next);
    });

    it.should("not retrieve properties on save is options.reload === false", function (next) {
        var employee = new Employee({
            lastName:"last" + 1,
            firstName:"first" + 1,
            midInitial:"m",
            gender:gender[1 % 2],
            street:"Street " + 1,
            city:"City " + 1,
            works:{
                companyName:"Google",
                salary:100000,
                salaryTwo:90000,
                salaryInner:80000,
                salaryThree:70000
            }
        });
        employee.save(null, {reload:false}).then(function () {
            assert.isUndefined(employee.salary);
            assert.isUndefined(employee.mySalary);
            assert.isUndefined(employee.salaryInner);
            assert.isUndefined(employee.salaryOther);
            next();
        }, next);
    });

    it.should("not retrieve properties on save is options.reloadMapped === false", function (next) {
        var employee = new Employee({
            lastName:"last" + 1,
            firstName:"first" + 1,
            midInitial:"m",
            gender:gender[1 % 2],
            street:"Street " + 1,
            city:"City " + 1,
            works:{
                companyName:"Google",
                salary:100000,
                salaryTwo:90000,
                salaryInner:80000,
                salaryThree:70000
            }
        });
        employee.save(null, {reloadMapped:false}).then(function () {
            assert.isUndefined(employee.salary);
            assert.isUndefined(employee.mySalary);
            assert.isUndefined(employee.salaryInner);
            assert.isUndefined(employee.salaryOther);
            next();
        }, next);
    });

    it.should("not retrieve properties on save is reloadOnSave === false", function (next) {
        var employee = new Employee({
            lastName:"last" + 1,
            firstName:"first" + 1,
            midInitial:"m",
            gender:gender[1 % 2],
            street:"Street " + 1,
            city:"City " + 1,
            works:{
                companyName:"Google",
                salary:100000,
                salaryTwo:90000,
                salaryInner:80000,
                salaryThree:70000
            }
        });
        Employee.reloadOnSave = false;
        employee.save(null).then(function () {
            assert.isUndefined(employee.salary);
            assert.isUndefined(employee.mySalary);
            assert.isUndefined(employee.salaryInner);
            assert.isUndefined(employee.salaryOther);
            Employee.reloadOnSave = true;
            next();
        }, next);
    });

    it.should("retrieve properties on update", function (next) {
        var employee = new Employee({
            lastName:"last" + 1,
            firstName:"first" + 1,
            midInitial:"m",
            gender:gender[1 % 2],
            street:"Street " + 1,
            city:"City " + 1,
            works:{
                companyName:"Google",
                salary:100000,
                salaryTwo:90000,
                salaryInner:80000,
                salaryThree:70000
            }
        });
        employee.save().then(function () {
            assert.equal(employee.salary, 100000);
            assert.equal(employee.mySalary, 90000);
            assert.equal(employee.salaryInner, 80000);
            assert.equal(employee.salaryOther, 70000);
            comb.serial([
                employee.works.update.bind(employee.works, {salary:1000, salaryTwo:900, salaryInner:800, salaryThree:700}),
                function () {
                    assert.equal(employee.salary, 100000);
                    assert.equal(employee.mySalary, 90000);
                    assert.equal(employee.salaryInner, 80000);
                    assert.equal(employee.salaryOther, 70000);
                },
                employee.update.bind(employee, ({firstName:"bob"}))
            ]).then(function () {
                    assert.equal(employee.salary, 1000);
                    assert.equal(employee.mySalary, 900);
                    assert.equal(employee.salaryInner, 800);
                    assert.equal(employee.salaryOther, 700);
                }).classic(next);
        }, next);
    });

    it.should("not retrieve properties on update if options.reload == false", function (next) {
        var employee = new Employee({
            lastName:"last" + 1,
            firstName:"first" + 1,
            midInitial:"m",
            gender:gender[1 % 2],
            street:"Street " + 1,
            city:"City " + 1,
            works:{
                companyName:"Google",
                salary:100000,
                salaryTwo:90000,
                salaryInner:80000,
                salaryThree:70000
            }
        });
        employee.save().then(function () {
            assert.equal(employee.salary, 100000);
            assert.equal(employee.mySalary, 90000);
            assert.equal(employee.salaryInner, 80000);
            assert.equal(employee.salaryOther, 70000);
            comb.serial([
                employee.works.update.bind(employee.works, {salary:1000, salaryTwo:900, salaryInner:800, salaryThree:700}),
                function () {
                    assert.equal(employee.salary, 100000);
                    assert.equal(employee.mySalary, 90000);
                    assert.equal(employee.salaryInner, 80000);
                    assert.equal(employee.salaryOther, 70000);
                },
                employee.update.bind(employee, {firstName:"bob"}, {reload:false})
            ]).then(function () {
                    assert.equal(employee.salary, 100000);
                    assert.equal(employee.mySalary, 90000);
                    assert.equal(employee.salaryInner, 80000);
                    assert.equal(employee.salaryOther, 70000);
                }).classic(next);
        }, next);
    });

    it.should("not retrieve properties on update if options.reloadMapped == false", function (next) {
        var employee = new Employee({
            lastName:"last" + 1,
            firstName:"first" + 1,
            midInitial:"m",
            gender:gender[1 % 2],
            street:"Street " + 1,
            city:"City " + 1,
            works:{
                companyName:"Google",
                salary:100000,
                salaryTwo:90000,
                salaryInner:80000,
                salaryThree:70000
            }
        });
        employee.save().then(function () {
            assert.equal(employee.salary, 100000);
            assert.equal(employee.mySalary, 90000);
            assert.equal(employee.salaryInner, 80000);
            assert.equal(employee.salaryOther, 70000);
            comb.serial([
                employee.works.update.bind(employee.works, {salary:1000, salaryTwo:900, salaryInner:800, salaryThree:700}),
                function () {
                    assert.equal(employee.salary, 100000);
                    assert.equal(employee.mySalary, 90000);
                    assert.equal(employee.salaryInner, 80000);
                    assert.equal(employee.salaryOther, 70000);
                },
                employee.update.bind(employee, {firstName:"bob"}, {reloadMapped:false})
            ]).then(function () {
                    assert.equal(employee.salary, 100000);
                    assert.equal(employee.mySalary, 90000);
                    assert.equal(employee.salaryInner, 80000);
                    assert.equal(employee.salaryOther, 70000);
                }).classic(next);
        }, next);
    });

    it.should("not retrieve properties on update if reloadOnUpdate == false", function (next) {
        var employee = new Employee({
            lastName:"last" + 1,
            firstName:"first" + 1,
            midInitial:"m",
            gender:gender[1 % 2],
            street:"Street " + 1,
            city:"City " + 1,
            works:{
                companyName:"Google",
                salary:100000,
                salaryTwo:90000,
                salaryInner:80000,
                salaryThree:70000
            }
        });
        employee.save().then(function () {
            Employee.reloadOnUpdate = false
            assert.equal(employee.salary, 100000);
            assert.equal(employee.mySalary, 90000);
            assert.equal(employee.salaryInner, 80000);
            assert.equal(employee.salaryOther, 70000);
            comb.serial([
                employee.works.update.bind(employee.works, {salary:1000, salaryTwo:900, salaryInner:800, salaryThree:700}),
                function () {
                    assert.equal(employee.salary, 100000);
                    assert.equal(employee.mySalary, 90000);
                    assert.equal(employee.salaryInner, 80000);
                    assert.equal(employee.salaryOther, 70000);
                },
                employee.update.bind(employee, {firstName:"bob"}, {reload:false})
            ]).then(function () {
                    assert.equal(employee.salary, 100000);
                    assert.equal(employee.mySalary, 90000);
                    assert.equal(employee.salaryInner, 80000);
                    assert.equal(employee.salaryOther, 70000);
                    Employee.reloadOnUpdate = true;
                }).classic(next);
        }, next);
    });

    it.should("retrieve properties on reload", function (next) {
        var employee = new Employee({
            lastName:"last" + 1,
            firstName:"first" + 1,
            midInitial:"m",
            gender:gender[1 % 2],
            street:"Street " + 1,
            city:"City " + 1,
            works:{
                companyName:"Google",
                salary:100000,
                salaryTwo:90000,
                salaryInner:80000,
                salaryThree:70000
            }
        });
        employee.save().then(function () {
            assert.equal(employee.salary, 100000);
            assert.equal(employee.mySalary, 90000);
            assert.equal(employee.salaryInner, 80000);
            assert.equal(employee.salaryOther, 70000);
            comb.serial([
                employee.works.update.bind(employee.works, {salary:1000, salaryTwo:900, salaryInner:800, salaryThree:700}),
                function () {
                    assert.equal(employee.salary, 100000);
                    assert.equal(employee.mySalary, 90000);
                    assert.equal(employee.salaryInner, 80000);
                    assert.equal(employee.salaryOther, 70000);
                },
                employee.reload.bind(employee)
            ]).then(function () {
                    assert.equal(employee.salary, 1000);
                    assert.equal(employee.mySalary, 900);
                    assert.equal(employee.salaryInner, 800);
                    assert.equal(employee.salaryOther, 700);
                }).classic(next);
        }, next);
    });

    it.afterAll(function () {
        return helper.dropModels();
    });
}).as(module);

