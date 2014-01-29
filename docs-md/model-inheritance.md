
#Model Inheritance



##[Class Table Inheritance](http://www.martinfowler.com/eaaCatalog/classTableInheritance.html)

Consider the following table model.

```
          employee
            - id
            - name (varchar)
            - kind (varchar)
     /                          \
 staff                        manager
   - id (fk employee)           - id (fk employee)
   - manager_id (fk manger)     - numStaff (number)
                                 |
                              executive
                                - id (fk manager)
```

* **employee**: This is the parent table of all employee instances.
* **staff**: Table that inherits from employee where and represents.
* **manager**: Another subclass of employee.
* **executive**: Subclass of manager that also inherits from employee through inhertiance

When setting up you tables the parent table should contain a String column that contains the "kind" of class it is. (i.e. employee, staff, manager, executive). This allows the plugin to return the proper instance type when querying the tables.

All other tables that inherit from employee should contain a foreign key to their direct super class that is the same name as the primary key of the parent table(**employee**). So, in the above example **staff** and **manager** both contain foreign keys to employee and **executive** contains a foreign key to **manager** and they are all named **id**.

To set up you models the base super class should contain the [ClassTableInheritancePlugin](./patio_plugins_ClassTableInheritance.html) plugin.

```
 var Employee = patio.addModel("employee", {
      plugins : [patio.plugins.ClassTableInheritancePlugin],
 }).configure({key : "kind"});
```

All sub classes should just inherit their super class

```
var Staff = patio.addModel("staff", Employee)
	.manyToOne("manager", {key : "managerId", fetchType : this.fetchType.EAGER});;

var Manager = patio.addModel("manager", Employee)
	.oneToMany("staff", {key : "managerId", fetchType : this.fetchType.EAGER});;

 ```

`Executive` inherits from `Manager`, and through inheritance will also receive the `oneToMany` relationship with staff

```
 var Executive = patio.addModel("executive",  Manager);
```

Working with models

```
var comb = require("comb"),
    when = comb.when;
when(
	new Employee({name:"Bob"}).save(),
    new Staff({name:"Greg"}).save(),
    new Manager({name:"Jane"}).save(),
    new Executive({name:"Sue"}).save()
).chain(function(){
      return Employee.all().chain(function(emps){
          var bob = emps[0], greg = emps[1], jane = emps[2], sue = emps[3];
          console.log(bob instanceof Employee); //true
          console.log(greg instanceof Employee);  //true
          console.log(greg instanceof Staff);  //true
          console.log(jane instanceof Employee);  //true
          console.log(jane instanceof Manager);  //true
          console.log(sue instanceof Employee);  //true
          console.log(sue instanceof Manager);  //true
          console.log(sue instanceof Executive);  //true
      });
 });
```
###Configuring

When setting up the [ClassTableInheritancePlugin](./patio_plugins_ClassTableInheritance.html) plugin you should include it as a plugin on the base model, and call configure in the `init` block of the Model.

**Options**

* **key** : The name of the column in the parent table that contains the name of the subclass that the row in the parent table represents.

```
var Employee = patio.addModel("employee", {
	plugins : [patio.plugins.ClassTableInheritancePlugin]
}).configure({key : "kind"});                       
```

* **keyCb** : A callback to invoke on on the key returned from the database. This is useful if you are working with other orms that save the keys differently.

```
var Employee = patio.addModel("employee", {
	plugins : [patio.plugins.ClassTableInheritancePlugin],
}).configure({keyCb : function(key){
	return key.toLowerCase();
}});                       
```

                    

