
#Associations


##Supported Association types


* **oneToMany** : Foreign key in associated model's table points to this model's primary key. Each current model object can be associated with more than one associated model objects. Each associated model object can be associated with only one current model object.

* **manyToOne** : Foreign key in current model's table points to associated model's primary key. Each associated model object can be associated with more than one current model objects. Each current model object can be associated with only one associated model object.

* **oneToOne** : Similar to one_to_many in terms of foreign keys, but only one object is associated to the current object through the association. The methods created are similar to many_to_one, except that the one_to_one setter method saves the passed object.

* **manyToMany** : A join table is used that has a foreign key that points to this model's primary key and a foreign key that points to the associated model's primary key. Each current model object can be associated with many associated model objects, and each associated model object can be associated with many current model objects.</li>


##Options



* **model** : The associated class or its name. If not given, uses the association's name, which is singularized unless the type is `MANY_TO_ONE`  or `ONE_TO_ONE` . For example, suppose we have the tables father and child, father being oneToMany with children. Yo would not need to specify the model for the following.

```
//you would not need to specify model for this
patio.addModel("father").oneToMany("children");
patio.addModel("child").manyToOne("father");
```

You would for the following;

```
//it would default to the model myChild
patio.addModel("father").oneToMany("myChildren", {model : "child"});
//it would default to the model myBiologicalFather
patio.addModel("child").manyToOne("myBiologicalFather", {model : "father"});
```


* **query** : The conditions to use to filter the association, can be any argument passed to [filter](./patio_Dataset.html#filter).

```
//WHERE age > 10
patio.addModel("father")
    .oneToMany("children", {query : {age : {gt : 10}}})
    .oneToMany("femaleChildren", {query : {gender : "female"}})
    .oneToMany("maleChildren", {query : {gender : "male"}});
```

* **dataset** : A function that is called in the scope of the model and called with the model as the first argument. The function must return a dataset that can be used as the base for all dataset operations.**NOTE:** The dataset returned will have all options applied to it.

```
patio.addModel("father")
    .oneToMany("letterBChildren", {model : "child", dataset : function(){
        //called in the scope of the model instance
        return this.db.from("child").filter({fatherId : this.id, name : {like : "B%"}});
    }});
```
   
* **distinct** : Use the DISTINCT clause when selecting associated objects. See [discinct](./patio_Dataset.html#distinct)

```
patio.addModel("class").manyToMany("students", {distinct : "gpa"});
```

* **limit** : Limit the number of records to the provided value. Use an array with two elements for the value to specify a limit (first element) and an offset (second element). See [limit](./patio_Dataset.html#limit).

```
patio.addModel("class").manyToMany("students", {limit : 10});

patio.addModel("student").manyToMany("classes", {limit : [10, 20]});
```


* **order** : the column/s order the association dataset by. Can be a one or more columns. See [order](./patio_Dataset.html#order).

```
patio.addModel("class").manyToMany("students", {order : "gpa"});

patio.addModel("student").manyToMany("classes", {order : ["firstName", sql.lastName.desc()]});
```


* readOnly : Do not add a setter method (for `MANY_TO_ONE`  or `ONE_TO_ONE`  associations), or add/remove/removeAll methods (for `ONE_TO_MANY`  and `MANY_TO_MANY`  associations).

```
//Make students read only.
patio.addModel("class").manyToMany("students", {readOnly : true});
```
   
* **select** : the columns to select. Defaults to the associated class's tableName.* in a `MANY_TO_MANY`  association, which means it doesn't include the attributes from the join table. If you want to include the join table attributes, you can use this option, but beware that the join table attributes can clash with attributes from the model table, so you should alias any attributes that have the same name in both the join table and the associated table.

```
patio.addModel("class").manyToMany("students", {select : ["firstName", "lastName"]});
```

###ManyToOne additional options:

* **key** : foreignKey in current model's table that references associated model's primary key. Defaults to : "{tableName}Id". Can use an array of strings for a composite key association.

```
patio.addModel("biologicalFather").oneToMany("children");;

patio.addModel("child").manyToOne("biologicalFather");

```

However this table structure would not work.

```
biological_father       child
|--id <--------|        |--id
|--name        |        |--name
               |------> |--biological_father_key

```

So you would need to define your models like this.

```
patio.addModel("biologicalFather").oneToMany("children", {key : "biologicalFatherKey"});;

patio.addModel("child").manyToOne("biologicalFather", {key : "biologicalFatherKey"});
```
   
   
* **primaryKey** : column in the associated table that the **key** option references. Defaults to the primary key of the associated table. Can use an array of strings for a composite key association.

So, for the following table structure

```
biological_father       child
|--id                   |--id
|--name <------|        |--name
               |------> |--step_father_key
```

You would set up the models as the following

```

patio.addModel("stepFather").oneToMany("children", {key : "stepFatherKey", primaryKey : "name"});

patio.addModel("child").manyToOne("stepFather", {key : "stepFatherKey", primaryKey : "name"});
```

###OneToMany and OneToOne additional options:

* **key** : foreign key in associated model's table that references current model's primary key, as a symbol. Defaults to "{thisTableName}Id". Can use an array of columns for a composite key association. **For examples see the ManyToOne examples above**.


* primaryKey : column in the current table that **key** option references. Defaults to primary key of the current table. Can use an array of strings for a composite key association. **For examples see the ManyToOne examples above**.
        

###ManyToMany additional options:

* **joinTable** : name of table that includes the foreign keys to both the current model and the associated model. Defaults to the name of current model and name of associated model, pluralized, sorted, and camelized.

For example if you had a joinTable names students_classes you would have to define your models like this:

```
patio.camelize = true;
patio.addModel("class").manyToMany("students", {joinTable:"studentsClasses"});
patio.addModel("student").manyToMany("classes", {joinTable:"studentsClasses"});
```

* **leftKey/rightKey**
  * **leftKey** : foreign key in join table that points to current model's primary key. Defaults to :"{tableName}Id". Can use an array of strings for a composite key association.

  * **rightKey** : foreign key in join table that points to associated model's primary key. Defaults to Defaults to :"{associatedTableName}Id". Can use an array of strings for a composite key association.

Suppose you had a table structure like the following:

```
class              classes_students       students
|--id <----------> |--class_key       |-> |--id
|--semester        |--student_key <---|   |--first_name
|--name                                   |--last_name
|--subject                                |--gpa
|--description                            |--is_honors
|--graded                                 |--classYear
```

You would set up you models like the following:

```
patio.camelize = true;
patio.addModel("class").manyToMany("students", {leftKey:"classKey", rightKey:"studentKey"});
patio.addModel("student").manyToMany("classes", {leftKey:"studentKey", rightKey:"classKey"});
```
* **leftPrimaryKey/rightPrimaryKey**
  * **leftPrimaryKey** : column in current table that **leftKey** points to. Defaults to primary key of current table. Can use an array of strings for a composite key association.
  * **rightPrimaryKey** : column in associated table that **rightKey** points to. Defaults to primary key of the associated table. Can use an array of strings for a composite key association.

Suppose you had a table structure like the following:

```
class              classes_students       students
|--id              |--first_name_key <--> |--first_name
|--semester        |--last_name_key <---> |--last_name
|--name <--------> |--name_key            |--id
|--subject <-----> |--subject_key         |--gpa
|--description                            |--is_honors
|--graded                                 |--classYear
```

You would set up you models like the following:

```
patio.camelize = true;
patio.addModel("class")
    .manyToMany("students", {
        //use the composite key of name and subject
        leftPrimaryKey:["name", "subject"],
        leftKey:["nameKey", "subjectKey"],
        rightPrimaryKey:["firstName", "lastName"],
        rightKey:["firstNameKey", "lastNameKey"]
    });
patio.addModel("student")
    manyToMany("classes", {
        leftPrimaryKey:["firstName", "lastName"],
        leftKey:["firstNameKey", "lastNameKey"],
        rightPrimaryKey:["name", "subject"],
        rightKey:["nameKey", "subjectKey"],
    });
```


##Filter Block
You may also pass a function to the association to perform additional filtering on the dataset.


Assume the student/class relation ship defined above with the conventional keys and jointable

```
patio.addModel("class")
    .manyToMany("students")
    .manyToMany("aboveAverageStudents", {model:"student"}, function(ds) {
        return ds.filter({gpa:{gte:3.5}});
    })
    .manyToMany("averageStudents", {model:"student"}, function(ds) {
        return ds.filter({gpa:{between:[2.5, 3.5]}});
    })
    .manyToMany("belowAverageStudents", {model:"student"}, function(ds) {
        return ds.filter({gpa:{lt:2.5}});
    });

patio.addModel("student")
    .manyToMany("classes")
    .manyToMany("fallClasses", {model : "class"}, function(ds){
        return ds.filter({semester : "FALL"});
    })
    .manyToMany("sprintClasses", {model : "class"}, function(ds){
        return ds.filter({semester : "SPRING"});
    });
```

##[oneToMany](./patio_Model.html#.oneToMany)

One of the most common forms of associations. One to Many is the inverse of Many to one. One to Many often describes a parent child relationship, where the One To Many [Model](./patio_Model.html) is the parent, and the many to one model is the child.

For example consider a BiologicalFather and his children. The father can have many children, but a child can have only one Biological Father.

Assuming you have the following table structure:

```
\\biological_father       child
\\|--id <--------|        |--id
\\|--name        |        |--name
\\               |------> |--biological_father_id

//set up camelization so that properties can be camelcase but will be inserted
//snake case (i.e. 'biologicalFather' becomes 'biological_father').
patio.camelize = true;
DB.createTable("biologicalFather", function(){
    this.primaryKey("id");
    this.name(String);
});
DB.createTable("child", function(){
    this.primaryKey("id");
    this.name(String);
    this.foreignKey("biologicalFatherId", "biologicalFather", {key : "id"});
});
```

The table biological_father has four fathers in it. Each row in child has a bio_father_id that is a foreign key to biological father. Fred has 3 children(Bobby, Alice, and Susan) while Scott has 1 child Brad.


You could represent the OneToMany association as follows:

```

 var BiologicalFather = patio.addModel("biologicalFather").oneToMany("children")

     //define Child  model
var Child = patio.addModel("child").manyToOne("biologicalFather");

patio.syncModels().chain(function(){
 BiologicalFather.save([
           {name:"Fred", children:[
                   {name:"Bobby"},
                   {name:"Alice"},
                   {name:"Susan"}
           ]},
           {name:"Ben"},
           {name:"Bob"},
           {name:"Scott", children:[
                   {name:"Brad"}
           ]}
     ]);

}, errorHandler);
```

Above we created a BiologicalFather and Child model. The BiologicalFather has a static initializer(init) that sets up the oneToMany association with the Child. The Child also has a static initializer that sets up the ManyToOne association with the BiologicalFather. When saving [Models](./patio_Model.html)s that have associations in you can nest the associations directly.

You can query each model by:

```
//syncModels only needs to be performed once
patio.syncModels().chain(function(){
	Child.findById(1).chain(function(child){
    	//lazy association so it returns a promise.
	    child.biologicalFather.chain(function(father){
    	     //father.name === "fred"
	    }, errorHandler);
	});

	BiologicalFather.findById(1).chain(function(father){
    	//lazy association so it returns a promise.
	    father.children.chain(function(children){
    	    //children.length === 3
	    }, errorHandler);
	});
}, errorHandler);
```

Notice the models set up above are `LAZY` loaded meaning the associations are not loaded until the association is accessed. When working with lazy loaded associations a Promise will **always** be returned,even if the value has already been cached. The promise will be resolved with the association value/s.

An `EAGER` model and query would look like this:

```
var BiologicalFather = patio.addModel("biologicalFather").oneToMany("children", {fetchType : this.fetchType.EAGER});
var Child = patio.addModel("child").manyToOne("biologicalFather", {fetchType : this.fetchType.EAGER});

//sync the models
patio.syncModels().chain(funciton(){
     Child.findById(1).chain(function(child){
         var father = child.biologicalFather;
         //father.name === "fred"
         //father.children.length === 3
    }, errorHandler);
}, errorHandler);
```
When working with eager associations the eagerly loaded association will be fetched on the load of a model.

**Note:** one should choose wisely when `EAGER`ly loading associations as it can severly inhibit performance.



###[oneToOne](./patio_Model.html#.oneToOne)

Similar to `ONE_TO_MANY`  in terms of foreign keys, but only one object is associated to the current object through the association. The methods created are similar to `MANY_TO_ONE` , except that the `ONE_TO_ONE`  setter method saves the passed object.

The reciprocal association to a ONE TO ONE is a MANY TO ONE. This be because in a one to one relationship the MANY TO ONE model's table contains the foreign key that references the ONE TO ONE (the parent) models table.


For example consider the following schema for states and their capitals.

```
//state               capital
//|--id <--------|    |--id
//|--name        |    |--population
//|--population  |    |--name
//|--founded     |    |--founded
//|--climate     |--->|--state_id
//|--description

db.createTable("state", function(){
    this.primaryKey("id");
    this.name(String)
    this.population("integer");
    this.founded(Date);
    this.climate(String);
    this.description("text");
});
db.createTable("capital", function(){
    this.primaryKey("id");
    this.population("integer");
    this.name(String);
    this.founded(Date);
    this.foreignKey("stateId", "state", {key:"id"});
});
```

In the above state and capital tables the state would contian the `ONE_TO_ONE`  relationship and captial would contain a MANY TO ONE relationship with state because captial contains a foreign key to state's id.

The models for the above schema would be declared as follows:
```
var State = patio.addModel("state").oneToOne("capital");
var Capital = patio.addModel("capital").manyToOne("state");
```

To insert data into state or capital you could create each individually and set the properties manually:

```
//BE SURE TO SYNC YOUR MODELS BEFORE USING THEM
var comb = require("comb"),
    when = comb.when;
var nebraska = new State({
	name:"Nebraska",
    population:1796619,
    founded:new Date(1867, 2, 4)
    climate:"continental",
});
var texas = new State({
    name:"Texas",
    population:25674681,
    founded:new Date(1845, 11, 29)
});    
var lincoln = new Capital({
	name:"Lincoln",
    founded:new Date(1856, 0, 1),
    population:258379
});
var austin = new Capital({
    name:"Austin",
    founded:new Date(1835, 0, 1),
    population:790390
});
when(
	nebraska.save(),
	texas.save(),
	lincoln.save(),
	austin.save()
).chain(function(){
	nebraska.capital = lincoln;
	texas.capital = austin;
	when(nebraska.save(), texas.save()).chain(function(){
	   //states and capitals are now saved
	   Capital.forEach(function(capital){
		   //returning the state promise from here will prevent the forEach from resolving until the 
		   //state hash been fetched
	   		return capital.state.chain(function(state){
	   			console.log("%s's capital is %s, state.name, capital.name);
	   		});
	   });
	}, errorHandler);
}, errorHandler);
```


The above example is pretty verbose, so `patio` allows you to nest the assignment of assoications on the creation of a model.

```
//BE SURE TO SYNC YOUR MODELS BEFORE USING THEM
var comb = require("comb"),
    when = comb.when;
var nebraska = new State({
	name:"Nebraska",
    population:1796619,
    founded:new Date(1867, 2, 4),
    climate:"continental",
    //capital will be automatically converted to a Capital instance
    capital : {
		name:"Lincoln",
	    founded:new Date(1856, 0, 1),
    	population:258379
	}
});
var texas = new State({
    name:"Texas",
    population:25674681,
    founded:new Date(1845, 11, 29),
   //capital will be automatically converted to a Capital instance
    capital : {
	    name:"Austin",
    	founded:new Date(1835, 0, 1),
	    population:790390
	}
});    
when(
	nebraska.save(),
	texas.save(),
).chain(function(){
	Capital.forEach(function(capital){
		//returning the state promise from here will prevent the forEach from resolving until the 
		//state hash been fetched
		return capital.state.chain(function(state){
			console.log("%s's capital is %s, state.name, capital.name);
		});
	});	
}, errorHandler);

```
   
To query:

```
State.order("name").forEach(function(state){
    //if you return a promise here it will prevent the foreach from
    //resolving until all inner processing has finished.
        
    return state.capital.chain(function(capital){
            console.log(comb.string.format("%s's capital is %s.", state.name, capital.name));
    })
});

Capital.order("name").forEach(function(capital){
    //if you return a promise here it will prevent the foreach from
    //resolving until all inner processing has finished.
    return capital.state.chain(function(state){
            console.log(comb.string.format("%s is the capital of %s.", capital.name, state.name));
    })
});
```
**Note:** when retrieving the state and captial properties they return a Promise because the properties are
lazy loaded. To change this set the **fetchType** to **EAGER**.


###[manyToMany](./patio_Model.html#.manyToMany)

A join table is used that has a foreign key that points to this model's primary key and a foreign key that
points to the associated model's primary key. Each current model object can be associated with many associated
model objects, and each associated model object can be associated with many current model objects.


Consider the following table schema

```
//class              classes_students       students
//|--id <----------> |--class_id        |-> |--id
//|--semester        |--student_id <----|   |--first_name
//|--name                                   |--last_name
//|--subject                                |--gpa
//|--description                            |--is_honors
//|--graded                                 |--classYear
    
db.createTable("class", function() {
    this.primaryKey("id");
    this.semester("char", {size:10});
    this.name(String);
    this.subject(String);
    this.description("text");
    this.graded(Boolean, {"default":true});
});
db.createTable("student", function() {
    this.primaryKey("id");
    this.firstName(String);
    this.lastName(String);
    //GPA
    this.gpa(sql.Decimal, {size:[1, 3], "default":0.0});
    //Honors Program?
    this.isHonors(Boolean, {"default":false});
    //freshman, sophmore, junior, or senior
    this.classYear("char");
});
//Join table    
db.createTable("classes_students", function() {
    this.foreignKey("studentId", "student", {key:"id"});
    this.foreignKey("classId", "class", {key:"id"});
});
```

**Notice** the join tables name is the plural form of each tables name in alphabetical order. In order for patio
to correctly guess the join table you must follow this convention. Otherwise you can specify the **joinTable**
option when creating the associations.
    
Standard model definition:
```
var Class = patio.addModel("class")
    .manyToMany("students", {fetchType:this.fetchType.EAGER, order:[sql.firstName.desc(), sql.lastName.desc()]})
    //custom filters, notice the specification of the model property.
    .manyToMany("aboveAverageStudents", {model:"student"}, function(ds) {
              return ds.filter({gpa:{gte:3.5}});
    })
    .manyToMany("averageStudents", {model:"student"}, function(ds) {
        return ds.filter({gpa:{between:[2.5, 3.5]}});
    })
    .manyToMany("belowAverageStudents", {model:"student"}, function(ds) {
        return ds.filter({gpa:{lt:2.5}});
    });
var Student = patio.addModel("student", {

    instance:{
      enroll:function(clas) {
        if (comb.isArray(clas)) {
          return this.addClasses(clas);
        } else {
          return this.addClass(clas);
        }
      }
    }
}).manyToMany("classes", {fetchType:this.fetchType.EAGER, order:sql.name.desc()});
```

In the above declarations both models fetch the default classes/students EAGERLY meaning that when
referencing the associations it **will not** return a Promise.


**Note:** The **aboveAverageStudents**, **averageStudents**, and **belowAverageStudents** all use a custom
filter to create the association.

To save items using the associations you can insert items individually
```
Class.save([
    {
      semester:"FALL",
      name:"Intro To JavaScript",
      subject:"Javascript!!!!",
      description:"This class will teach you about javascript's many uses!!!"
    },
    {
      semester:"FALL",
      name:"Pricipals Of Programming Languages",
      subject:"Computer Science",
      description:"Definition of programming languages. Global properties of algorithmic languages including "
        + "scope of declaration, storage allocation, grouping of statements, binding time. Subroutines, "
        + "coroutines and tasks. Comparison of several languages."
    },
    {
      semester:"FALL",
      name:"Theory Of Computation",
      subject:"Computer Science",
      description:"The course is intended to introduce the students to the theory of computation in a fashion "
        + "that emphasizes breadth and away from detailed analysis found in a normal undergraduate automata "
        + "course. The topics covered in the course include methods of proofs, finite automata, non-determinism,"
        + " regular expressions, context-free grammars, pushdown automata, no-context free languages, "
        + "Church-Turing Thesis, decidability, reducibility, and space and time complexity.."
    },
    {
      semester:"SPRING",
      name:"Compiler Construction",
      subject:"Computer Science",
      description:"Assemblers, interpreters and compilers. Compilation of simple expressions and statements. "
        + "Analysis of regular expressions. Organization of a compiler, including compile-time and run-time "
        + "symbol tables, lexical scan, syntax scan, object code generation and error diagnostics."
    }
  ]);
 Student.save([
    {
      firstName:"Bob",
      lastName:"Yukon",
      gpa:3.689,
      classYear:"Senior"
    },
    {
      firstName:"Greg",
      lastName:"Horn",
      gpa:3.689,
      classYear:"Sohpmore"
    },

    {
      firstName:"Sara",
      lastName:"Malloc",
      gpa:4.0,
      classYear:"Junior"
    },
    {
      firstName:"John",
      lastName:"Favre",
      gpa:2.867,
      classYear:"Junior"
    },
    {
      firstName:"Kim",
      lastName:"Bim",
      gpa:2.24,
      classYear:"Senior"
    },
    {
      firstName:"Alex",
      lastName:"Young",
      gpa:1.9,
      classYear:"Freshman"
    }
 ]);
```

You can manually associate them by assigning the associations.

```
//Retrieve All classes and students
comb.when(Class.order("name").all(), Student.order("firstName", "lastName").all()).chain(function(results) {
    var classes = results[0], students = results[1];
    students.map(function(student, i) {
        student.enroll(i === 0 ? classes : classes.slice(1));
    });
});
```

Or you can save a class or student with students or classes respectively.
```
Student.save({
    firstName:"Zach",
    lastName:"Igor",
    gpa:2.754,
    classYear:"Sophmore",
    //each class will be automatically converted to a Class object.
    classes:[
        {
            semester:"FALL",
            name:"Compiler Construction 2",
            subject:"Computer Science"
        },
        {
            semester:"FALL",
            name:"Operating Systems",
            subject:"Computer Science"
        }
    ]
});
```

To query MANY TO MANY associations:

```
var comb = require("comb"),
    when = comb.when,
	format = comb.string.format;

//helper function to format a student to a string
var createStudentArrayString = function(students){
   return students.map(function(student) {
            return format("%s %s", student.firstName, student.lastName);
         }).join("\n\t-");
};

//print the results
Students.order("firstName", "lastName").forEach(function(student) {
    var classes = student.classes;
    var classStr = !classes.length ? " no classes!" : "\n\t-" + classes.map(function(clas) {
                              return clas.name;
                      }).join("\n\t-");
    console.log("%s %s is enrolled in %s", student.firstName, student.lastName, classStr);
});
Class.order("name").forEach(function(cls) {
    //print out the students enrolled
    console.log('"%s" has the following students enrolled: \n\t-%s', cls.name, createStudentArrayString(cls.students)));

     //Load the LAZY loaded aboveAverageStudents, averageStudents and belowAverageStudents students.
     //The for each will not resolve the final promise until the when promise has
     //resolved.
    return when(cls.aboveAverageStudents, cls.averageStudents, cls.belowAverageStudents).chain(function(res) {
      	var aboveAverage = createStudentArrayString(res[0]),
        	average = createStudentArrayString(res[1]);
            belowAverage = createStudentArrayString(res[2]);

      	console.log('"%s" has the following above average students enrolled: \n\t-%s', cls.name, aboveAverage);
      	console.log('"%s" has the following average students enrolled: \n\t-%s', cls.name, average);
      	console.log('"%s" has the following below average students enrolled: \n\t-%s', cls.name, belowAverage);
    }, errorHandler);  
});
```

