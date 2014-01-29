#Models

Models are an optional feature in patio that can be extended to encapsulate, query, and associate tables.

When defining a model it is assumed that the database table already exists. So before defining a model you must create the table/s that the model requires to function including associations. To create a model you must [connect](./patio.html#connect) to a database.

An example model definition flow.

```
var comb = require("comb"),
    format = comb.string.format,
	patio = require("patio");
patio.camelize = true;

//if you want logging
patio.configureLogging();

//disconnect and error callback helpers
var disconnect = patio.disconnect.bind(patio);
var disconnectError = function(err) {
    patio.logError(err);
    patio.disconnect();
};

//create your DB
var DB = patio.connect("mysql://test:testpass@localhost:3306/sandbox");

//create an initial model. It cannot be used until it is synced. 
//To sync call User.sync or patio.syncModels is called
var User = patio.addModel("user");

var createSchema = function(){
	return DB.forceCreateTable("user", function(){
                this.primaryKey("id");
                this.firstName(String)
                this.lastName(String);
                this.password(String);
                this.dateOfBirth(Date);
                this.created(sql.TimeStamp);
                this.updated(sql.DateTime);
	});
};

//connect and create schema
connectAndCreateSchema()
    .chain(function(){
        //sync the model so it can be used
	    return patio.syncModels().chain(function(){
	         var myUser = new User({
    	         firstName : "Bob",
        	     lastName : "Yukon",
            	 password : "password",
	             dateOfBirth : new Date(1980, 8, 29)
    	     });
        	//save the user
	        return myUser.save().chain(function(user){
    	        console.log(format("%s %s's id is %d", user.firstName, user.lastName, user.id));
	        });
        });
}).chain(disconnect, disconnectError);
```
The flow for the above example is as follows:
	
 * Connects to a database and creates the "user" table.</li>
 * Sync the models. This gets the relevant table information from the "user" table
 * Create a new User
   * Save the user
   * Print out some user details, at this point the user is saved in the database
   * disconnect from the database
 * The final output should be "BobYukon's id is 1".

##Options

Models some options that allow for the customization of the way a model be haves when interacting
with the database.

* **typecastOnLoad** : Defaults to true. Set to false to prevent properties from being type casted when loaded from the database. See [patio.Database.typecastValue](./patio_Database.html#typecastValue)

```
patio.addModel("user", {
    static : {
        //override default
        typecastOnLoad : false
    }
});
```

* **typecastOnAssignment** : Defaults to true. Set to false to prevent properties from being type casted when set by something other than the return values from the database. See [patio.Database.typecastValue](./patio_Database.html#typecastValue)

```
patio.addModel("user", {
    static : {
        //override default
        typecastOnAssignment : false
    }
});
```
So the following would not be typecasted.

```                        
var myUser = new User();
myUser.updated = new Date(2004, 1, 1, 12, 12, 12); //would not be auto converted to a patio.sql.DateTime
myUser.updated = "2004-02-01 12:12:12" //would not be auto converted to a patio.sql.DateTime
```

* **typecastEmptyStringToNull** : Defaults to true. Set to false to prevent empty strings from being typecasted to null.

```
patio.addModel("user", {
    static : {
        //override default
        typecastEmptyStringToNull : false
    }
});
```                  
* **raiseOnTypecastError** : Defaults to true. Set to false to prevent errors thrown while type casting a value from being propogated. **USE WITH CARE**

```
patio.addModel("user", {
    static : {
        //override default
        raiseOnTypecastError : false
    }
});
```
* **useTransactions** :  Defaults to true. Set to false to prevent models from using transactions when saving, deleting, or updating. This applies to the model associations also.

```                    
patio.addModel("user", {
    static : {
        //override default
        useTransactions : false
    }
});
```

* **identifierOutputMethod** : Defaults to null. Set this to override the Dataset default method of converting identifiers returned from the database. See [patio.Dataset.identifierOutputMethod](./patio_Dataset.html#.identifierOutputMethod) 

```
patio.addModel("user", {
    static : {
        //override default
        identifierOutputMethod : "camelize"
    }
});
```

* **identifierInputMethod** : Defaults to null. Set this to override the Dataset default method of converting identifiers when sending them to the database. See [patio.Dataset.identifierInputMethod](./patio_Dataset.html#.identifierInputMethod).

```
patio.addModel("user", {
    static : {
        //override default
        identifierInputMethod : "underscore"
    }
});
```

* **camelize** : Defaults to null. Set this to true force this particular model's identifiers to be underscored when sent to the database and camelized when returned. This **WILL** override `patio.camelize`. **USE WITH CARE**

```
patio.addModel("user", {
    static : {
        //override default
        camelize : true
    }
});
```

* **underscore** : Defaults to null. Set this to force this particular model's identifiers to be camelized when sent to the database and underscored when returned. This **WILL** override `patio.underscore`. **USE WITH CARE**

```
patio.addModel("user", {
    static : {
        //override default
        underscore : true
    }
});
```

* **reloadOnSave** : Defaults to true. Set this to false to prevent the models properties from being reloaded from the database after a save operation. **Note**: If you set this to false and you have columns that have default values in the database and they are not explictly set they will **NOT** be loaded

```
patio.addModel("user", {
    static : {
        //override default
        reloadOnSave : false
    }
});
```

* **reloadOnUpdate** : Defaults to true. Set this to false to prevent the model's properties from being reloaded from the database when a model is updated. **Note**: If you set this to false and you have columns that have default values in the database and they are not explictly set they will **NOT** be refreshed.

```
patio.addModel("user", {
    static : {
        //override default
        reloadOnUpdate : false
    }
});
```                    

##Creating a model
To create a Model class to use within your code you use the [patio.addModel](.patio.html#addModel) method. 

```
var User = patio.addModel("user")

//you must sync the model before using it 
User.sync().chain(function(User){
    var myUser = new User({
        firstName : "Bob",
        lastName : "Yukon",
        password : "password",
        dateOfBirth : new Date(1980, 8, 29)
    });
    return myUser.save().chain(function(){
        console.log(format("%s %s was created at %s", myUser.firstName, myUser.lastName, myUser.created.toString()));
        console.log(format("%s %s's id is %d", myUser.firstName, myUser.lastName, myUser.id));
    });
}).chain(disconnect, disconnectError);
```
                
You may also use a dataset when adding a model. You might use this if you are using multiple databases. Or want to use a custom query as the base for a particular model.

```
var DB1 = patio.createConnection("my://connection/string");
var DB2 = patio.createConnection("my://connection/string2");
//user table in db1
var User1 = patio.addModel(DB1.from("user"));
//user table in db2
var User2 = patio.addModel(DB2.from("user"));
patio.syncModels().chain(function(User1,User2){
    var myUser1 = new User1({
        firstName : "Bob1",
        lastName : "Yukon1",
        password : "password",
        dateOfBirth : new Date(1980, 8, 29)
    });
    var myUser2 = new User2({
        firstName : "Bob2",
        lastName : "Yukon2",
        password : "password",
        dateOfBirth : new Date(1980, 8, 29)
    });
    return comb.when(myUser1.save(), myUser2.save()).chain(function(saved){
         console.log(format("%s %s was created at %s", myUser1.firstName, myUser1.lastName, myUser1.created.toString()));
         console.log(format("%s %s's id is %d", myUser1.firstName, myUser1.lastName, myUser1.id));

         console.log(format("%s %s was created at %s", myUser2.firstName, myUser2.lastName,myUser2.created.toString()));
         console.log(format("%s %s's id is %d", myUser2.firstName, myUser2.lastName, myUser2.id));
    });
});
```

##Custom setters and getters

###Setters

patio creates setters and getters for each column in the database if you want alter the value of a particular property before its set on the model you can use a custom setter.

For example if you wanted to ensure proper case and first and last name of a user:

```
var User = patio.addModel("user", {
    instance : {
        _setFirstName : function(firstName){
            return firstName.charAt(0).toUpperCase() + firstName.substr(1);
        },

        _setLastName : function(lastName){
            return lastName.charAt(0).toUpperCase() + lastName.substr(1);
        }
    }
});

patio.syncModels().chain(function(User){
    var myUser = new User({
        firstName : "bob",
        lastName : "yukon"
    });
    console.log(myUser.firstName); //Bob
    console.log(myUser.lastName);  //Yukon
});
```

###Getters

Custom getters can be used to change values returned from the database but not alter the value when persisting. 

For example if you wanted to return a value as an array but persist as a string you could do the following.

```
var User = patio.addModel("user", {
	instance : {
		_getRoles : function(roles){
			return roles.split(",");
		}
	}
});

patio.syncModels().chain(function(User){
    var myUser = new User({
        firstName : "bob",
        lastName : "yukon",
        roles : "admin,user,groupAdmin"
    });
    console.log(myUser.roles); //['admin', 'user','groupAdmin'];
});
```

You can also use the getters/setters in tandem.

Lets take the getters example from before but use a setter also


```
var User = patio.addModel("user", {
	instance : {
		_setRoles : function(roles){
			return roles.join(",");
		},

	
		_getRoles : function(roles){
			return roles.split(",");
		}
	}
});

patio.syncModels().chain(function(User){
    var myUser = new User({
        firstName : "bob",
        lastName : "yukon",
        roles : ["admin","user","groupAdmin"];
    });
    console.log(myUser.roles); //['admin', 'user','groupAdmin'];
    //INSERT INTO `user` (`first_name`, `last_name`, `roles`) VALUES ('bob', 'yukon', 'admin,user,groupAdmin')
    return myUser.save();
});
```


##Model hooks

Each model has the following hooks

* pre
  * **save<** : called right before the model is saved to the database
  * **update** : called right before the model is updated
  * **remove** : called right before a model is deleted
  * **load** : called right before a model is loaded with values from the database
* post
  * **save<** : called right after the model is saved to the database
  * **update** : called right after the model is updated
  * **remove** : called right after a model is deleted
  * **load** : called right after a model is loaded with values from the database

```
var User = patio.addModel("user", {
    pre:{
        "save":function(next){
            console.log("pre save!!!")
            next();
        },

        "remove" : function(next){
            console.log("pre remove!!!")
            next();
        }
    },

    post:{
        "save":function(next){
            console.log("post save!!!")
            next();
        },

        "remove" : function(next){
            console.log("post remove!!!")
            next();
        }
    },
    instance:{
        _setFirstName:function(firstName){
            return firstName.charAt(0).toUpperCase() + firstName.substr(1);
        },

        _setLastName:function(lastName){
            return lastName.charAt(0).toUpperCase() + lastName.substr(1);
        }
    }
 });
 ```
 
## Using a model

If you define a model you can either use the Models constructor directly.

```
//define the model
var User = patio.addModel("user");

patio.syncModels(function(err){
   if(err){
       console.log(err.stack);
   }else{
       var user = new User();
   }
})

```

or you can use `patio.getModel`

```

patio.addModel("user");

patio.syncModels(function(err){
   if(err){
       console.log(err.stack);
   }else{       
       var User = patio.getModel("user");
       var user = new User();
   }
})


```

###Mutli Database Models

If you are working with multiple databases and your model's table is not in the [patio.defaultDatabase]("./patio.html#defaultDatabase") (the first database you connected to) then you will need to pass in the database the model's table is in.
    
```    
var DB1 = patio.createConnection("my://connection/string");
var DB2 = patio.createConnection("my://connection/string2");
//user table in db1
var User1 = patio.addModel(DB1.from("user"));
//user table in db2
var User2 = patio.addModel(DB2.from("user"));
patio.syncModels(function(err){
   if(err){
      console.log(err.stack);
   }else{
       var user1 = new User1(), 
           user2 = new User2();
   }
});
```

###Creating

The static [save](./patio_Model.html#.save) can be used for saving a group of models at once. **Note** this method is not any more efficient than creating a model using new, just less verbose.

```
var Student = patio.getModel("student");
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
          classYear:"Sophomore"
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
 ]).chain(function(users){
   //All users have been saved
 }, disconnectError);
```

When saving a group of models the save method will use a transaction unless the `useTransactions` property is set to false. You can manually override the useTransactions property by passing in an additional options parameter with a transaction value set to false.

```
var Student = patio.getModel("student");
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
          classYear:"Sophomore"
      }
 ], {transaction : false}).chain(function(users){
     //work with the users
 });
```

If you have an instance of a model then you can use the [save](./patio_Model.html#save) method on the instance of the model.

```
var myUser = new User({
     firstName : "Bob",
     lastName : "Yukon",
     password : "password",
     dateOfBirth : new Date(1980, 8, 29)
});
//save the user
myUser.save().chain(function(user){
    //the save is complete
}, disconnectError);
```

You can also pass in values into the save method to set before saving.

```            
var myUser = new User();
//save the user
myUser.save({
     firstName : "Bob",
     lastName : "Yukon",
     password : "password",
     dateOfBirth : new Date(1980, 8, 29)
 }).chain(function(user){
    //the save is complete
}, disconnectError);
```

You can also pass in an options object to override options such as using a transaction.

```
var myUser = new User();
//save the user
myUser.save({
     firstName : "Bob",
     lastName : "Yukon",
     password : "password",
     dateOfBirth : new Date(1980, 8, 29)
 }, {transaction : false}).chain(function(user){
    //the save is complete
}, disconnectError);
```


###Reading

The Model contains static methods for all of the datasets methods listed in [patio.Dataset.ACTION_METHODS](./patio_Dataset.html#.ACTION_METHODS) as well as all the methods listed in [patio.Dataset.QUERY_METHODS](./patio_Dataset.html#.QUERY_METHODS).

Some of the most commonly used methods are:

* [forEach](./patio_Dataset.html#forEach)

```
User.forEach(function(user){
   console.log(user.firstName);
});

//you may also return the result of another query(or any promise) from a forEach block, 
//this will prevent the forEach's promise from resolving until all actions that occured in the block have 
//resolved.
var Blog = patio.addModel("blog");
User.forEach(funciton(user){
   //create a blog for each user
   return new Blog({userId : user.id}).save();

}).chain(function(users){
   //all users and blogs have been saved
}, disconnectError);

```

* [map](./patio_Dataset.html#map)

```
User.map(function(user){
   return user.firstName;
}).chain(function(names){
    console.log("User names are %s", names);
}, disconnectError);
```

* [all](./patio_Dataset.html#all)

```
User.all().chain(function(users){
   console.log(users.length);
}, disconnectError);
```

* [filter](./patio_Dataset.html#filter)

```
//find all users where first names begin with bo case insensitive
User.filter({firstName : /^bo/i}).all().chain(function(){
}, disconnectError);
```

* [one](./patio_Dataset.html#one)

```
User.filter({id : 1}).one().chain(function(user){
   console.log("%d - %s %s", user.id, user.firstName, user.lastName);
}, disconnectError);
```

* [first](./patioDataset.html#first)

```
//SELECT * FROM user WHERE first_name = 'bob' ORDER BY last_name LIMIT 1
User.filter({firstName : "bob"}).order("lastName").first().chain(function(user){
   console.log("%d - %s %s", user.id, user.firstName, user.lastName);
}, disconnectError);
```

* [last](./patio_Dataset.html#last)

```
//SELECT * FROM user WHERE first_name = 'bob' ORDER BY last_name DESC LIMIT 1
User.filter({firstName : "bob"}).order("lastName").last().chain(function(user){
   console.log("%d - %s %s", user.id, user.firstName, user.lastName);
}, disconnectError);
```

* [isEmpty](./patio_Dataset.html#isEmpty)

```

User.isEmpty().chain(function(isEmpty){
   if(isEmpty){
      console.log("user table is empty");
   }else{
      console.log("user table is not empty");
   }
}, disconnectError);

```

###Updating


The static [update](./patio_Model.html#.update) can be used for updating a batch of models.

```
 //BEGIN
 //UPDATE `user` SET `password` = NULL
 //COMMIT
 User.update({password : null});
```

You can also pass in a query to limit the models that are updated. The filter can be anything that [filter](./patio_Dataset.html#filter) accepts.

```
User.update({password : null}, function(){
    return this.lastAccessed.lte(comb.yearsAgo(1));
 });

//same as
User.update({password : null}, {lastAccess : {lte : comb.yearsAgo(1)}});
```
To prevent default transaction behavior you can pass in an additional transaction option

```
User.update({password : null}, {lastAccess : {lte : comb.yearsAgo(1)}}, {transaction : false});
```

If you have an instance of a model and you want to update it you can use the [update](./patio_Model.html#update) instance method.

```
var updateUsers = User.forEach(function(user){
    //returning the promise from update will cause the forEach not to resolve
    //until all updates have completed
    return user.update({fullName : user.firstName + " " + user.lastName});
});
updateUsers.chain(function(){
    //updates finished
});
```

as with save you can pass in an options object to prevent default behavior such as transactions.

```
var updateUsers = User.forEach(function(user){
    //returning the promise from update will cause the forEach not to resolve
    //until all updates have completed
    return user.update({fullName : user.firstName + " " + user.lastName}, {transaction : false});
});
updateUsers.chain(function(){
    //updates finished
});
```


###Deleting

The static [remove](./patio_Model.html#.remove) can be used for removing a batch of models. **Note** this method is not anymore efficient just a convenience.

```
//remove all models
User.remove();
```
   
To limit the models removed you can pass in a query. The filter can be anything that [filter](./patio_Dataset.html#filter) accepts.

```
//remove models that start with m
User.remove({lastName : {like : "m%"}});`
```
   
The default behavior of remove is to load each model and call remove on it. If you wish to just do a mass delete and not load each model you can pass in an additional options object with a key called `load` set to false.
**Note** If you do this then the pre/post remove hooks will not be called.
         
```         
//mass remove models without loading them
User.remove(null, {load : false});
```   
   
To prevent the default transaction behavior pass in the transaction option.

```
//removing models, not using a transaction
User.remove(null, {transaction : false});
```

If you have an instance of a model and you want to remove it you can use the [remove](./patio_Model.remove) instance method.

```
User.forEach(function(user){
    return user.remove();
}).chain(function(){
    //removed
});
```

To prevent the default transaction behavior pass in the transaction option

```
User.forEach(function(user){
    return user.remove({transaction : false});
}).chain(function(){
    //removed
});
```