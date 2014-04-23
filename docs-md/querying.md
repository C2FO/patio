#Querying


##Reading objects

patio provides a few separate methods for retrieving objects from the database. The underlying
method is [forEach](./patio_Dataset.html#forEach), which interates each row as the [patio.Database](./patio_Database.html) provides it. However, while [forEach](./patio_Dataset.html#forEach) can and often is used directly, in many cases there is a more convenient retrieval method you can use.

**Note** For all examples below a generic error handler is used which could be defined as follows

```
var errorHandler = function(err){
   console.log(err.stack);
}
```

##Getting a dataset

To get a dataset use the [DB.from](./patio_Database.html#from) method.

```
var DB = patio.connect(<CONNECTION_URI>)
var User = DB.from("user");
```

##Retrieving a Single Object

###Using a Primary Key

The [findById](./patio_Model.html#.findById) is the easiest method to use to find a model instance by its primary key value:

```
// Find user with primary key (id) 1
User.findById(1).chain(function(user){
    // SELECT * FROM user WHERE id = 1
}, errorHandler);
```

**Note** If there is no record with the given primary key, the promise will resolve with null.
         
###[first](./patio_Dataset.html#first)

If you just want the first record in the dataset use [first](./patio_Dataset.html#first).

```
User.first().chain(function(user){
    // SELECT * FROM user LIMIT 1
}, errorHandler);
```

Any options you pass to first will be used as a filter:

```
User.first({name : 'Bob'}).chain(function(bob){
    // SELECT * FROM user WHERE (name = 'Bob') LIMIT 1
}, errorHandler);
var sql = patio.sql;
User.first(sql.name.like('B%')).chain(function(user){
    // SELECT * FROM user WHERE (name LIKE 'B%') LIMIT 1
}, errorHandler);
```
               
###[last](./patio_Dataset.html#last)
                        
If you want the last record in the dataset use [last](./patio_Dataset.html#last).

**Note:** last will throws an Error if there is no order on the dataset. Because otherwise it would provide the same record as first, and most users would find that confusing.

**Note:** that last is not necessarily going to give you the last record in the dataset unless you give the dataset an unambiguous order.

```
 User.order("name").last().chain(function(user){
     // SELECT * FROM user ORDER BY name DESC LIMIT 1
}, errorHandler);
```
###[get](./patio_Dataset.html#get)
      
Sometimes, instead of wanting an entire row, you only want the value of a specific column. For this [get](./patio_Dataset.html#get) is the method you want:

```
User.get("name").chain(function(name){
    // SELECT name FROM user LIMIT 1
}, errorHandler);
```

##Reading Multiple Records

###[stream](./patio_Dataset.html#stream)

This method allows you to stream records from a database. This is useful if you have too much data to process in memory.

```
User
    .stream()
    .on("data", function(record){
       //
    })
    .on("error", errorHandler)
    .on("end", function(){
        console.log("all done")
    });
```

Stream also supports `pause` and `resume`

```
var stream = User
    .stream()
    .on("data", function(record){
       stream.pause(); //you wont get anymore records until resume is called.

    })
    .on("error", errorHandler)
    .on("end", function(){
        console.log("all done")
    });
```


###[all](./patio_Dataset.html#all)

If you want an array of all of the rows associated with the dataset you should use the [all](./patio_Dataset.html#all) method:

```
User.all().chain(function(users){
    // SELECT * FROM user
}, errorHandlers);
```

###Array methods

[patio.Dataset](./patio_Dataset.html) has a few array like methods such as

 * [forEach](./patio_Dataset.html#forEach)
 * [map](./patio_Dataset.html#map)

```
// SELECT * FROM user
User.forEach(function(user){
    console.log(user.name);
});
```

If you return a promise from forEach then the promise from forEach will not resolve until each promise has resolved.

```
// SELECT * FROM user
var forEachPromise = User.forEach(function(user){
    if(user.isVerified){
        return user.update({isVerified : false});
    }
}).chain(function(){
    //all user updated
}, errorHandler);
```               
 
[map](./patio_Dataset.html#map)  is like [forEach](./patio_Dataset.html#forEach) except that whatever is returning from the block is what the promise is resolved with, as opposed to forEach which always resolves with the original 
rows.

```                  
// SELECT * FROM user
User.map(function(user){
    return user.name;
}).chain(function(userNames){
    console.log(userNames);
}, errorHandler);
```
                    
If you return a promise from the mapping function then the promise from map will not resolve until each promise has resolved.

```    
// SELECT * FROM user
var mapPromise = User.map(function(user){
    return Blogs.filter({userId : userId}).map(function(blog){
          return blog.title;
    });
}).chain(function(userBlogTitles){
    userBlogTitles.forEach(function(blogTitles){
        console.log(blogTitles);
    });
}, errorHandler);
```   
[map](./patio_Dataset.html#map) method also can take an arugment other than a function this is useful if you just want to select a list of column values.

```               
User.map("name").chain(function(userNames){
    // SELECT * FROM user
}, errorHandler);
```

###[selectMap](./patio_Dataset.html#selectMap)  

```
User.selectMap("name").chain(function(names){
    // SELECT name FROM user
}, errorHandler);
```

###[selectOrderMap](./patio_Dataset.html#selectOrderMap)

```
User.selectOrderMap("name").chain(function(){
    // SELECT name FROM user ORDER BY name
}, errorHandler);
```

###[toHash](./patio_Dataset.html#toHash)                  

patio makes it easy to take an SQL query and return it as a plain object, using the [toHash](./patio_Dataset.html) method:

```
User.toHash("name", "id").chain(function(nameIdMap){
    // SELECT * FROM user
    //{"Bob Yukon":1,"Suzy Yukon":2}
}, errorHandler);

```               

The [toHash](./patio_Dataset.html#toHash) method uses the first column as the key and the second column as the value. So if you swap the two arguments the hash will have its keys and values transposed:

```
User.toHash("id", "name").chain(function(nameIdMap){
    // SELECT * FROM user
    //{"1":"Bob Yukon","2":"Suzy Yukon"}
}, errorHandler);

```
If you provide only one argument to [toHash](./patio_Dataset.html#toHash), it uses the entire object or model object as the value:

```
Users.toHash("name").chain(function(){
    // SELECT * FROM user
    //{"Bob Yukon":{"name":"Bob Yukon"},"Suzy Yukon":{"name":"Suzy Yukon"}}
}):
```

###[selectHash](./patio_Dataset.html#selectHash)
**Note**: [toHash](./patio_Dataset.html#toHash) selects all columns. However, [selectHash](./patio_Dataset.html#selectHash) will only select the columns specified.

```
User.selectHash("name", "id").chain(function(){
    // SELECT name, id FROM user
    //{bob : 1, suzy : 2}
});
```                    

#Filtering

##[filter](./patio_Dataset.html#filter)

The [filter](./patio_Dataset.html#filter) method is one of the most used methods when querying a dataset. The filter method similar to [where](./patio_Dataset.html#where) except that it will apply the filter to the WHERE or HAVING clause depending on if a HAVING clause should be used.


###Filtering With Objects

The most common format for providing filters is via an object. In general, patio treats conditions specified with an object as equality or inclusion. What type of condition is used depends on the values in the object.

```
// SELECT * FROM user WHERE id = 1
User.filter({id : 1})
 
// SELECT * FROM user WHERE name = 'bob' 
User.filter({name : 'bob'})
```


For arrays, patio uses the IN operator.

```                
// SELECT * FROM user WHERE id IN (1, 2)
User.filter({id : [1, 2]})
```

For datasets, patio uses the IN operator with a subselect:

```
// SELECT * FROM user WHERE id IN (SELECT user_id FROM blog);
User.filter({id : Blog.select("userId")});
```
                
For boolean values such as null, true, and false, patio uses the IS operator:

```                
// SELECT * FROM user WHERE name IS NULL
User.filter({name : null}) 
```

For RegExp, patio uses an SQL regular expression. Note that this is probably only supported onPostgreSQL and MySQL.

```
// SELECT * FROM user WHERE name ~ 'Bo$'
User.filter({name : /Bo$/});
```              

If there are multiple arguments in the hash, the filters are `AND`ed together:

```
//SELECT * FROM user WHERE id IN (1, 2, 3) AND name ~ 'Bo$'
 User.filter({id : [1,2,3], name : /Bo$/});
``` 
 
This works the same as if you used two separate filter calls:

```
 // SELECT * FROM user WHERE id IN (1, 2, 3) AND name ~ 'Bo$'
 User.filter({id : [1,2,3]}).filter({name : /Bo$/});
```   

If you nest hashes for a top level key then each condition will be applied to the key. This can often be used inplace of a filter block.

```
 // SELECT * FROM user WHERE ((name ~ 'ob$') AND (name >= 'A') AND (name <= 'Z'))
 User.filter({name : {like : /ob$/, between : ["A", "Z"]}});
 
  // SELECT * FROM user WHERE ((name ~ 'ob$') AND (name >= 'A') AND (name <= 'Z'))
 User.filter(function(){
    this.name.like(/ob$/).and(this.name.between("A", "Z"));
 });
```                

###Array of Two Element Arrays

If you use an array of two element arrays, it is treated as an Object. The only advantage to using an array of two element arrays is that it allows you to use values other than strings for keys, so you can do:

```
 // SELECT * FROM user WHERE name ~ 'oB$' AND name ~ '^Bo'
 User.filter([["name", /oB$/], [sql.name, /^Bo/]]);
```

###Filter Blocks

Functions can also be provided to the filter method. Functions act as a "virtual" filter.   

```                
 // SELECT * FROM user WHERE id > 5
 User.filter(function(){
    return this.id.gt(5)
 });
```   

If you provde both regular arguments and a function the results will be ANDed together.

```
// SELECT * FROM user WHERE name >= 'K' AND name <= 'M' AND id > 5                
User.filter({name : {between : ['K', 'M']}}, function(){
    return this.id.gt(5);
}); 
```

###Strings

If you have a Boolean column in the database you can provide just the column name.

```
 // SELECT * FROM user WHERE is_active
 User.filter("isActive");
```

**Note:** if you want the literal representation of string you must use the (literal)[./patio_sql.html#.literal] method.

```
 // SELECT * FROM user WHERE name < 'A'
 User.filter(sql.literal("name < 'A'"));
```

###Expressions
                
patio SQL expressions are instances of subclasses of [Epression][./patio_sql_Expression.html].

```
//SELECT * FROM user WHERE name LIKE 'B%'
User.filter(sql.name.like('B%'));
```

In this case [patio.sql.StringExpression](./patio_sql_StringExpression.html).[like](./patio_sql_StringExpression.html#like) returns a [patio.sql.BooleanExpression](./patio_sql_BooleanExpression.html) object, which is used directly in the filter.

You can use the sql methodMissing feature to create arbitrary complex expression.

```                
// SELECT * FROM user WHERE name LIKE 'B%' AND (b = 1 OR c != 3)
User.filter(sql.name.like('B%').and(sql.b.eq(1).or(sql.c.neq(3))));
```

You can combine these expression operators with functions:

```
// SELECT * FROM user WHERE ((a <= 1) OR NOT b(c) OR NOT d)
User.filter(function(){
    return this.a.gt(1).and(this.b("c").and(this.d)).not();
});
```
  
###Strings with Placeholders
                
patio also supports place holder strings.

```
// SELECT * FROM user WHERE name LIKE 'B%'
User.filter("name LIKE ?", 'B%')

```

This is the most common type of placeholder, where each question mark is substituted with the corresponding argument.

```
// SELECT * FROM user WHERE name LIKE 'B%' AND id = 1
User.filter("name LIKE ? AND id = ?", 'B%', 1)
```

You can also use named placeholders with an object, where the named placeholders use `{}` that contain the placeholder key name

```                
//SELECT * FROM user WHERE name LIKE 'B%' AND id = 1
User.filter("name LIKE {name} AND id = {id}", {name : 'B%', id : 1});
```

###Literal Strings

You can also provide a literal string using the [literal](./patio_sql.html#.literal)

```
// SELECT * FROM user WHERE id = 2
User.filter(sql.literal("id = 2"))
```

However, if you are using any untrusted input, you should use placeholders. In general, unless you are hardcoding values in the strings, you should use placeholders. You should never pass a string that has been built using concatenation becuase it can lead to SQL injection.

```
//id is some user input

 User.filter("id = " + id) //id could be anything so dont do it!
 User.filter("id = ?", id) //Do this as patio will escape it
 User.filter({ id : id}) // Best solution!
```

###Inverting filters               

You can use the [invert](.patio_Dataset.html#invert) method.

```
// SELECT * FROM user WHERE id != 5
User.filter({id : 5}).invert(); 

//OR

 // SELECT * FROM user WHERE id != 5
User.filter({id : {neq : 5}});
```

**NOTE:** the [invert](./patio_Dataset.html#invert) method inverts the entire filter!

```
// SELECT * FROM user WHERE id != 5 OR name <= 'A'
 User.filter({id : 5}).filter(function(){ 
    return this.name.gt('A');
}).invert();
```

###Excluding filters
You can use [exclude](./patio_Dataset.html#exclude) to invert only specific filters:

```
// SELECT * FROM user WHERE id != 5
User.exclude({id : 5});

// SELECT * FROM user WHERE id = 5 OR name <= 'A' 
User.filter({id : 5}).exclude(function(){ 
    return this.name.gt('A')
});
```

So to do a NOT IN with an array:

```
// SELECT * FROM user WHERE id NOT IN (1, 2)
User.exclude({id : [1, 2]});
```
Or to use the NOT LIKE operator:

```
// SELECT * FROM user WHERE name NOT LIKE '%o%'
User.exclude(sql.name.like('%o%')) 
```

###Removing
           
To remove all existing filters, use the [unfiltered](./patio_Dataset.html#unfiltered) method:

```
 // SELECT * FROM user
 User.filter({id : 1}).unfiltered();
```

##Ordering
                
To add order to an SQL statement use the [order](./patio_Dataset.html#order) method.

```
 // SELECT * FROM user ORDER BY id
 User.order("id");
```

You can also provide multiple columns to the order method.

```
 // SELECT * FROM album ORDER BY user_id, id
 User.order("userId", "id");
```
**Note:** order replaces any existing order

```
 User.order("id").order("name");
 // SELECT * FROM user ORDER BY name
```

If you want to append a column to the existing order use the [orderAppend](./patio_Dataset.html#orderAppend) method.

```
 // SELECT * FROM user ORDER BY id, name
 User.order("id").orderAppend("name");
```

If you want to prepend a column to the existing order use the [orderPrepend](./patio_Dataset.html#orderPrepend) method.

```
 User.order("id").orderPrepend("name");
 // SELECT * FROM user ORDER BY name, id
```

###Reversing                

To reverse the order of a SQL query use the [reverse](./patio_Dataset.html#reverse) method.

```
 // SELECT FROM user ORDER BY id DESC
 User.order("id").reverse();
```

You can also use the [desc](./patio_sql_OrderedMethods.html#desc) method.

```
 // SELECT FROM user ORDER BY id DESC
 User.order(sql.id.desc());
```

This allows for finer grained control of the ordering of columns.

```
 // SELECT FROM user ORDER BY name, id DESC
 User.order("name", sql.id.desc());
```

###Removing

To remove ordering use the [unordered](./patio_Dataset.html#unordered) method:


```
 User.order("name").unordered();
 // SELECT * FROM user
```

##Selecting columns

To only return certain columns use the [select](./patio_Dataset.html#select) method.

```
// SELECT id, name FROM user
User.select("id", "name");
```

**NOTE:** If you are dealing with Model objects, you'll want to include the primary key if you want to update or remove the object. You'll also want to include any keys (primary or foreign) related to associations you plan to use.

**NOTE:** If a column is not selected, and you attempt to access it, you will get null:

```
// SELECT name FROM user LIMIT 1                    
User.select("name").first().chain(function(user){
    //user.id === null                    
});
```

select replaces any columns previously selected.

```
 User.select("id").select("name");
 // SELECT name FROM user
```

Like order you can use the [selectAppend](./patio_Dataset.html#selectAppend) method to append columns to be returned.

```
// SELECT id, name FROM user
User.select("id").selectAppend("name");
```

To remove selected columns and revert to `SELECT *` use the [selectAll](./patio_Dataset.html#selectAll) method.

```
 User.select("id").selectAll();
 // SELECT * FROM user
```

##Distinct

To add a `DISTINCT` clause to filter out duplicate rows use the [distinct](./patio_Dataset.html#distinct) method.

**Note:** `DISTINCT` is separate from the select clause,

```
// SELECT DISTINCT name FROM user
User.distinct().select("name")
```

##Limit and Offset

To limit the number of rows returned use the [limit](./patio_Dataset.html#limit) method.

```
//SELECT * FROM user LIMIT 5
User.limit(5);
```

To provide an offset you can provide limit with a second argument.

The following would return the 11th through 15th records in the original dataset.

```
// SELECT * FROM user LIMIT 5 OFFSET 10
 User.limit(5, 10);
```

To remove the LIMIT and OFFSET clause use the [unlimited](./patio_Dataset.html#unlimited) method.
```
 // SELECT * FROM user
 User.limit(5, 10).unlimited();
```


##Grouping

The GROUP clause is used to results based on the values of a given group of columns. To provide grouping use the [group](./patio_Dataset.html#group) method:

```
 // SELECT * FROM user GROUP BY user_id
 User.group("userId");
```

You can remove an existing grouping use the [ungrouped](./patio_Dataset.html#ungrouped) method:

```
 User.group("userId").ungrouped();
 // SELECT * FROM user
```

A common use of grouping is to count based on the number of grouped rows, so patio provides a [groupAndCount](./patio_Dataset.html#groupAndCount) method.

```
// SELECT user_id, COUNT(*) AS count FROM user GROUP BY user_id
 User.groupAndCount("userId");
```

##Having

The HAVING clause filters the results after the grouping has been applied, instead of before.

```
 // SELECT user_id, COUNT(*) AS count FROM user GROUP BY user_id HAVING count >= 10
 User.groupAndCount("dateOfBirth").having({count : {gte : 10}});

```

If you have a HAVING clause then filter will apply the filter to the HAVING clause.

```
 // SELECT user_id, COUNT(*) AS count FROM user GROUP BY user_id HAVING count >= 10 AND count < 15
 User.groupAndCount("dateOfBirth").having({count : {gte : 10}}).filter({count : {lt : 15}});
```

##Where                

Unlike filter, the where method will always affect the WHERE clause:

```
// SELECT user_id, COUNT(*) AS count FROM user WHERE name LIKE 'A%' GROUP BY id HAVING count >= 10
 User.groupAndCount("id").having({count : {gte : 10}}).where({name  : {like : 'A%'}});
```

Both the WHERE clause and the HAVING clause can be removed by using the unfiltered method:

```
// SELECT user_id, COUNT(*) AS count FROM user GROUP BY user_id
User.groupAndCount("id").having({count : {gte : 10}}).where({name  : {like : 'A%'}}).unfiltered();
```

##Joins

To join a dataset to another table or dataset. The underlying method used is [joinTable](./patio_Dataset.html#joinTable):

```
// SELECT * FROM user INNER JOIN user ON blog.user_id = user.id
User.joinTable("inner", "blog", {userId : sql.id});
```
**Note:** unlike other querying methods when specifying the join condition you must specify the value as a [sql.Identifier](./patio_sql.html#.identifier) if it is a column otherwise it will be assumed to be a literal value.

```
// SELECT * FROM user INNER JOIN blog ON blog.user_id = user.id
User.joinTable("inner", "blog", {userId : sql.id});

// SELECT * FROM user INNER JOIN blog ON blog.userId = 'id'
User.joinTable("inner", "blog", {userId : "id"});
```

joinTable is not typically used directly, but instead by named join methods:

```
// SELECT * FROM user INNER JOIN blog ON blog.user_id = user.id
 User.join("blog", {userId : sql.id});

 // SELECT * FROM user LEFT JOIN blog ON blog.user_id = user.id
 User.leftJoin("blog", {userId: sql.id});
```


###Table/Dataset to Join

For the join methods, the first argument is generally the name of the table to which you are joining. However, you can also provide a

* model class:

```
 User.join(Blog, {userId : sql.id});
```

* dataset, in which case a subselect is used:
```
 //SELECT * FROM user INNER JOIN (SELECT * FROM blog WHERE (title < 'A')) AS t1 ON (t1.user_id = user.id)
 User.join(Blog.filter({title : {lt : 'A'}}), {userId : sql.id});
```

###Join Conditions

The second argument to the specialized join methods is the conditions to use when joining, which is similar to a filter expression, with a few minor exceptions.


**Implicit Qualification**

An object used as the join conditions operates similarly to a filter, except that keys are automatically qualified with the table from the first argument, and unqualified values, that are sql.Identifiers, are automatically qualified with the first table or the last table joined. 

**Note:** both the id key and the userId value are qualified.

```
 //SELECT * FROM user INNER JOIN blog ON (blog.userId = user.id)
 User.join("blog", {userId : sql.id});
```


Because patio uses the last joined table for implicit qualifications of values, you can do things like:

```
 //SELECT * FROM user
 //    INNER JOIN blog ON (blog.user_id = user.id)
 //    INNER JOIN posts ON (posts.blog_id = blog.id)


 User.join("blog", {userId : sql.id}).join("posts", {blogId : sql.id});
```
**Note** blogId is qualified with posts and id is qualified with blog.
                
                
Implicit qualification is not always correct:

```
// SELECT * FROM user INNER JOIN blog ON (blog.user_id = user.id) INNER JOIN posts ON (posts.user_id = blog.id)
User.join("blog", {userId : sql.id}).join("posts", {userId : sql.id});
```
id is qualified with blog instead of user. This is wrong as the foreign key posts.user_id refers to user.id, not blog.id. To fix this, you need to explicitly qualify when joining:

```
//SELECT * FROM user
//  INNER JOIN blog ON (blog.user_id = user.id)
//  INNER JOIN posts ON (posts.user_id = user.id)


User.join("blog", {userId : sql.id}).join("posts", {userId : sql.id.qualify("user")});

//OR

User.join("blog", {userId : sql.id}).join("posts", {userId : sql.user__id}).sql

```

Just like the dataset filter method the join expression can be an array of two element arrays.

```
// SELECT * FROM user
//     INNER JOIN blog ON ((blog.user_id = user.id) AND (blog.id >= 1) AND (blog.id <= 5))

User.join("blog", [[sql.userId, sql.id], [sql.id, {between : [1, 5]}]]).sql

```

###USING Joins

JOIN ON is the most common type of join condition, however USING is also another valid SQL join expr that patio supports.

JOIN USING is useful when the columns you are using have the same names in both tables.

```
// SELECT * FROM user INNER JOIN blog USING (user_id)
 User.join("blog", [sql.userId])
```

###NATURAL Joins

NATURAL Joins assume that all columns with the same names used for joining, so you do not need to use a join expression.

```
// SELECT * FROM user NATURAL JOIN blog
 User.naturalJoin("blog");
```

###Join Blocks

The block should accept 3 arguments, the table alias for the table currently being joined, the table alias for the last table joined (or first table), and an array of previous [patio.sql.JoinClause](./patio_sql_JoinClause.html)s.


This allows you to qualify columns similar to how the implicit qualification works, without worrying about the specific aliases being used. For example, if you wanted to join the user and blog tables, but only want user where the user's name comes before the blog's title.

```
 //SELECT * FROM user INNER JOIN blog
 //     ON ((blog.user_id = user.id) AND (user.name < blog.title))
 User.join("blog", {userId : sql.id}, function(currAlias, lastAlias, previousJoins){
            return sql.name.qualify(lastAlias).lt(sql.title.qualify(currAlias));
        })
```

or you could do this which is the same thing:

```
 User.join("blog", {userId : sql.id, title : {gt : sql.name.qualify("user")}});
//SELECT * FROM user INNER JOIN blog
//      ON ((blog.user_id = user.id) AND (blog.title > user.name))
```

##From

The FROM table is typically the first clause populated when creating a dataset. For a standard [patio.Model](./patio_Model.html), the dataset already has the FROM clause populated, and the most common way to create datasets is with the Database from method.

```
DB.from("user");
// SELECT * FROM user
```

However, you can also use the from method on the Dataset.

```
 User.from("user", "oldUser");
 // SELECT * FROM user, old_user

//Using from again will remove the previous FROM clause.
DB.from("user").from("oldUser");
 // SELECT * FROM old_user
```   

**Note:** multiple tables in the FROM clause use a cross join by default, so the number of rows will be number of user times the number of old user.


##Subselects

If you want to perform a subselect you can use the [fromSelf](./patio_Dataset.html#fromSelf) method.

```
 Blog.order("userId").limit(100).fromSelf().group("userId");
 //SELECT * FROM (SELECT * FROM user ORDER BY user_id LIMIT 100) AS t1 GROUP BY user_id
```

If you did not use the `fromSelf` method the query would be:

```
 // SELECT * FROM user GROUP BY user_id ORDER BY user_id LIMIT 100
 Blog.order("userId").limit(100).group("userId")
```

Without fromSelf, you are doing the grouping, and limiting the number of grouped records returned to 100. So assuming you have blogs written by more than 100 user, you'll end up with 100 results.


With fromSelf, you are limiting the number of records before grouping. So if the user with the lowest id had 100 blogs, you'd get 1 result, not 100.

##Locking for Update

patio allows you to easily add a FOR UPDATE clause to your queries so that the records returned can't be modified by another query until the current transaction commits. You just use the [forUpdate](./patio_Dataset.html#forUpdate) method:

```
 DB.transaction(function(){
    return User.forUpdate().first({id : 1}).chain(function(){
        // SELECT * FROM user WHERE id = 1 FOR UPDATE
        user.password = null;
        return user.save();
    });
 });
```

This will ensure that no other connection modifies the row between when you select it and when the transaction ends.

##Custom SQL

patio makes it easy to use custom SQL by providing the [fetch][./patio_Database.html#fetch] method.

```
 // SELECT * FROM user
 DB.fetch("SELECT * FROM user")
```

You can also use the withSql dataset method.
```
 DB.from("user").withSql("SELECT * FROM user");
 // SELECT * FROM user
```

You can also use placeholders:

```
 // SELECT * FROM user WHERE id = 5
 DB.fetch("SELECT * FROM user WHERE id = ?", 5);
 
 // SELECT * FROM user WHERE id = 5
 DB.from("user").withSql("SELECT * FROM user WHERE id = {id}", {id : 5});
```

##Checking for Records

To test if there are any records in the database use the isEmpty method

```
 User.isEmpty().chain(function(isEmpty){
    // SELECT 1 FROM user LIMIT 1
 }, errorHandler);
 User.filter({id : 0}).isEmpty().chain(function(isEmpty){
    // SELECT 1 FROM user WHERE id = 0 LIMIT 1
 },errorHandler);
 User.filter(sql.name.like('B%')).isEmpty().chain(function(isEmpty){
    // SELECT 1 FROM user WHERE name LIKE 'B%' LIMIT 1
 },errorHandler);

```
##Aggregate Calculations

There are dataset methods for each of the following aggregate calculations:

* [count](./patio_Dataset.html#count) : count just returns the number of records in the dataset.
 
```
 User.count().chain(function(count){
    // SELECT COUNT(*) AS count FROM user LIMIT 1
 });
```

* [sum](./patio_Dataset.html#sum) : makes a sum aggregate function call for the column.

```
 User.sum("id").chain(function(){
    // SELECT sum(id) FROM user LIMIT 1
 });

```

* [avg](./patio_Dataset.html#avg): makes a avg aggregate function call for the column.

```
 User.avg("id").chain(function(){
    // SELECT avg(id) FROM user LIMIT 1
 });
```

* [min](./patio_Dataset.html#min) : makes a min aggregate function call for the column.

```
 User.min("id").chain(function(){
    // SELECT sum(id) FROM user LIMIT 1
 });
```

* [max](./patio_Dataset.html#max): makes a max aggregate function call for the column.

```
 User.max("id").chain(function(){
    // SELECT sum(id) FROM user LIMIT 1
 });
```