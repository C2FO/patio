"use strict";
var patio = require("../index"),
    sql = patio.sql,
    comb = require("comb"),
    format = comb.string.format;

patio.camelize = true;

//comb.logging.Logger.getRootLogger().level = comb.logging.Level.ERROR;

//disconnect and error callback helpers
patio.configureLogging();
var disconnect = comb.hitch(patio, "disconnect");
var disconnectError = function(err){
    patio.logError(err);
    patio.disconnect();
};

var connectAndCreateSchema = function(){
    //This assumes new tables each time you could just connect to the database
    return patio.connectAndExecute("mysql://test:testpass@localhost:3306/sandbox",
        function(db, patio){
            //drop and recreate the user
            db.forceCreateTable("user", function(){
                this.primaryKey("id");
                this.firstName(String);
                this.lastName(String);
                this.password(String);
                this.dateOfBirth(Date);
                this.isVerified(Boolean, {"default":false});
                this.lastAccessed(Date);
                this.created(sql.TimeStamp);
                this.updated(sql.DateTime);
            });
        });
};

var defineModel = function(){
    return patio.addModel("user", {
        pre:{
            "save":function(next){
                console.log("pre save!!!")
                next();
            },

            "remove":function(next){
                console.log("pre remove!!!")
                next();
            }
        },

        post:{
            "save":function(next){
                console.log("post save!!!")
                next();
            },

            "remove":function(next){
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
};

//connect and create schema
connectAndCreateSchema()
    .chain(defineModel, disconnectError)
    .then(function(){
        var User = patio.getModel("user");
        var myUser = new User({
            firstName:"bob",
            lastName:"yukon",
            password:"password",
            dateOfBirth:new Date(1980, 8, 29)
        });
        console.log(User.order("userId").limit(100).group("userId").sql);
        //save the user
        myUser.save().then(function(){
            console.log(format("%s %s was created at %s", myUser.firstName, myUser.lastName, myUser.created.toString()));
            console.log(format("%s %s's id is %d", myUser.firstName, myUser.lastName, myUser.id));

            User.db.transaction(
                function(){
                    var ret = new comb.Promise();
                    User.forUpdate().first({id:1}).then(function(user){
                        // SELECT * FROM user WHERE id = 1 FOR UPDATE
                        user.password = null;
                        user.save().then(comb.hitch(ret, "callback"), comb.hitch(ret, "errback"));
                    }, comb.hitch(ret, "errback"));
                    return ret;
                }).then(function(){
                    User.removeById(myUser.id).then(disconnect, disconnectError);
                }, disconnectError)

        }, disconnectError);
    }, disconnectError);

