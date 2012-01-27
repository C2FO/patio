"use strict";
var patio = require("../index"),
    sql = patio.sql,
    comb = require("comb"),
    format = comb.string.format;

patio.camelize = true;

comb.logging.Logger.getRootLogger().level = comb.logging.Level.ERROR;

//disconnect and error callback helpers
var disconnect = comb.hitch(patio, "disconnect");
var disconnectError = function(err) {
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
                this.isVerified(Boolean, {"default" : false})
                this.created(sql.TimeStamp);
                this.updated(sql.DateTime);
            });
        });
};

var defineModel = function(){
    return patio.addModel("user");
};

//connect and create schema
connectAndCreateSchema()
    .chain(defineModel, disconnectError)
    .then(function(){
         var User = patio.getModel("user");
         var myUser = new User({
             firstName : "Bob",
             lastName : "Yukon",
             password : "password",
             dateOfBirth : new Date(1980, 8, 29)
         });
        //save the user
        myUser.save().then(function(){
            console.log(format("%s %s was created at %s", myUser.firstName, myUser.lastName, myUser.created.toString()));
            console.log(format("%s %s's id is %d", myUser.firstName, myUser.lastName, myUser.id));
            disconnect();
        }, disconnectError);
    }, disconnectError);

