"use strict";
var patio = require("../index"),
    sql = patio.sql,
    comb = require("comb"),
    format = comb.string.format;

patio.camelize = true;
var DB1 = patio.connect("mysql://test:testpass@localhost:3306/sandbox");
var DB2 = patio.connect("mysql://test:testpass@localhost:3306/sandbox2");


//disconnect and error callback helpers
patio.configureLogging();
patio.LOGGER.level = "ERROR";
var disconnect = comb.hitch(patio, "disconnect");
var disconnectError = function (err) {
    patio.logError(err);
    patio.disconnect();
};


var User1 = patio.addModel(DB1.from("user"));
var User2 = patio.addModel(DB2.from("user"))


var connectAndCreateSchema = function () {
    //This assumes new tables each time you could just connect to the database
    return comb.serial([
        function () {
            return comb.when(
                DB1.forceCreateTable("user", function () {
                    this.primaryKey("id");
                    this.firstName(String);
                    this.lastName(String);
                    this.password(String);
                    this.dateOfBirth(Date);
                    this.isVerified(Boolean, {"default":false})
                    this.created(sql.TimeStamp);
                    this.updated(sql.DateTime);
                }),
                //drop and recreate the user
                DB2.forceCreateTable("user", function () {
                    this.primaryKey("id");
                    this.firstName(String);
                    this.lastName(String);
                    this.password(String);
                    this.dateOfBirth(Date);
                    this.isVerified(Boolean, {"default":false})
                    this.created(sql.TimeStamp);
                    this.updated(sql.DateTime);
                })
            );
        },
        comb.hitch(patio, "syncModels")
    ]);
};

//connect and create schema
connectAndCreateSchema()
    .then(function (userModels) {
        var myUser1 = new User1({
            firstName:"Bob1",
            lastName:"Yukon1",
            password:"password",
            dateOfBirth:new Date(1980, 8, 29)
        });
        var myUser2 = new User2({
            firstName:"Bob2",
            lastName:"Yukon2",
            password:"password",
            dateOfBirth:new Date(1980, 8, 29)
        });
        comb.when(myUser1.save(), myUser2.save(), function () {
            console.log(format("%s %s was created at %s", myUser1.firstName, myUser1.lastName, myUser1.created.toString()));
            console.log(format("%s %s's id is %d", myUser1.firstName, myUser1.lastName, myUser1.id));

            console.log(format("%s %s was created at %s", myUser2.firstName, myUser2.lastName, myUser2.created.toString()));
            console.log(format("%s %s's id is %d", myUser2.firstName, myUser2.lastName, myUser2.id));
            disconnect();
        }, disconnectError);
        //save the user
    }, disconnectError);

