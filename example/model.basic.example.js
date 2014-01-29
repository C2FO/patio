"use strict";
var patio = require("../index"),
    sql = patio.sql,
    comb = require("comb"),
    format = comb.string.format;

patio.camelize = true;
var DB = patio.connect("mysql://test:testpass@localhost:3306/sandbox");
//disconnect and error callback helpers
patio.configureLogging();
var disconnect = function () {
    return patio.disconnect();
};
var disconnectError = function (err) {
    patio.logError(err);
    patio.disconnect();
};

var User = patio.addModel("user", {
    pre: {
        "save": function (next) {
            console.log("pre save!!!");
            next();
        },

        "remove": function (next) {
            console.log("pre remove!!!");
            next();
        }
    },

    post: {
        "save": function (next) {
            console.log("post save!!!");
            next();
        },

        "remove": function (next) {
            console.log("post remove!!!");
            next();
        }
    },
    instance: {
        _setFirstName: function (firstName) {
            return firstName.charAt(0).toUpperCase() + firstName.substr(1);
        },

        _setLastName: function (lastName) {
            return lastName.charAt(0).toUpperCase() + lastName.substr(1);
        }
    }
});


var connectAndCreateSchema = function () {
    //This assumes new tables each time you could just connect to the database
    return patio.connectAndExecute("mysql://test:testpass@localhost:3306/sandbox",
        function (db, patio) {
            //drop and recreate the user
            db.forceCreateTable("user", function () {
                this.primaryKey("id");
                this.firstName(String);
                this.lastName(String);
                this.password(String);
                this.dateOfBirth(Date);
                this.isVerified(Boolean, {"default": false});
                this.lastAccessed(Date);
                this.created(sql.TimeStamp);
                this.updated(sql.DateTime);
            });
            //sync the model
            patio.syncModels();
        });
};


//connect and create schema
connectAndCreateSchema().chain(function () {
    var myUser = new User({
        firstName: "bob",
        lastName: "yukon",
        password: "password",
        dateOfBirth: new Date(1980, 8, 29)
    });
    console.log(User.order("userId").limit(100).group("userId").sql);
    //save the user
    return myUser.save().chain(function () {
        console.log(format("%s %s was created at %s", myUser.firstName, myUser.lastName, myUser.created.toString()));
        console.log(format("%s %s's id is %d", myUser.firstName, myUser.lastName, myUser.id));

        return User.db.transaction(function () {
            var ret = new comb.Promise();
            return User.forUpdate().first({id: 1}).chain(function (user) {
                // SELECT * FROM user WHERE id = 1 FOR UPDATE
                user.password = null;
                return user.save();
            });
        }).chain(function () {
                return User.removeById(myUser.id);
            });
    });
}).chain(disconnect, disconnectError);

