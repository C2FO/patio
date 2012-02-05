"use strict";
var patio = require("../../index"),
    sql = patio.sql,
    comb = require("comb"),
    format = comb.string.format;

//comb.logging.Logger.getRootLogger().level = comb.logging.Level.ERROR;

//disconnect and error callback helpers
var disconnect = comb.hitch(patio, "disconnect");
var disconnectError = function(err){
    patio.logError(err);
    return patio.disconnect();
};
patio.configureLogging();
var connectAndCreateSchema = function(){
    //This assumes new tables each time you could just connect to the database
    return patio.connectAndExecute("mysql://test:testpass@localhost:3306/sandbox",
        function(db, patio){
            //drop and recreate the user
            db.forceCreateTable("user", function(){
                this.primaryKey("id");
                this.firstName(String)
                this.lastName(String);
                this.password(String);
                this.dateOfBirth(Date);
                this.created(sql.TimeStamp);
                this.updated(sql.DateTime);
            });
        });
};

var simpleTransaction = function(db){
    return db.transaction(function(){
        var ds = db.from("user");
        ds.insert({
            firstName:"Bob",
            lastName:"Yukon",
            password:"password",
            dateOfBirth:new Date(1980, 8, 29)
        });
        ds.insert({
            firstName:"Greg",
            lastName:"Kilosky",
            password:"password",
            dateOfBirth:new Date(1988, 7, 19)
        });

        ds.insert({
            firstName:"Jane",
            lastName:"Gorgenson",
            password:"password",
            dateOfBirth:new Date(1956, 1, 3)
        });
    })
};

var nestedTransaction = function(db){
    return db.transaction(function(){
        var ds = db.from("user");
        ds.insert({
            firstName:"Bob",
            lastName:"Yukon",
            password:"password",
            dateOfBirth:new Date(1980, 8, 29)
        });
        db.transaction(function(){
            ds.insert({
                firstName:"Greg",
                lastName:"Kilosky",
                password:"password",
                dateOfBirth:new Date(1988, 7, 19)
            });
            db.transaction(function(){
                ds.insert({
                    firstName:"Jane",
                    lastName:"Gorgenson",
                    password:"password",
                    dateOfBirth:new Date(1956, 1, 3)
                });
            })
        });
    });
};

var multipleTransactions = function(db){
    var ds = db.from("user");
    return comb.when(db.transaction(function(){
        ds.insert({
            firstName:"Bob",
            lastName:"Yukon",
            password:"password",
            dateOfBirth:new Date(1980, 8, 29)
        });
    }), db.transaction(function(){
        ds.insert({
            firstName:"Greg",
            lastName:"Kilosky",
            password:"password",
            dateOfBirth:new Date(1988, 7, 19)
        });
    }),
        db.transaction(function(){
            var ret = new comb.Promise();
            ds.insert({
                firstName:"Jane",
                lastName:"Gorgenson",
                password:"password",
                dateOfBirth:new Date(1956, 1, 3)
            }).then(function(){
                    ds.all(
                        function(user){
                            return ds.where({id:user.id}).update({firstName:user.firstName + 1});
                        }).then(comb.hitch(ret, "callback"), comb.hitch(ret, "errback"));
                }, comb.hitch(ret, "errback"));
            return ret;
        }));
};

var multipleTransactionsError = function(db){
    var ds = db.from("user");
    return comb.when(db.transaction(function(){
        ds.insert({
            firstName:"Bob",
            lastName:"Yukon",
            password:"password",
            dateOfBirth:new Date(1980, 8, 29)
        });
    }), db.transaction(function(){
        ds.insert({
            firstName:"Greg",
            lastName:"Kilosky",
            password:"password",
            dateOfBirth:new Date(1988, 7, 19)
        });
    }), db.transaction(function(){
        var ret = new comb.Promise();
        ds.insert({
            firstName:"Jane",
            lastName:"Gorgenson",
            password:"password",
            dateOfBirth:new Date(1956, 1, 3)
        }).then(function(){
                ds.all(
                    function(user){
                        ds.where({id:user.id}).update({firstName:user.firstName + 1});
                    }).then(comb.hitch(ret, "callback"), comb.hitch(ret, "errback"));
            }, comb.hitch(ret, "errback"));
        return ret;
    }));
};

var inOrderTransaction = function(db){
    var ds = db.from("user");
    return comb.executeInOrder(db, function(db){
        db.transaction(function(){
            ds.insert({
                firstName:"Bob",
                lastName:"Yukon",
                password:"password",
                dateOfBirth:new Date(1980, 8, 29)
            });
        });
        db.transaction(function(){
            ds.insert({
                firstName:"Greg",
                lastName:"Kilosky",
                password:"password",
                dateOfBirth:new Date(1988, 7, 19)
            });
        });
        db.transaction(function(){
            ds.insert({
                firstName:"Jane",
                lastName:"Gorgenson",
                password:"password",
                dateOfBirth:new Date(1956, 1, 3)
            });
        })
    });
};

var errorTransaciton = function(db){
    var ds = db.from("user");
    var ret = new comb.Promise();
    db.transaction(
        function(){
            var ds = db.from("user");
            ds.insert({
                firstName:"Bob",
                lastName:"Yukon",
                password:"password",
                dateOfBirth:new Date(1980, 8, 29)
            });
            db.transaction(function(){
                ds.insert({
                    firstName:"Greg",
                    lastName:"Kilosky",
                    password:"password",
                    dateOfBirth:new Date(1988, 7, 19)
                });
                throw "Error";
                db.transaction(function(){
                    ds.insert({
                        firstName:"Jane",
                        lastName:"Gorgenson",
                        password:"password",
                        dateOfBirth:new Date(1956, 1, 3)
                    });
                });
            });
        }).then(comb.hitch(ret, "errback"), function(){
            ds.count().then(
                function(count){
                    patio.logInfo(format("COUNT = " + count));
                    ret.callback();
                }).addErrback(comb.hitch(ret, "errback"));
        });
    return ret;
};

var errorCallbackTransaciton = function(db){
    var ds = db.from("user");
    var ret = new comb.Promise();
    db.transaction(
        function(){
            var ds = db.from("user");
            ds.insert({
                firstName:"Bob",
                lastName:"Yukon",
                password:"password",
                dateOfBirth:new Date(1980, 8, 29)
            });
            db.transaction(function(){
                ds.insert({
                    firstName:"Greg",
                    lastName:"Kilosky",
                    password:"password",
                    dateOfBirth:new Date(1988, 7, 19)
                });
                db.transaction(function(){
                    ds.insert({
                        firstName:"Jane",
                        lastName:"Gorgenson",
                        password:"password",
                        dateOfBirth:new Date(1956, 1, 3)
                    });
                    return new comb.Promise().errback("err");
                });
            });
        }).then(comb.hitch(ret, "errback"), function(){
            ds.count().then(
                function(count){
                    patio.logInfo(format("COUNT = " + count));
                    ret.callback();
                }).addErrback(comb.hitch(ret, "errback"));
        });
    return ret;
};


var connectAndExecute = function(cb){
    var ret = new comb.Promise();
    connectAndCreateSchema().then(function(db){
        cb(db).chain(disconnect, disconnectError).then(comb.hitch(ret, "callback"), comb.hitch(ret, "errback"))
    }, disconnectError);
    return ret;
};

connectAndExecute(multipleTransactions)
    .chain(comb.partial(connectAndExecute, nestedTransaction), disconnectError)
    .chain(comb.partial(connectAndExecute, inOrderTransaction), disconnectError)
    .chain(comb.partial(connectAndExecute, multipleTransactions), disconnectError)
    .chain(comb.partial(connectAndExecute, errorTransaciton), disconnectError)
    .chain(comb.partial(connectAndExecute, errorCallbackTransaciton), disconnectError);

