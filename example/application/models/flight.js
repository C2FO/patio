var patio = require("../../../index"),
    sql = patio.sql,
    comb = require("comb"),
    expressPlugin = require("../plugins/ExpressPlugin"),
    FlightLeg = require("./flightleg");


patio.addModel("flight", {
    plugins:[expressPlugin],
    instance:{
        toObject:function () {
            var obj = this._super(arguments);
            obj.weekdays = this.weekdaysArray;
            obj.legs = this.legs.map(function (l) {
                return l.toObject();
            });
            return obj;
        },

        _setWeekdays:function (weekdays) {
            this.weekdaysArray = weekdays.split(",");
            return weekdays;
        }
    },

    static:{

        init:function () {
            this._super(arguments);
            this.oneToMany("legs", {
                model:"flightLeg",
                orderBy:"scheduledDepartureTime",
                fetchType:this.fetchType.EAGER
            });

            this.addRoute("/flights/:airline", comb.hitch(this, function (params) {
                var ret = new comb.Promise();
                this.byAirline(params.airline).then(function (flights) {
                    ret.callback(flights.map(function (flight) {
                        return flight.toObject();
                    }));
                }, comb.hitch(ret, "errback"));
                return  ret;
            }));
            this.addRoute("/flights/departs/:airportCode", comb.hitch(this, function (params) {
                var ret = new comb.Promise();
                this.departsFrom(params.airportCode).then(function (flights) {
                    ret.callback(flights.map(function (flight) {
                        return flight.toObject();
                    }));
                }, comb.hitch(ret, "errback"));
                return  ret;
            }));
            this.addRoute("/flights/arrives/:airportCode", comb.hitch(this, function (params) {
                var ret = new comb.Promise();
                this.arrivesAt(params.airportCode).then(function (flights) {
                    ret.callback(flights.map(function (flight) {
                        return flight.toObject();
                    }));
                }, comb.hitch(ret, "errback"));
                return  ret;
            }));
        },

        byAirline:function (airline) {
            return this.filter({airline:airline}).all();
        },

        arrivesAt:function (airportCode) {
            return this.join(FlightLeg.select("flightId").filter({arrivalCode:airportCode}).distinct(), {flightId:sql.id}).all();
        },

        departsFrom:function (airportCode) {
            return this.join(FlightLeg.select("flightId").filter({departureCode:airportCode}).distinct(), {flightId:sql.id}).all();
        },

    }
}).as(module);

