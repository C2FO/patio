var patio = require("../../../lib"),
    comb = require("comb"),
    expressPlugin = require("../plugins/ExpressPlugin");


patio.addModel("flight", {
    plugins:[patio.plugins.CachePlugin, expressPlugin],
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
            return this.join(this.flightLeg.select("flightId").filter({arrivalCode:airportCode}).distinct(), {flightId:"id"}).all();
        },

        departsFrom:function (airportCode) {
            return this.join(this.flightLeg.select("flightId").filter({departureCode:airportCode}).distinct(), {flightId:"id"}).all();
        },

        getters:{
            flightLeg:function () {
                if (!this.__flightLeg) {
                    this.__flightLeg = this.patio.getModel("flightLeg");
                }
                return this.__flightLeg;
            }
        }
    }
});

