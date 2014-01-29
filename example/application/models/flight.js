var patio = require("../../../index"),
    sql = patio.sql,
    comb = require("comb"),
    expressPlugin = require("../plugins/ExpressPlugin"),
    FlightLeg = require("./flightleg");


patio.addModel("flight", {
    plugins: [expressPlugin],
    instance: {
        toObject: function () {
            var obj = this._super(arguments);
            obj.weekdays = this.weekdaysArray;
            obj.legs = this.legs.map(function (l) {
                return l.toObject();
            });
            return obj;
        },

        _setWeekdays: function (weekdays) {
            this.weekdaysArray = weekdays.split(",");
            return weekdays;
        }
    },

    static: {

        init: function () {
            this._super(arguments);
            this.oneToMany("legs", {
                model: "flightLeg",
                orderBy: "scheduledDepartureTime",
                fetchType: this.fetchType.EAGER
            });

            this.addRoute("/flights/:airline", comb.hitch(this, function (params) {
                return this.byAirline(params.airline).chain(function (flights) {
                    return comb(flights).invoke("toObject");
                });
            }));
            this.addRoute("/flights/departs/:airportCode", comb.hitch(this, function (params) {
                return this.departsFrom(params.airportCode).chain(function (flights) {
                    return comb(flights).invoke("toObject");
                });
            }));
            this.addRoute("/flights/arrives/:airportCode", comb.hitch(this, function (params) {
                return this.arrivesAt(params.airportCode).chain(function (flights) {
                    return comb(flights).invoke("toObject");
                });
            }));
        },

        byAirline: function (airline) {
            return this.filter({airline: airline}).all();
        },

        arrivesAt: function (airportCode) {
            return this.join(FlightLeg.select("flightId").filter({arrivalCode: airportCode}).distinct(), {flightId: sql.id}).all();
        },

        departsFrom: function (airportCode) {
            return this.join(FlightLeg.select("flightId").filter({departureCode: airportCode}).distinct(), {flightId: sql.id}).all();
        },

    }
}).as(module);

