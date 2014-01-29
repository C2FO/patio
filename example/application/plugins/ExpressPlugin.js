var patio = require("../../../index"), comb = require("comb");

/*
 * Very simple express routing for a model
 * */
module.exports = exports = comb.define(null, {
    static: {

        addRoute: function (route, cb) {
            this.routes.push(["get", route, cb]);
        },

        findByIdRoute: function (params) {
            var ret = new comb.Promise();
            return this.findById(params.id).chain(function (model) {
                if (model) {
                    return model.toObject();
                } else {
                    throw new Error("Could not find a model with id " + id);
                }
            });
        },

        removeByIdRoute: function (params) {
            return this.removeById(params.id);
        },

        __routeProxy: function (cb) {
            return function (req, res) {
                comb.when(cb(req.params)).chain(comb.hitch(res, "send"), function (err) {
                    res.send({error: err.message});
                });
            }
        },

        route: function (app) {
            var routes = this.routes;
            for (var i in routes) {
                var route = routes[i];
                app[route[0]](route[1], this.__routeProxy(route[2]));
            }
        },

        getters: {
            routes: function () {
                if (comb.isUndefined(this.__routes)) {
                    var routes = this.__routes = [
                        ["get", "/" + this.tableName + "/:id", comb.hitch(this, "findByIdRoute")],
                        ["delete", "/" + this.tableName + "/:id", comb.hitch(this, "removeByIdRoute")]
                    ];
                }
                return this.__routes;
            }

        }
    }
});