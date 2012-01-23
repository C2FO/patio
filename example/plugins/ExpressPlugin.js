var patio = require("../../lib"), comb = require("comb");

/*
 * Very simple express routing for a model
 * */
module.exports = exports = comb.define(null, {
    static:{

        addRoute:function (route, cb) {
            this.routes.push(["get", route, cb]);
        },

        findByIdRoute:function (params) {
            var ret = new comb.Promise();
            this.findById(params.id).then(function (model) {
                ret.callback(model ? model.toObject() : {error:"Could not find a model with id " + id});
            }, comb.hitch(ret, "errback"));
            return ret;
        },

        removeByIdRoute:function (params) {
            return this.removeById(params.id);
        },

        __routeProxy:function (cb) {
            return function (req, res) {
                comb.when(cb(req.params)).then(comb.hitch(res, "send"), function (err) {
                    res.send({error:err.message});
                });
            }
        },

        route:function (app) {
            var routes = this.routes;
            for (var i in routes) {
                var route = routes[i];
                app[route[0]](route[1], this.__routeProxy(route[2]));
            }
        },

        getters:{
            routes:function () {
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