var it = require('it'),
    assert = require('assert'),
    patio = require("../../lib"),
    Dataset = patio.Dataset,
    Database = patio.Database,
    sql = patio.SQL,
    Identifier = sql.Identifier,
    SQLFunction = sql.SQLFunction,
    LiteralString = sql.LiteralString,
    helper = require("../helpers/helper"),
    MockDatabase = helper.MockDatabase,
    SchemaDatabase = helper.SchemaDatabase,
    MockDataset = helper.MockDataset,
    comb = require("comb"),
    when = comb.when,
    serial = comb.serial;


it.describe("Dataset graphing",function (it) {

    patio.quoteIdentifiers = false;
    patio.identifierInputMethod = null;
    patio.identifierOutputMethod = null;

    var DS1 = comb.define(Dataset, {
        instance:{
            getters:{
                columns:function () {
                    return new comb.Promise().callback(["id", "x", "y"]);
                }
            }
        }
    });

    var DS2 = comb.define(Dataset, {
        instance:{
            getters:{
                columns:function () {
                    return new comb.Promise().callback(["id", "x", "y", "graphId"]);
                }
            }
        }
    });

    var DS3 = comb.define(Dataset, {
        instance:{
            getters:{
                columns:function () {
                    return new comb.Promise().callback(["id", "name", "x", "y", "linesX"]);
                }
            }
        }
    });

    var dss = {};
    var dbc = comb.define(Database, {
        instance:{
            from:function (name) {
                var args = comb.argsToArray(arguments);
                var block = comb.isFunction(args[args.length - 1]) ? args.pop() : null;
                var ds = dss[name];
                return block ? ds.filter(block) : ds;
            }
        }
    });
    var db = new dbc();
    var ds1 = new DS1(db).from("points");
    var ds2 = new DS2(db).from("lines");
    var ds3 = new DS3(db).from("graphs");
    dss = comb.merge(dss, {points:ds1, lines:ds2, graphs:ds3});

    it.describe("#graph", function (it) {
        it.should("not modify the current dataset's opts", function () {
            var o1 = ds1.__opts;
            var o2 = comb.merge({}, o1);
            return ds1.graph(ds2, {x:sql.identifier("id")}).chain(function (dsOne) {
                assert.deepEqual(ds1.__opts, o1);
                assert.deepEqual(ds1.__opts, o2);
                assert.notDeepEqual(dsOne.__opts, o1);
            });
        });

        it.should("accept a simple dataset and pass the table to join", function () {
            return ds1.graph(ds2, {x:sql.identifier("id")}).chain(function (ds) {
                assert.equal(ds.sql, 'SELECT points.id, points.x, points.y, lines.id AS lines_id, lines.x AS lines_x, lines.y AS lines_y, lines.graphId FROM points LEFT OUTER JOIN lines ON (lines.x = points.id)');
            });
        });

        it.should("accept a complex dataset and pass it directly to join", function () {
            return ds1.graph(ds2.filter({x:1}), {x:sql.identifier("id")}).chain(function (ds) {
                assert.equal(ds.sql, 'SELECT points.id, points.x, points.y, t1.id AS t1_id, t1.x AS t1_x, t1.y AS t1_y, t1.graphId FROM points LEFT OUTER JOIN (SELECT * FROM lines WHERE (x = 1)) AS t1 ON (t1.x = points.id)');
            });
        });

        it.should("work on fromSelf datasets", function () {
            return when(
                ds1.fromSelf().graph(ds2, {x:sql.identifier("id")}).chain(function (ds) {
                    assert.equal(ds.sql, 'SELECT t1.id, t1.x, t1.y, lines.id AS lines_id, lines.x AS lines_x, lines.y AS lines_y, lines.graphId FROM (SELECT * FROM points) AS t1 LEFT OUTER JOIN lines ON (lines.x = t1.id)');
                }),
                ds1.graph(ds2.fromSelf(), {x:sql.identifier("id")}).chain(function (ds) {
                    assert.equal(ds.sql, 'SELECT points.id, points.x, points.y, t1.id AS t1_id, t1.x AS t1_x, t1.y AS t1_y, t1.graphId FROM points LEFT OUTER JOIN (SELECT * FROM (SELECT * FROM lines) AS t1) AS t1 ON (t1.x = points.id)');
                }),
                ds1.fromSelf().fromSelf().graph(ds2.fromSelf().fromSelf(), {x:sql.identifier("id")}).chain(function (ds) {
                    assert.equal(ds.sql, 'SELECT t1.id, t1.x, t1.y, t2.id AS t2_id, t2.x AS t2_x, t2.y AS t2_y, t2.graphId FROM (SELECT * FROM (SELECT * FROM points) AS t1) AS t1 LEFT OUTER JOIN (SELECT * FROM (SELECT * FROM (SELECT * FROM lines) AS t1) AS t1) AS t2 ON (t2.x = t1.id)');
                }),
                ds1.from(ds1, ds3).graph(ds2.fromSelf(), {x:sql.identifier("id")}).chain(function (ds) {
                    assert.equal(ds.sql, 'SELECT t1.id, t1.x, t1.y, t3.id AS t3_id, t3.x AS t3_x, t3.y AS t3_y, t3.graphId FROM (SELECT * FROM (SELECT * FROM points) AS t1, (SELECT * FROM graphs) AS t2) AS t1 LEFT OUTER JOIN (SELECT * FROM (SELECT * FROM lines) AS t1) AS t3 ON (t3.x = t1.id)');
                })
            );
        });

        it.should("accept a string table name as the dataset", function () {
            return ds1.graph("lines", {x:sql.identifier("id")}).chain(function (ds) {
                assert.equal(ds.sql, 'SELECT points.id, points.x, points.y, lines.id AS lines_id, lines.x AS lines_x, lines.y AS lines_y, lines.graphId FROM points LEFT OUTER JOIN lines ON (lines.x = points.id)');
            });
        });

        it.should("accept an object that responds to dataset as the dataset", function () {
            return when(
                ds1.graph({dataset:ds2}, {x:sql.identifier("id")}).chain(function (ds) {
                    assert.equal(ds.sql, 'SELECT points.id, points.x, points.y, lines.id AS lines_id, lines.x AS lines_x, lines.y AS lines_y, lines.graphId FROM points LEFT OUTER JOIN lines ON (lines.x = points.id)');
                }),
                ds1.graph({dataset:"lines"}, {x:sql.identifier("id")}).chain(function (ds) {
                    assert.equal(ds.sql, 'SELECT points.id, points.x, points.y, lines.id AS lines_id, lines.x AS lines_x, lines.y AS lines_y, lines.graphId FROM points LEFT OUTER JOIN lines ON (lines.x = points.id)');
                })
            );
        });

        it.should("throw an error if an object with dataset is not used", function () {
            assert.throws(function () {
                ds1.graph({}, {x:sql.identifier("id")});
            });
        });

        it.should("accept a tableAlias option", function () {
            return ds1.graph("lines", {x:sql.identifier("id")}, {tableAlias:"planes"}).chain(function (ds) {
                assert.equal(ds.sql, 'SELECT points.id, points.x, points.y, planes.id AS planes_id, planes.x AS planes_x, planes.y AS planes_y, planes.graphId FROM points LEFT OUTER JOIN lines AS planes ON (planes.x = points.id)');
            });
        });

        it.should("accept a implicitQualifier option", function () {
            return ds1.graph("lines", {x:sql.identifier("id")}, {implicitQualifier:"planes"}).chain(function (ds) {
                assert.equal(ds.sql, 'SELECT points.id, points.x, points.y, lines.id AS lines_id, lines.x AS lines_x, lines.y AS lines_y, lines.graphId FROM points LEFT OUTER JOIN lines ON (lines.x = planes.id)');
            });
        });

        it.should("accept a joinType option", function (next) {
            return ds1.graph("lines", {x:sql.identifier("id")}, {joinType:"inner"}).chain(function (ds) {
                assert.equal(ds.sql, 'SELECT points.id, points.x, points.y, lines.id AS lines_id, lines.x AS lines_x, lines.y AS lines_y, lines.graphId FROM points INNER JOIN lines ON (lines.x = points.id)');
            });
        });

        it.should("not select any columns from the graphed table if select option is false", function () {
            return ds1.graph("lines", {x:sql.identifier("id")}, {select:false}).chain(function (d) {
                return d.graph("graphs", {id:sql.identifier("graphId")}).chain(function (ds) {
                    assert.equal(ds.sql, 'SELECT points.id, points.x, points.y, graphs.id AS graphs_id, graphs.name, graphs.x AS graphs_x, graphs.y AS graphs_y, graphs.linesX FROM points LEFT OUTER JOIN lines ON (lines.x = points.id) LEFT OUTER JOIN graphs ON (graphs.id = lines.graphId)');
                });
            });
        });

        it.should("use the given columns if :select option is used", function () {
            return ds1.graph("lines", {x:sql.identifier("id")}, {select:["x", "graphId"]}).chain(function (d) {
                return d.graph("graphs", {id:sql.identifier("graphId")}).chain(function (ds) {
                    assert.equal(ds.sql, 'SELECT points.id, points.x, points.y, lines.x AS lines_x, lines.graphId, graphs.id AS graphs_id, graphs.name, graphs.x AS graphs_x, graphs.y AS graphs_y, graphs.linesX FROM points LEFT OUTER JOIN lines ON (lines.x = points.id) LEFT OUTER JOIN graphs ON (graphs.id = lines.graphId)');
                });
            });
        });

        it.should("pass all join_conditions to join_table", function () {
            return ds1.graph(ds2, [
                ["x", sql.identifier("id")],
                ["y", sql.identifier("id")]
            ]).chain(function (ds) {
                    assert.equal(ds.sql, 'SELECT points.id, points.x, points.y, lines.id AS lines_id, lines.x AS lines_x, lines.y AS lines_y, lines.graphId FROM points LEFT OUTER JOIN lines ON ((lines.x = points.id) AND (lines.y = points.id))');
                });
        });

        it.should("accept a block instead of conditions and pass it to join_table", function () {
            return ds1.graph(ds2,
                function (ja, lja, js) {
                    return [
                        [this.x.qualify(ja), this.id.qualify(lja)],
                        [this.y.qualify(ja), this.id.qualify(lja)]
                    ];
                }).chain(function (ds) {
                    assert.equal(ds.sql, 'SELECT points.id, points.x, points.y, lines.id AS lines_id, lines.x AS lines_x, lines.y AS lines_y, lines.graphId FROM points LEFT OUTER JOIN lines ON ((lines.x = points.id) AND (lines.y = points.id))');
                });
        });

        it.should("not add columns if graph is called after set_graph_aliases", function () {
            var ds = ds1.setGraphAliases([
                ["x", ["points", "x"]],
                ["y", ["lines", "y"]]
            ]);
            assert.equal(ds.sql, 'SELECT points.x, lines.y FROM points');
            return ds.graph("lines", {x:sql.identifier("id")}).chain(function (ds) {
                assert.equal(ds.sql, 'SELECT points.x, lines.y FROM points LEFT OUTER JOIN lines ON (lines.x = points.id)');
            });
        });

        it.should("allow graphing of multiple datasets", function () {
            return ds1.graph(ds2, {x:sql.identifier("id")}).chain(function (d) {
                return d.graph(ds3, {id:sql.identifier("graphId")}).chain(function (ds) {
                    assert.equal(ds.sql, 'SELECT points.id, points.x, points.y, lines.id AS lines_id, lines.x AS lines_x, lines.y AS lines_y, lines.graphId, graphs.id AS graphs_id, graphs.name, graphs.x AS graphs_x, graphs.y AS graphs_y, graphs.linesX FROM points LEFT OUTER JOIN lines ON (lines.x = points.id) LEFT OUTER JOIN graphs ON (graphs.id = lines.graphId)');
                });
            });
        });


        it.should("allow graphing of the same dataset multiple times", function () {
            return ds1.graph(ds2, {x:sql.identifier("id")}).chain(function (d) {
                return d.graph(ds2, {y:sql.identifier("points__id")}, {tableAlias:"graph"}).chain(function (ds) {
                    assert.equal(ds.sql, 'SELECT points.id, points.x, points.y, lines.id AS lines_id, lines.x AS lines_x, lines.y AS lines_y, lines.graphId, graph.id AS graph_id, graph.x AS graph_x, graph.y AS graph_y, graph.graphId AS graph_graphId FROM points LEFT OUTER JOIN lines ON (lines.x = points.id) LEFT OUTER JOIN lines AS graph ON (graph.y = points.id)');
                });
            });
        });

        it.should("throw an error if the table/table alias has already been used", function () {
            assert.throws(comb.hitch(ds1, "graph", ds1, {x:sql.identifier("id")}));
            assert.doesNotThrow(comb.hitch(ds1, "graph", ds2, {x:sql.identifier("id")}));
            return when(
                ds1.graph(ds2, {x:sql.identifier("id")}).chain(function (d) {
                    assert.throws(function () {
                        d.graph(ds2, {x:sql.identifier("id")});
                    });
                }),
                ds1.graph(ds2, {x:sql.identifier("id")}).chain(function (d) {
                    assert.doesNotThrow(function () {
                        d.graph(ds2, {x:sql.identifier("id")}, {tableAlias:"blah"});
                    });
                })
            );
        });
    });

    it.describe("#setGraphAliases", function (it) {

        it.should("setGraphAliases and addGraphAliases should not modify the current dataset's opts", function () {
            var o1 = ds1.__opts;
            var o2 = comb.merge({}, o1);
            var d1 = ds1.setGraphAliases({x:["graphs", "id"]});
            assert.deepEqual(ds1.__opts, o1);
            assert.deepEqual(ds1.__opts, o2);
            assert.notDeepEqual(d1.__opts, o1);
            var o3 = ds1.__opts;
            var o4 = comb.merge({}, o3);
            var d2 = ds1.addGraphAliases({y:["blah", "id"]});
            assert.deepEqual(ds1.__opts, o3);
            assert.deepEqual(ds1.__opts, o4);
            assert.notDeepEqual(d2.__opts, o2);

        });

        it.should("specify the graph mapping", function () {
            return ds1.graph("lines", {x:sql.identifier("id")}).chain(function (ds) {
                assert.equal(ds.sql, 'SELECT points.id, points.x, points.y, lines.id AS lines_id, lines.x AS lines_x, lines.y AS lines_y, lines.graphId FROM points LEFT OUTER JOIN lines ON (lines.x = points.id)');
                ds = ds.setGraphAliases({x:["points", "x"], y:["lines", "y"]});
                assert.equal(ds.sql, 'SELECT points.x, lines.y FROM points LEFT OUTER JOIN lines ON (lines.x = points.id)');
            });
        });

        it.should("allow a third entry to specify an expression to use other than the default", function () {
            return ds1.graph("lines", {x:sql.identifier("id")}).chain(function (ds) {
                var d = ds.setGraphAliases({x:["points", "x", 1], y:["lines", "y", sql.random.sqlFunction]});
                assert.equal(d.sql, 'SELECT 1 AS x, random() AS y FROM points LEFT OUTER JOIN lines ON (lines.x = points.id)');
            });
        });

        it.should("only alias columns if necessary", function () {
            var ds = ds1.setGraphAliases({x:["points", "x"], y:["lines", "y"]});
            assert.equal(ds.sql, 'SELECT points.x, lines.y FROM points');

            ds = ds1.setGraphAliases({x1:["points", "x"], y:["lines", "y"]});
            assert.equal(ds.sql, 'SELECT points.x AS x1, lines.y FROM points');
        });
    });

    it.describe("#addGraphAliases", function (it) {

        it.should("addGraphAliases should add columns to the graph mapping", function () {
            return ds1.graph("lines", {x:sql.identifier("id")}).chain(function (ds) {
                var d = ds.setGraphAliases({x:["points", "q"]}).addGraphAliases({y:["lines", "r"]});
                assert.equal(d.sql, 'SELECT points.q AS x, lines.r AS y FROM points LEFT OUTER JOIN lines ON (lines.x = points.id)');
            });
        });
    });


    it.describe("#ungraphed", function (it) {

        it.should("remove the splitting of result sets into component tables", function (next) {
            return ds1.graph(ds2, {x:sql.identifier("id")}).chain(function (ds) {
                ds = ds.ungraphed();
                ds.fetchRows = function (sql, block) {
                    return comb.async.array([{id:1, x:2, y:3, lines_id:4, lines_x:5, lines_y:6, graphId:7}]);
                };
                return ds.all().chain(function (res) {
                    assert.deepEqual(res, [
                        {id:1, x:2, y:3, lines_id:4, lines_x:5, lines_y:6, graphId:7}
                    ]);
                });
            });
        });
    });

    it.describe("#graphEach", function (it) {

        it.should("split the result set into component tables", function () {
            return when(
                ds1.graph(ds2, {x:sql.identifier("id")}).chain(function (ds) {
                    ds.fetchRows = function (sql, block) {
                        return comb.async.array([
                            {id:1, x:2, y:3, lines_id:4, lines_x:5, lines_y:6, graphId:7}
                        ]);
                    };
                    return ds.all().chain(function (results) {
                        assert.lengthOf(results, 1);
                        assert.deepEqual(results[0], {points:{id:1, x:2, y:3}, lines:{id:4, x:5, y:6, graphId:7}});
                    });
                }),
                ds1.graph(ds2, {x:sql.identifier("id")}).chain(function (d) {
                    return  d.graph(ds3, {id:"graphId"}).chain(function (ds) {
                        ds.fetchRows = function (sql, block) {
                            return comb.async.array([{id:1, x:2, y:3, lines_id:4, lines_x:5, lines_y:6, graphId:7, graphs_id:8, name:9, graphs_x:10, graphs_y:11, linesX:12}]);
                        };
                        return ds.all().chain(function (results) {
                            assert.lengthOf(results, 1);
                            assert.deepEqual(results[0], {points:{id:1, x:2, y:3}, lines:{id:4, x:5, y:6, graphId:7}, graphs:{id:8, name:9, x:10, y:11, linesX:12}});
                        });
                    });
                }),

                ds1.graph(ds2, {x:sql.identifier("id")}).chain(function (d) {
                    return d.graph(ds2, {y:sql.identifier("points__id")}, {tableAlias:"graph"}).chain(function (ds) {
                        ds.fetchRows = function (sql, block) {
                            return comb.async.array([{id:1, x:2, y:3, lines_id:4, lines_x:5, lines_y:6, graphId:7, graph_id:8, graph_x:9, graph_y:10, graph_graphId:11}]);
                        };
                        return ds.all().chain(function (results) {
                            assert.lengthOf(results, 1);
                            assert.deepEqual(results[0], {points:{id:1, x:2, y:3}, lines:{id:4, x:5, y:6, graphId:7}, graph:{id:8, x:9, y:10, graphId:11}});
                        });
                    });
                })
            );
        });

        it.should("give a null value instead of a hash when all values for a table are null", function () {
            return when(
                ds1.graph(ds2, {x:sql.identifier("id")}).chain(function (ds) {
                    ds.fetchRows = function (sql, block) {
                        return comb.async.array([{id:1, x:2, y:3, lines_id:null, lines_x:null, lines_y:null, graphId:null}]);
                    };
                    return ds.all().chain(function (r) {
                        assert.lengthOf(r, 1);
                        assert.deepEqual(r[0], {points:{id:1, x:2, y:3}, lines:null});
                    });
                }),
                ds1.graph(ds2, {x:sql.identifier("id")}).chain(function (d) {
                    return d.graph(ds3, {id:sql.identifier("graphId")}).chain(function (ds) {
                        ds.fetchRows = function (sql, block) {
                            return comb.async.array([
                                {id:1, x:2, y:3, lines_id:4, lines_x:5, lines_y:6, graphId:7, graphs_id:null, name:null, graphs_x:null, graphs_y:null, linesX:null},
                                {id:2, x:4, y:5, lines_id:null, lines_x:null, lines_y:null, graphId:null, graphs_id:null, name:null, graphs_x:null, graphs_y:null, linesX:null},
                                {id:3, x:5, y:6, lines_id:4, lines_x:5, lines_y:6, graphId:7, graphs_id:7, name:8, graphs_x:9, graphs_y:10, linesX:11},
                                {id:3, x:5, y:6, lines_id:7, lines_x:5, lines_y:8, graphId:9, graphs_id:9, name:10, graphs_x:10, graphs_y:11, linesX:12}
                            ]);
                        };
                        return ds.all(null).chain(function (r) {
                            assert.lengthOf(r, 4);
                            assert.deepEqual(r[0], {points:{id:1, x:2, y:3}, lines:{id:4, x:5, y:6, graphId:7}, graphs:null});
                            assert.deepEqual(r[1], {points:{id:2, x:4, y:5}, lines:null, graphs:null});
                            assert.deepEqual(r[2], {points:{id:3, x:5, y:6}, lines:{id:4, x:5, y:6, graphId:7}, graphs:{id:7, name:8, x:9, y:10, linesX:11}});
                            assert.deepEqual(r[3], {points:{id:3, x:5, y:6}, lines:{id:7, x:5, y:8, graphId:9}, graphs:{id:9, name:10, x:10, y:11, linesX:12}});
                        });
                    });
                })
            );
        });

        it.should("not give a null value instead of a hash when any value for a table is false", function (next) {
            return ds1.graph(ds2, {x:sql.identifier("id")}).chain(function (ds) {
                ds.fetchRows = function (sql, block) {
                    return comb.async.array([{id:1, x:2, y:3, lines_id:null, lines_x:false, lines_y:null, graphId:null}]);
                };
                return ds.all().chain(function (r) {
                    assert.deepEqual(r, [
                        {points:{id:1, x:2, y:3}, lines:{id:null, x:false, y:null, graphId:null}}
                    ]);
                });
            });
        });

        it.should("not included tables graphed with the {select : false} option in the result set", function () {
            return ds1.graph("lines", {x:sql.identifier("id")}, {select:false}).chain(function (d) {
                return d.graph("graphs", {id:sql.identifier("graphId")}).chain(function (ds) {
                    ds.fetchRows = function (sql, block) {
                        return comb.async.array([{id:1, x:2, y:3, graphs_id:8, name:9, graphs_x:10, graphs_y:11, linesX:12}]);
                    };
                    return ds.all().chain(function (results) {
                        assert.lengthOf(results, 1);
                        assert.deepEqual(results[0], {points:{id:1, x:2, y:3}, graphs:{id:8, name:9, x:10, y:11, linesX:12}});
                    });
                });
            });
        });

        it.should("only include the columns selected with setGraphAliases and addGraphAliases, if called", function () {
            return when(
                ds1.graph("lines", {x:sql.identifier("id")}).chain(function (ds) {
                    ds = ds.setGraphAliases({x:["points", "x"], y:["lines", "y"]});
                    ds.fetchRows = function (sql, block) {
                        return comb.async.array([{x:2, y:3}]);
                    };
                    return ds.all().chain(function (results) {
                        assert.lengthOf(results, 1);
                        assert.deepEqual(results[0], {points:{x:2}, lines:{y:3}});
                    });
                }),

                ds1.graph("lines", {x:sql.identifier("id")}).chain(function (ds) {
                    ds = ds.setGraphAliases({x:["points", "x"]});
                    ds.fetchRows = function (sql, block) {
                        return comb.async.array([{x:2}]);
                    };
                    return ds.all(null,function (err, results) {
                        assert.lengthOf(results, 1);
                        assert.deepEqual(results[0], {points:{x:2}, lines:null});
                    }).chain(function () {
                            ds = ds.addGraphAliases({q:["points", "r", 18]});
                            ds.fetchRows = function (sql, block) {
                                return comb.async.array([{x:2, q:18}]);
                            };
                            return ds.all(null, function (err, results) {
                                assert.deepEqual(results, [
                                    {points:{x:2, r:18}, lines:null}
                                ]);
                            });
                        });

                })
            );
        });

        it.should("correctly map values when setGraphAliases is used with a third argument for each entry", function () {
            return ds1.graph("lines", {x:sql.identifier("id")}).chain(function (ds) {
                ds = ds.setGraphAliases({x:["points", "z1", 2], y:["lines", "z2", sql.random.sqlFunction]});
                ds.fetchRows = function (sql, block) {
                    return comb.async.array([{x:2, y:3}]);
                };
                return ds.all(null).chain(function (results) {
                    assert.deepEqual(results, [
                        {points:{z1:2}, lines:{z2:3}}
                    ]);
                });
            });
        });

        it.should("run the rowCb for graphed datasets", function (next) {
            ds1.rowCb = function (h) {
                Object.keys(h).forEach(function (k) {
                    h[k] *= 2;
                });
                return h;
            };
            ds2.rowCb = function (h) {
                Object.keys(h).forEach(function (k) {
                    h[k] *= 3;
                });
                return h;
            };
            return ds1.graph(ds2, {x:sql.identifier("id")}).chain(function (ds) {
                ds.fetchRows = function (sql, block) {
                    return comb.async.array([{id:1, x:2, y:3, lines_id:4, lines_x:5, lines_y:6, graphId:7}]);
                };
                return ds.all().chain(function (results) {
                    assert.deepEqual(results, [
                        {points:{id:2, x:4, y:6}, lines:{id:12, x:15, y:18, graphId:21}}
                    ]);
                });
            });
        });

    });


    it.afterAll(function () {
        return patio.disconnect();
    });
});