var vows = require('vows'),
    assert = require('assert'),
    patio = require("index"),
    helper = require("../helpers/helper"),
    MockDatabase = helper.MockDatabase,
    SchemaDatabase = helper.SchemaDatabase,
    MockDataset = helper.MockDataset,
    Dataset = patio.Dataset,
    sql = patio.SQL,
    Identifier = sql.Identifier,
    SQLFunction = sql.SQLFunction,
    LiteralString = sql.LiteralString,
    QualifiedIdentifier = sql.QualifiedIdentifier,
    comb = require("comb"),
    hitch = comb.hitch;

var ret = (module.exports = exports = new comb.Promise());
var suite = vows.describe("Dataset queries");
patio.identifierInputMethod = null;
patio.identifierOutputMethod = null;

suite.addBatch({
    "a simple datatset ":{

        topic:function () {
            return new Dataset().from("test");
        },

        "should format a SELECT statement ":function (ds) {
            assert.equal(ds.selectSql, "SELECT * FROM test");
        },

        "should format a delete statement":function (ds) {
            assert.equal(ds.deleteSql, 'DELETE FROM test');
        },
        "should format a truncate statement":function (ds) {
            assert.equal(ds.truncateSql, 'TRUNCATE TABLE test');
        },
        "should format an insert statement with default values":function (ds) {
            assert.equal(ds.insertSql(), 'INSERT INTO test DEFAULT VALUES');
        },
        "should format an insert statement with hash":function (ds) {
            assert.equal(ds.insertSql({name:'wxyz', price:342}), "INSERT INTO test (name, price) VALUES ('wxyz', 342)");
            assert.equal(ds.insertSql({}), "INSERT INTO test DEFAULT VALUES");
        },

        "should format an insert statement with an object that has a values property":function (ds) {
            var v = {values:{a:1}};
            assert.equal(ds.insertSql(v), "INSERT INTO test (a) VALUES (1)");
            assert.equal(ds.insertSql({}), "INSERT INTO test DEFAULT VALUES");
        },

        "should format an insert statement with an arbitrary value":function (ds) {
            assert.equal(ds.insertSql(123), "INSERT INTO test VALUES (123)");
        },

        "should format an insert statement with sub-query":function (ds) {
            var sub = new Dataset().from("something").filter({x:2});
            assert.equal(ds.insertSql(sub), "INSERT INTO test SELECT * FROM something WHERE (x = 2)");

        },

        "should format an insert statement with array":function (ds) {
            assert.equal(ds.insertSql('a', 2, 6.5), "INSERT INTO test VALUES ('a', 2, 6.5)");
        },

        "should format an update statement":function (ds) {
            assert.equal(ds.updateSql({name:'abc'}), "UPDATE test SET name = 'abc'");
        }
    },

    "A dataset with multiple tables in its FROM clause":{
        topic:function () {
            return new Dataset().from("t1", "t2");
        },

        "should raise on updateSql":function (ds) {
            assert.throws(comb.hitch(ds, ds.updateSql, {a:1}));
        },

        "should raise on deleteSql":function (ds) {
            assert.throws(function(){
                ds.deleteSql;
            });
        },

        "should raise on //truncateSql":function (ds) {
            assert.throws(function(){
                ds.truncateSql;
            });
        },

        "should raise on //insertSql":function (ds) {
            assert.throws(comb.hitch(ds, ds.insertSql));
        },

        "should generate a SELECT query FROM all specified tables":function (ds) {
            assert.equal(ds.selectSql, "SELECT * FROM t1, t2");
        }
    },

    "Dataset.unusedTableAlias ":{

        topic:function () {
            return new Dataset().from("test");
        },

        "should return given symbol if it hasn't already been used":function (ds) {
            assert.equal(ds.unusedTableAlias("blah"), "blah")
        },

        "should return a symbol specifying an alias that hasn't already been used if it has already been used":function (ds) {
            assert.equal(ds.unusedTableAlias("test"), "test0");
            assert.equal(ds.from("test", "test0").unusedTableAlias("test"), "test1");
            assert.equal(ds.from("test", "test0").crossJoin("test1").unusedTableAlias("test"), "test2");
        },

        "should return an appropriate symbol if given other forms of identifiers":function (ds) {
            ds.mergeOptions({from:null});
            ds.from("test");
            assert.equal(ds.unusedTableAlias('test'), "test0");
            assert.equal(ds.unusedTableAlias("b__t___test"), "test0");
            assert.equal(ds.unusedTableAlias("b__test"), "test0");
            assert.equal(ds.unusedTableAlias(new Identifier("test").qualify("b")), "test0");
            assert.equal(ds.unusedTableAlias(new Identifier("b").as(new Identifier("test"))), "test0");
            assert.equal(ds.unusedTableAlias(new Identifier("b").as("test")), "test0");
            assert.equal(ds.unusedTableAlias(new Identifier("test")), "test0");
        }
    },

    "Dataset.exists":{
        topic:function () {
            var ds1 = new Dataset().from("test");
            return {
                ds1:ds1,
                ds2:ds1.filter({price:{lt:100}}),
                ds3:ds1.filter({price:{gt:50}})
            };
        },

        "should work in filters":function (ds) {
            assert.equal(ds.ds1.filter(ds.ds2.exists).sql, 'SELECT * FROM test WHERE (EXISTS (SELECT * FROM test WHERE (price < 100)))');
            assert.equal(ds.ds1.filter(ds.ds2.exists.and(ds.ds3.exists)).sql, 'SELECT * FROM test WHERE (EXISTS (SELECT * FROM test WHERE (price < 100)) AND EXISTS (SELECT * FROM test WHERE (price > 50)))');
        },

        "should work in SELECT":function (ds) {
            assert.equal(ds.ds1.select(ds.ds2.exists.as("a"), ds.ds3.exists.as("b")).sql, 'SELECT EXISTS (SELECT * FROM test WHERE (price < 100)) AS a, EXISTS (SELECT * FROM test WHERE (price > 50)) AS b FROM test');
        }
    },

    "Dataset.where":{
        topic:function () {
            var dataset = new Dataset().from("test");
            return {
                dataset:dataset,
                d1:dataset.where({region:"Asia"}),
                d2:dataset.where("region = ?", "Asia"),
                d3:dataset.where("a = 1")
            }
        },

        "should just clone if given an empty argument":function (ds) {
            assert.equal(ds.dataset.where({}).sql, ds.dataset.sql);
            assert.equal(ds.dataset.where([]).sql, ds.dataset.sql);
            assert.equal(ds.dataset.where("").sql, ds.dataset.sql);

            assert.equal(ds.dataset.filter({}).sql, ds.dataset.sql);
            assert.equal(ds.dataset.filter([]).sql, ds.dataset.sql);
            assert.equal(ds.dataset.filter("").sql, ds.dataset.sql);

        },

        "should work with hashes":function (ds) {
            assert.equal(ds.dataset.where({name:'xyz', price:342}).selectSql, "SELECT * FROM test WHERE ((name = 'xyz') AND (price = 342))");
        },

        "should work with a string with placeholders and arguments for those placeholders":function (ds) {
            assert.equal(ds.dataset.where('price < ? AND id in ?', 100, [1, 2, 3]).selectSql, "SELECT * FROM test WHERE (price < 100 AND id in (1, 2, 3))");
        },

        "should not modify passed array with placeholders":function (ds) {
            var a = ['price < ? AND id in ?', 100, 1, 2, 3]
            var b = a.slice(0);
            ds.dataset.where(a);
            assert.deepEqual(b, a);
        },

        "should work with strings (custom sql expressions)":function (ds) {
            assert.equal(ds.dataset.where('(a = 1 AND b = 2)').selectSql, "SELECT * FROM test WHERE ((a = 1 AND b = 2))");
        },

        "should work with a string with named placeholders and a hash of placeholder value arguments":function (ds) {
            assert.equal(ds.dataset.where('price < {price} AND id in {ids}', {price:100, ids:[1, 2, 3]}).selectSql, "SELECT * FROM test WHERE (price < 100 AND id in (1, 2, 3))")
        },

        "should not modify passed array with named placeholders":function (ds) {
            var a = ['price < {price} AND id in {ids}', {price:100}];
            var b = a.slice(0);
            ds.dataset.where(a)
            assert.deepEqual(b, a);
        },

        "should not replace named placeholders that don't existin in the hash":function (ds) {
            assert.equal(ds.dataset.where('price < {price} AND id in {ids}', {price:100}).selectSql, "SELECT * FROM test WHERE (price < 100 AND id in {ids})")
        },

        "should handle partial names":function (ds) {
            assert.equal(ds.dataset.where('price < {price} AND id = {p}', {p:2, price:100}).selectSql, "SELECT * FROM test WHERE (price < 100 AND id = 2)");
        },

        "should affect SELECT, delete and update statements":function (ds) {
            assert.equal(ds.d1.selectSql, "SELECT * FROM test WHERE (region = 'Asia')");
            assert.equal(ds.d1.deleteSql, "DELETE FROM test WHERE (region = 'Asia')");
            assert.equal(ds.d1.updateSql({GDP:0}), "UPDATE test SET GDP = 0 WHERE (region = 'Asia')");

            assert.equal(ds.d2.selectSql, "SELECT * FROM test WHERE (region = 'Asia')");
            assert.equal(ds.d2.deleteSql, "DELETE FROM test WHERE (region = 'Asia')");
            assert.equal(ds.d2.updateSql({GDP:0}), "UPDATE test SET GDP = 0 WHERE (region = 'Asia')");

            assert.equal(ds.d3.selectSql, "SELECT * FROM test WHERE (a = 1)");
            assert.equal(ds.d3.deleteSql, "DELETE FROM test WHERE (a = 1)");
            assert.equal(ds.d3.updateSql({GDP:0}), "UPDATE test SET GDP = 0 WHERE (a = 1)");

        },

        "should be composable using AND operator (for scoping)":function (ds) {
            // hashes are merged, no problem
            assert.equal(ds.d1.where({size:'big'}).selectSql, "SELECT * FROM test WHERE ((region = 'Asia') AND (size = 'big'))");

            // hash and string
            assert.equal(ds.d1.where('population > 1000').selectSql, "SELECT * FROM test WHERE ((region = 'Asia') AND (population > 1000))");
            assert.equal(ds.d1.where('(a > 1) OR (b < 2)').selectSql, "SELECT * FROM test WHERE ((region = 'Asia') AND ((a > 1) OR (b < 2)))");

            // hash and array
            assert.equal(ds.d1.where('GDP > ?', 1000).selectSql, "SELECT * FROM test WHERE ((region = 'Asia') AND (GDP > 1000))");

            // array and array
            assert.equal(ds.d2.where('GDP > ?', 1000).selectSql, "SELECT * FROM test WHERE ((region = 'Asia') AND (GDP > 1000))");

            // array and hash
            assert.equal(ds.d2.where({name:['Japan', 'China']}).selectSql, "SELECT * FROM test WHERE ((region = 'Asia') AND (name IN ('Japan', 'China')))");

            // array and string
            assert.equal(ds.d2.where('GDP > ?').selectSql, "SELECT * FROM test WHERE ((region = 'Asia') AND (GDP > ?))");

            // string and string
            assert.equal(ds.d3.where('b = 2').selectSql, "SELECT * FROM test WHERE ((a = 1) AND (b = 2))");

            // string and hash
            assert.equal(ds.d3.where({c:3}).selectSql, "SELECT * FROM test WHERE ((a = 1) AND (c = 3))");

            // string and array
            assert.equal(ds.d3.where('d = ?', 4).selectSql, "SELECT * FROM test WHERE ((a = 1) AND (d = 4))");

            assert.equal(ds.d3.where({e:{lt:5}}).selectSql, "SELECT * FROM test WHERE ((a = 1) AND (e < 5))");
        },

        "should accept ranges":function (ds) {
            assert.equal(ds.dataset.filter({id:{between:[4, 7]}}).sql, 'SELECT * FROM test WHERE ((id >= 4) AND (id <= 7))');
            assert.equal(ds.dataset.filter({table__id:{between:[4, 7]}}).sql, 'SELECT * FROM test WHERE ((table.id >= 4) AND (table.id <= 7))');
        },

        "should accept null":function (ds) {
            assert.equal(ds.dataset.filter({owner_id:null}).sql, 'SELECT * FROM test WHERE (owner_id IS NULL)');
        },

        "should accept a subquery":function (ds) {
            assert.equal(ds.dataset.filter('gdp > ?', ds.d1.select(new SQLFunction("avg", "gdp"))).sql, "SELECT * FROM test WHERE (gdp > (SELECT avg(gdp) FROM test WHERE (region = 'Asia')))");
        },

        "should handle all types of IN/NOT IN queries":function (ds) {
            assert.equal(ds.dataset.filter({id:ds.d1.select("id")}).sql, "SELECT * FROM test WHERE (id IN (SELECT id FROM test WHERE (region = 'Asia')))");
            assert.equal(ds.dataset.filter({id:[]}).sql, "SELECT * FROM test WHERE (id != id)");
            assert.equal(ds.dataset.filter({id:[1, 2]}).sql, "SELECT * FROM test WHERE (id IN (1, 2))");
            assert.equal(ds.dataset.filter({"id1,id2":ds.d1.select("id1", "id2")}).sql, "SELECT * FROM test WHERE ((id1, id2) IN (SELECT id1, id2 FROM test WHERE (region = 'Asia')))");
            assert.equal(ds.dataset.filter({"id1,id2":[]}).sql, "SELECT * FROM test WHERE ((id1 != id1) AND (id2 != id2))");
            assert.equal(ds.dataset.filter({"id1,id2":[
                [1, 2],
                [3, 4]
            ]}).sql, "SELECT * FROM test WHERE ((id1, id2) IN ((1, 2), (3, 4)))");

            assert.equal(ds.dataset.exclude({id:ds.d1.select("id")}).sql, "SELECT * FROM test WHERE (id NOT IN (SELECT id FROM test WHERE (region = 'Asia')))");
            //assert.equal(ds.dataset.exclude({id : []}).sql, "SELECT * FROM test WHERE (1 = 1)");
            assert.equal(ds.dataset.exclude({id:[1, 2]}).sql, "SELECT * FROM test WHERE (id NOT IN (1, 2))");
            assert.equal(ds.dataset.exclude({"id1,id2":ds.d1.select("id1", "id2")}).sql, "SELECT * FROM test WHERE ((id1, id2) NOT IN (SELECT id1, id2 FROM test WHERE (region = 'Asia')))");
            assert.equal(ds.dataset.exclude({"id1,id2":[]}).sql, "SELECT * FROM test WHERE (1 = 1)");
            assert.equal(ds.dataset.exclude({"id1,id2":[
                [1, 2],
                [3, 4]
            ]}).sql, "SELECT * FROM test WHERE ((id1, id2) NOT IN ((1, 2), (3, 4)))");
        },

        "should accept a subquery for an EXISTS clause":function (ds) {
            var a = ds.dataset.filter({price:{lt:100}});
            assert.equal(ds.dataset.filter(a.exists).sql, 'SELECT * FROM test WHERE (EXISTS (SELECT * FROM test WHERE (price < 100)))');
        },

        "should accept proc expressions":function (ds) {
            var d = ds.d1.select(sql.gdp.avg());

            assert.equal(ds.dataset.filter(
                function () {
                    return this.gdp.gt(d);
                }).sql, "SELECT * FROM test WHERE (gdp > (SELECT avg(gdp) FROM test WHERE (region = 'Asia')))");
            assert.equal(ds.dataset.filter(
                function () {
                    return this.a.lt(1);
                }).sql, 'SELECT * FROM test WHERE (a < 1)');

            assert.equal(ds.dataset.filter(
                function () {
                    return this.a.gte(1).and(this.b.lte(2));
                }).sql, 'SELECT * FROM test WHERE ((a >= 1) AND (b <= 2))');

            assert.equal(ds.dataset.filter(
                function () {
                    return this.c.like('ABC%');
                }).sql, "SELECT * FROM test WHERE (c LIKE 'ABC%')");

            assert.equal(ds.dataset.filter(
                function () {
                    return this.c.like('ABC%', '%XYZ');
                }).sql, "SELECT * FROM test WHERE ((c LIKE 'ABC%') OR (c LIKE '%XYZ'))");
        },

        "should work for grouped datasets":function (ds) {
            assert.equal(ds.dataset.group("a").filter({b:1}).sql, 'SELECT * FROM test WHERE (b = 1) GROUP BY a');
        },

        "should accept true and false as arguments":function (ds) {
            assert.equal(ds.dataset.filter(true).sql, "SELECT * FROM test WHERE 't'");
            assert.equal(ds.dataset.filter(false).sql, "SELECT * FROM test WHERE 'f'");
        },

        "should allow the use of multiple arguments":function (ds) {
            assert.equal(ds.dataset.filter(new Identifier("a"), new Identifier("b")).sql, 'SELECT * FROM test WHERE (a AND b)');
            assert.equal(ds.dataset.filter(new Identifier("a"), {b:1}).sql, 'SELECT * FROM test WHERE (a AND (b = 1))');
            assert.equal(ds.dataset.filter(new Identifier("a"), {c:{gt:3}}, {b:1}).sql, 'SELECT * FROM test WHERE (a AND (c > 3) AND (b = 1))');
        },

        "should allow the use of blocks and arguments simultaneously":function (ds) {
            assert.equal(ds.dataset.filter({zz:{lt:3}},
                function () {
                    return this.yy.gt(3);
                }).sql, 'SELECT * FROM test WHERE ((zz < 3) AND (yy > 3))');
        },

        "should yield an sql object to the cb":function (ds) {
            var x = null;
            ds.dataset.filter(function (r) {
                x = r;
                return false;
            });
            assert.deepEqual(x, sql);
            assert.equal(ds.dataset.filter(
                function (test) {
                    return test.name.lt("b").and(test.table__id.eq(1)).or(test.is_active(test.blah, test.xx, test.x__y_z));
                }).sql, "SELECT * FROM test WHERE (((name < 'b') AND (table.id = 1)) OR is_active(blah, xx, x.y_z))");
        },

        "should eval the block in the context of sql if sql isnt an arugment":function (ds) {
            var x = null;
            ds.dataset.filter(function (r) {
                x = this;
                return false;
            });
            assert.deepEqual(x, sql);
            assert.equal(ds.dataset.filter(
                function (test) {
                    return this.name.lt("b").and(this.table__id.eq(1)).or(this.is_active(this.blah(), this.xx(), this.x__y_z()));
                }).sql, "SELECT * FROM test WHERE (((name < 'b') AND (table.id = 1)) OR is_active(blah, xx, x.y_z))");
        },


        "should raise an error if an invalid argument is used":function (ds) {
            assert.throws(comb.hitch(ds.dataset, "filter", 1));
        },

        "should be backwards comptable with the old query style":function () {

        }
    },

    "Dataset.or":{
        topic:function () {
            var dataset = new Dataset().from("test");
            return {
                dataset:dataset,
                d1:dataset.where({x:1})
            }
        },

        "should raise if no filter exists":function (ds) {
            var dataset = ds.dataset;
            assert.throws(comb.hitch(dataset, "or", {a:1}));
        },

        "should add an alternative expression to the where clause":function (ds) {
            assert.equal(ds.d1.or({y:2}).sql, "SELECT * FROM test WHERE ((x = 1) OR (y = 2))");
        },

        "should accept all forms of filters":function (ds) {
            assert.equal(ds.d1.or("y > ?", 2).sql, 'SELECT * FROM test WHERE ((x = 1) OR (y > 2))');
            assert.equal(ds.d1.or({yy:{gt:3}}).sql, 'SELECT * FROM test WHERE ((x = 1) OR (yy > 3))');
            assert.equal(ds.d1.or(sql.yy.gt(3)).sql, 'SELECT * FROM test WHERE ((x = 1) OR (yy > 3))');
        },

        "should correctly add parens to give predictable results":function (ds) {
            assert.equal(ds.d1.filter({y:2}).or({z:3}).sql, 'SELECT * FROM test WHERE (((x = 1) AND (y = 2)) OR (z = 3))');
            assert.equal(ds.d1.or({y:2}).filter({z:3}).sql, 'SELECT * FROM test WHERE (((x = 1) OR (y = 2)) AND (z = 3))');
        }

    },

    "Dataset.and":{
        topic:function () {
            var dataset = new Dataset().from("test");
            return {
                dataset:dataset,
                d1:dataset.where({x:1})
            }
        },

        "should raise if no filter exists":function (ds) {
            var dataset = ds.dataset;
            assert.throws(comb.hitch(dataset, "and", {a:1}));
        },

        "should add an alternative expression to the where clause":function (ds) {
            assert.equal(ds.d1.and({y:2}).sql, "SELECT * FROM test WHERE ((x = 1) AND (y = 2))");
        },

        "should accept all forms of filters":function (ds) {
            assert.equal(ds.d1.and("y > ?", 2).sql, 'SELECT * FROM test WHERE ((x = 1) AND (y > 2))');
            assert.equal(ds.d1.and({yy:{gt:3}}).sql, 'SELECT * FROM test WHERE ((x = 1) AND (yy > 3))');
        },

        "should correctly add parens to give predictable results":function (ds) {
            assert.equal(ds.d1.and({y:2}).or({z:3}).sql, 'SELECT * FROM test WHERE (((x = 1) AND (y = 2)) OR (z = 3))');
            assert.equal(ds.d1.or({y:2}).and({z:3}).sql, 'SELECT * FROM test WHERE (((x = 1) OR (y = 2)) AND (z = 3))');
        }

    },

    "dataset.exclude":{
        topic:new Dataset().from("test"),

        "should correctly negate the expression when one condition is given":function (ds) {
            assert.equal(ds.exclude({region:'Asia'}).selectSql, "SELECT * FROM test WHERE (region != 'Asia')");
        },

        "should take multiple conditions as a hash and express the logic correctly in SQL":function (ds) {
            assert.equal(ds.exclude({region:'Asia', name:'Japan'}).selectSql, "SELECT * FROM test WHERE ((region != 'Asia') OR (name != 'Japan'))");
        },

        "should parenthesize a single string condition correctly":function (ds) {
            assert.equal(ds.exclude("region = 'Asia' AND name = 'Japan'").selectSql, "SELECT * FROM test WHERE NOT (region = 'Asia' AND name = 'Japan')")
        },

        "should parenthesize an array condition correctly":function (ds) {
            assert.equal(ds.exclude('region = ? AND name = ?', 'Asia', 'Japan').selectSql, "SELECT * FROM test WHERE NOT (region = 'Asia' AND name = 'Japan')");
        },

        "should correctly parenthesize when it is used twice":function (ds) {
            assert.equal(ds.exclude({region:'Asia'}).exclude({name:'Japan'}).selectSql, "SELECT * FROM test WHERE ((region != 'Asia') AND (name != 'Japan'))");
        },

        "should support proc expressions":function (ds) {
            assert.equal(ds.exclude({id:{lt:6}}).selectSql, 'SELECT * FROM test WHERE (id >= 6)');
        }
    },


    "dataset.invert":{

        topic:new Dataset().from("test"),

        "should raise error if the dataset is not filtered":function (ds) {
            assert.throws(comb.hitch(ds, "invert"));
        },

        "should invert current filter if dataset is filtered":function (ds) {
            assert.equal(ds.filter(new sql.Identifier("x")).invert().sql, 'SELECT * FROM test WHERE NOT x');
        },

        "should invert both having and where if both are preset":function (ds) {
            var ident = new sql.Identifier("x");
            assert.equal(ds.filter(ident).group(ident).having(ident).invert().sql, 'SELECT * FROM test WHERE NOT x GROUP BY x HAVING NOT x');
        }
    },

    "dataset.having":{
        topic:function () {
            var dataset = new Dataset().from("test");
            var grouped = dataset.group(sql.region).select(sql.region, sql.population.sum(), sql.gdp.avg());
            var d1 = grouped.having(sql.sum("population").gt(10));
            var d2 = grouped.having({region:'Asia'});
            var columns = "region, sum(population), avg(gdp)";
            return {
                dataset:dataset,
                grouped:grouped,
                d1:d1,
                d2:d2,
                columns:columns
            }
        },

        "should just clone if given an empty argument":function (topic) {
            var dataset = topic.dataset;
            assert.equal(dataset.having({}).sql, dataset.sql);
            assert.equal(dataset.having([]).sql, dataset.sql);
            assert.equal(dataset.having('').sql, dataset.sql);
        },

        "should affect SELECT statements":function (topic) {
            assert.equal(
                topic.d1.selectSql,
                "SELECT " + topic.columns + " FROM test GROUP BY region HAVING (sum(population) > 10)");
        },

        "should support proc expressions":function (topic) {
            assert.equal(topic.grouped.having(sql.sum("population").gt(10)).sql, "SELECT " + topic.columns + " FROM test GROUP BY region HAVING (sum(population) > 10)");
        },

        "should work with and on the having clause":function (topic) {
            assert.equal(topic.grouped.having(sql.a().sqlNumber.gt(1)).and(sql.b().sqlNumber.lt(2)).sql, "SELECT " + topic.columns + " FROM test GROUP BY region HAVING ((a > 1) AND (b < 2))");
        }
    },

    "a grouped dataset":{
        topic:function () {
            return new Dataset().from("test").group("type_id");
        },

        "should raise when trying to generate an update statement":function (dataset) {
            assert.throws(function () {
                dataset.updateSql({id:0});
            });
        },

        "should raise when trying to generate a delete statement":function (dataset) {
            assert.throws(function () {
                dataset.deleteSql;
            });
        },

        "should raise when trying to generate a truncate statement":function (dataset) {
            assert.throws(function () {
                dataset.truncateSql;
            });
        },

        "should raise when trying to generate an insert statement":function (dataset) {
            assert.throws(function () {
                dataset.insertSql()
            });
        },

        "should specify the grouping in generated SELECT statement":function (dataset) {
            assert.equal(dataset.selectSql, "SELECT * FROM test GROUP BY type_id");
        }
    },

    "Dataset.groupBy":{
        topic:function () {
            return new Dataset().from("test").groupBy("type_id");
        },

        "should raise when trying to generate an update statement":function (dataset) {
            assert.throws(function () {
                dataset.updateSql({id:0});
            });
        },

        "should raise when trying to generate a delete statement":function (dataset) {
            assert.throws(function () {
                dataset.deleteSql;
            });
        },

        "should raise when trying to generate a truncate statement":function (dataset) {
            assert.throws(function () {
                dataset.truncateSql;
            });
        },

        "should raise when trying to generate an insert statement":function (dataset) {
            assert.throws(function () {
                dataset.insertSql()
            });
        },

        "should specify the grouping in generated SELECT statement":function (dataset) {
            assert.equal(dataset.selectSql, "SELECT * FROM test GROUP BY type_id");
            assert.equal(dataset.groupBy("a", "b").selectSql, "SELECT * FROM test GROUP BY a, b");
            assert.equal(dataset.groupBy({type_id:null}).selectSql, "SELECT * FROM test GROUP BY (type_id IS NULL)");
        },

        "should ungroup when passed null, empty, or no arguments":function (dataset) {
            assert.equal(dataset.groupBy().selectSql, "SELECT * FROM test");
            assert.equal(dataset.groupBy(null).selectSql, "SELECT * FROM test");
        },

        "should undo previous grouping":function (dataset) {
            assert.equal(dataset.groupBy("a").groupBy("b").selectSql, "SELECT * FROM test GROUP BY b");
            assert.equal(dataset.groupBy("a", "b").groupBy().selectSql, "SELECT * FROM test");
        },

        "should be aliased as #group":function (dataset) {
            assert.equal(dataset.group({type_id:null}).selectSql, "SELECT * FROM test GROUP BY (type_id IS NULL)");
        }
    },

    "Dataset.as":{
        topic:new Dataset().from("test"),
        "should set up an alias":function (dataset) {
            assert.equal(dataset.select(dataset.limit(1).select("name").as("n")).sql, 'SELECT (SELECT name FROM test LIMIT 1) AS n FROM test');
        }
    },

    "Dataset.literal":{
        topic:new Dataset().from("test"),

        "should escape strings properly":function (dataset) {
            assert.equal(dataset.literal('abc'), "'abc'");
            assert.equal(dataset.literal('a"x"bc'), "'a\"x\"bc'");
            assert.equal(dataset.literal("a'bc"), "'a''bc'");
            assert.equal(dataset.literal("a''bc"), "'a''''bc'");
            assert.equal(dataset.literal("a\\bc"), "'a\\\\bc'");
            assert.equal(dataset.literal("a\\\\bc"), "'a\\\\\\\\bc'");
            assert.equal(dataset.literal("a\\'bc"), "'a\\\\''bc'");
        },

        "should literalize numbers properly":function (dataset) {
            assert.equal(dataset.literal(1), "1");
            assert.equal(dataset.literal(1.5), "1.5");
        },

        "should literalize nil as NULL":function (dataset) {
            assert.equal(dataset.literal(null), "NULL");
        },

        "should literalize an array properly":function (dataset) {
            assert.equal(dataset.literal([]), "(NULL)");
            assert.equal(dataset.literal([1, 'abc', 3]), "(1, 'abc', 3)");
            assert.equal(dataset.literal([1, "a'b''c", 3]), "(1, 'a''b''''c', 3)");
        },

        "should literalize symbols as column references":function (dataset) {
            assert.equal(dataset.literal(sql.name), "name");
            assert.equal(dataset.literal("items__name"), "items.name");
        },

        "should call sql_literal with dataset on type if not natively supported and the object responds to it":function (dataset) {
            var a = function () {
            };
            a.prototype.sqlLiteral = function (ds) {
                return  "called ";
            };
            assert.equal(dataset.literal(new a()), "called ");
        },

        "should raise an error for unsupported types with no sql_literal method":function (dataset) {
            assert.throws(function () {
                dataset.literal(new function () {
                });
            });
        },

        "should literalize datasets as subqueries":function (dataset) {
            var d = dataset.from("test");
            assert.equal(d.literal(d), "(" + d.sql + ")");
        },


        "should literalize TimeStamp properly":function (dataset) {
            var d = new sql.TimeStamp();
            assert.equal(dataset.literal(d), comb.date.format(d.date, "'yyyy-MM-dd HH:mm:ss'"));
        },


        "should literalize Date properly":function (dataset) {
            var d = new Date();
            assert.equal(dataset.literal(d), comb.string.format("'%[yyyy-MM-dd]D'", [d]));
        },

        "should not modify literal strings":function (dataset) {
            assert.equal(dataset.literal(sql['col1 + 2']), 'col1 + 2');
            assert.equal(dataset.updateSql({a:sql['a + 2']}), 'UPDATE test SET a = a + 2');
        }
    },

    "Dataset.from":{
        topic:new Dataset(),

        "should accept a Dataset":function (dataset) {
            assert.doesNotThrow(function () {
                dataset.from(dataset)
            });
        },

        "should format a Dataset as a subquery if it has had options set":function (dataset) {
            assert.equal(dataset.from(dataset.from("a").where({a:1})).selectSql, "SELECT * FROM (SELECT * FROM a WHERE (a = 1)) AS t1");
        },

        "should automatically alias sub-queries":function (dataset) {
            assert.equal(dataset.from(dataset.from("a").group("b")).selectSql, "SELECT * FROM (SELECT * FROM a GROUP BY b) AS t1");

            var d1 = dataset.from("a").group("b");
            var d2 = dataset.from("c").group("d");

            assert.equal(dataset.from(d1, d2).sql, "SELECT * FROM (SELECT * FROM a GROUP BY b) AS t1, (SELECT * FROM c GROUP BY d) AS t2");
        },

        "should accept a hash for aliasing":function (dataset) {
            assert.equal(dataset.from({a:"b"}).sql, "SELECT * FROM a AS b");
            assert.equal(dataset.from(dataset.from("a").group("b").as("c")).sql, "SELECT * FROM (SELECT * FROM a GROUP BY b) AS c");
        },

        "should always use a subquery if given a dataset":function (dataset) {
            assert.equal(dataset.from(dataset.from("a")).selectSql, "SELECT * FROM (SELECT * FROM a) AS t1");
        },

        "should remove all FROM tables if called with no arguments":function (dataset) {
            assert.equal(dataset.from().sql, 'SELECT *');
        },

        "should accept sql functions":function (dataset) {
            assert.equal(dataset.from(sql.abc("def")).selectSql, "SELECT * FROM abc(def)");
            assert.equal(dataset.from(sql.a("i")).selectSql, "SELECT * FROM a(i)");
        },

        "should accept :schema__table___alias symbol format":function (dataset) {
            assert.equal(dataset.from("abc__def").selectSql, "SELECT * FROM abc.def");
            assert.equal(dataset.from("abc__def___d").selectSql, "SELECT * FROM abc.def AS d");
            assert.equal(dataset.from("abc___def").selectSql, "SELECT * FROM abc AS def");
        }
    },

    "Dataset.SELECT":{
        topic:new Dataset().from("test"),

        "should accept variable arity":function (dataset) {
            assert.equal(dataset.select("name").sql, 'SELECT name FROM test');
            assert.equal(dataset.select("a", "b", "test__c").sql, 'SELECT a, b, test.c FROM test');
        },

        "should accept symbols and literal strings":function (dataset) {
            assert.equal(dataset.select("aaa").sql, 'SELECT aaa FROM test');
            assert.equal(dataset.select("a", "b").sql, 'SELECT a, b FROM test');
            assert.equal(dataset.select("test__cc", 'test.d AS e').sql, 'SELECT test.cc, test.d AS e FROM test');
            assert.equal(dataset.select('test.d AS e', "test__cc").sql, 'SELECT test.d AS e, test.cc FROM test');

            assert.equal(dataset.select("test.*").sql, 'SELECT test.* FROM test');
            assert.equal(dataset.select(sql["test__name"].as("n")).sql, 'SELECT test.name AS n FROM test');
            assert.equal(dataset.select("test__name___n").sql, 'SELECT test.name AS n FROM test');
        },

        "should use the wildcard if no arguments are given":function (dataset) {
            assert.equal(dataset.select().sql, 'SELECT * FROM test');
        },

        "should accept a hash for AS values":function (dataset) {
            assert.equal(dataset.select({name:'n', "__ggh":'age'}).sql, "SELECT name AS n, __ggh AS age FROM test");
        },

        "should accept arbitrary objects and literalize them correctly":function (dataset) {
            assert.equal(dataset.select(1, "a", "\'t\'").sql, "SELECT 1, a, 't' FROM test");
            assert.equal(dataset.select(null, sql.sum("t"), "x___y").sql, "SELECT NULL, sum(t), x AS y FROM test");
            assert.equal(dataset.select(null, 1, {x:"y"}).sql, "SELECT NULL, 1, x AS y FROM test");
        },

        "should accept a block that yields a virtual row":function (dataset) {
            var a = dataset.select(function (o) {
                return o.a;
            });
            var b = dataset.select(function () {
                return this.a(1);
            });
            var c = dataset.select(function (o) {
                return o.a(1, 2);
            });
            var d = dataset.select(function () {
                return ["a", this.a(1, 2)];
            });
            assert.equal(a.sql, "SELECT a FROM test");
            assert.equal(b.sql, 'SELECT a(1) FROM test');
            assert.equal(c.sql, 'SELECT a(1, 2) FROM test');
            assert.equal(d.sql, 'SELECT a, a(1, 2) FROM test');
        },

        "should merge regular arguments with argument returned from block":function (dataset) {
            var a = dataset.select("b", function () {
                return "a";
            });
            var b = dataset.select("b", "c", function (o) {
                return o.a(1);
            });
            var c = dataset.select("b", function () {
                return ["a", this.a(1, 2)];
            });
            var d = dataset.select("b", "c", function (o) {
                return [o.a, o.a(1, 2)];
            });
            assert.equal(a.sql, "SELECT b, a FROM test");
            assert.equal(b.sql, "SELECT b, c, a(1) FROM test");
            assert.equal(c.sql, 'SELECT b, a, a(1, 2) FROM test');
            assert.equal(d.sql, 'SELECT b, c, a, a(1, 2) FROM test');
        }
    },

    "Dataset.selectAll":{

        topic:new Dataset().from("test"),

        "should SELECT the wildcard":function (dataset) {
            assert.equal(dataset.selectAll().sql, 'SELECT * FROM test');
        },

        "should overrun the previous SELECT option":function (dataset) {
            assert.equal(dataset.select("a", "b", "c").selectAll().sql, 'SELECT * FROM test');
        }
    },

    "Dataset.selectMore":{
        topic:new Dataset().from("test"),

        "should act like #SELECT for datasets with no selection":function (dataset) {
            assert.equal(dataset.selectMore("a", "b").sql, 'SELECT a, b FROM test');
            assert.equal(dataset.selectAll().selectMore("a", "b").sql, 'SELECT a, b FROM test');
            assert.equal(dataset.select("blah").selectAll().selectMore("a", "b").sql, 'SELECT a, b FROM test');
        },

        "should add to the currently selected columns":function (dataset) {
            assert.equal(dataset.select("a").selectMore("b").sql, 'SELECT a, b FROM test');
            assert.equal(dataset.select("a.*").selectMore("b.*").sql, 'SELECT a.*, b.* FROM test');
        },

        "should accept a block that yields a virtual row":function (dataset) {
            assert.equal(dataset.select("a").selectMore(
                function (o) {
                    return o.b;
                }).sql, 'SELECT a, b FROM test');
            assert.equal(dataset.select("a.*").selectMore("b.*",
                function () {
                    return this.b(1)
                }).sql, 'SELECT a.*, b.*, b(1) FROM test');
        }
    },

    "Dataset.selectAppend":{
        topic:new Dataset().from("test"),

        "should SELECT * in addition to columns if no columns selected":function (dataset) {
            assert.equal(dataset.selectAppend("a", "b").sql, 'SELECT *, a, b FROM test');
            assert.equal(dataset.selectAll().selectAppend("a", "b").sql, 'SELECT *, a, b FROM test');
            assert.equal(dataset.select("blah").selectAll().selectAppend("a", "b").sql, 'SELECT *, a, b FROM test');
        },

        "should add to the currently selected columns":function (dataset) {
            assert.equal(dataset.select("a").selectAppend("b").sql, 'SELECT a, b FROM test');
            assert.equal(dataset.select("a.*").selectAppend("b.*").sql, 'SELECT a.*, b.* FROM test');
        },

        "should accept a block that yields a virtual row":function (dataset) {
            assert.equal(dataset.select("a").selectAppend(
                function (o) {
                    return o.b
                }).sql, 'SELECT a, b FROM test');
            assert.equal(dataset.select("a.*").selectAppend("b.*",
                function () {
                    return this.b(1)
                }).sql, 'SELECT a.*, b.*, b(1) FROM test');
        }
    },

    "Dataset.order":{
        topic:new Dataset().from("test"),

        "should include an ORDER BY clause in the SELECT statement":function (dataset) {
            assert.equal(dataset.order("name").sql, 'SELECT * FROM test ORDER BY name');
        },

        "should accept multiple arguments":function (dataset) {
            assert.equal(dataset.order("name", sql.price.desc()).sql, 'SELECT * FROM test ORDER BY name, price DESC');
        },

        "should accept :nulls options for asc and desc":function (dataset) {
            assert.equal(dataset.order(sql.name.asc({nulls:"last"}), sql.price.desc({nulls:"first"})).sql, 'SELECT * FROM test ORDER BY name ASC NULLS LAST, price DESC NULLS FIRST');
        },

        "should overrun a previous ordering":function (dataset) {
            assert.equal(dataset.order("name").order("stamp").sql, 'SELECT * FROM test ORDER BY stamp');
        },

        "should accept a literal string":function (dataset) {
            assert.equal(dataset.order(sql['dada ASC']).sql, 'SELECT * FROM test ORDER BY dada ASC');
        },

        "should accept a hash as an expression":function (dataset) {
            assert.equal(dataset.order({name:null}).sql, 'SELECT * FROM test ORDER BY (name IS NULL)');
        },

        "should accept a nil to remove ordering":function (dataset) {
            assert.equal(dataset.order("bah").order(null).sql, 'SELECT * FROM test');
        },

        "should accept a block that yields a virtual row":function (dataset) {
            assert.equal(dataset.order(
                function (o) {
                    return o.a
                }).sql, 'SELECT * FROM test ORDER BY a');
            assert.equal(dataset.order(
                function () {
                    return this.a(1)
                }).sql, 'SELECT * FROM test ORDER BY a(1)');
            assert.equal(dataset.order(
                function (o) {
                    return o.a(1, 2)
                }).sql, 'SELECT * FROM test ORDER BY a(1, 2)');
            assert.equal(dataset.order(
                function () {
                    return [this.a, this.a(1, 2)]
                }).sql, 'SELECT * FROM test ORDER BY a, a(1, 2)');
        },

        "should merge regular arguments with argument returned from block":function (dataset) {
            assert.equal(dataset.order("b",
                function () {
                    return this.a
                }).sql, 'SELECT * FROM test ORDER BY b, a');
            assert.equal(dataset.order("b", "c",
                function (o) {
                    return o.a(1)
                }).sql, 'SELECT * FROM test ORDER BY b, c, a(1)');
            assert.equal(dataset.order("b",
                function () {
                    return [this.a, this.a(1, 2)]
                }).sql, 'SELECT * FROM test ORDER BY b, a, a(1, 2)');
            assert.equal(dataset.order("b", "c",
                function (o) {
                    return [o.a, o.a(1, 2)]
                }).sql, 'SELECT * FROM test ORDER BY b, c, a, a(1, 2)');
        }


    },

    "Dataset.unfiltered":{
        topic:new Dataset().from("test"),
        "should remove filtering from the dataset":function (dataset) {
            assert.equal(dataset.filter({score:1}).unfiltered().sql, 'SELECT * FROM test');
        }
    },

    "Dataset.unlimited":{
        topic:new Dataset().from("test"),
        "should remove limit and offset from the dataset":function (dataset) {
            assert.equal(dataset.limit(1, 2).unlimited().sql, 'SELECT * FROM test');
        }
    },

    "Dataset.ungrouped":{
        topic:new Dataset().from("test"),
        "should remove group and having clauses from the dataset":function (dataset) {
            assert.equal(dataset.group("a").having("b").ungrouped().sql, 'SELECT * FROM test');
        }
    },

    "Dataset.unordered":{
        topic:new Dataset().from("test"),
        "should remove ordering from the dataset":function (dataset) {
            assert.equal(dataset.order("name").unordered().sql, 'SELECT * FROM test');
        }
    },
    "Dataset.withSql":{
        topic:new Dataset().from("test"),

        "should use static sql":function (dataset) {
            assert.equal(dataset.withSql('SELECT 1 FROM test').sql, 'SELECT 1 FROM test');
        },

        "should work with placeholders":function (dataset) {
            assert.equal(dataset.withSql('SELECT ? FROM test', 1).sql, 'SELECT 1 FROM test');
        },

        "should work with named placeholders":function (dataset) {
            assert.equal(dataset.withSql('SELECT {x} FROM test', {x:1}).sql, 'SELECT 1 FROM test');
        }
    },

    "Dataset.orderBy":{
        topic:new Dataset().from("test"),

        "should include an ORDER BY clause in the SELECT statement":function (dataset) {
            dataset.orderBy("name").sql, 'SELECT * FROM test ORDER BY name'
        },

        "should accept multiple arguments":function (dataset) {
            assert.equal(dataset.orderBy("name", sql.price.desc()).sql, 'SELECT * FROM test ORDER BY name, price DESC');
        },

        "should overrun a previous ordering":function (dataset) {
            assert.equal(dataset.orderBy("name").order("stamp").sql, 'SELECT * FROM test ORDER BY stamp');
        },

        "should accept a string":function (dataset) {
            assert.equal(dataset.orderBy('dada ASC').sql, 'SELECT * FROM test ORDER BY dada ASC');
        },

        "should accept a nil to remove ordering":function (dataset) {
            assert.equal(dataset.orderBy("bah").orderBy(null).sql, 'SELECT * FROM test');
        }
    },


    "Dataset['orderMore|orderAppend|orderPrepend']":{
        topic:new Dataset().from("test"),

        "should include an ORDER BY clause in the SELECT statement":function (dataset) {
            assert.equal(dataset.orderMore("name").sql, 'SELECT * FROM test ORDER BY name');
            assert.equal(dataset.orderAppend("name").sql, 'SELECT * FROM test ORDER BY name');
            assert.equal(dataset.orderPrepend("name").sql, 'SELECT * FROM test ORDER BY name');
        },

        "should add to the }, of a previous ordering":function (dataset) {
            assert.equal(dataset.order("name").orderMore(sql.stamp.desc()).sql, 'SELECT * FROM test ORDER BY name, stamp DESC');
            assert.equal(dataset.order("name").orderAppend(sql.stamp.desc()).sql, 'SELECT * FROM test ORDER BY name, stamp DESC');
            assert.equal(dataset.order("name").orderPrepend(sql.stamp.desc()).sql, 'SELECT * FROM test ORDER BY stamp DESC, name');
        },

        "should accept a block that returns a filter":function (dataset) {
            assert.equal(dataset.order("a").orderMore(function (o) {
                    return o.b;
                }).sql, 'SELECT * FROM test ORDER BY a, b');
            assert.equal(dataset.order("a", "b").orderMore("c", "d", function () {
                    return [this.e, this.f(1, 2)];
                }).sql, 'SELECT * FROM test ORDER BY a, b, c, d, e, f(1, 2)');
            assert.equal(dataset.order("a").orderAppend(function (o) {
                    return o.b;
                }).sql, 'SELECT * FROM test ORDER BY a, b');
            assert.equal(dataset.order("a", "b").orderAppend("c", "d", function () {
                    return [this.e, this.f(1, 2)];
                }).sql, 'SELECT * FROM test ORDER BY a, b, c, d, e, f(1, 2)');

            assert.equal(dataset.order("a").orderPrepend(function (o) {
                return o.b;
            }).sql, 'SELECT * FROM test ORDER BY b, a');
            assert.equal(dataset.order("a", "b").orderPrepend("c", "d", function () {
                return [this.e, this.f(1, 2)];
            }).sql, 'SELECT * FROM test ORDER BY c, d, e, f(1, 2), a, b');
        }
    },

    "Dataset.reverseOrder":{
        topic:new Dataset().from("test"),

        "should use DESC as default order":function (dataset) {
            assert.equal(dataset.reverseOrder("name").sql, 'SELECT * FROM test ORDER BY name DESC');
        },

        "should invert the order given":function (dataset) {
            assert.equal(dataset.reverseOrder(sql.name.desc()).sql, 'SELECT * FROM test ORDER BY name ASC');
        },

        "should invert the order for ASC expressions":function (dataset) {
            assert.equal(dataset.reverseOrder(sql.name.asc()).sql, 'SELECT * FROM test ORDER BY name DESC');
        },

        "should accept multiple arguments":function (dataset) {
            assert.equal(dataset.reverseOrder("name", sql.price.desc()).sql, 'SELECT * FROM test ORDER BY name DESC, price ASC');
        },

        "should handles NULLS ordering correctly when reversing":function (dataset) {
            assert.equal(dataset.reverseOrder(sql.name.asc({nulls:"first"}), sql.price.desc({nulls:"last"})).sql, 'SELECT * FROM test ORDER BY name DESC NULLS LAST, price ASC NULLS FIRST');
        },

        "should reverse a previous ordering if no arguments are given":function (dataset) {
            assert.equal(dataset.order("name").reverseOrder().sql, 'SELECT * FROM test ORDER BY name DESC');
            assert.equal(dataset.order(sql.clumsy.desc(), "fool").reverseOrder().sql, 'SELECT * FROM test ORDER BY clumsy ASC, fool DESC');
        },

        "should return an unordered dataset for a dataset with no order":function (dataset) {
            assert.equal(dataset.unordered().reverseOrder().sql, 'SELECT * FROM test');
        },

        "should have reverse alias":function (dataset) {
            assert.equal(dataset.order("name").reverse().sql, 'SELECT * FROM test ORDER BY name DESC');
        }
    },

    "Dataset.limit":{
        topic:new Dataset().from("test"),

        "should include a LIMIT clause in the SELECT statement":function (dataset) {
            assert.equal(dataset.limit(10).sql, 'SELECT * FROM test LIMIT 10');
        },

        "should accept ranges":function (dataset) {
            assert.equal(dataset.limit([3, 7]).sql, 'SELECT * FROM test LIMIT 5 OFFSET 3');
        },

        "should include an offset if a second argument is given":function (dataset) {
            assert.equal(dataset.limit(6, 10).sql, 'SELECT * FROM test LIMIT 6 OFFSET 10');
        },

        "should convert regular strings to integers":function (dataset) {
            assert.equal(dataset.limit('6', 'a() - 1').sql, 'SELECT * FROM test LIMIT 6 OFFSET 0');
        },

        "should not convert literal strings to integers":function (dataset) {
            assert.equal(dataset.limit('6', sql['a() - 1']).sql, 'SELECT * FROM test LIMIT 6 OFFSET a() - 1');
        },

        "should not convert other objects":function (dataset) {
            assert.equal(dataset.limit(6, new sql.SQLFunction("a").minus(1)).sql, 'SELECT * FROM test LIMIT 6 OFFSET (a() - 1)');
        },

        "should work with fixed sql datasets":function (dataset) {
            dataset.__opts["sql"] = 'SELECT * from cccc';
            assert.equal(dataset.limit(6, 10).sql, 'SELECT * FROM (SELECT * from cccc) AS t1 LIMIT 6 OFFSET 10');
        },

        "should raise an error if an invalid limit or offset is used":function (dataset) {
            assert.throws(function () {
                dataset.limit(-1)
            });
            assert.throws(function () {
                dataset.limit(0)
            });
            assert.doesNotThrow(function () {
                dataset.limit(1)
            });
            assert.throws(function () {
                dataset.limit(1, -1)
            });
            assert.doesNotThrow(function () {
                dataset.limit(1, 0)
            });
            assert.doesNotThrow(function () {
                dataset.limit(1, 1)
            });
        }
    },

    "Dataset.qualifiedColumnName":{
        topic:new Dataset().from("test"),

        "should return the literal value if not given a symbol":function (dataset) {
            assert.equal(dataset.literal(dataset.qualifiedColumnName(new sql.LiteralString("'ccc__b'"), "items")), "'ccc__b'");
            assert.equal(dataset.literal(dataset.qualifiedColumnName(3), "items"), '3');
            assert.equal(dataset.literal(dataset.qualifiedColumnName(new sql.LiteralString("a")), "items"), 'a');
        },

        "should qualify the column with the supplied table name if given an unqualified symbol":function (dataset) {
            assert.equal(dataset.literal(dataset.qualifiedColumnName("b1", "items")), 'items.b1');
        },

        "should not changed the qualifed column's table if given a qualified symbol":function (dataset) {
            assert.equal(dataset.literal(dataset.qualifiedColumnName("ccc__b", "items")), 'ccc.b');
        }
    },

    "Dataset.firstSourceAlias":{
        topic:new Dataset(),

        "should be the entire first source if not aliased":function (ds) {
            assert.deepEqual(ds.from("t").firstSourceAlias, new Identifier("t"));
            assert.deepEqual(ds.from(new sql.Identifier("t__a")).firstSourceAlias, new sql.Identifier("t__a"));
            assert.deepEqual(ds.from("s__t").firstSourceAlias, new QualifiedIdentifier("s", "t"));
            assert.deepEqual(ds.from(sql.t.qualify("s")).firstSourceAlias.table, sql.t.qualify("s").table);
            assert.deepEqual(ds.from(sql.t.qualify("s")).firstSourceAlias.column.value, sql.t.qualify("s").column.value);
        },


        "should be the alias if aliased":function (ds) {
            assert.equal(ds.from("t___a").firstSourceAlias, "a");
            assert.equal(ds.from("s__t___a").firstSourceAlias, "a");
            assert.equal(ds.from(sql.t.as("a")).firstSourceAlias, "a");
        },


        "should be aliased as firstSource":function (ds) {
            assert.deepEqual(ds.from("t").firstSourceAlias, new Identifier("t"));
            assert.deepEqual(ds.from(new sql.Identifier("t__a")).firstSourceAlias, new sql.Identifier("t__a"));
            assert.equal(ds.from("s__t___a").firstSourceAlias, "a");
            assert.equal(ds.from(sql.t.as("a")).firstSourceAlias, "a");
        },

        "should raise exception if table doesn't have a source":function (ds) {
            assert.throws(function(){
                ds.firstSourceAlias;
            });
        }

    },

    "Dataset.firstSourceTable":{
        topic:new Dataset(),

        "should be the entire first source if not aliased":function (ds) {
            assert.deepEqual(ds.from("t").firstSourceTable, new Identifier("t"));
            assert.deepEqual(ds.from(new sql.Identifier("t__a")).firstSourceTable, new sql.Identifier("t__a"));
            assert.deepEqual(ds.from("s__t").firstSourceTable, new QualifiedIdentifier("s", "t"));
            assert.deepEqual(ds.from(sql.t.qualify("s")).firstSourceTable.table, sql.t.qualify("s").table);
            assert.deepEqual(ds.from(sql.t.qualify("s")).firstSourceTable.column.value, sql.t.qualify("s").column.value);
        },

        "should be the unaliased part if aliased":function (ds) {
            assert.deepEqual(ds.from("t___a").firstSourceTable, new Identifier("t"));
            assert.deepEqual(ds.from("s__t___a").firstSourceTable, new QualifiedIdentifier("s", "t"));
            assert.deepEqual(ds.from(sql.t.as("a")).firstSourceTable.value, new Identifier("t").value);
        },

        "should raise exception if table doesn't have a source":function (ds) {
            assert.throws(function(){
                ds.firstSourceTable;
            });
        }
    },

    "Dataset.fromSelf":{
        topic:new Dataset().from("test").select("name").limit(1),

        "should set up a default alias":function (ds) {
            assert.equal(ds.fromSelf().sql, 'SELECT * FROM (SELECT name FROM test LIMIT 1) AS t1');
        },

        "should modify only the new dataset":function (ds) {
            assert.equal(ds.fromSelf().select("bogus").sql, 'SELECT bogus FROM (SELECT name FROM test LIMIT 1) AS t1');
        },

        "should use the user-specified alias":function (ds) {
            assert.equal(ds.fromSelf({alias:"someName"}).sql, 'SELECT * FROM (SELECT name FROM test LIMIT 1) AS someName');
        },

        "should use the user-specified alias for joins":function (ds) {
            assert.equal(ds.fromSelf({alias:"someName"}).innerJoin("posts", {alias:"name"}).sql, 'SELECT * FROM (SELECT name FROM test LIMIT 1) AS someName INNER JOIN posts ON (posts.alias = someName.name)');
        }
    },

    "Dataset.joinTable":{
        topic:function () {
            var d = new MockDataset().from("items");
            d.quoteIdentifiers = true;
            return d;
        },

        "should format the JOIN clause properly":function (d) {
            assert.equal(d.joinTable("leftOuter", "categories", {categoryId:"id"}).sql, 'SELECT * FROM "items" LEFT OUTER JOIN "categories" ON ("categories"."categoryId" = "items"."id")');
        },


        "should handle multiple conditions on the same join table column":function (d) {
            assert.equal(d.joinTable("leftOuter", "categories", [
                ["categoryId", "id"],
                ["categoryId", [1, 2, 3]]
            ]).sql, 'SELECT * FROM "items" LEFT OUTER JOIN "categories" ON (("categories"."categoryId" = "items"."id") AND ("categories"."categoryId" IN (1, 2, 3)))');
        },


        "should include WHERE clause if applicable":function (d) {
            assert.equal(d.filter(sql.price.sqlNumber.lt(100)).joinTable("rightOuter", "categories", {categoryId:"id"}).sql, 'SELECT * FROM "items" RIGHT OUTER JOIN "categories" ON ("categories"."categoryId" = "items"."id") WHERE ("price" < 100)');
        },


        "should include ORDER BY clause if applicable":function (d) {
            assert.equal(d.order("stamp").joinTable("fullOuter", "categories", {categoryId:"id"}).sql, 'SELECT * FROM "items" FULL OUTER JOIN "categories" ON ("categories"."categoryId" = "items"."id") ORDER BY "stamp"');
        },


        "should support multiple joins":function (d) {
            assert.equal(d.joinTable("inner", "b", {itemsId:"id"}).joinTable("leftOuter", "c", {b_id:"b__id"}).sql, 'SELECT * FROM "items" INNER JOIN "b" ON ("b"."itemsId" = "items"."id") LEFT OUTER JOIN "c" ON ("c"."b_id" = "b"."id")');
        },


        "should support arbitrary join types":function (d) {
            assert.equal(d.joinTable("magic", "categories", {categoryId:"id"}).sql, 'SELECT * FROM "items" MAGIC JOIN "categories" ON ("categories"."categoryId" = "items"."id")');
        },


        "should support many join methods":function (d) {
            assert.equal(d.leftOuterJoin("categories", {categoryId:"id"}).sql, 'SELECT * FROM "items" LEFT OUTER JOIN "categories" ON ("categories"."categoryId" = "items"."id")');
            assert.equal(d.rightOuterJoin("categories", {categoryId:"id"}).sql, 'SELECT * FROM "items" RIGHT OUTER JOIN "categories" ON ("categories"."categoryId" = "items"."id")');
            assert.equal(d.fullOuterJoin("categories", {categoryId:"id"}).sql, 'SELECT * FROM "items" FULL OUTER JOIN "categories" ON ("categories"."categoryId" = "items"."id")');
            assert.equal(d.innerJoin("categories", {categoryId:"id"}).sql, 'SELECT * FROM "items" INNER JOIN "categories" ON ("categories"."categoryId" = "items"."id")');
            assert.equal(d.leftJoin("categories", {categoryId:"id"}).sql, 'SELECT * FROM "items" LEFT JOIN "categories" ON ("categories"."categoryId" = "items"."id")');
            assert.equal(d.rightJoin("categories", {categoryId:"id"}).sql, 'SELECT * FROM "items" RIGHT JOIN "categories" ON ("categories"."categoryId" = "items"."id")');
            assert.equal(d.fullJoin("categories", {categoryId:"id"}).sql, 'SELECT * FROM "items" FULL JOIN "categories" ON ("categories"."categoryId" = "items"."id")');
            assert.equal(d.naturalJoin("categories").sql, 'SELECT * FROM "items" NATURAL JOIN "categories"');
            assert.equal(d.naturalLeftJoin("categories").sql, 'SELECT * FROM "items" NATURAL LEFT JOIN "categories"');
            assert.equal(d.naturalRightJoin("categories").sql, 'SELECT * FROM "items" NATURAL RIGHT JOIN "categories"');
            assert.equal(d.naturalFullJoin("categories").sql, 'SELECT * FROM "items" NATURAL FULL JOIN "categories"');
            assert.equal(d.crossJoin("categories").sql, 'SELECT * FROM "items" CROSS JOIN "categories"');
        },


        "should raise an error if additional arguments are provided to join methods that don't take conditions":function (d) {
            assert.throws(d, "naturalJoin", "categories", {id:"id"});
            assert.throws(d, "naturalLeftJoin", "categories", {id:"id"});
            assert.throws(d, "naturalRightJoin", "categories", {id:"id"});
            assert.throws(d, "naturalFullJoin", "categories", {id:"id"});
            assert.throws(d, "crossJoin", "categories", {id:"id"});
        },


        "should raise an error if blocks are provided to join methods that don't pass them":function (d) {
            assert.throws(d, "naturalJoin", "categories", function () {
            });
            assert.throws(d, "naturalLeftJoin", "categories", function () {
            });
            assert.throws(d, "naturalRightJoin", "categories", function () {
            });
            assert.throws(d, "naturalFullJoin", "categories", function () {
            });
            assert.throws(d, "crossJoin", "categories", function () {
            });
        },


        "should default to a plain join if nil is used for the type":function (d) {
            assert.equal(d.joinTable(null, "categories", {categoryId:"id"}).sql, 'SELECT * FROM "items"  JOIN "categories" ON ("categories"."categoryId" = "items"."id")');
        },


        "should use an inner join for Dataset.join":function (d) {
            assert.equal(d.join("categories", {categoryId:"id"}).sql, 'SELECT * FROM "items" INNER JOIN "categories" ON ("categories"."categoryId" = "items"."id")');
        },


        "should support aliased tables using a string":function (d) {
            assert.equal(d.from('stats').join('players', {id:"playerId"}, 'p').sql, 'SELECT * FROM "stats" INNER JOIN "players" AS "p" ON ("p"."id" = "stats"."playerId")');
        },


        "should support aliased tables using the :table_alias option":function (d) {
            assert.equal(d.from('stats').join('players', {id:"playerId"}, {tableAlias:"p"}).sql, 'SELECT * FROM "stats" INNER JOIN "players" AS "p" ON ("p"."id" = "stats"."playerId")');
        },


        "should support using an alias for the FROM when doing the first join with unqualified condition columns":function (d) {
            var ds = new MockDataset().from({foo:"f"});
            ds.quoteIdentifiers = true;
            assert.equal(ds.joinTable("inner", "bar", {id:"barId"}).sql, 'SELECT * FROM "foo" AS "f" INNER JOIN "bar" ON ("bar"."id" = "f"."barId")');
        },


        "should support implicit schemas in from table symbols":function (d) {
            assert.equal(d.from("s__t").join("u__v", {id:"playerId"}).sql, 'SELECT * FROM "s"."t" INNER JOIN "u"."v" ON ("u"."v"."id" = "s"."t"."playerId")');
        },


        "should support implicit aliases in from table symbols":function (d) {
            assert.equal(d.from("t___z").join("v___y", {id:"playerId"}).sql, 'SELECT * FROM "t" AS "z" INNER JOIN "v" AS "y" ON ("y"."id" = "z"."playerId")');
            assert.equal(d.from("s__t___z").join("u__v___y", {id:"playerId"}).sql, 'SELECT * FROM "s"."t" AS "z" INNER JOIN "u"."v" AS "y" ON ("y"."id" = "z"."playerId")');
        },


        "should support AliasedExpressions":function (d) {
            assert.equal(d.from(sql.s.as("t")).join(sql.u.as("v"), {id:"playerId"}).sql, 'SELECT * FROM "s" AS "t" INNER JOIN "u" AS "v" ON ("v"."id" = "t"."playerId")');
        },


        "should support the 'implicitQualifierOption":function (d) {
            assert.equal(d.from('stats').join('players', {id:"playerId"}, {implicitQualifier:"p"}).sql, 'SELECT * FROM "stats" INNER JOIN "players" ON ("players"."id" = "p"."playerId")');
        },


        "should allow for arbitrary conditions in the JOIN clause":function (d) {
            assert.equal(d.joinTable("leftOuter", "categories", {status:0}).sql, 'SELECT * FROM "items" LEFT OUTER JOIN "categories" ON ("categories"."status" = 0)');
            assert.equal(d.joinTable("leftOuter", "categories", {categorizableType:new LiteralString("'Post'")}).sql, 'SELECT * FROM "items" LEFT OUTER JOIN "categories" ON ("categories"."categorizableType" = \'Post\')');
            assert.equal(d.joinTable("leftOuter", "categories", {timestamp:new LiteralString("CURRENT_TIMESTAMP")}).sql, 'SELECT * FROM "items" LEFT OUTER JOIN "categories" ON ("categories"."timestamp" = CURRENT_TIMESTAMP)');
            assert.equal(d.joinTable("leftOuter", "categories", {status:[1, 2, 3]}).sql, 'SELECT * FROM "items" LEFT OUTER JOIN "categories" ON ("categories"."status" IN (1, 2, 3))');
        },


        "should raise error for a table without a source":function (d) {
            assert.throws(hitch(new Dataset(), "join", "players", {id:"playerId"}));
        },


        "should support joining datasets":function (d) {
            var ds = new Dataset().from("categories");
            assert.equal(d.joinTable("leftOuter", ds, {itemId:"id"}).sql, 'SELECT * FROM "items" LEFT OUTER JOIN (SELECT * FROM categories) AS "t1" ON ("t1"."itemId" = "items"."id")');
            ds = ds.filter({active:true});
            assert.equal(d.joinTable("leftOuter", ds, {itemId:"id"}).sql, 'SELECT * FROM "items" LEFT OUTER JOIN (SELECT * FROM categories WHERE (active IS TRUE)) AS "t1" ON ("t1"."itemId" = "items"."id")');
            assert.equal(d.fromSelf().joinTable("leftOuter", ds, {itemId:"id"}).sql, 'SELECT * FROM (SELECT * FROM "items") AS "t1" LEFT OUTER JOIN (SELECT * FROM categories WHERE (active IS TRUE)) AS "t2" ON ("t2"."itemId" = "t1"."id")');
        },


        "should support joining datasets and aliasing the join":function (d) {
            var ds = new Dataset().from("categories");
            assert.equal(d.joinTable("leftOuter", ds, {"ds__itemId":"id"}, "ds").sql, 'SELECT * FROM "items" LEFT OUTER JOIN (SELECT * FROM categories) AS "ds" ON ("ds"."itemId" = "items"."id")');
        },


        "should support joining multiple datasets":function (d) {
            var ds = new Dataset().from("categories");
            var ds2 = new Dataset().from("nodes").select("name");
            var ds3 = new Dataset().from("attributes").filter("name = 'blah'");

            assert.equal(d.joinTable("leftOuter", ds, {itemId:"id"}).joinTable("inner", ds2, {nodeId:"id"}).joinTable("rightOuter", ds3, {attributeId:"id"}).sql,
                'SELECT * FROM "items" LEFT OUTER JOIN (SELECT * FROM categories) AS "t1" ON ("t1"."itemId" = "items"."id") '
                    + 'INNER JOIN (SELECT name FROM nodes) AS "t2" ON ("t2"."nodeId" = "t1"."id") '
                    + 'RIGHT OUTER JOIN (SELECT * FROM attributes WHERE (name = \'blah\')) AS "t3" ON ("t3"."attributeId" = "t2"."id")'
            );
        },


        "should support joining objects that have a tableName property":function (d) {
            var ds = {tableName:"categories"};
            assert.equal(d.join(ds, {itemId:"id"}).sql, 'SELECT * FROM "items" INNER JOIN "categories" ON ("categories"."itemId" = "items"."id")');
        },


        "should support using a SQL String as the join condition":function (d) {
            assert.equal(d.join("categories", "c.item_id = items.id", "c").sql, 'SELECT * FROM "items" INNER JOIN "categories" AS "c" ON (c.item_id = items.id)');
        },


        "should support using a boolean column as the join condition":function (d) {
            assert.equal(d.join("categories", sql.active).sql, 'SELECT * FROM "items" INNER JOIN "categories" ON "active"');
        },


        "should support using an expression as the join condition":function (d) {
            assert.equal(d.join("categories", sql.number.sqlNumber.gt(10)).sql, 'SELECT * FROM "items" INNER JOIN "categories" ON ("number" > 10)');
        },


        "should support natural and cross joins using null":function (d) {
            assert.equal(d.joinTable("natural", "categories").sql, 'SELECT * FROM "items" NATURAL JOIN "categories"');
            assert.equal(d.joinTable("cross", "categories", null).sql, 'SELECT * FROM "items" CROSS JOIN "categories"');
            assert.equal(d.joinTable("natural", "categories", null, "c").sql, 'SELECT * FROM "items" NATURAL JOIN "categories" AS "c"');
        },


        "should support joins with a USING clause if an array of strings is used":function (d) {
            assert.equal(d.join("categories", ["id"]).sql, 'SELECT * FROM "items" INNER JOIN "categories" USING ("id")');
            assert.equal(d.join("categories", ["id1", "id2"]).sql, 'SELECT * FROM "items" INNER JOIN "categories" USING ("id1", "id2")');
        },


        "should emulate JOIN USING (poorly) if the dataset doesn't support it":function (d) {
            d.supportsJoinUsing = false;
            assert.equal(d.join("categories", ["id"]).sql, 'SELECT * FROM "items" INNER JOIN "categories" ON ("categories"."id" = "items"."id")');
            d.supportsJoinUsing = true;
        },


        "should raise an error if using an array of symbols with a block":function (d) {
            assert.throws(function () {
                d.join("categories", ["id"], function (j, lj, js) {
                    return false;
                });
            });
        },

        "should support using a block that receieves the join table/alias, last join table/alias, and array of previous joins":function (d) {
            d.join("categories", function (joinAlias, lastJoinAlias, joins) {
                assert.equal(joinAlias, "categories");
                assert.equal(lastJoinAlias, "items");
                assert.deepEqual(joins, []);
            });


            d.from({items:"i"}).join("categories", null, "c", function (joinAlias, lastJoinAlias, joins) {
                assert.equal(joinAlias, "c");
                assert.equal(lastJoinAlias, "i");
                assert.deepEqual(joins, []);
            });

            d.from("items___i").join("categories", null, "c", function (joinAlias, lastJoinAlias, joins) {
                assert.equal(joinAlias, "c");
                assert.equal(lastJoinAlias, "i");
                assert.deepEqual(joins, []);
            });

            d.join("blah").join("categories", null, "c", function (joinAlias, lastJoinAlias, joins) {
                assert.equal(joinAlias, "c");
                assert.equal(lastJoinAlias, "blah");
                assert.instanceOf(joins, Array);
                assert.lengthOf(joins, 1);
                assert.instanceOf(joins[0], sql.JoinClause)
                assert.equal(joins[0].joinType, "inner");
            });

            d.joinTable("natural", "blah", null, "b").join("categories", null, "c", function (joinAlias, lastJoinAlias, joins) {
                assert.equal(joinAlias, "c");
                assert.equal(lastJoinAlias, "b");
                assert.instanceOf(joins, Array);
                assert.lengthOf(joins, 1);
                assert.instanceOf(joins[0], sql.JoinClause)
                assert.equal(joins[0].joinType, "natural");
            });

            d.join("blah").join("categories").join("blah2", function (joinAlias, lastJoinAlias, joins) {
                assert.equal(joinAlias, "blah2");
                assert.equal(lastJoinAlias, "categories");
                assert.instanceOf(joins, Array);
                assert.lengthOf(joins, 2);
                assert.instanceOf(joins[0], sql.JoinClause)
                assert.equal(joins[0].table, "blah");
                assert.instanceOf(joins[1], sql.JoinClause)
                assert.equal(joins[1].table, "categories");
            });

        },


        "should use the block result as the only condition if no condition is given":function (d) {
            assert.equal(d.join("categories",
                function (j, lj, js) {
                    return this.b.qualify(j).eq(this.c.qualify(lj));
                }).sql, 'SELECT * FROM "items" INNER JOIN "categories" ON ("categories"."b" = "items"."c")');
            assert.equal(d.join("categories",
                function (j, lj, js) {
                    return this.b.qualify(j).gt(this.c.qualify(lj));
                }).sql, 'SELECT * FROM "items" INNER JOIN "categories" ON ("categories"."b" > "items"."c")');
        },


        "should combine the block conditions and argument conditions if both given":function (d) {
            assert.equal(d.join("categories", {a:"d"},
                function (j, lj, js) {
                    return this.b.qualify(j).eq(this.c.qualify(lj))
                }).sql, 'SELECT * FROM "items" INNER JOIN "categories" ON (("categories"."a" = "items"."d") AND ("categories"."b" = "items"."c"))');
            assert.equal(d.join("categories", {a:"d"},
                function (j, lj, js) {
                    return this.b.qualify(j).gt(this.c.qualify(lj));
                }).sql,
                'SELECT * FROM "items" INNER JOIN "categories" ON (("categories"."a" = "items"."d") AND ("categories"."b" > "items"."c"))');
        },


        "should prefer explicit aliases over implicit":function (d) {
            assert.equal(d.from("items___i").join("categories___c", {categoryId:"id"}, {tableAlias:"c2", implicitQualifier:"i2"}).sql, 'SELECT * FROM "items" AS "i" INNER JOIN "categories" AS "c2" ON ("c2"."categoryId" = "i2"."id")');
            assert.equal(d.from(sql.items.as("i")).join(sql.categories.as("c"), {categoryId:"id"}, {tableAlias:"c2", implicitQualifier:"i2"}).sql, 'SELECT * FROM "items" AS "i" INNER JOIN "categories" AS "c2" ON ("c2"."categoryId" = "i2"."id")');
        },


        "should not allow insert, update, delete, or truncate":function (d) {
            var ds = d.join("categories", {a:"d"});
            assert.throws(hitch(ds, "insertSql"));
            assert.throws(hitch(ds, "updateSql", {a:1}));
            assert.throws(function(){
                ds.deleteSql;
            });
            assert.throws(function(){
                ds.truncateSql;
            });
        },


        "should raise an error if an invalid option is passed":function (d) {
            assert.throws(hitch(d, "join", "c", ["id"], null));
        }

    },

    "Dataset.distinct":{
        topic:new MockDatabase().from("test").select("name"),

        "should include DISTINCT clause in statement":function (dataset) {
            assert.equal(dataset.distinct().sql, 'SELECT DISTINCT name FROM test');
        },

        "should raise an error if columns given and DISTINCT ON is not supported":function (dataset) {
            assert.doesNotThrow(hitch(dataset, "distinct"));
            assert.throws(hitch(dataset, "distinct", "a"));
        },

        "should use DISTINCT ON if columns are given and DISTINCT ON is supported":function (dataset) {
            dataset.supportsDistinctOn = true;
            assert.equal(dataset.distinct("a", "b").sql, 'SELECT DISTINCT ON (a, b) name FROM test');
            assert.equal(dataset.distinct(sql.stamp.cast("integer"), {nodeId:null}).sql, 'SELECT DISTINCT ON (CAST(stamp AS integer), (nodeId IS NULL)) name FROM test');
        },

        "should do a subselect for count":function () {
            var db = new MockDatabase();
            var ds = db.from("test").select("name");
            ds.distinct().count();
            assert.deepEqual(db.sqls, ['SELECT COUNT(*) AS count FROM (SELECT DISTINCT name FROM test) AS t1 LIMIT 1']);
        }
    },

    "Dataset.groupAndCount":{
        topic:new Dataset().from("test"),


        "should format SQL properly":function (ds) {
            assert.equal(ds.groupAndCount("name").sql, "SELECT name, count(*) AS count FROM test GROUP BY name");
        },

        "should accept multiple columns for grouping":function (ds) {
            assert.equal(ds.groupAndCount("a", "b").sql, "SELECT a, b, count(*) AS count FROM test GROUP BY a, b");
        },


        "should format column aliases in the SELECT clause but not in the group clause":function (ds) {
            assert.equal(ds.groupAndCount("name___n").sql, "SELECT name AS n, count(*) AS count FROM test GROUP BY name");
            assert.equal(ds.groupAndCount("name__n").sql, "SELECT name.n, count(*) AS count FROM test GROUP BY name.n");
        },

        "should handle identifiers":function (ds) {
            assert.equal(ds.groupAndCount(new Identifier("name___n")).sql, "SELECT name___n, count(*) AS count FROM test GROUP BY name___n");
        },

        "should handle literal strings":function (ds) {
            assert.equal(ds.groupAndCount(new LiteralString("name")).sql, "SELECT name, count(*) AS count FROM test GROUP BY name");
        },

        "should handle aliased expressions":function (ds) {
            assert.equal(ds.groupAndCount(sql.name.as("n")).sql, "SELECT name AS n, count(*) AS count FROM test GROUP BY name");
            assert.equal(ds.groupAndCount("name___n").sql, "SELECT name AS n, count(*) AS count FROM test GROUP BY name");
        }

    },

    "Dataset.set":{
        topic:function () {
            var c = comb.define(patio.Dataset, {
                instance:{

                    update:function () {
                        this.lastSql = this.updateSql.apply(this, arguments);
                    }
                }
            });

            return new c().from("items");
        },

        "should act as alias to update":function (ds) {
            ds.set({x:3});
            assert.equal(ds.lastSql, 'UPDATE items SET x = 3');
        }
    },

    "Dataset compound operations":{
        topic:function () {
            return {
                a:new Dataset().from("a").filter({z:1}),
                b:new Dataset().from("b").filter({z:2})
            };
        },

        "should support UNION and UNION ALL":function (ds) {
            var a = ds.a, b = ds.b;
            assert.equal(a.union(b).sql, "SELECT * FROM (SELECT * FROM a WHERE (z = 1) UNION SELECT * FROM b WHERE (z = 2)) AS t1");
            assert.equal(b.union(a, true).sql, "SELECT * FROM (SELECT * FROM b WHERE (z = 2) UNION ALL SELECT * FROM a WHERE (z = 1)) AS t1");
            assert.equal(b.union(a, {all:true}).sql, "SELECT * FROM (SELECT * FROM b WHERE (z = 2) UNION ALL SELECT * FROM a WHERE (z = 1)) AS t1");
        },

        "should support INTERSECT and INTERSECT ALL":function (ds) {
            var a = ds.a, b = ds.b;
            assert.equal(a.intersect(b).sql, "SELECT * FROM (SELECT * FROM a WHERE (z = 1) INTERSECT SELECT * FROM b WHERE (z = 2)) AS t1");
            assert.equal(b.intersect(a, true).sql, "SELECT * FROM (SELECT * FROM b WHERE (z = 2) INTERSECT ALL SELECT * FROM a WHERE (z = 1)) AS t1");
            assert.equal(b.intersect(a, {all:true}).sql, "SELECT * FROM (SELECT * FROM b WHERE (z = 2) INTERSECT ALL SELECT * FROM a WHERE (z = 1)) AS t1");
        },

        "should support EXCEPT and EXCEPT ALL":function (ds) {
            var a = ds.a, b = ds.b;
            assert.equal(a.except(b).sql, "SELECT * FROM (SELECT * FROM a WHERE (z = 1) EXCEPT SELECT * FROM b WHERE (z = 2)) AS t1");
            assert.equal(b.except(a, true).sql, "SELECT * FROM (SELECT * FROM b WHERE (z = 2) EXCEPT ALL SELECT * FROM a WHERE (z = 1)) AS t1");
            assert.equal(b.except(a, {all:true}).sql, "SELECT * FROM (SELECT * FROM b WHERE (z = 2) EXCEPT ALL SELECT * FROM a WHERE (z = 1)) AS t1");
        },

        "should support alias option for specifying identifier":function (ds) {
            var a = ds.a, b = ds.b;
            assert.equal(a.union(b, {alias:"xx"}).sql, "SELECT * FROM (SELECT * FROM a WHERE (z = 1) UNION SELECT * FROM b WHERE (z = 2)) AS xx");
            assert.equal(a.intersect(b, {alias:"xx"}).sql, "SELECT * FROM (SELECT * FROM a WHERE (z = 1) INTERSECT SELECT * FROM b WHERE (z = 2)) AS xx");
            assert.equal(a.except(b, {alias:"xx"}).sql, "SELECT * FROM (SELECT * FROM a WHERE (z = 1) EXCEPT SELECT * FROM b WHERE (z = 2)) AS xx");
        },

        "should support {fromSelf : false} option to not wrap the compound in a SELECT * FROM (...)":function (ds) {
            var a = ds.a, b = ds.b;
            assert.equal(b.union(a, {fromSelf:false}).sql, "SELECT * FROM b WHERE (z = 2) UNION SELECT * FROM a WHERE (z = 1)");
            assert.equal(b.intersect(a, {fromSelf:false}).sql, "SELECT * FROM b WHERE (z = 2) INTERSECT SELECT * FROM a WHERE (z = 1)");
            assert.equal(b.except(a, {fromSelf:false}).sql, "SELECT * FROM b WHERE (z = 2) EXCEPT SELECT * FROM a WHERE (z = 1)");

            assert.equal(b.union(a, {fromSelf:false, all:true}).sql, "SELECT * FROM b WHERE (z = 2) UNION ALL SELECT * FROM a WHERE (z = 1)");
            assert.equal(b.intersect(a, {fromSelf:false, all:true}).sql, "SELECT * FROM b WHERE (z = 2) INTERSECT ALL SELECT * FROM a WHERE (z = 1)");
            assert.equal(b.except(a, {fromSelf:false, all:true}).sql, "SELECT * FROM b WHERE (z = 2) EXCEPT ALL SELECT * FROM a WHERE (z = 1)");
        },

        "should raise an InvalidOperation if INTERSECT or EXCEPT is used and they are not supported":function (ds) {
            var a = ds.a, b = ds.b;
            a.supportsIntersectExcept = false;
            assert.throws(hitch(a, "intersect", b));
            assert.throws(hitch(a, "intersect", b, true));
            assert.throws(hitch(a, "except", b));
            assert.throws(hitch(a, "except", b, true));
            a.supportsIntersectExcept = true;
        },

        "should raise an InvalidOperation if INTERSECT ALL or EXCEPT ALL is used and they are not supported":function (ds) {
            var a = ds.a, b = ds.b;
            a.supportsIntersectExceptAll = false;
            assert.doesNotThrow(hitch(a, "intersect", b));
            assert.throws(hitch(a, "intersect", b, true));
            assert.doesNotThrow(hitch(a, "except", b));
            assert.throws(hitch(a, "except", b, true));
            a.supportsIntersectExceptAll = true;
        },

        "should handle chained compound operations":function (ds) {
            var a = ds.a, b = ds.b;
            assert.equal(a.union(b).union(a, true).sql, "SELECT * FROM (SELECT * FROM (SELECT * FROM a WHERE (z = 1) UNION SELECT * FROM b WHERE (z = 2)) AS t1 UNION ALL SELECT * FROM a WHERE (z = 1)) AS t1");
            assert.equal(a.intersect(b, true).intersect(a).sql, "SELECT * FROM (SELECT * FROM (SELECT * FROM a WHERE (z = 1) INTERSECT ALL SELECT * FROM b WHERE (z = 2)) AS t1 INTERSECT SELECT * FROM a WHERE (z = 1)) AS t1");
            assert.equal(a.except(b).except(a, true).sql, "SELECT * FROM (SELECT * FROM (SELECT * FROM a WHERE (z = 1) EXCEPT SELECT * FROM b WHERE (z = 2)) AS t1 EXCEPT ALL SELECT * FROM a WHERE (z = 1)) AS t1");
        },

        "should use a subselect when using a compound operation with a dataset that already has a compound operation":function (ds) {
            var a = ds.a, b = ds.b;
            assert.equal(a.union(b.union(a, true)).sql, "SELECT * FROM (SELECT * FROM a WHERE (z = 1) UNION SELECT * FROM (SELECT * FROM b WHERE (z = 2) UNION ALL SELECT * FROM a WHERE (z = 1)) AS t1) AS t1");
            assert.equal(a.intersect(b.intersect(a), true).sql, "SELECT * FROM (SELECT * FROM a WHERE (z = 1) INTERSECT ALL SELECT * FROM (SELECT * FROM b WHERE (z = 2) INTERSECT SELECT * FROM a WHERE (z = 1)) AS t1) AS t1");
            assert.equal(a.except(b.except(a, true)).sql, "SELECT * FROM (SELECT * FROM a WHERE (z = 1) EXCEPT SELECT * FROM (SELECT * FROM b WHERE (z = 2) EXCEPT ALL SELECT * FROM a WHERE (z = 1)) AS t1) AS t1");
        },

        "should order and limit properly when using UNION, INTERSECT, or EXCEPT":function (ds) {
            var a = ds.a, b = ds.b;
            var dataset = new Dataset().from("test");
            assert.equal(dataset.union(dataset).limit(2).sql, "SELECT * FROM (SELECT * FROM test UNION SELECT * FROM test) AS t1 LIMIT 2");
            assert.equal(dataset.limit(2).intersect(dataset).sql, "SELECT * FROM (SELECT * FROM (SELECT * FROM test LIMIT 2) AS t1 INTERSECT SELECT * FROM test) AS t1");
            assert.equal(dataset.except(dataset.limit(2)).sql, "SELECT * FROM (SELECT * FROM test EXCEPT SELECT * FROM (SELECT * FROM test LIMIT 2) AS t1) AS t1");

            assert.equal(dataset.union(dataset).order("num").sql, "SELECT * FROM (SELECT * FROM test UNION SELECT * FROM test) AS t1 ORDER BY num");
            assert.equal(dataset.order("num").intersect(dataset).sql, "SELECT * FROM (SELECT * FROM (SELECT * FROM test ORDER BY num) AS t1 INTERSECT SELECT * FROM test) AS t1");
            assert.equal(dataset.except(dataset.order("num")).sql, "SELECT * FROM (SELECT * FROM test EXCEPT SELECT * FROM (SELECT * FROM test ORDER BY num) AS t1) AS t1");

            assert.equal(dataset.limit(2).order("a").union(dataset.limit(3).order("b")).order("c").limit(4).sql, "SELECT * FROM (SELECT * FROM (SELECT * FROM test ORDER BY a LIMIT 2) AS t1 UNION SELECT * FROM (SELECT * FROM test ORDER BY b LIMIT 3) AS t1) AS t1 ORDER BY c LIMIT 4");
        }

    },

    "Dataset.updateSql":{
        topic:new Dataset().from("items"),


        "should accept strings":function (ds) {
            assert.equal(ds.updateSql("a = b"), "UPDATE items SET a = b");
        },

        "should handle implicitly qualified symbols":function (ds) {
            assert.equal(ds.updateSql({items__a:sql.b}), "UPDATE items SET items.a = b");
        },

        "should accept hash with string keys":function (ds) {
            assert.equal(ds.updateSql({c:"d"}), "UPDATE items SET c = 'd'");
        },

        "should accept array subscript references":function (ds) {
            assert.equal(ds.updateSql(sql.day.sqlSubscript(1).eq("d")), "UPDATE items SET day[1] = 'd'");
        },

        "should accept array subscript references as hash and string":function (ds) {
            assert.equal(ds.updateSql(sql.day.sqlSubscript(1).eq("d"), {c:"d"}, "a=b"), "UPDATE items SET day[1] = 'd', c = 'd', a=b");
        }
    },

    "Dataset.insertSql":{
        topic:new Dataset().from("items"),

        "should accept hash with symbol keys":function (ds) {
            assert.equal(ds.insertSql({c:'d'}), "INSERT INTO items (c) VALUES ('d')");
        },

        "should accept hash with string keys":function (ds) {
            assert.equal(ds.insertSql({c:'d'}), "INSERT INTO items (c) VALUES ('d')");
        },

        "should accept array subscript references":function (ds) {
            assert.equal(ds.insertSql(sql.day.sqlSubscript(1).eq("d")), "INSERT INTO items (day[1]) VALUES ('d')");
        },

        "should raise an Error if the dataset has no sources":function (ds) {
            assert.throws(hitch(new Dataset(), "insertSql"));
        },

        "should accept datasets":function (ds) {
            assert.equal(ds.insertSql(ds), "INSERT INTO items SELECT * FROM items");
        },

        "should accept datasets with columns":function (ds) {
            assert.equal(ds.insertSql(["a", "b"], ds), "INSERT INTO items (a, b) SELECT * FROM items");
        },

        "should raise if given bad values":function (ds) {
            assert.throws(hitch(ds.mergeOptions({values:'a'}), "_insertSql"));
        },

        "should accept separate values":function (ds) {
            assert.equal(ds.insertSql(1), "INSERT INTO items VALUES (1)");
            assert.equal(ds.insertSql(1, 2), "INSERT INTO items VALUES (1, 2)");
            assert.equal(ds.insertSql(1, 2, 3), "INSERT INTO items VALUES (1, 2, 3)");
        },

        "should accept a single array of values":function (ds) {
            assert.equal(ds.insertSql([1, 2, 3]), "INSERT INTO items VALUES (1, 2, 3)");
        },

        "should accept an array of columns and an array of values":function (ds) {
            assert.equal(ds.insertSql(["a", "b", "c"], [1, 2, 3]), "INSERT INTO items (a, b, c) VALUES (1, 2, 3)");
        },


        "should accept a single LiteralString":function (ds) {
            assert.equal(ds.insertSql(sql.literal('VALUES (1, 2, 3)')), "INSERT INTO items VALUES (1, 2, 3)");
        },

        "should accept an array of columns and an LiteralString":function (ds) {
            assert.equal(ds.insertSql(["a", "b", "c"], sql.literal('VALUES (1, 2, 3)')), "INSERT INTO items (a, b, c) VALUES (1, 2, 3)");
        },

        "should accept an object that responds to values and returns a hash by using that hash as the columns and values":function (ds) {
            var o = {values:{c:"d"}};
            assert.equal(ds.insertSql(o), "INSERT INTO items (c) VALUES ('d')");
        },

        "should accept an object that responds to values and returns something other than a hash by using the object itself as a single value":function (ds) {
            var o = new Date(2000, 0, 1);
            o.values = function () {
                return  this
            };
            assert.equal(ds.insertSql(o), "INSERT INTO items VALUES ('2000-01-01')");
        }
    },


    "Dataset.grep":{
        topic:function () {
            return  new Dataset().from("posts");
        },

        "should format a SQL filter correctly":function (ds) {
            assert.equal(ds.grep("title", 'javasScript').sql,
                "SELECT * FROM posts WHERE ((title LIKE 'javasScript'))");
        },

        "should support multiple columns":function (ds) {
            assert.equal(ds.grep(["title", "body"], 'javasScript').sql,
                "SELECT * FROM posts WHERE ((title LIKE 'javasScript') OR (body LIKE 'javasScript'))");
        },

        "should support multiple search terms":function (ds) {
            assert.equal(ds.grep("title", ['abc', 'def']).sql,
                "SELECT * FROM posts WHERE ((title LIKE 'abc') OR (title LIKE 'def'))");
        },

        "should support multiple columns and search terms":function (ds) {
            assert.equal(ds.grep(["title", "body"], ['abc', 'def']).sql,
                "SELECT * FROM posts WHERE ((title LIKE 'abc') OR (title LIKE 'def') OR (body LIKE 'abc') OR (body LIKE 'def'))");
        },

        "should support the :all_patterns option":function (ds) {
            assert.equal(ds.grep(["title", "body"], ['abc', 'def'], {allPatterns:true}).sql,
                "SELECT * FROM posts WHERE (((title LIKE 'abc') OR (body LIKE 'abc')) AND ((title LIKE 'def') OR (body LIKE 'def')))");
        },

        "should support the :allColumns option":function (ds) {
            assert.equal(ds.grep(["title", "body"], ['abc', 'def'], {allColumns:true}).sql,
                "SELECT * FROM posts WHERE (((title LIKE 'abc') OR (title LIKE 'def')) AND ((body LIKE 'abc') OR (body LIKE 'def')))");
        },

        "should support the :case_insensitive option":function (ds) {
            assert.equal(ds.grep(["title", "body"], ['abc', 'def'], {caseInsensitive:true}).sql,
                "SELECT * FROM posts WHERE ((title ILIKE 'abc') OR (title ILIKE 'def') OR (body ILIKE 'abc') OR (body ILIKE 'def'))");
        },

        "should support the :all_patterns and :allColumns options together":function (ds) {
            assert.equal(ds.grep(["title", "body"], ['abc', 'def'], {allPatterns:true, allColumns:true}).sql,
                "SELECT * FROM posts WHERE ((title LIKE 'abc') AND (body LIKE 'abc') AND (title LIKE 'def') AND (body LIKE 'def'))");
        },

        "should support the :all_patterns and :case_insensitive options together":function (ds) {
            assert.equal(ds.grep(["title", "body"], ['abc', 'def'], {allPatterns:true, caseInsensitive:true}).sql,
                "SELECT * FROM posts WHERE (((title ILIKE 'abc') OR (body ILIKE 'abc')) AND ((title ILIKE 'def') OR (body ILIKE 'def')))");
        },

        "should support the :allColumns and :case_insensitive options together":function (ds) {
            assert.equal(ds.grep(["title", "body"], ['abc', 'def'], {allColumns:true, caseInsensitive:true}).sql,
                "SELECT * FROM posts WHERE (((title ILIKE 'abc') OR (title ILIKE 'def')) AND ((body ILIKE 'abc') OR (body ILIKE 'def')))");
        },

        "should support the :all_patterns, :allColumns, and :caseInsensitive options together":function (ds) {
            assert.equal(ds.grep(["title", "body"], ['abc', 'def'], {allPatterns:true, allColumns:true, caseInsensitive:true}).sql,
                "SELECT * FROM posts WHERE ((title ILIKE 'abc') AND (body ILIKE 'abc') AND (title ILIKE 'def') AND (body ILIKE 'def'))");
        },

        "should support regexps though the database may not support it":function (ds) {
            assert.equal(ds.grep("title", /javasScript/).sql,
                "SELECT * FROM posts WHERE ((title ~ 'javasScript'))");

            assert.equal(ds.grep("title", [/^javasScript/, 'javasScript']).sql,
                "SELECT * FROM posts WHERE ((title ~ '^javasScript') OR (title LIKE 'javasScript'))");
        },

        "should support searching against other columns":function (ds) {
            assert.equal(ds.grep("title", sql.identifier("body")).sql,
                "SELECT * FROM posts WHERE ((title LIKE body))");
        }
    },


    "Dataset setDefaults":{
        topic:function () {
            return new Dataset().from("items").setDefaults({x:1});
        },

        "should set the default values for inserts":function (ds) {
            assert.equal(ds.insertSql(), "INSERT INTO items (x) VALUES (1)");
            assert.equal(ds.insertSql({x:2}), "INSERT INTO items (x) VALUES (2)");
            assert.equal(ds.insertSql({y:2}), "INSERT INTO items (x, y) VALUES (1, 2)");
            assert.equal(ds.setDefaults({y:2}).insertSql(), "INSERT INTO items (x, y) VALUES (1, 2)");
            assert.equal(ds.setDefaults({x:2}).insertSql(), "INSERT INTO items (x) VALUES (2)");
        },

        "should set the default values for updates":function (ds) {
            assert.equal(ds.updateSql(), "UPDATE items SET x = 1");
            assert.equal(ds.updateSql({x:2}), "UPDATE items SET x = 2");
            assert.equal(ds.updateSql({y:2}), "UPDATE items SET x = 1, y = 2");
            assert.equal(ds.setDefaults({y:2}).updateSql(), "UPDATE items SET x = 1, y = 2");
            assert.equal(ds.setDefaults({x:2}).updateSql(), "UPDATE items SET x = 2");
        }
    },

    "Dataset setOverrides":{
        topic:function () {
            return new Dataset().from("items").setOverrides({x:1})
        },

        "should override the given values for inserts":function (ds) {
            assert.equal(ds.insertSql(), "INSERT INTO items (x) VALUES (1)");
            assert.equal(ds.insertSql({x:2}), "INSERT INTO items (x) VALUES (1)");
            assert.equal(ds.insertSql({y:2}), "INSERT INTO items (y, x) VALUES (2, 1)");
            assert.equal(ds.setOverrides({y:2}).insertSql(), "INSERT INTO items (x, y) VALUES (1, 2)");
            assert.equal(ds.setOverrides({x:2}).insertSql(), "INSERT INTO items (x) VALUES (2)");
        },

        "should override the given values for updates":function (ds) {
            assert.equal(ds.updateSql(), "UPDATE items SET x = 1");
            assert.equal(ds.updateSql({x:2}), "UPDATE items SET x = 1");
            assert.equal(ds.updateSql({y:2}), "UPDATE items SET y = 2, x = 1");
            assert.equal(ds.setOverrides({y:2}).updateSql(), "UPDATE items SET x = 1, y = 2");
            assert.equal(ds.setOverrides({x:2}).updateSql(), "UPDATE items SET x = 2");
        }
    },

    "patio.Dataset.qualify":{
        topic:new MockDatabase().from("t"),
        "should qualify to the given table":function (ds) {
            assert.equal(ds.filter(
                function () {
                    return this.a.lt(this.b);
                }).qualify("e").sql, 'SELECT e.* FROM t WHERE (e.a < e.b)');
        },

        "should qualify to the first source if no table if given":function (ds) {
            assert.equal(ds.filter(
                function () {
                    return this.a.lt(this.b);
                }).qualify().sql, 'SELECT t.* FROM t WHERE (t.a < t.b)');
        }
    },

    "patio.Dataset.qualifyTo":{
        topic:new MockDatabase().from("t"),

        "should qualify to the given table":function (ds) {
            assert.equal(ds.filter(
                function (e) {
                    return e.a.lt(e.b);
                }).qualifyTo("e").sql, 'SELECT e.* FROM t WHERE (e.a < e.b)');
        },

        "should not qualify to the given table if withSql is used":function (ds) {
            assert.equal(ds.withSql("SELECT * FROM test WHERE (a < b)").qualifyTo("e").sql, 'SELECT * FROM test WHERE (a < b)');
        }
    },

    "patio.Dataset.qualifyToFirstSource":{
        topic:new MockDatabase().from("t"),

        "should qualifyTo the first source":function (ds) {
            assert.equal(ds.qualifyToFirstSource().sql, 'SELECT t.* FROM t');
        },

        "should handle the SELECT, order, where, having, and group options/clauses":function (ds) {
            assert.equal(ds.select("a").filter({a:1}).order("a").group("a").having("a").qualifyToFirstSource().sql, 'SELECT t.a FROM t WHERE (t.a = 1) GROUP BY t.a HAVING t.a ORDER BY t.a');
        },

        "should handle the SELECT using a table.* if all columns are currently selected":function (ds) {
            assert.equal(ds.filter({a:1}).order("a").group("a").having("a").qualifyToFirstSource().sql, 'SELECT t.* FROM t WHERE (t.a = 1) GROUP BY t.a HAVING t.a ORDER BY t.a');
        },

        "should handle hashes in SELECT option":function (ds) {
            assert.equal(ds.select({a:"b"}).qualifyToFirstSource().sql, 'SELECT t.a AS b FROM t');
        },

        "should handle strings":function (ds) {
            assert.equal(ds.select("a", "b__c", "d___e", "f__g___h").qualifyToFirstSource().sql, 'SELECT t.a, b.c, t.d AS e, f.g AS h FROM t');
        },

        "should handle arrays":function (ds) {
            assert.equal(ds.filter({a:[sql.b, sql.c]}).qualifyToFirstSource().sql, 'SELECT t.* FROM t WHERE (t.a IN (t.b, t.c))');
        },

        "should handle hashes":function (ds) {
            assert.equal(ds.select(sql["case"]({b:{c:1}}, false)).qualifyToFirstSource().sql, "SELECT (CASE WHEN t.b THEN (t.c = 1) ELSE 'f' END) FROM t");
        },

        "should handle Identifiers":function (ds) {
            assert.equal(ds.select(sql.a).qualifyToFirstSource().sql, 'SELECT t.a FROM t');
        },

        "should handle OrderedExpressions":function (ds) {
            assert.equal(ds.order(sql.a.desc(), sql.b.asc()).qualifyToFirstSource().sql, 'SELECT t.* FROM t ORDER BY t.a DESC, t.b ASC');
        },

        "should handle AliasedExpressions":function (ds) {
            assert.equal(ds.select(sql.a.as("b")).qualifyToFirstSource().sql, 'SELECT t.a AS b FROM t');
        },

        "should handle CaseExpressions":function (ds) {
            assert.equal(ds.filter(sql["case"]({a:sql.b}, sql.c, sql.d)).qualifyToFirstSource().sql, 'SELECT t.* FROM t WHERE (CASE t.d WHEN t.a THEN t.b ELSE t.c END)');
        },


        "should handle Casts":function (ds) {
            assert.equal(ds.filter(sql.a.cast("boolean")).qualifyToFirstSource().sql, 'SELECT t.* FROM t WHERE CAST(t.a AS boolean)');
        },

        "should handle Functions":function (ds) {
            assert.equal(ds.filter(sql.a("b", 1)).qualifyToFirstSource().sql, 'SELECT t.* FROM t WHERE a(t.b, 1)');
        },

        "should handle ComplexExpressions":function (ds) {
            assert.equal(ds.filter(
                function (t) {
                    return t.a.plus(t.b).lt(t.c.minus(3))
                }).qualifyToFirstSource().sql, 'SELECT t.* FROM t WHERE ((t.a + t.b) < (t.c - 3))');
        },


        "should handle Subscripts":function (ds) {
            assert.equal(ds.filter(sql.a.sqlSubscript(sql.b, 3)).qualifyToFirstSource().sql, 'SELECT t.* FROM t WHERE t.a[t.b, 3]');
        },

        "should handle PlaceholderLiteralStrings":function (ds) {
            assert.equal(ds.filter('? > ?', sql.a, 1).qualifyToFirstSource().sql, 'SELECT t.* FROM t WHERE (t.a > 1)');
        },

        "should handle PlaceholderLiteralStrings with named placeholders":function (ds) {
            assert.equal(ds.filter('{a} > {b}', {a:sql.c, b:1}).qualifyToFirstSource().sql, 'SELECT t.* FROM t WHERE (t.c > 1)');
        },


        "should handle all other objects by returning them unchanged":function (ds) {
            assert.equal(ds.select(sql.literal("'a'")).filter(sql.a(3)).filter('blah').order(sql.literal(true)).group(sql.literal('a > ?', [1])).having(false).qualifyToFirstSource().sql, "SELECT 'a' FROM t WHERE (a(3) AND (blah)) GROUP BY a > 1 HAVING 'f' ORDER BY true");
        },

        "should not handle numeric or string expressions ":function (ds) {
            assert.throws(function () {
                ds.filter(sql.identifier("a").plus(sql.b));
            });
            assert.throws(function () {
                ds.filter(sql.sqlStringJoin(["a", "b", "c"], " "));
            });
        }
    },

    "patio.Dataset with and withRecursive":{
        topic:function () {
            var db = new MockDatabase();
            return {
                db:db,
                ds:db.from("t")
            };
        },

        "with should take a name and dataset and use a WITH clause":function (o) {
            var db = o.db, ds = o.ds;
            assert.equal(ds["with"]("t", db.from("x")).sql, 'WITH t AS (SELECT * FROM x) SELECT * FROM t');
        },

        "withRecursive should take a name, nonrecursive dataset, and recursive dataset, and use a WITH clause":function (o) {
            var db = o.db, ds = o.ds;
            assert.equal(ds.withRecursive("t", db.from("x"), db.from("t")).sql, 'WITH t AS (SELECT * FROM x UNION ALL SELECT * FROM t) SELECT * FROM t');
        },

        "with and withRecursive should add to existing WITH clause if called multiple times":function (o) {
            var db = o.db, ds = o.ds;
            assert.equal(ds["with"]("t", db.from("x"))["with"]("j", db.from("y")).sql, 'WITH t AS (SELECT * FROM x), j AS (SELECT * FROM y) SELECT * FROM t');
            assert.equal(ds.withRecursive("t", db.from("x"), db.from("t")).withRecursive("j", db.from("y"), db.from("j")).sql, 'WITH t AS (SELECT * FROM x UNION ALL SELECT * FROM t), j AS (SELECT * FROM y UNION ALL SELECT * FROM j) SELECT * FROM t');
            assert.equal(ds["with"]("t", db.from("x")).withRecursive("j", db.from("y"), db.from("j")).sql, 'WITH t AS (SELECT * FROM x), j AS (SELECT * FROM y UNION ALL SELECT * FROM j) SELECT * FROM t');
        },

        "with and withRecursive should take an :args option":function (o) {
            var db = o.db, ds = o.ds;
            assert.equal(ds["with"]("t", db.from("x"), {args:["b"]}).sql, 'WITH t(b) AS (SELECT * FROM x) SELECT * FROM t');
            assert.equal(ds.withRecursive("t", db.from("x"), db.from("t"), {args:["b", "c"]}).sql, 'WITH t(b, c) AS (SELECT * FROM x UNION ALL SELECT * FROM t) SELECT * FROM t');
        },

        "withRecursive should take an unionAll=>false option":function (o) {
            var db = o.db, ds = o.ds;
            assert.equal(ds.withRecursive("t", db.from("x"), db.from("t"), {unionAll:false}).sql, 'WITH t AS (SELECT * FROM x UNION SELECT * FROM t) SELECT * FROM t');
        },

        "with and withRecursive should raise an error unless the dataset supports CTEs":function (o) {
            var db = o.db, ds = o.ds;
            ds.supportsCte = false;
            assert.throws(hitch(ds, "with", db.from("x"), {args:["b"]}));
            assert.throws(hitch(ds, "withRecursive", db.from("x"), db.from("t"), {args:["b", "c"]}));
        }
    },


    "Dataset.lockStyle and forUpdate":{
        topic:new MockDatabase().from("t"),

        "forUpdate should use FOR UPDATE":function (ds) {
            assert.equal(ds.forUpdate().sql, "SELECT * FROM t FOR UPDATE");
        },

        "#lock_style should accept symbols":function (ds) {
            assert.equal(ds.lockStyle("update").sql, "SELECT * FROM t FOR UPDATE");
        },

        "#lock_style should accept strings for arbitrary SQL":function (ds) {
            assert.equal(ds.lockStyle("FOR SHARE").sql, "SELECT * FROM t FOR SHARE");
        }
    }
});

suite.addBatch({
    "A Dataset should be backwards compatable with the old query structure":{

        topic:new Dataset().from("test"),


        'when finding all records with limited fields':function (ds) {
            assert.equal(ds.select(["a", "b", "c"]).sql, "SELECT ('a', 'b', 'c') FROM test");
        },

        'when using logic operators ':function (ds) {
            assert.equal(ds.eq({x:0}).sql, "SELECT * FROM test WHERE (x = 0)");
            assert.equal(ds.find({x:0}).sql, "SELECT * FROM test WHERE (x = 0)");
            assert.equal(ds.eq({x:1}).sql, "SELECT * FROM test WHERE (x = 1)");
            assert.equal(ds.find({x:1}).sql, "SELECT * FROM test WHERE (x = 1)");

            assert.equal(ds.neq({x:0}).sql, "SELECT * FROM test WHERE (x != 0)");
            assert.equal(ds.find({x:{neq:0}}).sql, "SELECT * FROM test WHERE (x != 0)");
            assert.equal(ds.neq({x:1}).sql, "SELECT * FROM test WHERE (x != 1)");
            assert.equal(ds.find({x:{neq:1}}).sql, "SELECT * FROM test WHERE (x != 1)");

            assert.equal(ds.gt({x:0}).sql, "SELECT * FROM test WHERE (x > 0)");
            assert.equal(ds.find({x:{gt:0}}).sql, "SELECT * FROM test WHERE (x > 0)");
            assert.equal(ds.gte({x:0}).sql, "SELECT * FROM test WHERE (x >= 0)");
            assert.equal(ds.find({x:{gte:0}}).sql, "SELECT * FROM test WHERE (x >= 0)");

            assert.equal(ds.gt({x:1}).sql, "SELECT * FROM test WHERE (x > 1)");
            assert.equal(ds.find({x:{gt:1}}).sql, "SELECT * FROM test WHERE (x > 1)");
            assert.equal(ds.gte({x:1}).sql, "SELECT * FROM test WHERE (x >= 1)");
            assert.equal(ds.find({x:{gte:1}}).sql, "SELECT * FROM test WHERE (x >= 1)");

            assert.equal(ds.lt({x:0}).sql, "SELECT * FROM test WHERE (x < 0)");
            assert.equal(ds.find({x:{lt:0}}).sql, "SELECT * FROM test WHERE (x < 0)");
            assert.equal(ds.lte({x:0}).sql, "SELECT * FROM test WHERE (x <= 0)");
            assert.equal(ds.find({x:{lte:0}}).sql, "SELECT * FROM test WHERE (x <= 0)");

            assert.equal(ds.lt({x:1}).sql, "SELECT * FROM test WHERE (x < 1)");
            assert.equal(ds.find({x:{lt:1}}).sql, "SELECT * FROM test WHERE (x < 1)");
            assert.equal(ds.lte({x:1}).sql, "SELECT * FROM test WHERE (x <= 1)");
            assert.equal(ds.find({x:{lte:1}}).sql, "SELECT * FROM test WHERE (x <= 1)");

            assert.equal(ds.find({x:{lt:1, gt:10}}).sql, "SELECT * FROM test WHERE ((x < 1) AND (x > 10))");
        },

        "when using like ":function (ds) {

            assert.equal(ds.like("title", 'javasScript').sql,
                "SELECT * FROM test WHERE ((title LIKE 'javasScript'))");
            assert.equal(ds.find({title:{like:'javasScript'}}).sql,
                "SELECT * FROM test WHERE (title LIKE 'javasScript')");
            assert.equal(ds.find({title:{iLike:'javasScript'}}).sql,
                "SELECT * FROM test WHERE (title ILIKE 'javasScript')");

            assert.equal(ds.find({title:{like:/javasScript/i}}).sql,
                "SELECT * FROM test WHERE (title ~* 'javasScript')");
            assert.equal(ds.find({title:{iLike:/javasScript/}}).sql,
                "SELECT * FROM test WHERE (title ~* 'javasScript')");
        },

        "when using between/notBetween":function (ds) {
            assert.equal(ds.between({x:[1, 2]}).sql, "SELECT * FROM test WHERE ((x >= 1) AND (x <= 2))");
            assert.equal(ds.find({x:{between:[1, 2]}}).sql, "SELECT * FROM test WHERE ((x >= 1) AND (x <= 2))");
            assert.throws(function () {
                ds.between({x:"a"});
            });

            assert.equal(ds.notBetween({x:[1, 2]}).sql, "SELECT * FROM test WHERE ((x < 1) OR (x > 2))");
            assert.equal(ds.find({x:{notBetween:[1, 2]}}).sql, "SELECT * FROM test WHERE ((x < 1) OR (x > 2))");
            assert.throws(function () {
                ds.notBetween({x:"a"});
            });
        },


        'when using is/isNot':function (ds) {
            assert.equal(ds.is({flag:true}).sql, "SELECT * FROM test WHERE (flag IS TRUE)");
            assert.equal(ds.isTrue("flag").sql, "SELECT * FROM test WHERE (flag IS TRUE)");
            assert.equal(ds.isTrue("flag", "otherFlag").sql, "SELECT * FROM test WHERE ((flag IS TRUE) AND (otherFlag IS TRUE))");
            assert.equal(ds.is({flag:false}).sql, "SELECT * FROM test WHERE (flag IS FALSE)");
            assert.equal(ds.isFalse("flag").sql, "SELECT * FROM test WHERE (flag IS FALSE)");
            assert.equal(ds.isFalse("flag", "otherFlag").sql, "SELECT * FROM test WHERE ((flag IS FALSE) AND (otherFlag IS FALSE))");
            assert.equal(ds.is({flag:null}).sql, "SELECT * FROM test WHERE (flag IS NULL)");
            assert.equal(ds.isNull("flag").sql, "SELECT * FROM test WHERE (flag IS NULL)");
            assert.equal(ds.isNull("flag", "otherFlag").sql, "SELECT * FROM test WHERE ((flag IS NULL) AND (otherFlag IS NULL))");
            assert.equal(ds.is({flag:true, otherFlag:false, yetAnotherFlag:null}).sql,
                'SELECT * FROM test WHERE ((flag IS TRUE) AND (otherFlag IS FALSE) AND (yetAnotherFlag IS NULL))');

            assert.equal(ds.find({flag:{is:true}}).sql, "SELECT * FROM test WHERE (flag IS TRUE)");
            assert.equal(ds.find({flag:{is:false}}).sql, "SELECT * FROM test WHERE (flag IS FALSE)");
            assert.equal(ds.find({flag:{is:null}}).sql, "SELECT * FROM test WHERE (flag IS NULL)");
            assert.equal(ds.find({flag:{is:true}, otherFlag:{is:false}, yetAnotherFlag:{is:null}}).sql,
                'SELECT * FROM test WHERE ((flag IS TRUE) AND (otherFlag IS FALSE) AND (yetAnotherFlag IS NULL))');

            assert.equal(ds.find({"flag,otherFlag":{isNot:null}}).sql, "SELECT * FROM test WHERE ((flag IS NOT NULL) AND (otherFlag IS NOT NULL))");

            assert.equal(ds.isNot({flag:true}).sql, "SELECT * FROM test WHERE (flag IS NOT TRUE)");
            assert.equal(ds.isNotTrue("flag").sql, "SELECT * FROM test WHERE (flag IS NOT TRUE)");
            assert.equal(ds.isNotTrue("flag", "otherFlag").sql, "SELECT * FROM test WHERE ((flag IS NOT TRUE) AND (otherFlag IS NOT TRUE))");
            assert.equal(ds.isNot({flag:false}).sql, "SELECT * FROM test WHERE (flag IS NOT FALSE)");
            assert.equal(ds.isNotFalse("flag").sql, "SELECT * FROM test WHERE (flag IS NOT FALSE)");
            assert.equal(ds.isNotFalse("flag", "otherFlag").sql, "SELECT * FROM test WHERE ((flag IS NOT FALSE) AND (otherFlag IS NOT FALSE))");
            assert.equal(ds.isNot({flag:null}).sql, "SELECT * FROM test WHERE (flag IS NOT NULL)");
            assert.equal(ds.isNotNull("flag").sql, "SELECT * FROM test WHERE (flag IS NOT NULL)");
            assert.equal(ds.isNotNull("flag", "otherFlag").sql, "SELECT * FROM test WHERE ((flag IS NOT NULL) AND (otherFlag IS NOT NULL))");
            assert.equal(ds.isNot({flag:true, otherFlag:false, yetAnotherFlag:null}).sql,
                "SELECT * FROM test WHERE ((flag IS NOT TRUE) AND (otherFlag IS NOT FALSE) AND (yetAnotherFlag IS NOT NULL))");

            assert.equal(ds.find({flag:{isNot:true}}).sql, "SELECT * FROM test WHERE (flag IS NOT TRUE)");
            assert.equal(ds.find({flag:{isNot:false}}).sql, "SELECT * FROM test WHERE (flag IS NOT FALSE)");
            assert.equal(ds.find({flag:{isNot:null}}).sql, "SELECT * FROM test WHERE (flag IS NOT NULL)");
            assert.equal(ds.find({flag:{isNot:true}, otherFlag:{isNot:false}, yetAnotherFlag:{isNot:null}}).sql,
                "SELECT * FROM test WHERE ((flag IS NOT TRUE) AND (otherFlag IS NOT FALSE) AND (yetAnotherFlag IS NOT NULL))");

            assert.equal(ds.find({flag:{isNot:true}, otherFlag:{is:false}, yetAnotherFlag:{isNot:null}}).sql,
                "SELECT * FROM test WHERE ((flag IS NOT TRUE) AND (otherFlag IS FALSE) AND (yetAnotherFlag IS NOT NULL))");
            assert.equal(ds.find({flag:{is:true}, otherFlag:{isNot:false}, yetAnotherFlag:{is:null}}).sql,
                "SELECT * FROM test WHERE ((flag IS TRUE) AND (otherFlag IS NOT FALSE) AND (yetAnotherFlag IS NULL))");


            assert.throws(function () {
                ds.isTrue(["flag"]);
            });

            assert.throws(function () {
                ds.isFalse(["flag"]);
            })

            assert.throws(function () {
                ds.isNull(["flag"]);
            });

            assert.throws(function () {
                ds.isNotNull(["flag"]);
            })

            assert.throws(function () {
                ds.isNotTrue(["flag"]);
            });

            assert.throws(function () {
                ds.isNotFalse(["flag"]);
            })

        }
    }
});

suite.run({reporter : vows.reporter.spec}, function(){
    patio.disconnect();
    ret.callback();
});
