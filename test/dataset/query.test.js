var it = require('it'),
    assert = require('assert'),
    patio = require("index"),
    helper = require("../helpers/helper"),
    MockDatabase = helper.MockDatabase,
    MockDataset = helper.MockDataset,
    Dataset = patio.Dataset,
    sql = patio.SQL,
    Identifier = sql.Identifier,
    SQLFunction = sql.SQLFunction,
    LiteralString = sql.LiteralString,
    QualifiedIdentifier = sql.QualifiedIdentifier,
    comb = require("comb"),
    hitch = comb.hitch;


it.describe("Dataset queries",function (it) {
    patio.identifierInputMethod = null;
    patio.identifierOutputMethod = null;

    it.describe("sql statements", function (it) {
        var ds = new Dataset().from("test");
        it.should("format a SELECT statement ", function () {
            assert.equal(ds.selectSql, "SELECT * FROM test");
        });

        it.should("format a delete statement", function () {
            assert.equal(ds.deleteSql, 'DELETE FROM test');
        });
        it.should("format a truncate statement", function () {
            assert.equal(ds.truncateSql, 'TRUNCATE TABLE test');
        });
    });

    it.describe("#insertSql", function (it) {
        var ds = new Dataset().from("test");
        it.should("format an insert statement with default values", function () {
            assert.equal(ds.insertSql(), 'INSERT INTO test DEFAULT VALUES');
        });
        it.should("format an insert statement with hash", function () {
            assert.equal(ds.insertSql({name: 'wxyz', price: 342}), "INSERT INTO test (name, price) VALUES ('wxyz', 342)");
            assert.equal(ds.insertSql({}), "INSERT INTO test DEFAULT VALUES");
        });

        it.should("format an insert statement with an object that has a values property", function () {
            var v = {values: {a: 1}};
            assert.equal(ds.insertSql(v), "INSERT INTO test (a) VALUES (1)");
            assert.equal(ds.insertSql({}), "INSERT INTO test DEFAULT VALUES");
        });

        it.should("format an insert statement with an arbitrary value", function () {
            assert.equal(ds.insertSql(123), "INSERT INTO test VALUES (123)");
        });

        it.should("format an insert statement with sub-query", function () {
            var sub = new Dataset().from("something").filter({x: 2});
            assert.equal(ds.insertSql(sub), "INSERT INTO test SELECT * FROM something WHERE (x = 2)");

        });

        it.should("format an insert statement with array", function () {
            assert.equal(ds.insertSql('a', 2, 6.5), "INSERT INTO test VALUES ('a', 2, 6.5)");
        });

        it.should("format an update statement", function () {
            assert.equal(ds.updateSql({name: 'abc'}), "UPDATE test SET name = 'abc'");
        });

        it.should("accept hash with string keys", function () {
            assert.equal(ds.insertSql({c: 'd'}), "INSERT INTO test (c) VALUES ('d')");
        });

        it.should("accept hash with string keys", function () {
            assert.equal(ds.insertSql({c: 'd'}), "INSERT INTO test (c) VALUES ('d')");
        });

        it.should("accept array subscript references", function () {
            assert.equal(ds.insertSql(sql.day.sqlSubscript(1).eq("d")), "INSERT INTO test (day[1]) VALUES ('d')");
        });

        it.should("accept buffers", function () {
            assert.equal(ds.insertSql(new Buffer("this is a test")), "INSERT INTO test VALUES (X'7468697320697320612074657374')");
        });

        it.should("raise an Error if the dataset has no sources", function () {
            assert.throws(hitch(new Dataset(), "insertSql"));
        });

        it.should("accept datasets", function () {
            assert.equal(ds.insertSql(ds), "INSERT INTO test SELECT * FROM test");
        });

        it.should("accept datasets with columns", function () {
            assert.equal(ds.insertSql(["a", "b"], ds), "INSERT INTO test (a, b) SELECT * FROM test");
        });

        it.should("raise if given bad values", function () {
            assert.throws(hitch(ds.mergeOptions({values: 'a'}), "_insertSql"));
        });

        it.should("accept separate values", function () {
            assert.equal(ds.insertSql(1), "INSERT INTO test VALUES (1)");
            assert.equal(ds.insertSql(1, 2), "INSERT INTO test VALUES (1, 2)");
            assert.equal(ds.insertSql(1, 2, 3), "INSERT INTO test VALUES (1, 2, 3)");
        });

        it.should("accept a single array of values", function () {
            assert.equal(ds.insertSql([1, 2, 3]), "INSERT INTO test VALUES (1, 2, 3)");
        });

        it.should("accept an array of columns and an array of values", function () {
            assert.equal(ds.insertSql(["a", "b", "c"], [1, 2, 3]), "INSERT INTO test (a, b, c) VALUES (1, 2, 3)");
        });


        it.should("accept a single LiteralString", function () {
            assert.equal(ds.insertSql(sql.literal('VALUES (1, 2, 3)')), "INSERT INTO test VALUES (1, 2, 3)");
        });


        it.should("accept an array of columns and an LiteralString", function () {
            assert.equal(ds.insertSql(["a", "b", "c"], sql.literal('VALUES (1, 2, 3)')), "INSERT INTO test (a, b, c) VALUES (1, 2, 3)");
        });

        it.should("accept an object that responds to values and returns a hash by using that hash as the columns and values", function () {
            var o = {values: {c: "d"}};
            assert.equal(ds.insertSql(o), "INSERT INTO test (c) VALUES ('d')");
        });

        it.should("accept an object that responds to values and returns something other than a hash by using the object itself as a single value", function () {
            var o = new Date(2000, 0, 1);
            o.values = function () {
                return  this;
            };
            assert.equal(ds.insertSql(o), "INSERT INTO test VALUES ('2000-01-01')");
        });

    });


    it.describe("A dataset with multiple tables in its FROM clause", function (it) {

        var ds = new Dataset().from("t1", "t2");

        it.should("raise on updateSql", function () {
            assert.throws(comb.hitch(ds, ds.updateSql, {a: 1}));
        });

        it.should("raise on deleteSql", function () {
            assert.throws(function () {
                return ds.deleteSql;
            });
        });

        it.should("raise on //truncateSql", function () {
            assert.throws(function () {
                return ds.truncateSql;
            });
        });

        it.should("raise on //insertSql", function () {
            assert.throws(comb.hitch(ds, ds.insertSql));
        });

        it.should("generate a SELECT query FROM all specified tables", function () {
            assert.equal(ds.selectSql, "SELECT * FROM t1, t2");
        });
    });

    it.describe("#unusedTableAlias ", function (it) {

        var ds = new Dataset().from("test");


        it.should("return given string if it hasn't already been used", function () {
            assert.equal(ds.unusedTableAlias("blah"), "blah");
        });

        it.should("return a string specifying an alias that hasn't already been used if it has already been used", function () {
            assert.equal(ds.unusedTableAlias("test"), "test0");
            assert.equal(ds.from("test", "test0").unusedTableAlias("test"), "test1");
            assert.equal(ds.from("test", "test0").crossJoin("test1").unusedTableAlias("test"), "test2");
        });

        it.should("return an appropriate string if given other forms of identifiers", function () {
            ds.mergeOptions({from: null});
            ds.from("test");
            assert.equal(ds.unusedTableAlias('test'), "test0");
            assert.equal(ds.unusedTableAlias("b__t___test"), "test0");
            assert.equal(ds.unusedTableAlias("b__test"), "test0");
            assert.equal(ds.unusedTableAlias(new Identifier("test").qualify("b")), "test0");
            assert.equal(ds.unusedTableAlias(new Identifier("b").as(new Identifier("test"))), "test0");
            assert.equal(ds.unusedTableAlias(new Identifier("b").as("test")), "test0");
            assert.equal(ds.unusedTableAlias(new Identifier("test")), "test0");
        });
    });

    it.describe("#exists", function (it) {
        var ds1 = new Dataset().from("test"),
            ds2 = ds1.filter({price: {lt: 100}}),
            ds3 = ds1.filter({price: {gt: 50}});

        it.should("work in filters", function () {
            assert.equal(ds1.filter(ds2.exists).sql, 'SELECT * FROM test WHERE (EXISTS (SELECT * FROM test WHERE (price < 100)))');
            assert.equal(ds1.filter(ds2.exists.and(ds3.exists)).sql, 'SELECT * FROM test WHERE (EXISTS (SELECT * FROM test WHERE (price < 100)) AND EXISTS (SELECT * FROM test WHERE (price > 50)))');
        });

        it.should("work in SELECT", function () {
            assert.equal(ds1.select(ds2.exists.as("a"), ds3.exists.as("b")).sql, 'SELECT EXISTS (SELECT * FROM test WHERE (price < 100)) AS a, EXISTS (SELECT * FROM test WHERE (price > 50)) AS b FROM test');
        });
    });

    it.describe("#where", function (it) {
        var dataset = new Dataset().from("test"),
            d1 = dataset.where({region: "Asia"}),
            d2 = dataset.where("region = ?", "Asia"),
            d3 = dataset.where(sql.literal("a = 1"));


        it.should("just clone if given an empty argument", function () {
            assert.equal(dataset.where({}).sql, dataset.sql);
            assert.equal(dataset.where([]).sql, dataset.sql);
            assert.equal(dataset.where("").sql, dataset.sql);

            assert.equal(dataset.filter({}).sql, dataset.sql);
            assert.equal(dataset.filter([]).sql, dataset.sql);
            assert.equal(dataset.filter("").sql, dataset.sql);

        });

        it.should("work with hashes", function () {
            assert.equal(dataset.where({name: 'xyz', price: 342}).selectSql, "SELECT * FROM test WHERE ((name = 'xyz') AND (price = 342))");
        });

        it.should("work with a string with placeholders and arguments for those placeholders", function () {
            assert.equal(dataset.where('price < ? AND id in ?', 100, [1, 2, 3]).selectSql, "SELECT * FROM test WHERE (price < 100 AND id in (1, 2, 3))");
        });

        it.should("not modify passed array with placeholders", function () {
            var a = ['price < ? AND id in ?', 100, 1, 2, 3];
            var b = a.slice(0);
            dataset.where(a);
            assert.deepEqual(b, a);
        });

        it.should("work with strings (custom sql expressions)", function () {
            assert.equal(dataset.where(sql.literal('(a = 1 AND b = 2)')).selectSql, "SELECT * FROM test WHERE ((a = 1 AND b = 2))");
        });

        it.should("work with a string with named placeholders and a hash of placeholder value arguments", function () {
            assert.equal(dataset.where('price < {price} AND id in {ids}', {price: 100, ids: [1, 2, 3]}).selectSql, "SELECT * FROM test WHERE (price < 100 AND id in (1, 2, 3))");
        });

        it.should("not modify passed array with named placeholders", function () {
            var a = ['price < {price} AND id in {ids}', {price: 100}];
            var b = a.slice(0);
            dataset.where(a);
            assert.deepEqual(b, a);
        });

        it.should("not replace named placeholders that don't existin in the hash", function () {
            assert.equal(dataset.where('price < {price} AND id in {ids}', {price: 100}).selectSql, "SELECT * FROM test WHERE (price < 100 AND id in {ids})");
        });

        it.should("handle partial names", function () {
            assert.equal(dataset.where('price < {price} AND id = {p}', {p: 2, price: 100}).selectSql, "SELECT * FROM test WHERE (price < 100 AND id = 2)");
        });

        it.should("affect SELECT, delete and update statements", function () {
            assert.equal(d1.selectSql, "SELECT * FROM test WHERE (region = 'Asia')");
            assert.equal(d1.deleteSql, "DELETE FROM test WHERE (region = 'Asia')");
            assert.equal(d1.updateSql({GDP: 0}), "UPDATE test SET GDP = 0 WHERE (region = 'Asia')");

            assert.equal(d2.selectSql, "SELECT * FROM test WHERE (region = 'Asia')");
            assert.equal(d2.deleteSql, "DELETE FROM test WHERE (region = 'Asia')");
            assert.equal(d2.updateSql({GDP: 0}), "UPDATE test SET GDP = 0 WHERE (region = 'Asia')");

            assert.equal(d3.selectSql, "SELECT * FROM test WHERE (a = 1)");
            assert.equal(d3.deleteSql, "DELETE FROM test WHERE (a = 1)");
            assert.equal(d3.updateSql({GDP: 0}), "UPDATE test SET GDP = 0 WHERE (a = 1)");

        });

        it.should("be composable using AND operator (for scoping)", function () {
            assert.equal(d1.where({size: 'big'}).selectSql, "SELECT * FROM test WHERE ((region = 'Asia') AND (size = 'big'))");

            assert.equal(d1.where(sql.literal('population > 1000')).selectSql, "SELECT * FROM test WHERE ((region = 'Asia') AND (population > 1000))");
            assert.equal(d1.where(sql.literal('(a > 1) OR (b < 2)')).selectSql, "SELECT * FROM test WHERE ((region = 'Asia') AND ((a > 1) OR (b < 2)))");

            assert.equal(d1.where('GDP > ?', 1000).selectSql, "SELECT * FROM test WHERE ((region = 'Asia') AND (GDP > 1000))");

            assert.equal(d2.where('GDP > ?', 1000).selectSql, "SELECT * FROM test WHERE ((region = 'Asia') AND (GDP > 1000))");

            assert.equal(d2.where({name: ['Japan', 'China']}).selectSql, "SELECT * FROM test WHERE ((region = 'Asia') AND (name IN ('Japan', 'China')))");

            assert.equal(d2.where(sql.literal('GDP > ?')).selectSql, "SELECT * FROM test WHERE ((region = 'Asia') AND (GDP > ?))");

            assert.equal(d2.where(sql.literal('GDP > ?')).selectSql, "SELECT * FROM test WHERE ((region = 'Asia') AND (GDP > ?))");

            assert.equal(d2.where(sql.literal('(size = ?)', sql.identifier("big"))).selectSql, "SELECT * FROM test WHERE ((region = 'Asia') AND (size = big))");

            assert.equal(d3.where({c: 3}).selectSql, "SELECT * FROM test WHERE ((a = 1) AND (c = 3))");

            assert.equal(d3.where('d = ?', 4).selectSql, "SELECT * FROM test WHERE ((a = 1) AND (d = 4))");

            assert.equal(d3.where({e: {lt: 5}}).selectSql, "SELECT * FROM test WHERE ((a = 1) AND (e < 5))");
        });

        it.should("accept ranges", function () {
            assert.equal(dataset.filter({id: {between: [4, 7]}}).sql, 'SELECT * FROM test WHERE ((id >= 4) AND (id <= 7))');
            assert.equal(dataset.filter({table__id: {between: [4, 7]}}).sql, 'SELECT * FROM test WHERE ((table.id >= 4) AND (table.id <= 7))');
        });

        it.should("accept null", function () {
            assert.equal(dataset.filter({owner_id: null}).sql, 'SELECT * FROM test WHERE (owner_id IS NULL)');
        });

        it.should("accept a subquery", function () {
            assert.equal(dataset.filter('gdp > ?', d1.select(new SQLFunction("avg", "gdp"))).sql, "SELECT * FROM test WHERE (gdp > (SELECT avg(gdp) FROM test WHERE (region = 'Asia')))");
        });

        it.should("handle all types of IN/NOT IN queries", function () {
            assert.equal(dataset.filter({id: d1.select("id")}).sql, "SELECT * FROM test WHERE (id IN (SELECT id FROM test WHERE (region = 'Asia')))");
            assert.equal(dataset.filter({id: []}).sql, "SELECT * FROM test WHERE (id != id)");
            assert.equal(dataset.filter({id: [1, 2]}).sql, "SELECT * FROM test WHERE (id IN (1, 2))");
            assert.equal(dataset.filter({"id1,id2": d1.select("id1", "id2")}).sql, "SELECT * FROM test WHERE ((id1, id2) IN (SELECT id1, id2 FROM test WHERE (region = 'Asia')))");
            assert.equal(dataset.filter({"id1,id2": []}).sql, "SELECT * FROM test WHERE ((id1 != id1) AND (id2 != id2))");
            assert.equal(dataset.filter({"id1,id2": [
                [1, 2],
                [3, 4]
            ]}).sql, "SELECT * FROM test WHERE ((id1, id2) IN ((1, 2), (3, 4)))");

            assert.equal(dataset.exclude({id: d1.select("id")}).sql, "SELECT * FROM test WHERE (id NOT IN (SELECT id FROM test WHERE (region = 'Asia')))");
            //assert.equal(dataset.exclude({id : []}).sql, "SELECT * FROM test WHERE (1 = 1)");
            assert.equal(dataset.exclude({id: [1, 2]}).sql, "SELECT * FROM test WHERE (id NOT IN (1, 2))");
            assert.equal(dataset.exclude({"id1,id2": d1.select("id1", "id2")}).sql, "SELECT * FROM test WHERE ((id1, id2) NOT IN (SELECT id1, id2 FROM test WHERE (region = 'Asia')))");
            assert.equal(dataset.exclude({"id1,id2": []}).sql, "SELECT * FROM test WHERE (1 = 1)");
            assert.equal(dataset.exclude({"id1,id2": [
                [1, 2],
                [3, 4]
            ]}).sql, "SELECT * FROM test WHERE ((id1, id2) NOT IN ((1, 2), (3, 4)))");
        });

        it.should("accept a subquery for an EXISTS clause", function () {
            var a = dataset.filter({price: {lt: 100}});
            assert.equal(dataset.filter(a.exists).sql, 'SELECT * FROM test WHERE (EXISTS (SELECT * FROM test WHERE (price < 100)))');
        });

        it.should("accept proc expressions", function () {
            var d = d1.select(sql.gdp.avg());

            assert.equal(dataset.filter(
                function () {
                    return this.gdp.gt(d);
                }).sql, "SELECT * FROM test WHERE (gdp > (SELECT avg(gdp) FROM test WHERE (region = 'Asia')))");
            assert.equal(dataset.filter(
                function () {
                    return this.a.lt(1);
                }).sql, 'SELECT * FROM test WHERE (a < 1)');

            assert.equal(dataset.filter(
                function () {
                    return this.a.gte(1).and(this.b.lte(2));
                }).sql, 'SELECT * FROM test WHERE ((a >= 1) AND (b <= 2))');

            assert.equal(dataset.filter(
                function () {
                    return this.c.like('ABC%');
                }).sql, "SELECT * FROM test WHERE (c LIKE 'ABC%')");

            assert.equal(dataset.filter(
                function () {
                    return this.c.like('ABC%', '%XYZ');
                }).sql, "SELECT * FROM test WHERE ((c LIKE 'ABC%') OR (c LIKE '%XYZ'))");
        });

        it.should("work for grouped datasets", function () {
            assert.equal(dataset.group("a").filter({b: 1}).sql, 'SELECT * FROM test WHERE (b = 1) GROUP BY a');
        });

        it.should("accept true and false as arguments", function () {
            assert.equal(dataset.filter(true).sql, "SELECT * FROM test WHERE 't'");
            assert.equal(dataset.filter(false).sql, "SELECT * FROM test WHERE 'f'");
        });

        it.should("allow the use of multiple arguments", function () {
            assert.equal(dataset.filter(new Identifier("a"), new Identifier("b")).sql, 'SELECT * FROM test WHERE (a AND b)');
            assert.equal(dataset.filter(new Identifier("a"), {b: 1}).sql, 'SELECT * FROM test WHERE (a AND (b = 1))');
            assert.equal(dataset.filter(new Identifier("a"), {c: {gt: 3}}, {b: 1}).sql, 'SELECT * FROM test WHERE (a AND (c > 3) AND (b = 1))');
        });

        it.should("allow the use of blocks and arguments simultaneously", function () {
            assert.equal(dataset.filter({zz: {lt: 3}},
                function () {
                    return this.yy.gt(3);
                }).sql, 'SELECT * FROM test WHERE ((zz < 3) AND (yy > 3))');
        });

        it.should("yield an sql object to the cb", function () {
            var x = null;
            dataset.filter(function (r) {
                x = r;
                return false;
            });
            assert.deepEqual(x, sql);
            assert.equal(dataset.filter(
                function (test) {
                    return test.name.lt("b").and(test.table__id.eq(1)).or(test.is_active(test.blah, test.xx, test.x__y_z));
                }).sql, "SELECT * FROM test WHERE (((name < 'b') AND (table.id = 1)) OR is_active(blah, xx, x.y_z))");
        });

        it.should("eval the block in the context of sql if sql isnt an argument", function () {
            var x = null;
            dataset.filter(function (r) {
                x = this;
                return false;
            });
            assert.deepEqual(x, sql);
            assert.equal(dataset.filter(
                function (test) {
                    return this.name.lt("b").and(this.table__id.eq(1)).or(this.is_active(this.blah(), this.xx(), this.x__y_z()));
                }).sql, "SELECT * FROM test WHERE (((name < 'b') AND (table.id = 1)) OR is_active(blah, xx, x.y_z))");
        });


        it.should("raise an error if an invalid argument is used", function () {
            assert.throws(comb.hitch(dataset, "filter", 1));
        });

    });

    it.describe("#or", function (it) {
        var dataset = new Dataset().from("test"),
            d1 = dataset.where({x: 1});

        it.should("raise if no filter exists", function () {
            assert.throws(comb.hitch(dataset, "or", {a: 1}));
        });

        it.should("add an alternative expression to the where clause", function () {
            assert.equal(d1.or({y: 2}).sql, "SELECT * FROM test WHERE ((x = 1) OR (y = 2))");
        });

        it.should("accept all forms of filters", function () {
            assert.equal(d1.or("y > ?", 2).sql, 'SELECT * FROM test WHERE ((x = 1) OR (y > 2))');
            assert.equal(d1.or({yy: {gt: 3}}).sql, 'SELECT * FROM test WHERE ((x = 1) OR (yy > 3))');
            assert.equal(d1.or(sql.yy.gt(3)).sql, 'SELECT * FROM test WHERE ((x = 1) OR (yy > 3))');
        });

        it.should("correctly add parens to give predictable results", function () {
            assert.equal(d1.filter({y: 2}).or({z: 3}).sql, 'SELECT * FROM test WHERE (((x = 1) AND (y = 2)) OR (z = 3))');
            assert.equal(d1.or({y: 2}).filter({z: 3}).sql, 'SELECT * FROM test WHERE (((x = 1) OR (y = 2)) AND (z = 3))');
        });

    });

    it.describe("#andGroupedOr", function (it) {
        var dataset = new Dataset().from("test"),
            d1 = dataset.where({x: 1});

        it.should("raise if no filter exists", function () {
            assert.throws(comb.hitch(dataset, "andGroupedOr", [
                {a: 1},
                {y: 2}
            ]));
        });

        it.should("add an alternate expression of ORed conditions wrapped in parens to the where clause", function () {
            assert.equal(d1.andGroupedOr([
                ['y', 2],
                ['y', 3]
            ]).sql, "SELECT * FROM test WHERE ((x = 1) AND ((y = 2) OR (y = 3)))");
        });
    });

    it.describe("#andGroupedAnd", function (it) {
        var dataset = new Dataset().from("test"),
            d1 = dataset.where({x: 1});

        it.should("raise if no filter exists", function () {
            assert.throws(comb.hitch(dataset, "andGroupedAnd", [
                {a: 1},
                {y: 2}
            ]));
        });

        it.should("add an alternate expression of ORed conditions wrapped in parens to the where clause", function () {
            assert.equal(d1.andGroupedAnd([
                ['y', 2],
                ['y', 3]
            ]).sql, "SELECT * FROM test WHERE ((x = 1) AND (y = 2) AND (y = 3))");
        });

    });

    it.describe("#orGroupedAnd", function (it) {
        var dataset = new Dataset().from("test"),
            d1 = dataset.where({x: 1});

        it.should("raise if no filter exists", function () {
            assert.throws(comb.hitch(dataset, "orGroupedAnd", [
                {a: 1},
                {y: 2}
            ]));
        });

        it.should("add an additional expression of ANDed conditions wrapped in parens to the where clause", function () {
            assert.equal(d1.orGroupedAnd([
                ['x', 2],
                ['y', 3]
            ]).sql, "SELECT * FROM test WHERE ((x = 1) OR ((x = 2) AND (y = 3)))");
        });

    });

    it.describe("#orGroupedOr", function (it) {
        var dataset = new Dataset().from("test"),
            d1 = dataset.where({x: 1, y: "z"});

        it.should("raise if no filter exists", function () {
            assert.throws(comb.hitch(dataset, "orGroupedAnd", [
                {a: 1},
                {y: 2}
            ]));
        });

        it.should("add an additional expression of ANDed conditions wrapped in parens to the where clause", function () {
            assert.equal(d1.orGroupedOr([
                ['x', 2],
                ['y', 3]
            ]).sql, "SELECT * FROM test WHERE (((x = 1) AND (y = 'z')) OR (x = 2) OR (y = 3))");
        });

    });

    it.describe("#and", function (it) {
        var dataset = new Dataset().from("test"),
            d1 = dataset.where({x: 1});

        it.should("raise if no filter exists", function () {
            assert.throws(comb.hitch(dataset, "and", {a: 1}));
        });

        it.should("add an alternative expression to the where clause", function () {
            assert.equal(d1.and({y: 2}).sql, "SELECT * FROM test WHERE ((x = 1) AND (y = 2))");
        });

        it.should("accept all forms of filters", function () {
            assert.equal(d1.and("y > ?", 2).sql, 'SELECT * FROM test WHERE ((x = 1) AND (y > 2))');
            assert.equal(d1.and({yy: {gt: 3}}).sql, 'SELECT * FROM test WHERE ((x = 1) AND (yy > 3))');
        });

        it.should("correctly add parens to give predictable results", function () {
            assert.equal(d1.and({y: 2}).or({z: 3}).sql, 'SELECT * FROM test WHERE (((x = 1) AND (y = 2)) OR (z = 3))');
            assert.equal(d1.or({y: 2}).and({z: 3}).sql, 'SELECT * FROM test WHERE (((x = 1) OR (y = 2)) AND (z = 3))');
        });

    });

    it.describe("#exclude", function (it) {
        var ds = new Dataset().from("test");

        it.should("correctly negate the expression when one condition is given", function () {
            assert.equal(ds.exclude({region: 'Asia'}).selectSql, "SELECT * FROM test WHERE (region != 'Asia')");
        });

        it.should("take multiple conditions as a hash and express the logic correctly in SQL", function () {
            assert.equal(ds.exclude({region: 'Asia', name: 'Japan'}).selectSql, "SELECT * FROM test WHERE ((region != 'Asia') OR (name != 'Japan'))");
        });

        it.should("parenthesize a single string condition correctly", function () {
            assert.equal(ds.exclude(sql.literal("region = 'Asia' AND name = 'Japan'")).selectSql, "SELECT * FROM test WHERE NOT (region = 'Asia' AND name = 'Japan')");
        });

        it.should("parenthesize an array condition correctly", function () {
            assert.equal(ds.exclude('region = ? AND name = ?', 'Asia', 'Japan').selectSql, "SELECT * FROM test WHERE NOT (region = 'Asia' AND name = 'Japan')");
        });

        it.should("correctly parenthesize when it is used twice", function () {
            assert.equal(ds.exclude({region: 'Asia'}).exclude({name: 'Japan'}).selectSql, "SELECT * FROM test WHERE ((region != 'Asia') AND (name != 'Japan'))");
        });

        it.should("support proc expressions", function () {
            assert.equal(ds.exclude({id: {lt: 6}}).selectSql, 'SELECT * FROM test WHERE (id >= 6)');
        });
    });


    it.describe("#invert", function (it) {

        var ds = new Dataset().from("test");

        it.should("raise error if the dataset is not filtered", function () {
            assert.throws(comb.hitch(ds, "invert"));
        });

        it.should("invert current filter if dataset is filtered", function () {
            assert.equal(ds.filter(new sql.Identifier("x")).invert().sql, 'SELECT * FROM test WHERE NOT x');
        });

        it.should("invert both having and where if both are preset", function () {
            var ident = new sql.Identifier("x");
            assert.equal(ds.filter(ident).group(ident).having(ident).invert().sql, 'SELECT * FROM test WHERE NOT x GROUP BY x HAVING NOT x');
        });
    });

    it.describe("having", function (it) {
        var dataset = new Dataset().from("test");
        var grouped = dataset.group(sql.region).select(sql.region, sql.population.sum(), sql.gdp.avg());
        var d1 = grouped.having(sql.sum("population").gt(10));
        var d2 = grouped.having({region: 'Asia'});
        var columns = "region, sum(population), avg(gdp)";

        it.should("just clone if given an empty argument", function () {
            assert.equal(dataset.having({}).sql, dataset.sql);
            assert.equal(dataset.having([]).sql, dataset.sql);
            assert.equal(dataset.having('').sql, dataset.sql);
        });

        it.should("affect SELECT statements", function () {
            assert.equal(
                d1.selectSql,
                "SELECT " + columns + " FROM test GROUP BY region HAVING (sum(population) > 10)");
        });

        it.should("support proc expressions", function () {
            assert.equal(grouped.having(sql.sum("population").gt(10)).sql, "SELECT " + columns + " FROM test GROUP BY region HAVING (sum(population) > 10)");
        });

        it.should("work with and on the having clause", function () {
            assert.equal(grouped.having(sql.a().sqlNumber.gt(1)).and(sql.b().sqlNumber.lt(2)).sql, "SELECT " + columns + " FROM test GROUP BY region HAVING ((a > 1) AND (b < 2))");
        });
    });

    it.describe("a grouped dataset", function (it) {

        var dataset = new Dataset().from("test").group("type_id");


        it.should("raise when trying to generate an update statement", function () {
            assert.throws(function () {
                dataset.updateSql({id: 0});
            });
        });

        it.should("raise when trying to generate a delete statement", function () {
            assert.throws(function () {
                return dataset.deleteSql;
            });
        });

        it.should("raise when trying to generate a truncate statement", function () {
            assert.throws(function () {
                return dataset.truncateSql;
            });
        });

        it.should("raise when trying to generate an insert statement", function () {
            assert.throws(function () {
                return dataset.insertSql();
            });
        });

        it.should("specify the grouping in generated SELECT statement", function () {
            assert.equal(dataset.selectSql, "SELECT * FROM test GROUP BY type_id");
        });
    });

    it.describe("#groupBy", function (it) {

        var dataset = new Dataset().from("test").groupBy("type_id");

        it.should("raise when trying to generate an update statement", function () {
            assert.throws(function () {
                dataset.updateSql({id: 0});
            });
        });

        it.should("raise when trying to generate a delete statement", function () {
            assert.throws(function () {
                return dataset.deleteSql;
            });
        });

        it.should("raise when trying to generate a truncate statement", function () {
            assert.throws(function () {
                return dataset.truncateSql;
            });
        });

        it.should("raise when trying to generate an insert statement", function () {
            assert.throws(function () {
                return dataset.insertSql();
            });
        });

        it.should("specify the grouping in generated SELECT statement", function () {
            assert.equal(dataset.selectSql, "SELECT * FROM test GROUP BY type_id");
            assert.equal(dataset.groupBy("a", "b").selectSql, "SELECT * FROM test GROUP BY a, b");
            assert.equal(dataset.groupBy({type_id: null}).selectSql, "SELECT * FROM test GROUP BY (type_id IS NULL)");
        });

        it.should("ungroup when passed null, empty, or no arguments", function () {
            assert.equal(dataset.groupBy().selectSql, "SELECT * FROM test");
            assert.equal(dataset.groupBy(null).selectSql, "SELECT * FROM test");
        });

        it.should("undo previous grouping", function () {
            assert.equal(dataset.groupBy("a").groupBy("b").selectSql, "SELECT * FROM test GROUP BY b");
            assert.equal(dataset.groupBy("a", "b").groupBy().selectSql, "SELECT * FROM test");
        });

        it.should("be aliased as #group", function () {
            assert.equal(dataset.group({type_id: null}).selectSql, "SELECT * FROM test GROUP BY (type_id IS NULL)");
        });
    });

    it.describe("#as", function (it) {
        var dataset = new Dataset().from("test");
        it.should("set up an alias", function () {
            assert.equal(dataset.select(dataset.limit(1).select("name").as("n")).sql, 'SELECT (SELECT name FROM test LIMIT 1) AS n FROM test');
        });
    });

    it.describe("#literal", function (it) {
        var dataset = new Dataset().from("test");

        it.should("escape strings properly", function () {
            assert.equal(dataset.literal('abc'), "'abc'");
            assert.equal(dataset.literal('a"x"bc'), "'a\"x\"bc'");
            assert.equal(dataset.literal("a'bc"), "'a''bc'");
            assert.equal(dataset.literal("a''bc"), "'a''''bc'");
            assert.equal(dataset.literal("a\\bc"), "'a\\\\bc'");
            assert.equal(dataset.literal("a\\\\bc"), "'a\\\\\\\\bc'");
            assert.equal(dataset.literal("a\\'bc"), "'a\\\\''bc'");
        });

        it.should("literalize numbers properly", function () {
            assert.equal(dataset.literal(1), "1");
            assert.equal(dataset.literal(1.5), "1.5");
        });

        it.should("literalize nil as NULL", function () {
            assert.equal(dataset.literal(null), "NULL");
        });

        it.should("literalize an array properly", function () {
            assert.equal(dataset.literal([]), "(NULL)");
            assert.equal(dataset.literal([1, 'abc', 3]), "(1, 'abc', 3)");
            assert.equal(dataset.literal([1, "a'b''c", 3]), "(1, 'a''b''''c', 3)");
        });

        it.should("literalize string as column references", function () {
            assert.equal(dataset.literal(sql.name), "name");
            assert.equal(dataset.literal("items__name"), "items.name");
        });

        it.should("call sqlLiteral with dataset on type if not natively supported and the object responds to it", function () {
            var a = function () {
            };
            a.prototype.sqlLiteral = function (ds) {
                return  "called ";
            };
            assert.equal(dataset.literal(new a()), "called ");
        });

        it.should("raise an error for unsupported types with no sqlLiteral method", function () {
            assert.throws(function () {
                dataset.literal(function () {
                });
            });
        });

        it.should("literalize datasets as subqueries", function () {
            var d = dataset.from("test");
            assert.equal(d.literal(d), "(" + d.sql + ")");
        });


        it.should("literalize TimeStamp properly", function () {
            var d = new sql.TimeStamp();
            assert.equal(dataset.literal(d), comb.date.format(d.date, "'yyyy-MM-dd HH:mm:ss'"));
        });


        it.should("literalize Date properly", function () {
            var d = new Date();
            assert.equal(dataset.literal(d), comb.string.format("'%[yyyy-MM-dd]D'", [d]));
        });

        it.should("not modify literal strings", function () {
            assert.equal(dataset.literal(sql['col1 + 2']), 'col1 + 2');
            assert.equal(dataset.updateSql({a: sql['a + 2']}), 'UPDATE test SET a = a + 2');
        });

        it.should("convert literals properly", function () {
            assert.equal(dataset.literal(sql.literal("(hello, world)")), "(hello, world)");
            assert.equal(dataset.literal("('hello', 'world')"), "'(''hello'', ''world'')'");
            assert.equal(dataset.literal("(hello, world)"), "'(hello, world)'");
            assert.equal(dataset.literal("hello, world"), "'hello, world'");
            assert.equal(dataset.literal('("hello", "world")'), "'(\"hello\", \"world\")'");
            assert.equal(dataset.literal("(hello, world)'"), "'(hello, world)'''");
            assert.equal(dataset.literal("\\'\\'"), "'\\\\''\\\\'''");
            assert.strictEqual(dataset.literal(1), "1");
            assert.strictEqual(dataset.literal(1.0), "1");
            assert.strictEqual(dataset.literal(1.01), "1.01");
            assert.equal(dataset.literal(sql.hello.lt(1)), '(hello < 1)');
            assert.equal(dataset.literal(sql.hello.gt(1)), '(hello > 1)');
            assert.equal(dataset.literal(sql.hello.lte(1)), '(hello <= 1)');
            assert.equal(dataset.literal(sql.hello.gte(1)), '(hello >= 1)');
            assert.equal(dataset.literal(sql.hello.like("test")), "(hello LIKE 'test')");
            assert.equal(dataset.literal(dataset.from("test").order("name")), "(SELECT * FROM test ORDER BY name)");
            assert.equal(dataset.literal([1, 2, 3]), "(1, 2, 3)");
            assert.equal(dataset.literal([1, "2", 3]), "(1, '2', 3)");
            assert.equal(dataset.literal([1, "\\'\\'", 3]), "(1, '\\\\''\\\\''', 3)");
            assert.equal(dataset.literal(new sql.Year(2009)), '2009');
            assert.equal(dataset.literal(new sql.TimeStamp(2009, 10, 10, 10, 10)), "'2009-11-10 10:10:00'");
            assert.equal(dataset.literal(new sql.DateTime(2009, 10, 10, 10, 10)), "'2009-11-10 10:10:00'");
            assert.equal(dataset.literal(new Date(2009, 10, 10)), "'2009-11-10'");
            assert.equal(dataset.literal(new sql.Time(11, 10, 10)), "'11:10:10'");
            assert.equal(dataset.literal(null), "NULL");
            assert.equal(dataset.literal(true), "'t'");
            assert.equal(dataset.literal(false), "'f'");
            assert.equal(dataset.literal({a: "b"}), "(a = 'b')");
            assert.throws(comb.hitch(dataset, "literal", /a/));
        });
    });

    it.describe("#from", function (it) {
        var dataset = new Dataset();

        it.should("accept a Dataset", function () {
            assert.doesNotThrow(function () {
                dataset.from(dataset);
            });
        });

        it.should("format a Dataset as a subquery if it has had options set", function () {
            assert.equal(dataset.from(dataset.from("a").where({a: 1})).selectSql, "SELECT * FROM (SELECT * FROM a WHERE (a = 1)) AS t1");
        });

        it.should("automatically alias sub-queries", function () {
            assert.equal(dataset.from(dataset.from("a").group("b")).selectSql, "SELECT * FROM (SELECT * FROM a GROUP BY b) AS t1");

            var d1 = dataset.from("a").group("b");
            var d2 = dataset.from("c").group("d");

            assert.equal(dataset.from(d1, d2).sql, "SELECT * FROM (SELECT * FROM a GROUP BY b) AS t1, (SELECT * FROM c GROUP BY d) AS t2");
        });

        it.should("accept a hash for aliasing", function () {
            assert.equal(dataset.from({a: "b"}).sql, "SELECT * FROM a AS b");
            assert.equal(dataset.from(dataset.from("a").group("b").as("c")).sql, "SELECT * FROM (SELECT * FROM a GROUP BY b) AS c");
        });

        it.should("always use a subquery if given a dataset", function () {
            assert.equal(dataset.from(dataset.from("a")).selectSql, "SELECT * FROM (SELECT * FROM a) AS t1");
        });

        it.should("remove all FROM tables if called with no arguments", function () {
            assert.equal(dataset.from().sql, 'SELECT *');
        });

        it.should("accept sql functions", function () {
            assert.equal(dataset.from(sql.abc("def")).selectSql, "SELECT * FROM abc(def)");
            assert.equal(dataset.from(sql.a("i")).selectSql, "SELECT * FROM a(i)");
        });

        it.should("accept schema__table___alias string format", function () {
            assert.equal(dataset.from("abc__def").selectSql, "SELECT * FROM abc.def");
            assert.equal(dataset.from("abc__def___d").selectSql, "SELECT * FROM abc.def AS d");
            assert.equal(dataset.from("abc___def").selectSql, "SELECT * FROM abc AS def");
        });
    });

    it.describe("#select", function (it) {
        var dataset = new Dataset().from("test");

        it.should("accept variable arity", function () {
            assert.equal(dataset.select("name").sql, 'SELECT name FROM test');
            assert.equal(dataset.select("a", "b", "test__c").sql, 'SELECT a, b, test.c FROM test');
        });

        it.should("accept ColumnAll expression", function () {
            assert.equal(dataset.select(sql.test.all()).sql, 'SELECT test.* FROM test');
            assert.equal(dataset.select(new sql.ColumnAll("test")).sql, 'SELECT test.* FROM test');
        });

        it.should("accept strings and literal strings", function () {
            assert.equal(dataset.select("aaa").sql, 'SELECT aaa FROM test');
            assert.equal(dataset.select("a", "b").sql, 'SELECT a, b FROM test');
            assert.equal(dataset.select("test__cc", 'test.d AS e').sql, 'SELECT test.cc, test.d AS e FROM test');
            assert.equal(dataset.select('test.d AS e', "test__cc").sql, 'SELECT test.d AS e, test.cc FROM test');

            assert.equal(dataset.select("test.*").sql, 'SELECT test.* FROM test');
            assert.equal(dataset.select(sql.test__name.as("n")).sql, 'SELECT test.name AS n FROM test');
            assert.equal(dataset.select("test__name___n").sql, 'SELECT test.name AS n FROM test');
        });

        it.should("use the wildcard if no arguments are given", function () {
            assert.equal(dataset.select().sql, 'SELECT * FROM test');
        });

        it.should("accept a hash for AS values", function () {
            assert.equal(dataset.select({name: 'n', "__ggh": 'age'}).sql, "SELECT name AS n, __ggh AS age FROM test");
        });

        it.should("accept arbitrary objects and literalize them correctly", function () {
            assert.equal(dataset.select(1, "a", "\'t\'").sql, "SELECT 1, a, 't' FROM test");
            assert.equal(dataset.select(null, sql.sum("t"), "x___y").sql, "SELECT NULL, sum(t), x AS y FROM test");
            assert.equal(dataset.select(null, 1, {x: "y"}).sql, "SELECT NULL, 1, x AS y FROM test");
        });

        it.should("accept a block that yields a virtual row", function () {
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
        });

        it.should("merge regular arguments with argument returned from block", function () {
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
        });
    });

    it.describe("#selectAll", function (it) {

        var dataset = new Dataset().from("test");

        it.should("SELECT the wildcard", function () {
            assert.equal(dataset.selectAll().sql, 'SELECT * FROM test');
        });

        it.should("overrun the previous SELECT option", function () {
            assert.equal(dataset.select("a", "b", "c").selectAll().sql, 'SELECT * FROM test');
        });
    });

    it.describe("#selectMore", function (it) {
        var dataset = new Dataset().from("test");

        it.should("act like #SELECT for datasets with no selection", function () {
            assert.equal(dataset.selectMore("a", "b").sql, 'SELECT a, b FROM test');
            assert.equal(dataset.selectAll().selectMore("a", "b").sql, 'SELECT a, b FROM test');
            assert.equal(dataset.select("blah").selectAll().selectMore("a", "b").sql, 'SELECT a, b FROM test');
        });

        it.should("add to the currently selected columns", function () {
            assert.equal(dataset.select("a").selectMore("b").sql, 'SELECT a, b FROM test');
            assert.equal(dataset.select("a.*").selectMore("b.*").sql, 'SELECT a.*, b.* FROM test');
        });

        it.should("accept a block that yields a virtual row", function () {
            assert.equal(dataset.select("a").selectMore(
                function (o) {
                    return o.b;
                }).sql, 'SELECT a, b FROM test');
            assert.equal(dataset.select("a.*").selectMore("b.*",
                function () {
                    return this.b(1);
                }).sql, 'SELECT a.*, b.*, b(1) FROM test');
        });
    });

    it.describe("selectAppend", function (it) {
        var dataset = new Dataset().from("test");

        it.should("SELECT * in addition to columns if no columns selected", function () {
            assert.equal(dataset.selectAppend("a", "b").sql, 'SELECT *, a, b FROM test');
            assert.equal(dataset.selectAll().selectAppend("a", "b").sql, 'SELECT *, a, b FROM test');
            assert.equal(dataset.select("blah").selectAll().selectAppend("a", "b").sql, 'SELECT *, a, b FROM test');
        });

        it.should("add to the currently selected columns", function () {
            assert.equal(dataset.select("a").selectAppend("b").sql, 'SELECT a, b FROM test');
            assert.equal(dataset.select("a.*").selectAppend("b.*").sql, 'SELECT a.*, b.* FROM test');
        });

        it.should("accept a block that yields a virtual row", function () {
            assert.equal(dataset.select("a").selectAppend(
                function (o) {
                    return o.b;
                }).sql, 'SELECT a, b FROM test');
            assert.equal(dataset.select("a.*").selectAppend("b.*",
                function () {
                    return this.b(1);
                }).sql, 'SELECT a.*, b.*, b(1) FROM test');
        });
    });

    it.describe("#order", function (it) {
        var dataset = new Dataset().from("test");

        it.should("include an ORDER BY clause in the SELECT statement", function () {
            assert.equal(dataset.order("name").sql, 'SELECT * FROM test ORDER BY name');
        });

        it.should("accept multiple arguments", function () {
            assert.equal(dataset.order("name", sql.price.desc()).sql, 'SELECT * FROM test ORDER BY name, price DESC');
        });

        it.should("accept :nulls options for asc and desc", function () {
            assert.equal(dataset.order(sql.name.asc({nulls: "last"}), sql.price.desc({nulls: "first"})).sql, 'SELECT * FROM test ORDER BY name ASC NULLS LAST, price DESC NULLS FIRST');
        });

        it.should("overrun a previous ordering", function () {
            assert.equal(dataset.order("name").order("stamp").sql, 'SELECT * FROM test ORDER BY stamp');
        });

        it.should("accept a literal string", function () {
            assert.equal(dataset.order(sql['dada ASC']).sql, 'SELECT * FROM test ORDER BY dada ASC');
        });

        it.should("accept a hash as an expression", function () {
            assert.equal(dataset.order({name: null}).sql, 'SELECT * FROM test ORDER BY (name IS NULL)');
        });

        it.should("accept a nil to remove ordering", function () {
            assert.equal(dataset.order("bah").order(null).sql, 'SELECT * FROM test');
        });

        it.should("accept a block that yields a virtual row", function () {
            assert.equal(dataset.order(
                function (o) {
                    return o.a;
                }).sql, 'SELECT * FROM test ORDER BY a');
            assert.equal(dataset.order(
                function () {
                    return this.a(1);
                }).sql, 'SELECT * FROM test ORDER BY a(1)');
            assert.equal(dataset.order(
                function (o) {
                    return o.a(1, 2);
                }).sql, 'SELECT * FROM test ORDER BY a(1, 2)');
            assert.equal(dataset.order(
                function () {
                    return [this.a, this.a(1, 2)];
                }).sql, 'SELECT * FROM test ORDER BY a, a(1, 2)');
        });

        it.should("merge regular arguments with argument returned from block", function () {
            assert.equal(dataset.order("b",
                function () {
                    return this.a;
                }).sql, 'SELECT * FROM test ORDER BY b, a');
            assert.equal(dataset.order("b", "c",
                function (o) {
                    return o.a(1);
                }).sql, 'SELECT * FROM test ORDER BY b, c, a(1)');
            assert.equal(dataset.order("b",
                function () {
                    return [this.a, this.a(1, 2)];
                }).sql, 'SELECT * FROM test ORDER BY b, a, a(1, 2)');
            assert.equal(dataset.order("b", "c",
                function (o) {
                    return [o.a, o.a(1, 2)];
                }).sql, 'SELECT * FROM test ORDER BY b, c, a, a(1, 2)');
        });


    });

    it.describe("#unfiltered", function (it) {
        var dataset = new Dataset().from("test");
        it.should("remove filtering from the dataset", function () {
            assert.equal(dataset.filter({score: 1}).unfiltered().sql, 'SELECT * FROM test');
        });
    });

    it.describe("#unlimited", function (it) {
        var dataset = new Dataset().from("test");
        it.should("remove limit and offset from the dataset", function () {
            assert.equal(dataset.limit(1, 2).unlimited().sql, 'SELECT * FROM test');
        });
    });

    it.describe("#ungrouped", function (it) {
        var dataset = new Dataset().from("test");
        it.should("remove group and having clauses from the dataset", function () {
            assert.equal(dataset.group("a").having("b").ungrouped().sql, 'SELECT * FROM test');
        });
    });

    it.describe("#unordered", function (it) {
        var dataset = new Dataset().from("test");
        it.should("remove ordering from the dataset", function () {
            assert.equal(dataset.order("name").unordered().sql, 'SELECT * FROM test');
        });
    });
    it.describe("#withSql", function (it) {
        var dataset = new Dataset().from("test");

        it.should("use static sql", function () {
            assert.equal(dataset.withSql('SELECT 1 FROM test').sql, 'SELECT 1 FROM test');
        });

        it.should("work with placeholders", function () {
            assert.equal(dataset.withSql('SELECT ? FROM test', 1).sql, 'SELECT 1 FROM test');
        });

        it.should("work with named placeholders", function () {
            assert.equal(dataset.withSql('SELECT {x} FROM test', {x: 1}).sql, 'SELECT 1 FROM test');
        });
    });

    it.describe("#orderBy", function (it) {
        var dataset = new Dataset().from("test");

        it.should("include an ORDER BY clause in the SELECT statement", function () {
            assert.equal(dataset.orderBy("name").sql, 'SELECT * FROM test ORDER BY name');
        });

        it.should("accept multiple arguments", function () {
            assert.equal(dataset.orderBy("name", sql.price.desc()).sql, 'SELECT * FROM test ORDER BY name, price DESC');
        });

        it.should("overrun a previous ordering", function () {
            assert.equal(dataset.orderBy("name").order("stamp").sql, 'SELECT * FROM test ORDER BY stamp');
        });

        it.should("accept a string", function () {
            assert.equal(dataset.orderBy('dada ASC').sql, 'SELECT * FROM test ORDER BY dada ASC');
        });

        it.should("accept a nil to remove ordering", function () {
            assert.equal(dataset.orderBy("bah").orderBy(null).sql, 'SELECT * FROM test');
        });
    });


    it.describe("#orderMore, #orderAppend, #orderPrepend", function (it) {
        var dataset = new Dataset().from("test");

        it.should("include an ORDER BY clause in the SELECT statement", function () {
            assert.equal(dataset.orderMore("name").sql, 'SELECT * FROM test ORDER BY name');
            assert.equal(dataset.orderAppend("name").sql, 'SELECT * FROM test ORDER BY name');
            assert.equal(dataset.orderPrepend("name").sql, 'SELECT * FROM test ORDER BY name');
        });

        it.should("add to the }, of a previous ordering", function () {
            assert.equal(dataset.order("name").orderMore(sql.stamp.desc()).sql, 'SELECT * FROM test ORDER BY name, stamp DESC');
            assert.equal(dataset.order("name").orderAppend(sql.stamp.desc()).sql, 'SELECT * FROM test ORDER BY name, stamp DESC');
            assert.equal(dataset.order("name").orderPrepend(sql.stamp.desc()).sql, 'SELECT * FROM test ORDER BY stamp DESC, name');
        });

        it.should("accept a block that returns a filter", function () {
            assert.equal(dataset.order("a").orderMore(function (o) {
                return o.b;
            }).sql, 'SELECT * FROM test ORDER BY a, b');
            assert.equal(dataset.order("a", "b").orderMore("c", "d",function () {
                return [this.e, this.f(1, 2)];
            }).sql, 'SELECT * FROM test ORDER BY a, b, c, d, e, f(1, 2)');
            assert.equal(dataset.order("a").orderAppend(function (o) {
                return o.b;
            }).sql, 'SELECT * FROM test ORDER BY a, b');
            assert.equal(dataset.order("a", "b").orderAppend("c", "d",function () {
                return [this.e, this.f(1, 2)];
            }).sql, 'SELECT * FROM test ORDER BY a, b, c, d, e, f(1, 2)');

            assert.equal(dataset.order("a").orderPrepend(function (o) {
                return o.b;
            }).sql, 'SELECT * FROM test ORDER BY b, a');
            assert.equal(dataset.order("a", "b").orderPrepend("c", "d",function () {
                return [this.e, this.f(1, 2)];
            }).sql, 'SELECT * FROM test ORDER BY c, d, e, f(1, 2), a, b');
        });
    });

    it.describe("#reverseOrder", function (it) {
        var dataset = new Dataset().from("test");

        it.should("use DESC as default order", function () {
            assert.equal(dataset.reverseOrder("name").sql, 'SELECT * FROM test ORDER BY name DESC');
        });

        it.should("invert the order given", function () {
            assert.equal(dataset.reverseOrder(sql.name.desc()).sql, 'SELECT * FROM test ORDER BY name ASC');
        });

        it.should("invert the order for ASC expressions", function () {
            assert.equal(dataset.reverseOrder(sql.name.asc()).sql, 'SELECT * FROM test ORDER BY name DESC');
        });

        it.should("accept multiple arguments", function () {
            assert.equal(dataset.reverseOrder("name", sql.price.desc()).sql, 'SELECT * FROM test ORDER BY name DESC, price ASC');
        });

        it.should("handles NULLS ordering correctly when reversing", function () {
            assert.equal(dataset.reverseOrder(sql.name.asc({nulls: "first"}), sql.price.desc({nulls: "last"})).sql, 'SELECT * FROM test ORDER BY name DESC NULLS LAST, price ASC NULLS FIRST');
        });

        it.should("reverse a previous ordering if no arguments are given", function () {
            assert.equal(dataset.order("name").reverseOrder().sql, 'SELECT * FROM test ORDER BY name DESC');
            assert.equal(dataset.order(sql.clumsy.desc(), "fool").reverseOrder().sql, 'SELECT * FROM test ORDER BY clumsy ASC, fool DESC');
        });

        it.should("return an unordered dataset for a dataset with no order", function () {
            assert.equal(dataset.unordered().reverseOrder().sql, 'SELECT * FROM test');
        });

        it.should("have reverse alias", function () {
            assert.equal(dataset.order("name").reverse().sql, 'SELECT * FROM test ORDER BY name DESC');
        });
    });

    it.describe("#limit", function (it) {
        var dataset = new Dataset().from("test");

        it.should("include a LIMIT clause in the SELECT statement", function () {
            assert.equal(dataset.limit(10).sql, 'SELECT * FROM test LIMIT 10');
        });

        it.should("accept ranges", function () {
            assert.equal(dataset.limit([3, 7]).sql, 'SELECT * FROM test LIMIT 5 OFFSET 3');
        });

        it.should("include an offset if a second argument is given", function () {
            assert.equal(dataset.limit(6, 10).sql, 'SELECT * FROM test LIMIT 6 OFFSET 10');
        });

        it.should("convert regular strings to integers", function () {
            assert.equal(dataset.limit('6', 'a() - 1').sql, 'SELECT * FROM test LIMIT 6 OFFSET 0');
        });

        it.should("not convert literal strings to integers", function () {
            assert.equal(dataset.limit('6', sql['a() - 1']).sql, 'SELECT * FROM test LIMIT 6 OFFSET a() - 1');
        });

        it.should("not convert other objects", function () {
            assert.equal(dataset.limit(6, new sql.SQLFunction("a").minus(1)).sql, 'SELECT * FROM test LIMIT 6 OFFSET (a() - 1)');
        });

        it.should("work with fixed sql datasets", function () {
            dataset.__opts.sql = 'SELECT * from cccc';
            assert.equal(dataset.limit(6, 10).sql, 'SELECT * FROM (SELECT * from cccc) AS t1 LIMIT 6 OFFSET 10');
        });

        it.should("raise an error if an invalid limit or offset is used", function () {
            assert.throws(function () {
                dataset.limit(-1);
            });
            assert.throws(function () {
                dataset.limit(0);
            });
            assert.doesNotThrow(function () {
                dataset.limit(1);
            });
            assert.throws(function () {
                dataset.limit(1, -1);
            });
            assert.doesNotThrow(function () {
                dataset.limit(1, 0);
            });
            assert.doesNotThrow(function () {
                dataset.limit(1, 1);
            });
        });
    });

    it.describe("#qualifiedColumnName", function (it) {
        var dataset = new Dataset().from("test");

        it.should("return the literal value if not given a string", function () {
            assert.equal(dataset.literal(dataset.qualifiedColumnName(new sql.LiteralString("'ccc__b'"), "items")), "'ccc__b'");
            assert.equal(dataset.literal(dataset.qualifiedColumnName(3), "items"), '3');
            assert.equal(dataset.literal(dataset.qualifiedColumnName(new sql.LiteralString("a")), "items"), 'a');
        });

        it.should("qualify the column with the supplied table name if given an unqualified string", function () {
            assert.equal(dataset.literal(dataset.qualifiedColumnName("b1", "items")), 'items.b1');
        });

        it.should("not changed the qualifed column's table if given a qualified string", function () {
            assert.equal(dataset.literal(dataset.qualifiedColumnName("ccc__b", "items")), 'ccc.b');
        });
    });


    it.describe("#firstSourceAlias", function (it) {
        var ds = new Dataset();

        it.should("be the entire first source if not aliased", function () {
            assert.deepEqual(ds.from("t").firstSourceAlias, new Identifier("t"));
            assert.deepEqual(ds.from(new sql.Identifier("t__a")).firstSourceAlias, new sql.Identifier("t__a"));
            assert.deepEqual(ds.from("s__t").firstSourceAlias, new QualifiedIdentifier("s", "t"));
            assert.deepEqual(ds.from(sql.t.qualify("s")).firstSourceAlias.table, sql.t.qualify("s").table);
            assert.deepEqual(ds.from(sql.t.qualify("s")).firstSourceAlias.column.value, sql.t.qualify("s").column.value);
        });


        it.should("be the alias if aliased", function () {
            assert.equal(ds.from("t___a").firstSourceAlias, "a");
            assert.equal(ds.from("s__t___a").firstSourceAlias, "a");
            assert.equal(ds.from(sql.t.as("a")).firstSourceAlias, "a");
        });


        it.should("be aliased as firstSource", function () {
            assert.deepEqual(ds.from("t").firstSourceAlias, new Identifier("t"));
            assert.deepEqual(ds.from(new sql.Identifier("t__a")).firstSourceAlias, new sql.Identifier("t__a"));
            assert.equal(ds.from("s__t___a").firstSourceAlias, "a");
            assert.equal(ds.from(sql.t.as("a")).firstSourceAlias, "a");
        });

        it.should("raise exception if table doesn't have a source", function () {
            assert.throws(function () {
                return ds.firstSourceAlias;
            });
        });

    });

    it.describe("#firstSourceTable", function (it) {
        var ds = new Dataset();

        it.should("be the entire first source if not aliased", function () {
            assert.deepEqual(ds.from("t").firstSourceTable, new Identifier("t"));
            assert.deepEqual(ds.from(new sql.Identifier("t__a")).firstSourceTable, new sql.Identifier("t__a"));
            assert.deepEqual(ds.from("s__t").firstSourceTable, new QualifiedIdentifier("s", "t"));
            assert.deepEqual(ds.from(sql.t.qualify("s")).firstSourceTable.table, sql.t.qualify("s").table);
            assert.deepEqual(ds.from(sql.t.qualify("s")).firstSourceTable.column.value, sql.t.qualify("s").column.value);
        });

        it.should("be the unaliased part if aliased", function () {
            assert.deepEqual(ds.from("t___a").firstSourceTable, new Identifier("t"));
            assert.deepEqual(ds.from("s__t___a").firstSourceTable, new QualifiedIdentifier("s", "t"));
            assert.deepEqual(ds.from(sql.t.as("a")).firstSourceTable.value, new Identifier("t").value);
        });

        it.should("raise exception if table doesn't have a source", function () {
            assert.throws(function () {
                return ds.firstSourceTable;
            });
        });
    });

    it.describe("#fromSelf", function (it) {
        var ds = new Dataset().from("test").select("name").limit(1);

        it.should("set up a default alias", function () {
            assert.equal(ds.fromSelf().sql, 'SELECT * FROM (SELECT name FROM test LIMIT 1) AS t1');
        });

        it.should("modify only the new dataset", function () {
            assert.equal(ds.fromSelf().select("bogus").sql, 'SELECT bogus FROM (SELECT name FROM test LIMIT 1) AS t1');
        });

        it.should("use the user-specified alias", function () {
            assert.equal(ds.fromSelf({alias: "someName"}).sql, 'SELECT * FROM (SELECT name FROM test LIMIT 1) AS someName');
        });

        it.should("use the user-specified alias for joins", function () {
            assert.equal(ds.fromSelf({alias: "someName"}).innerJoin("posts", {alias: sql.identifier("name")}).sql, 'SELECT * FROM (SELECT name FROM test LIMIT 1) AS someName INNER JOIN posts ON (posts.alias = someName.name)');
        });
    });

    it.describe("#joinTable", function (it) {
        var d = new MockDataset().from("items");
        d.quoteIdentifiers = true;

        it.should("format the JOIN clause properly", function () {
            assert.equal(d.joinTable("leftOuter", "categories", {categoryId: sql.identifier("id")}).sql, 'SELECT * FROM "items" LEFT OUTER JOIN "categories" ON ("categories"."categoryId" = "items"."id")');
        });


        it.should("handle multiple conditions on the same join table column", function () {
            assert.equal(d.joinTable("leftOuter", "categories", [
                ["categoryId", sql.identifier("id")],
                ["categoryId", [1, 2, 3]]
            ]).sql, 'SELECT * FROM "items" LEFT OUTER JOIN "categories" ON (("categories"."categoryId" = "items"."id") AND ("categories"."categoryId" IN (1, 2, 3)))');
        });


        it.should("include WHERE clause if applicable", function () {
            assert.equal(d.filter(sql.price.sqlNumber.lt(100)).joinTable("rightOuter", "categories", {categoryId: sql.identifier("id")}).sql, 'SELECT * FROM "items" RIGHT OUTER JOIN "categories" ON ("categories"."categoryId" = "items"."id") WHERE ("price" < 100)');
        });


        it.should("include ORDER BY clause if applicable", function () {
            assert.equal(d.order("stamp").joinTable("fullOuter", "categories", {categoryId: sql.identifier("id")}).sql, 'SELECT * FROM "items" FULL OUTER JOIN "categories" ON ("categories"."categoryId" = "items"."id") ORDER BY "stamp"');
        });


        it.should("support multiple joins", function () {
            assert.equal(d.joinTable("inner", "b", {itemsId: sql.identifier("id")}).joinTable("leftOuter", "c", {b_id: sql.identifier("b__id")}).sql, 'SELECT * FROM "items" INNER JOIN "b" ON ("b"."itemsId" = "items"."id") LEFT OUTER JOIN "c" ON ("c"."b_id" = "b"."id")');
        });


        it.should("support arbitrary join types", function () {
            assert.equal(d.joinTable("magic", "categories", {categoryId: sql.identifier("id")}).sql, 'SELECT * FROM "items" MAGIC JOIN "categories" ON ("categories"."categoryId" = "items"."id")');
        });


        it.should("support many join methods", function () {
            assert.equal(d.leftOuterJoin("categories", {categoryId: sql.identifier("id")}).sql, 'SELECT * FROM "items" LEFT OUTER JOIN "categories" ON ("categories"."categoryId" = "items"."id")');
            assert.equal(d.rightOuterJoin("categories", {categoryId: sql.identifier("id")}).sql, 'SELECT * FROM "items" RIGHT OUTER JOIN "categories" ON ("categories"."categoryId" = "items"."id")');
            assert.equal(d.fullOuterJoin("categories", {categoryId: sql.identifier("id")}).sql, 'SELECT * FROM "items" FULL OUTER JOIN "categories" ON ("categories"."categoryId" = "items"."id")');
            assert.equal(d.innerJoin("categories", {categoryId: sql.identifier("id")}).sql, 'SELECT * FROM "items" INNER JOIN "categories" ON ("categories"."categoryId" = "items"."id")');
            assert.equal(d.leftJoin("categories", {categoryId: sql.identifier("id")}).sql, 'SELECT * FROM "items" LEFT JOIN "categories" ON ("categories"."categoryId" = "items"."id")');
            assert.equal(d.rightJoin("categories", {categoryId: sql.identifier("id")}).sql, 'SELECT * FROM "items" RIGHT JOIN "categories" ON ("categories"."categoryId" = "items"."id")');
            assert.equal(d.fullJoin("categories", {categoryId: sql.identifier("id")}).sql, 'SELECT * FROM "items" FULL JOIN "categories" ON ("categories"."categoryId" = "items"."id")');
            assert.equal(d.naturalJoin("categories").sql, 'SELECT * FROM "items" NATURAL JOIN "categories"');
            assert.equal(d.naturalLeftJoin("categories").sql, 'SELECT * FROM "items" NATURAL LEFT JOIN "categories"');
            assert.equal(d.naturalRightJoin("categories").sql, 'SELECT * FROM "items" NATURAL RIGHT JOIN "categories"');
            assert.equal(d.naturalFullJoin("categories").sql, 'SELECT * FROM "items" NATURAL FULL JOIN "categories"');
            assert.equal(d.crossJoin("categories").sql, 'SELECT * FROM "items" CROSS JOIN "categories"');
        });


        it.should("raise an error if additional arguments are provided to join methods that don't take conditions", function () {
            assert.throws(d, "naturalJoin", "categories", {id: sql.identifier("id")});
            assert.throws(d, "naturalLeftJoin", "categories", {id: sql.identifier("id")});
            assert.throws(d, "naturalRightJoin", "categories", {id: sql.identifier("id")});
            assert.throws(d, "naturalFullJoin", "categories", {id: sql.identifier("id")});
            assert.throws(d, "crossJoin", "categories", {id: sql.identifier("id")});
        });


        it.should("raise an error if blocks are provided to join methods that don't pass them", function () {
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
        });


        it.should("default to a plain join if nil is used for the type", function () {
            assert.equal(d.joinTable(null, "categories", {categoryId: sql.identifier("id")}).sql, 'SELECT * FROM "items"  JOIN "categories" ON ("categories"."categoryId" = "items"."id")');
        });


        it.should("use an inner join for Dataset.join", function () {
            assert.equal(d.join("categories", {categoryId: sql.identifier("id")}).sql, 'SELECT * FROM "items" INNER JOIN "categories" ON ("categories"."categoryId" = "items"."id")');
        });


        it.should("support aliased tables using a string", function () {
            assert.equal(d.from('stats').join('players', {id: sql.identifier("playerId")}, 'p').sql, 'SELECT * FROM "stats" INNER JOIN "players" AS "p" ON ("p"."id" = "stats"."playerId")');
        });


        it.should("support aliased tables using the :table_alias option", function () {
            assert.equal(d.from('stats').join('players', {id: sql.identifier("playerId")}, {tableAlias: "p"}).sql, 'SELECT * FROM "stats" INNER JOIN "players" AS "p" ON ("p"."id" = "stats"."playerId")');
        });


        it.should("support using an alias for the FROM when doing the first join with unqualified condition columns", function () {
            var ds = new MockDataset().from({foo: "f"});
            ds.quoteIdentifiers = true;
            assert.equal(ds.joinTable("inner", "bar", {id: sql.identifier("barId")}).sql, 'SELECT * FROM "foo" AS "f" INNER JOIN "bar" ON ("bar"."id" = "f"."barId")');
        });


        it.should("support implicit schemas in from table strings", function () {
            assert.equal(d.from("s__t").join("u__v", {id: sql.identifier("playerId")}).sql, 'SELECT * FROM "s"."t" INNER JOIN "u"."v" ON ("u"."v"."id" = "s"."t"."playerId")');
        });


        it.should("support implicit aliases in from table strings", function () {
            assert.equal(d.from("t___z").join("v___y", {id: sql.identifier("playerId")}).sql, 'SELECT * FROM "t" AS "z" INNER JOIN "v" AS "y" ON ("y"."id" = "z"."playerId")');
            assert.equal(d.from("s__t___z").join("u__v___y", {id: sql.identifier("playerId")}).sql, 'SELECT * FROM "s"."t" AS "z" INNER JOIN "u"."v" AS "y" ON ("y"."id" = "z"."playerId")');
        });


        it.should("support AliasedExpressions", function () {
            assert.equal(d.from(sql.s.as("t")).join(sql.u.as("v"), {id: sql.identifier("playerId")}).sql, 'SELECT * FROM "s" AS "t" INNER JOIN "u" AS "v" ON ("v"."id" = "t"."playerId")');
        });


        it.should("support the 'implicitQualifierOption", function () {
            assert.equal(d.from('stats').join('players', {id: sql.identifier("playerId")}, {implicitQualifier: "p"}).sql, 'SELECT * FROM "stats" INNER JOIN "players" ON ("players"."id" = "p"."playerId")');
        });


        it.should("allow for arbitrary conditions in the JOIN clause", function () {
            assert.equal(d.joinTable("leftOuter", "categories", {status: 0}).sql, 'SELECT * FROM "items" LEFT OUTER JOIN "categories" ON ("categories"."status" = 0)');
            assert.equal(d.joinTable("leftOuter", "categories", {categorizableType: new LiteralString("'Post'")}).sql, 'SELECT * FROM "items" LEFT OUTER JOIN "categories" ON ("categories"."categorizableType" = \'Post\')');
            assert.equal(d.joinTable("leftOuter", "categories", {timestamp: new LiteralString("CURRENT_TIMESTAMP")}).sql, 'SELECT * FROM "items" LEFT OUTER JOIN "categories" ON ("categories"."timestamp" = CURRENT_TIMESTAMP)');
            assert.equal(d.joinTable("leftOuter", "categories", {status: [1, 2, 3]}).sql, 'SELECT * FROM "items" LEFT OUTER JOIN "categories" ON ("categories"."status" IN (1, 2, 3))');
        });


        it.should("raise error for a table without a source", function () {
            assert.throws(hitch(new Dataset(), "join", "players", {id: sql.identifier("playerId")}));
        });


        it.should("support joining datasets", function () {
            var ds = new Dataset().from("categories");
            assert.equal(d.joinTable("leftOuter", ds, {itemId: sql.identifier("id")}).sql, 'SELECT * FROM "items" LEFT OUTER JOIN (SELECT * FROM categories) AS "t1" ON ("t1"."itemId" = "items"."id")');
            ds = ds.filter({active: true});
            assert.equal(d.joinTable("leftOuter", ds, {itemId: sql.identifier("id")}).sql, 'SELECT * FROM "items" LEFT OUTER JOIN (SELECT * FROM categories WHERE (active IS TRUE)) AS "t1" ON ("t1"."itemId" = "items"."id")');
            assert.equal(d.fromSelf().joinTable("leftOuter", ds, {itemId: sql.identifier("id")}).sql, 'SELECT * FROM (SELECT * FROM "items") AS "t1" LEFT OUTER JOIN (SELECT * FROM categories WHERE (active IS TRUE)) AS "t2" ON ("t2"."itemId" = "t1"."id")');
        });


        it.should("support joining datasets and aliasing the join", function () {
            var ds = new Dataset().from("categories");
            assert.equal(d.joinTable("leftOuter", ds, {"ds__itemId": sql.identifier("id")}, "ds").sql, 'SELECT * FROM "items" LEFT OUTER JOIN (SELECT * FROM categories) AS "ds" ON ("ds"."itemId" = "items"."id")');
        });


        it.should("support joining multiple datasets", function () {
            var ds = new Dataset().from("categories");
            var ds2 = new Dataset().from("nodes").select("name");
            var ds3 = new Dataset().from("attributes").filter(sql.literal("name = 'blah'"));

            assert.equal(d.joinTable("leftOuter", ds, {itemId: sql.identifier("id")}).joinTable("inner", ds2, {nodeId: sql.identifier("id")}).joinTable("rightOuter", ds3, {attributeId: sql.identifier("id")}).sql,
                'SELECT * FROM "items" LEFT OUTER JOIN (SELECT * FROM categories) AS "t1" ON ("t1"."itemId" = "items"."id") ' +
                    'INNER JOIN (SELECT name FROM nodes) AS "t2" ON ("t2"."nodeId" = "t1"."id") ' +
                    'RIGHT OUTER JOIN (SELECT * FROM attributes WHERE (name = \'blah\')) AS "t3" ON ("t3"."attributeId" = "t2"."id")'
            );
        });


        it.should("support joining objects that have a tableName property", function () {
            var ds = {tableName: "categories"};
            assert.equal(d.join(ds, {itemId: sql.identifier("id")}).sql, 'SELECT * FROM "items" INNER JOIN "categories" ON ("categories"."itemId" = "items"."id")');
        });


        it.should("support using a SQL String as the join condition", function () {
            assert.equal(d.join("categories", sql.literal("c.item_id = items.id"), "c").sql, 'SELECT * FROM "items" INNER JOIN "categories" AS "c" ON (c.item_id = items.id)');
        });


        it.should("support using a boolean column as the join condition", function () {
            assert.equal(d.join("categories", sql.active).sql, 'SELECT * FROM "items" INNER JOIN "categories" ON "active"');
        });


        it.should("support using an expression as the join condition", function () {
            assert.equal(d.join("categories", sql.number.sqlNumber.gt(10)).sql, 'SELECT * FROM "items" INNER JOIN "categories" ON ("number" > 10)');
        });


        it.should("support natural and cross joins using null", function () {
            assert.equal(d.joinTable("natural", "categories").sql, 'SELECT * FROM "items" NATURAL JOIN "categories"');
            assert.equal(d.joinTable("cross", "categories", null).sql, 'SELECT * FROM "items" CROSS JOIN "categories"');
            assert.equal(d.joinTable("natural", "categories", null, "c").sql, 'SELECT * FROM "items" NATURAL JOIN "categories" AS "c"');
        });


        it.should("support joins with a USING clause if an array of strings is used", function () {
            assert.equal(d.join("categories", ["id"]).sql, 'SELECT * FROM "items" INNER JOIN "categories" USING ("id")');
            assert.equal(d.join("categories", ["id1", "id2"]).sql, 'SELECT * FROM "items" INNER JOIN "categories" USING ("id1", "id2")');
        });


        it.should("emulate JOIN USING (poorly) if the dataset doesn't support it", function () {
            d.supportsJoinUsing = false;
            assert.equal(d.join("categories", [sql.identifier("id")]).sql, 'SELECT * FROM "items" INNER JOIN "categories" ON ("categories"."id" = "items"."id")');
            d.supportsJoinUsing = true;
        });


        it.should("raise an error if using an array of identifiers with a block", function () {
            assert.throws(function () {
                d.join("categories", [sql.identifier("id")], function (j, lj, js) {
                    return false;
                });
            });
        });

        it.should("support using a block that receieves the join table/alias, last join table/alias, and array of previous joins", function () {
            d.join("categories", function (joinAlias, lastJoinAlias, joins) {
                assert.equal(joinAlias, "categories");
                assert.equal(lastJoinAlias, "items");
                assert.deepEqual(joins, []);
            });


            d.from({items: "i"}).join("categories", null, "c", function (joinAlias, lastJoinAlias, joins) {
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
                assert.instanceOf(joins[0], sql.JoinClause);
                assert.equal(joins[0].joinType, "inner");
            });

            d.joinTable("natural", "blah", null, "b").join("categories", null, "c", function (joinAlias, lastJoinAlias, joins) {
                assert.equal(joinAlias, "c");
                assert.equal(lastJoinAlias, "b");
                assert.instanceOf(joins, Array);
                assert.lengthOf(joins, 1);
                assert.instanceOf(joins[0], sql.JoinClause);
                assert.equal(joins[0].joinType, "natural");
            });

            d.join("blah").join("categories").join("blah2", function (joinAlias, lastJoinAlias, joins) {
                assert.equal(joinAlias, "blah2");
                assert.equal(lastJoinAlias, "categories");
                assert.instanceOf(joins, Array);
                assert.lengthOf(joins, 2);
                assert.instanceOf(joins[0], sql.JoinClause);
                assert.equal(joins[0].table, "blah");
                assert.instanceOf(joins[1], sql.JoinClause);
                assert.equal(joins[1].table, "categories");
            });

        });


        it.should("use the block result as the only condition if no condition is given", function () {
            assert.equal(d.join("categories",
                function (j, lj, js) {
                    return this.b.qualify(j).eq(this.c.qualify(lj));
                }).sql, 'SELECT * FROM "items" INNER JOIN "categories" ON ("categories"."b" = "items"."c")');
            assert.equal(d.join("categories",
                function (j, lj, js) {
                    return this.b.qualify(j).gt(this.c.qualify(lj));
                }).sql, 'SELECT * FROM "items" INNER JOIN "categories" ON ("categories"."b" > "items"."c")');
        });


        it.should("combine the block conditions and argument conditions if both given", function () {
            assert.equal(d.join("categories", {a: sql.identifier("d")},
                function (j, lj, js) {
                    return this.b.qualify(j).eq(this.c.qualify(lj));
                }).sql, 'SELECT * FROM "items" INNER JOIN "categories" ON (("categories"."a" = "items"."d") AND ("categories"."b" = "items"."c"))');
            assert.equal(d.join("categories", {a: sql.identifier("d")},
                function (j, lj, js) {
                    return this.b.qualify(j).gt(this.c.qualify(lj));
                }).sql,
                'SELECT * FROM "items" INNER JOIN "categories" ON (("categories"."a" = "items"."d") AND ("categories"."b" > "items"."c"))');
        });


        it.should("prefer explicit aliases over implicit", function () {
            assert.equal(d.from("items___i").join("categories___c", {categoryId: sql.identifier("id")}, {tableAlias: "c2", implicitQualifier: "i2"}).sql, 'SELECT * FROM "items" AS "i" INNER JOIN "categories" AS "c2" ON ("c2"."categoryId" = "i2"."id")');
            assert.equal(d.from(sql.items.as("i")).join(sql.categories.as("c"), {categoryId: sql.identifier("id")}, {tableAlias: "c2", implicitQualifier: "i2"}).sql, 'SELECT * FROM "items" AS "i" INNER JOIN "categories" AS "c2" ON ("c2"."categoryId" = "i2"."id")');
        });


        it.should("not allow insert, update, delete, or truncate", function () {
            var ds = d.join("categories", {a: "d"});
            assert.throws(hitch(ds, "insertSql"));
            assert.throws(hitch(ds, "updateSql", {a: 1}));
            assert.throws(function () {
                return ds.deleteSql;
            });
            assert.throws(function () {
                return ds.truncateSql;
            });
        });


        it.should("raise an error if an invalid option is passed", function () {
            assert.throws(hitch(d, "join", "c", ["id"], null));
        });

    });

    it.describe("#distinct", function (it) {
        var dataset = new MockDatabase().from("test").select("name");

        it.should("include DISTINCT clause in statement", function () {
            assert.equal(dataset.distinct().sql, 'SELECT DISTINCT name FROM test');
        });

        it.should("raise an error if columns given and DISTINCT ON is not supported", function () {
            assert.doesNotThrow(hitch(dataset, "distinct"));
            assert.throws(hitch(dataset, "distinct", "a"));
        });

        it.should("use DISTINCT ON if columns are given and DISTINCT ON is supported", function () {
            dataset.supportsDistinctOn = true;
            assert.equal(dataset.distinct("a", "b").sql, 'SELECT DISTINCT ON (a, b) name FROM test');
            assert.equal(dataset.distinct(sql.stamp.cast("integer"), {nodeId: null}).sql, 'SELECT DISTINCT ON (CAST(stamp AS integer), (nodeId IS NULL)) name FROM test');
        });

        it.should("do a subselect for count", function () {
            var db = new MockDatabase();
            var ds = db.from("test").select("name");
            ds.distinct().count();
            assert.deepEqual(db.sqls, ['SELECT COUNT(*) AS count FROM (SELECT DISTINCT name FROM test) AS t1 LIMIT 1']);
        });
    });

    it.describe("groupAndCount", function (it) {
        var ds = new Dataset().from("test");


        it.should("format SQL properly", function () {
            assert.equal(ds.groupAndCount("name").sql, "SELECT name, count(*) AS count FROM test GROUP BY name");
        });

        it.should("accept multiple columns for grouping", function () {
            assert.equal(ds.groupAndCount("a", "b").sql, "SELECT a, b, count(*) AS count FROM test GROUP BY a, b");
        });


        it.should("format column aliases in the SELECT clause but not in the group clause", function () {
            assert.equal(ds.groupAndCount("name___n").sql, "SELECT name AS n, count(*) AS count FROM test GROUP BY name");
            assert.equal(ds.groupAndCount("name__n").sql, "SELECT name.n, count(*) AS count FROM test GROUP BY name.n");
        });

        it.should("handle identifiers", function () {
            assert.equal(ds.groupAndCount(new Identifier("name___n")).sql, "SELECT name___n, count(*) AS count FROM test GROUP BY name___n");
        });

        it.should("handle literal strings", function () {
            assert.equal(ds.groupAndCount(new LiteralString("name")).sql, "SELECT name, count(*) AS count FROM test GROUP BY name");
        });

        it.should("handle aliased expressions", function () {
            assert.equal(ds.groupAndCount(sql.name.as("n")).sql, "SELECT name AS n, count(*) AS count FROM test GROUP BY name");
            assert.equal(ds.groupAndCount("name___n").sql, "SELECT name AS n, count(*) AS count FROM test GROUP BY name");
        });

    });

    it.describe("#set", function (it) {
        var ds = new (comb.define(patio.Dataset, {
            instance: {

                update: function () {
                    this.lastSql = this.updateSql.apply(this, arguments);
                }
            }
        }))().from("items");


        it.should("act as alias to update", function () {
            ds.set({x: 3});
            assert.equal(ds.lastSql, 'UPDATE items SET x = 3');
        });
    });

    it.describe("compound operations", function (it) {
        var a = new Dataset().from("a").filter({z: 1}),
            b = new Dataset().from("b").filter({z: 2});

        it.should("support UNION and UNION ALL", function () {
            assert.equal(a.union(b).sql, "SELECT * FROM (SELECT * FROM a WHERE (z = 1) UNION SELECT * FROM b WHERE (z = 2)) AS t1");
            assert.equal(b.union(a, true).sql, "SELECT * FROM (SELECT * FROM b WHERE (z = 2) UNION ALL SELECT * FROM a WHERE (z = 1)) AS t1");
            assert.equal(b.union(a, {all: true}).sql, "SELECT * FROM (SELECT * FROM b WHERE (z = 2) UNION ALL SELECT * FROM a WHERE (z = 1)) AS t1");
        });

        it.should("support INTERSECT and INTERSECT ALL", function () {
            assert.equal(a.intersect(b).sql, "SELECT * FROM (SELECT * FROM a WHERE (z = 1) INTERSECT SELECT * FROM b WHERE (z = 2)) AS t1");
            assert.equal(b.intersect(a, true).sql, "SELECT * FROM (SELECT * FROM b WHERE (z = 2) INTERSECT ALL SELECT * FROM a WHERE (z = 1)) AS t1");
            assert.equal(b.intersect(a, {all: true}).sql, "SELECT * FROM (SELECT * FROM b WHERE (z = 2) INTERSECT ALL SELECT * FROM a WHERE (z = 1)) AS t1");
        });

        it.should("support EXCEPT and EXCEPT ALL", function () {
            assert.equal(a.except(b).sql, "SELECT * FROM (SELECT * FROM a WHERE (z = 1) EXCEPT SELECT * FROM b WHERE (z = 2)) AS t1");
            assert.equal(b.except(a, true).sql, "SELECT * FROM (SELECT * FROM b WHERE (z = 2) EXCEPT ALL SELECT * FROM a WHERE (z = 1)) AS t1");
            assert.equal(b.except(a, {all: true}).sql, "SELECT * FROM (SELECT * FROM b WHERE (z = 2) EXCEPT ALL SELECT * FROM a WHERE (z = 1)) AS t1");
        });

        it.should("support alias option for specifying identifier", function () {
            assert.equal(a.union(b, {alias: "xx"}).sql, "SELECT * FROM (SELECT * FROM a WHERE (z = 1) UNION SELECT * FROM b WHERE (z = 2)) AS xx");
            assert.equal(a.intersect(b, {alias: "xx"}).sql, "SELECT * FROM (SELECT * FROM a WHERE (z = 1) INTERSECT SELECT * FROM b WHERE (z = 2)) AS xx");
            assert.equal(a.except(b, {alias: "xx"}).sql, "SELECT * FROM (SELECT * FROM a WHERE (z = 1) EXCEPT SELECT * FROM b WHERE (z = 2)) AS xx");
        });

        it.should("support {fromSelf : false} option to not wrap the compound in a SELECT * FROM (...)", function () {
            assert.equal(b.union(a, {fromSelf: false}).sql, "SELECT * FROM b WHERE (z = 2) UNION SELECT * FROM a WHERE (z = 1)");
            assert.equal(b.intersect(a, {fromSelf: false}).sql, "SELECT * FROM b WHERE (z = 2) INTERSECT SELECT * FROM a WHERE (z = 1)");
            assert.equal(b.except(a, {fromSelf: false}).sql, "SELECT * FROM b WHERE (z = 2) EXCEPT SELECT * FROM a WHERE (z = 1)");

            assert.equal(b.union(a, {fromSelf: false, all: true}).sql, "SELECT * FROM b WHERE (z = 2) UNION ALL SELECT * FROM a WHERE (z = 1)");
            assert.equal(b.intersect(a, {fromSelf: false, all: true}).sql, "SELECT * FROM b WHERE (z = 2) INTERSECT ALL SELECT * FROM a WHERE (z = 1)");
            assert.equal(b.except(a, {fromSelf: false, all: true}).sql, "SELECT * FROM b WHERE (z = 2) EXCEPT ALL SELECT * FROM a WHERE (z = 1)");
        });

        it.should("raise an InvalidOperation if INTERSECT or EXCEPT is used and they are not supported", function () {
            a.supportsIntersectExcept = false;
            assert.throws(hitch(a, "intersect", b));
            assert.throws(hitch(a, "intersect", b, true));
            assert.throws(hitch(a, "except", b));
            assert.throws(hitch(a, "except", b, true));
            a.supportsIntersectExcept = true;
        });

        it.should("raise an InvalidOperation if INTERSECT ALL or EXCEPT ALL is used and they are not supported", function () {
            a.supportsIntersectExceptAll = false;
            assert.doesNotThrow(hitch(a, "intersect", b));
            assert.throws(hitch(a, "intersect", b, true));
            assert.doesNotThrow(hitch(a, "except", b));
            assert.throws(hitch(a, "except", b, true));
            a.supportsIntersectExceptAll = true;
        });

        it.should("handle chained compound operations", function () {
            assert.equal(a.union(b).union(a, true).sql, "SELECT * FROM (SELECT * FROM (SELECT * FROM a WHERE (z = 1) UNION SELECT * FROM b WHERE (z = 2)) AS t1 UNION ALL SELECT * FROM a WHERE (z = 1)) AS t1");
            assert.equal(a.intersect(b, true).intersect(a).sql, "SELECT * FROM (SELECT * FROM (SELECT * FROM a WHERE (z = 1) INTERSECT ALL SELECT * FROM b WHERE (z = 2)) AS t1 INTERSECT SELECT * FROM a WHERE (z = 1)) AS t1");
            assert.equal(a.except(b).except(a, true).sql, "SELECT * FROM (SELECT * FROM (SELECT * FROM a WHERE (z = 1) EXCEPT SELECT * FROM b WHERE (z = 2)) AS t1 EXCEPT ALL SELECT * FROM a WHERE (z = 1)) AS t1");
        });

        it.should("use a subselect when using a compound operation with a dataset that already has a compound operation", function () {
            assert.equal(a.union(b.union(a, true)).sql, "SELECT * FROM (SELECT * FROM a WHERE (z = 1) UNION SELECT * FROM (SELECT * FROM b WHERE (z = 2) UNION ALL SELECT * FROM a WHERE (z = 1)) AS t1) AS t1");
            assert.equal(a.intersect(b.intersect(a), true).sql, "SELECT * FROM (SELECT * FROM a WHERE (z = 1) INTERSECT ALL SELECT * FROM (SELECT * FROM b WHERE (z = 2) INTERSECT SELECT * FROM a WHERE (z = 1)) AS t1) AS t1");
            assert.equal(a.except(b.except(a, true)).sql, "SELECT * FROM (SELECT * FROM a WHERE (z = 1) EXCEPT SELECT * FROM (SELECT * FROM b WHERE (z = 2) EXCEPT ALL SELECT * FROM a WHERE (z = 1)) AS t1) AS t1");
        });

        it.should("order and limit properly when using UNION, INTERSECT, or EXCEPT", function () {
            var dataset = new Dataset().from("test");
            assert.equal(dataset.union(dataset).limit(2).sql, "SELECT * FROM (SELECT * FROM test UNION SELECT * FROM test) AS t1 LIMIT 2");
            assert.equal(dataset.limit(2).intersect(dataset).sql, "SELECT * FROM (SELECT * FROM (SELECT * FROM test LIMIT 2) AS t1 INTERSECT SELECT * FROM test) AS t1");
            assert.equal(dataset.except(dataset.limit(2)).sql, "SELECT * FROM (SELECT * FROM test EXCEPT SELECT * FROM (SELECT * FROM test LIMIT 2) AS t1) AS t1");

            assert.equal(dataset.union(dataset).order("num").sql, "SELECT * FROM (SELECT * FROM test UNION SELECT * FROM test) AS t1 ORDER BY num");
            assert.equal(dataset.order("num").intersect(dataset).sql, "SELECT * FROM (SELECT * FROM (SELECT * FROM test ORDER BY num) AS t1 INTERSECT SELECT * FROM test) AS t1");
            assert.equal(dataset.except(dataset.order("num")).sql, "SELECT * FROM (SELECT * FROM test EXCEPT SELECT * FROM (SELECT * FROM test ORDER BY num) AS t1) AS t1");

            assert.equal(dataset.limit(2).order("a").union(dataset.limit(3).order("b")).order("c").limit(4).sql, "SELECT * FROM (SELECT * FROM (SELECT * FROM test ORDER BY a LIMIT 2) AS t1 UNION SELECT * FROM (SELECT * FROM test ORDER BY b LIMIT 3) AS t1) AS t1 ORDER BY c LIMIT 4");
        });

    });

    it.describe("#updateSql", function (it) {
        var ds = new Dataset().from("items");


        it.should("accept strings", function () {
            assert.equal(ds.updateSql("a = b"), "UPDATE items SET a = b");
        });

        it.should("handle implicitly qualified strings", function () {
            assert.equal(ds.updateSql({items__a: sql.b}), "UPDATE items SET items.a = b");
        });

        it.should("accept hash with string keys", function () {
            assert.equal(ds.updateSql({c: "d"}), "UPDATE items SET c = 'd'");
        });

        it.should("accept array subscript references", function () {
            assert.equal(ds.updateSql(sql.day.sqlSubscript(1).eq("d")), "UPDATE items SET day[1] = 'd'");
        });

        it.should("accept array subscript references as hash and string", function () {
            assert.equal(ds.updateSql(sql.day.sqlSubscript(1).eq("d"), {c: "d"}, "a=b"), "UPDATE items SET day[1] = 'd', c = 'd', a=b");
        });
    });


    it.describe("#grep", function (it) {
        var ds = new Dataset().from("posts");

        it.should("format a SQL filter correctly", function () {
            assert.equal(ds.grep("title", 'javasScript').sql,
                "SELECT * FROM posts WHERE ((title LIKE 'javasScript'))");
        });

        it.should("support multiple columns", function () {
            assert.equal(ds.grep(["title", "body"], 'javasScript').sql,
                "SELECT * FROM posts WHERE ((title LIKE 'javasScript') OR (body LIKE 'javasScript'))");
        });

        it.should("support multiple search terms", function () {
            assert.equal(ds.grep("title", ['abc', 'def']).sql,
                "SELECT * FROM posts WHERE ((title LIKE 'abc') OR (title LIKE 'def'))");
        });

        it.should("support multiple columns and search terms", function () {
            assert.equal(ds.grep(["title", "body"], ['abc', 'def']).sql,
                "SELECT * FROM posts WHERE ((title LIKE 'abc') OR (title LIKE 'def') OR (body LIKE 'abc') OR (body LIKE 'def'))");
        });

        it.should("support the :all_patterns option", function () {
            assert.equal(ds.grep(["title", "body"], ['abc', 'def'], {allPatterns: true}).sql,
                "SELECT * FROM posts WHERE (((title LIKE 'abc') OR (body LIKE 'abc')) AND ((title LIKE 'def') OR (body LIKE 'def')))");
        });

        it.should("support the :allColumns option", function () {
            assert.equal(ds.grep(["title", "body"], ['abc', 'def'], {allColumns: true}).sql,
                "SELECT * FROM posts WHERE (((title LIKE 'abc') OR (title LIKE 'def')) AND ((body LIKE 'abc') OR (body LIKE 'def')))");
        });

        it.should("support the :case_insensitive option", function () {
            assert.equal(ds.grep(["title", "body"], ['abc', 'def'], {caseInsensitive: true}).sql,
                "SELECT * FROM posts WHERE ((title ILIKE 'abc') OR (title ILIKE 'def') OR (body ILIKE 'abc') OR (body ILIKE 'def'))");
        });

        it.should("support the :all_patterns and :allColumns options together", function () {
            assert.equal(ds.grep(["title", "body"], ['abc', 'def'], {allPatterns: true, allColumns: true}).sql,
                "SELECT * FROM posts WHERE ((title LIKE 'abc') AND (body LIKE 'abc') AND (title LIKE 'def') AND (body LIKE 'def'))");
        });

        it.should("support the :all_patterns and :case_insensitive options together", function () {
            assert.equal(ds.grep(["title", "body"], ['abc', 'def'], {allPatterns: true, caseInsensitive: true}).sql,
                "SELECT * FROM posts WHERE (((title ILIKE 'abc') OR (body ILIKE 'abc')) AND ((title ILIKE 'def') OR (body ILIKE 'def')))");
        });

        it.should("support the :allColumns and :case_insensitive options together", function () {
            assert.equal(ds.grep(["title", "body"], ['abc', 'def'], {allColumns: true, caseInsensitive: true}).sql,
                "SELECT * FROM posts WHERE (((title ILIKE 'abc') OR (title ILIKE 'def')) AND ((body ILIKE 'abc') OR (body ILIKE 'def')))");
        });

        it.should("support the :all_patterns, :allColumns, and :caseInsensitive options together", function () {
            assert.equal(ds.grep(["title", "body"], ['abc', 'def'], {allPatterns: true, allColumns: true, caseInsensitive: true}).sql,
                "SELECT * FROM posts WHERE ((title ILIKE 'abc') AND (body ILIKE 'abc') AND (title ILIKE 'def') AND (body ILIKE 'def'))");
        });

        it.should("support regexps though the database may not support it", function () {
            assert.equal(ds.grep("title", /javasScript/).sql,
                "SELECT * FROM posts WHERE ((title ~ 'javasScript'))");

            assert.equal(ds.grep("title", [/^javasScript/, 'javasScript']).sql,
                "SELECT * FROM posts WHERE ((title ~ '^javasScript') OR (title LIKE 'javasScript'))");
        });

        it.should("support searching against other columns", function () {
            assert.equal(ds.grep("title", sql.identifier("body")).sql,
                "SELECT * FROM posts WHERE ((title LIKE body))");
        });
    });


    it.describe("#setDefaults", function (it) {
        var ds = new Dataset().from("items").setDefaults({x: 1});

        it.should("set the default values for inserts", function () {
            assert.equal(ds.insertSql(), "INSERT INTO items (x) VALUES (1)");
            assert.equal(ds.insertSql({x: 2}), "INSERT INTO items (x) VALUES (2)");
            assert.equal(ds.insertSql({y: 2}), "INSERT INTO items (x, y) VALUES (1, 2)");
            assert.equal(ds.setDefaults({y: 2}).insertSql(), "INSERT INTO items (x, y) VALUES (1, 2)");
            assert.equal(ds.setDefaults({x: 2}).insertSql(), "INSERT INTO items (x) VALUES (2)");
        });

        it.should("set the default values for updates", function () {
            assert.equal(ds.updateSql(), "UPDATE items SET x = 1");
            assert.equal(ds.updateSql({x: 2}), "UPDATE items SET x = 2");
            assert.equal(ds.updateSql({y: 2}), "UPDATE items SET x = 1, y = 2");
            assert.equal(ds.setDefaults({y: 2}).updateSql(), "UPDATE items SET x = 1, y = 2");
            assert.equal(ds.setDefaults({x: 2}).updateSql(), "UPDATE items SET x = 2");
        });
    });

    it.describe("#setOverrides", function (it) {
        var ds = new Dataset().from("items").setOverrides({x: 1});

        it.should("override the given values for inserts", function () {
            assert.equal(ds.insertSql(), "INSERT INTO items (x) VALUES (1)");
            assert.equal(ds.insertSql({x: 2}), "INSERT INTO items (x) VALUES (1)");
            assert.equal(ds.insertSql({y: 2}), "INSERT INTO items (y, x) VALUES (2, 1)");
            assert.equal(ds.setOverrides({y: 2}).insertSql(), "INSERT INTO items (x, y) VALUES (1, 2)");
            assert.equal(ds.setOverrides({x: 2}).insertSql(), "INSERT INTO items (x) VALUES (2)");
        });

        it.should("override the given values for updates", function () {
            assert.equal(ds.updateSql(), "UPDATE items SET x = 1");
            assert.equal(ds.updateSql({x: 2}), "UPDATE items SET x = 1");
            assert.equal(ds.updateSql({y: 2}), "UPDATE items SET y = 2, x = 1");
            assert.equal(ds.setOverrides({y: 2}).updateSql(), "UPDATE items SET x = 1, y = 2");
            assert.equal(ds.setOverrides({x: 2}).updateSql(), "UPDATE items SET x = 2");
        });
    });

    it.describe("#qualify", function (it) {
        var ds = new MockDatabase().from("t");
        it.should("qualify to the given table", function () {
            assert.equal(ds.filter(
                function () {
                    return this.a.lt(this.b);
                }).qualify("e").sql, 'SELECT e.* FROM t WHERE (e.a < e.b)');
        });

        it.should("qualify to the first source if no table if given", function () {
            assert.equal(ds.filter(
                function () {
                    return this.a.lt(this.b);
                }).qualify().sql, 'SELECT t.* FROM t WHERE (t.a < t.b)');
        });
    });

    it.describe("#qualifyTo", function (it) {
        var ds = new MockDatabase().from("t");

        it.should("qualify to the given table", function () {
            assert.equal(ds.filter(
                function (e) {
                    return e.a.lt(e.b);
                }).qualifyTo("e").sql, 'SELECT e.* FROM t WHERE (e.a < e.b)');
        });

        it.should("not qualify to the given table if withSql is used", function () {
            assert.equal(ds.withSql("SELECT * FROM test WHERE (a < b)").qualifyTo("e").sql, 'SELECT * FROM test WHERE (a < b)');
        });
    });

    it.describe("#alwaysQualify", function (it) {
        var dataset = new Dataset().from("test");
        it.should("qualify to the .firstSourceAlias if a table is not specified", function () {
            assert.equal(dataset.alwaysQualify().filter({id: 1}).sql, "SELECT test.* FROM test WHERE (test.id = 1)");
        });

        it.should("qualify to the table if a table is specified", function () {
            assert.equal(dataset.alwaysQualify("someTable").filter({id: 1}).sql, "SELECT someTable.* FROM test WHERE (someTable.id = 1)");
        });

        it.should("not qualify to the given table if withSql is used", function () {
            assert.equal(dataset.alwaysQualify().withSql("SELECT * FROM test WHERE (a < b)").qualifyTo("e").sql, 'SELECT * FROM test WHERE (a < b)');
        });

    });

    it.describe("#qualifyToFirstSource", function (it) {
        var ds = new MockDatabase().from("t");

        it.should("qualifyTo the first source", function () {
            assert.equal(ds.qualifyToFirstSource().sql, 'SELECT t.* FROM t');
        });

        it.should("handle the SELECT, order, where, having, and group options/clauses", function () {
            assert.equal(ds.select("a").filter({a: 1}).order("a").group("a").having("a").qualifyToFirstSource().sql, 'SELECT t.a FROM t WHERE (t.a = 1) GROUP BY t.a HAVING t.a ORDER BY t.a');
        });

        it.should("handle the SELECT using a table.* if all columns are currently selected", function () {
            assert.equal(ds.filter({a: 1}).order("a").group("a").having("a").qualifyToFirstSource().sql, 'SELECT t.* FROM t WHERE (t.a = 1) GROUP BY t.a HAVING t.a ORDER BY t.a');
        });

        it.should("handle hashes in SELECT option", function () {
            assert.equal(ds.select({a: "b"}).qualifyToFirstSource().sql, 'SELECT t.a AS b FROM t');
        });

        it.should("handle strings", function () {
            assert.equal(ds.select("a", "b__c", "d___e", "f__g___h").qualifyToFirstSource().sql, 'SELECT t.a, b.c, t.d AS e, f.g AS h FROM t');
        });

        it.should("handle arrays", function () {
            assert.equal(ds.filter({a: [sql.b, sql.c]}).qualifyToFirstSource().sql, 'SELECT t.* FROM t WHERE (t.a IN (t.b, t.c))');
        });

        it.should("handle hashes", function () {
            assert.equal(ds.select(sql["case"]({b: {c: 1}}, false)).qualifyToFirstSource().sql, "SELECT (CASE WHEN t.b THEN (t.c = 1) ELSE 'f' END) FROM t");
        });

        it.should("handle Identifiers", function () {
            assert.equal(ds.select(sql.a).qualifyToFirstSource().sql, 'SELECT t.a FROM t');
        });

        it.should("handle OrderedExpressions", function () {
            assert.equal(ds.order(sql.a.desc(), sql.b.asc()).qualifyToFirstSource().sql, 'SELECT t.* FROM t ORDER BY t.a DESC, t.b ASC');
        });

        it.should("handle AliasedExpressions", function () {
            assert.equal(ds.select(sql.a.as("b")).qualifyToFirstSource().sql, 'SELECT t.a AS b FROM t');
        });

        it.should("handle CaseExpressions", function () {
            assert.equal(ds.filter(sql["case"]({a: sql.b}, sql.c, sql.d)).qualifyToFirstSource().sql, 'SELECT t.* FROM t WHERE (CASE t.d WHEN t.a THEN t.b ELSE t.c END)');
        });


        it.should("handle Casts", function () {
            assert.equal(ds.filter(sql.a.cast("boolean")).qualifyToFirstSource().sql, 'SELECT t.* FROM t WHERE CAST(t.a AS boolean)');
        });

        it.should("handle Functions", function () {
            assert.equal(ds.filter(sql.a("b", 1)).qualifyToFirstSource().sql, 'SELECT t.* FROM t WHERE a(t.b, 1)');
        });

        it.should("handle ComplexExpressions", function () {
            assert.equal(ds.filter(
                function (t) {
                    return t.a.plus(t.b).lt(t.c.minus(3));
                }).qualifyToFirstSource().sql, 'SELECT t.* FROM t WHERE ((t.a + t.b) < (t.c - 3))');
        });


        it.should("handle Subscripts", function () {
            assert.equal(ds.filter(sql.a.sqlSubscript(sql.b, 3)).qualifyToFirstSource().sql, 'SELECT t.* FROM t WHERE t.a[t.b, 3]');
        });

        it.should("handle PlaceholderLiteralStrings", function () {
            assert.equal(ds.filter('? > ?', sql.a, 1).qualifyToFirstSource().sql, 'SELECT t.* FROM t WHERE (t.a > 1)');
        });

        it.should("handle PlaceholderLiteralStrings with named placeholders", function () {
            assert.equal(ds.filter('{a} > {b}', {a: sql.c, b: 1}).qualifyToFirstSource().sql, 'SELECT t.* FROM t WHERE (t.c > 1)');
        });


        it.should("handle all other objects by returning them unchanged", function () {
            assert.equal(ds.select(sql.literal("'a'")).filter(sql.a(3)).filter(sql.literal('blah')).order(sql.literal(true)).group(sql.literal('a > ?', [1])).having(false).qualifyToFirstSource().sql, "SELECT 'a' FROM t WHERE (a(3) AND (blah)) GROUP BY a > 1 HAVING 'f' ORDER BY true");
        });

        it.should("not handle numeric or string expressions ", function () {
            assert.throws(function () {
                ds.filter(sql.identifier("a").plus(sql.b));
            });
            assert.throws(function () {
                ds.filter(sql.sqlStringJoin(["a", "b", "c"], " "));
            });
        });
    });

    it.describe("#with and #withRecursive", function (it) {
        var db = new MockDatabase(),
            ds = db.from("t");

        it.should("with should take a name and dataset and use a WITH clause", function () {
            assert.equal(ds["with"]("t", db.from("x")).sql, 'WITH t AS (SELECT * FROM x) SELECT * FROM t');
        });

        it.should("withRecursive should take a name, nonrecursive dataset, and recursive dataset, and use a WITH clause", function () {
            assert.equal(ds.withRecursive("t", db.from("x"), db.from("t")).sql, 'WITH t AS (SELECT * FROM x UNION ALL SELECT * FROM t) SELECT * FROM t');
        });

        it.should("with and withRecursive should add to existing WITH clause if called multiple times", function () {
            assert.equal(ds["with"]("t", db.from("x"))["with"]("j", db.from("y")).sql, 'WITH t AS (SELECT * FROM x), j AS (SELECT * FROM y) SELECT * FROM t');
            assert.equal(ds.withRecursive("t", db.from("x"), db.from("t")).withRecursive("j", db.from("y"), db.from("j")).sql, 'WITH t AS (SELECT * FROM x UNION ALL SELECT * FROM t), j AS (SELECT * FROM y UNION ALL SELECT * FROM j) SELECT * FROM t');
            assert.equal(ds["with"]("t", db.from("x")).withRecursive("j", db.from("y"), db.from("j")).sql, 'WITH t AS (SELECT * FROM x), j AS (SELECT * FROM y UNION ALL SELECT * FROM j) SELECT * FROM t');
        });

        it.should("with and withRecursive should take an args option", function () {
            assert.equal(ds["with"]("t", db.from("x"), {args: ["b"]}).sql, 'WITH t(b) AS (SELECT * FROM x) SELECT * FROM t');
            assert.equal(ds.withRecursive("t", db.from("x"), db.from("t"), {args: ["b", "c"]}).sql, 'WITH t(b, c) AS (SELECT * FROM x UNION ALL SELECT * FROM t) SELECT * FROM t');
        });

        it.should("withRecursive should take an unionAll : false option", function () {
            assert.equal(ds.withRecursive("t", db.from("x"), db.from("t"), {unionAll: false}).sql, 'WITH t AS (SELECT * FROM x UNION SELECT * FROM t) SELECT * FROM t');
        });

        it.should("with and withRecursive should raise an error unless the dataset supports CTEs", function () {
            ds.supportsCte = false;
            assert.throws(hitch(ds, "with", db.from("x"), {args: ["b"]}));
            assert.throws(hitch(ds, "withRecursive", db.from("x"), db.from("t"), {args: ["b", "c"]}));
        });
    });


    it.describe("#forUpdate", function (it) {
        var ds = new MockDatabase().from("t");
        it.should("use FOR UPDATE", function () {
            assert.equal(ds.forUpdate().sql, "SELECT * FROM t FOR UPDATE");
        });
    });

    it.describe("#lockStyle", function (it) {
        var ds = new MockDatabase().from("t");
        it.should("accept strings", function () {
            assert.equal(ds.lockStyle("update").sql, "SELECT * FROM t FOR UPDATE");
        });

        it.should("accept strings for arbitrary SQL", function () {
            assert.equal(ds.lockStyle("FOR SHARE").sql, "SELECT * FROM t FOR SHARE");
        });
    });


    it.describe("moose style queries", function (it) {
        var ds = new Dataset().from("test");

        it.should('finding all records with limited fields', function () {
            assert.equal(ds.select(["a", "b", "c"]).sql, "SELECT a, b, c FROM test");
        });

        it.should('support logic operators ', function () {
            assert.equal(ds.eq({x: 0}).sql, "SELECT * FROM test WHERE (x = 0)");
            assert.equal(ds.find({x: 0}).sql, "SELECT * FROM test WHERE (x = 0)");
            assert.equal(ds.eq({x: 1}).sql, "SELECT * FROM test WHERE (x = 1)");
            assert.equal(ds.find({x: 1}).sql, "SELECT * FROM test WHERE (x = 1)");

            assert.equal(ds.neq({x: 0}).sql, "SELECT * FROM test WHERE (x != 0)");
            assert.equal(ds.find({x: {neq: 0}}).sql, "SELECT * FROM test WHERE (x != 0)");
            assert.equal(ds.neq({x: 1}).sql, "SELECT * FROM test WHERE (x != 1)");
            assert.equal(ds.find({x: {neq: 1}}).sql, "SELECT * FROM test WHERE (x != 1)");

            assert.equal(ds.gt({x: 0}).sql, "SELECT * FROM test WHERE (x > 0)");
            assert.equal(ds.find({x: {gt: 0}}).sql, "SELECT * FROM test WHERE (x > 0)");
            assert.equal(ds.gte({x: 0}).sql, "SELECT * FROM test WHERE (x >= 0)");
            assert.equal(ds.find({x: {gte: 0}}).sql, "SELECT * FROM test WHERE (x >= 0)");

            assert.equal(ds.gt({x: 1}).sql, "SELECT * FROM test WHERE (x > 1)");
            assert.equal(ds.find({x: {gt: 1}}).sql, "SELECT * FROM test WHERE (x > 1)");
            assert.equal(ds.gte({x: 1}).sql, "SELECT * FROM test WHERE (x >= 1)");
            assert.equal(ds.find({x: {gte: 1}}).sql, "SELECT * FROM test WHERE (x >= 1)");

            assert.equal(ds.lt({x: 0}).sql, "SELECT * FROM test WHERE (x < 0)");
            assert.equal(ds.find({x: {lt: 0}}).sql, "SELECT * FROM test WHERE (x < 0)");
            assert.equal(ds.lte({x: 0}).sql, "SELECT * FROM test WHERE (x <= 0)");
            assert.equal(ds.find({x: {lte: 0}}).sql, "SELECT * FROM test WHERE (x <= 0)");

            assert.equal(ds.lt({x: 1}).sql, "SELECT * FROM test WHERE (x < 1)");
            assert.equal(ds.find({x: {lt: 1}}).sql, "SELECT * FROM test WHERE (x < 1)");
            assert.equal(ds.lte({x: 1}).sql, "SELECT * FROM test WHERE (x <= 1)");
            assert.equal(ds.find({x: {lte: 1}}).sql, "SELECT * FROM test WHERE (x <= 1)");

            assert.equal(ds.find({x: {lt: 1, gt: 10}}).sql, "SELECT * FROM test WHERE ((x < 1) AND (x > 10))");
        });

        it.should("support like ", function () {

            assert.equal(ds.like("title", 'javasScript').sql,
                "SELECT * FROM test WHERE ((title LIKE 'javasScript'))");
            assert.equal(ds.find({title: {like: 'javasScript'}}).sql,
                "SELECT * FROM test WHERE (title LIKE 'javasScript')");
            assert.equal(ds.find({title: {iLike: 'javasScript'}}).sql,
                "SELECT * FROM test WHERE (title ILIKE 'javasScript')");

            assert.equal(ds.find({title: {like: /javasScript/i}}).sql,
                "SELECT * FROM test WHERE (title ~* 'javasScript')");
            assert.equal(ds.find({title: {iLike: /javasScript/}}).sql,
                "SELECT * FROM test WHERE (title ~* 'javasScript')");
        });

        it.should("support between/notBetween", function () {
            assert.equal(ds.between({x: [1, 2]}).sql, "SELECT * FROM test WHERE ((x >= 1) AND (x <= 2))");
            assert.equal(ds.find({x: {between: [1, 2]}}).sql, "SELECT * FROM test WHERE ((x >= 1) AND (x <= 2))");
            assert.throws(function () {
                ds.between({x: "a"});
            });

            assert.equal(ds.notBetween({x: [1, 2]}).sql, "SELECT * FROM test WHERE ((x < 1) OR (x > 2))");
            assert.equal(ds.find({x: {notBetween: [1, 2]}}).sql, "SELECT * FROM test WHERE ((x < 1) OR (x > 2))");
            assert.throws(function () {
                ds.notBetween({x: "a"});
            });
        });


        it.should('support is/isNot', function () {
            assert.equal(ds.is({flag: true}).sql, "SELECT * FROM test WHERE (flag IS TRUE)");
            assert.equal(ds.isTrue("flag").sql, "SELECT * FROM test WHERE (flag IS TRUE)");
            assert.equal(ds.isTrue("flag", "otherFlag").sql, "SELECT * FROM test WHERE ((flag IS TRUE) AND (otherFlag IS TRUE))");
            assert.equal(ds.is({flag: false}).sql, "SELECT * FROM test WHERE (flag IS FALSE)");
            assert.equal(ds.isFalse("flag").sql, "SELECT * FROM test WHERE (flag IS FALSE)");
            assert.equal(ds.isFalse("flag", "otherFlag").sql, "SELECT * FROM test WHERE ((flag IS FALSE) AND (otherFlag IS FALSE))");
            assert.equal(ds.is({flag: null}).sql, "SELECT * FROM test WHERE (flag IS NULL)");
            assert.equal(ds.isNull("flag").sql, "SELECT * FROM test WHERE (flag IS NULL)");
            assert.equal(ds.isNull("flag", "otherFlag").sql, "SELECT * FROM test WHERE ((flag IS NULL) AND (otherFlag IS NULL))");
            assert.equal(ds.is({flag: true, otherFlag: false, yetAnotherFlag: null}).sql,
                'SELECT * FROM test WHERE ((flag IS TRUE) AND (otherFlag IS FALSE) AND (yetAnotherFlag IS NULL))');

            assert.equal(ds.find({flag: {is: true}}).sql, "SELECT * FROM test WHERE (flag IS TRUE)");
            assert.equal(ds.find({flag: {is: false}}).sql, "SELECT * FROM test WHERE (flag IS FALSE)");
            assert.equal(ds.find({flag: {is: null}}).sql, "SELECT * FROM test WHERE (flag IS NULL)");
            assert.equal(ds.find({flag: {is: true}, otherFlag: {is: false}, yetAnotherFlag: {is: null}}).sql,
                'SELECT * FROM test WHERE ((flag IS TRUE) AND (otherFlag IS FALSE) AND (yetAnotherFlag IS NULL))');

            assert.equal(ds.find({"flag,otherFlag": {isNot: null}}).sql, "SELECT * FROM test WHERE ((flag IS NOT NULL) AND (otherFlag IS NOT NULL))");

            assert.equal(ds.isNot({flag: true}).sql, "SELECT * FROM test WHERE (flag IS NOT TRUE)");
            assert.equal(ds.isNotTrue("flag").sql, "SELECT * FROM test WHERE (flag IS NOT TRUE)");
            assert.equal(ds.isNotTrue("flag", "otherFlag").sql, "SELECT * FROM test WHERE ((flag IS NOT TRUE) AND (otherFlag IS NOT TRUE))");
            assert.equal(ds.isNot({flag: false}).sql, "SELECT * FROM test WHERE (flag IS NOT FALSE)");
            assert.equal(ds.isNotFalse("flag").sql, "SELECT * FROM test WHERE (flag IS NOT FALSE)");
            assert.equal(ds.isNotFalse("flag", "otherFlag").sql, "SELECT * FROM test WHERE ((flag IS NOT FALSE) AND (otherFlag IS NOT FALSE))");
            assert.equal(ds.isNot({flag: null}).sql, "SELECT * FROM test WHERE (flag IS NOT NULL)");
            assert.equal(ds.isNotNull("flag").sql, "SELECT * FROM test WHERE (flag IS NOT NULL)");
            assert.equal(ds.isNotNull("flag", "otherFlag").sql, "SELECT * FROM test WHERE ((flag IS NOT NULL) AND (otherFlag IS NOT NULL))");
            assert.equal(ds.isNot({flag: true, otherFlag: false, yetAnotherFlag: null}).sql,
                "SELECT * FROM test WHERE ((flag IS NOT TRUE) AND (otherFlag IS NOT FALSE) AND (yetAnotherFlag IS NOT NULL))");

            assert.equal(ds.find({flag: {isNot: true}}).sql, "SELECT * FROM test WHERE (flag IS NOT TRUE)");
            assert.equal(ds.find({flag: {isNot: false}}).sql, "SELECT * FROM test WHERE (flag IS NOT FALSE)");
            assert.equal(ds.find({flag: {isNot: null}}).sql, "SELECT * FROM test WHERE (flag IS NOT NULL)");
            assert.equal(ds.find({flag: {isNot: true}, otherFlag: {isNot: false}, yetAnotherFlag: {isNot: null}}).sql,
                "SELECT * FROM test WHERE ((flag IS NOT TRUE) AND (otherFlag IS NOT FALSE) AND (yetAnotherFlag IS NOT NULL))");

            assert.equal(ds.find({flag: {isNot: true}, otherFlag: {is: false}, yetAnotherFlag: {isNot: null}}).sql,
                "SELECT * FROM test WHERE ((flag IS NOT TRUE) AND (otherFlag IS FALSE) AND (yetAnotherFlag IS NOT NULL))");
            assert.equal(ds.find({flag: {is: true}, otherFlag: {isNot: false}, yetAnotherFlag: {is: null}}).sql,
                "SELECT * FROM test WHERE ((flag IS TRUE) AND (otherFlag IS NOT FALSE) AND (yetAnotherFlag IS NULL))");


            assert.throws(function () {
                ds.isTrue(["flag"]);
            });

            assert.throws(function () {
                ds.isFalse(["flag"]);
            });

            assert.throws(function () {
                ds.isNull(["flag"]);
            });

            assert.throws(function () {
                ds.isNotNull(["flag"]);
            });

            assert.throws(function () {
                ds.isNotTrue(["flag"]);
            });

            assert.throws(function () {
                ds.isNotFalse(["flag"]);
            });

        });
    });


    it.afterAll(comb.hitch(patio, "disconnect"));

}).as(module);
