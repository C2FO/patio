var vows = require('vows'),
    assert = require('assert'),
    patio = require("index"),
    Dataset = patio.Dataset,
    Database = patio.Database,
    sql = patio.SQL,
    Identifier = sql.Identifier,
    SQLFunction = sql.SQLFunction,
    LiteralString = sql.LiteralString,
    comb = require("comb");

var ret = (module.exports = exports = new comb.Promise());
var suite = vows.describe("Dataset graphing");

patio.quoteIdentifiers = false;
patio.identifierInputMethod = null;
patio.identifierOutputMethod = null;


suite.addBatch({
    "a dataset":{
        topic:new Dataset().from("test2"),

        "should convert literals properly":function (ds) {
            assert.equal(ds.literal(sql.literal("(hello, world)")), "(hello, world)");
            assert.equal(ds.literal("('hello', 'world')"), "'(''hello'', ''world'')'");
            assert.equal(ds.literal("(hello, world)"), "'(hello, world)'");
            assert.equal(ds.literal("hello, world"), "'hello, world'");
            assert.equal(ds.literal('("hello", "world")'), "'(\"hello\", \"world\")'");
            assert.equal(ds.literal("(\hello\, \world\)'"), "'(hello, world)'''");
            assert.equal(ds.literal("\\'\\'"), "'\\\\''\\\\'''");
            assert.strictEqual(ds.literal(1), "1");
            assert.strictEqual(ds.literal(1.0), "1");
            assert.strictEqual(ds.literal(1.01), "1.01");
            assert.equal(ds.literal(sql.hello.lt(1)),'(hello < 1)');
            assert.equal(ds.literal(sql.hello.gt(1)),'(hello > 1)');
            assert.equal(ds.literal(sql.hello.lte(1)),'(hello <= 1)');
            assert.equal(ds.literal(sql.hello.gte(1)),'(hello >= 1)');
            assert.equal(ds.literal(sql.hello.like("test")),"(hello LIKE 'test')");
            assert.equal(ds.literal(ds.from("test").order("name")),"(SELECT * FROM test ORDER BY name)");
            assert.equal(ds.literal([1,2,3]),"(1, 2, 3)");
            assert.equal(ds.literal([1,"2",3]),"(1, '2', 3)");
            assert.equal(ds.literal([1,"\\'\\'",3]),"(1, '\\\\''\\\\''', 3)");
            assert.equal(ds.literal(new sql.Year(2009)), '2009')
            assert.equal(ds.literal(new sql.TimeStamp(2009, 10, 10,10,10)), "'2009-11-10 10:10:00'");
            assert.equal(ds.literal(new sql.DateTime(2009, 10, 10,10,10)), "'2009-11-10 10:10:00'");
            assert.equal(ds.literal(new Date(2009,10,10)), "'2009-11-10'");
            assert.equal(ds.literal(new sql.Time(11,10,10)), "'11:10:10'");
            assert.equal(ds.literal(null), "NULL");
            assert.equal(ds.literal(true), "'t'");
            assert.equal(ds.literal(false), "'f'");
            assert.equal(ds.literal({a : "b"}), "(a = 'b')");
            assert.throws(comb.hitch(ds, "literal", /a/));
        }
    }
});

suite.run({reporter : vows.reporter.spec}, function(){
    patio.disconnect();
    ret.callback();
});