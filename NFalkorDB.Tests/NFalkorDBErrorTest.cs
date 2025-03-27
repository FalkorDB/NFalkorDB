using StackExchange.Redis;
using Xunit;

namespace NFalkorDB.Tests
{
    public class NFalkorDBErrorTest : BaseTest
    {
        private ConnectionMultiplexer _muxr;
        private Graph _api;

        public NFalkorDBErrorTest() : base() { }

        protected override void BeforeTest()
        {
            _muxr = ConnectionMultiplexer.Connect(RedisConnectionString);

            _api = new FalkorDB(_muxr.GetDatabase(0)).SelectGraph("social");

            Assert.NotNull(_api.Query("CREATE (:person{mixed_prop: 'strval'}), (:person{mixed_prop: 50})"));
        }

        protected override void AfterTest()
        {
            _api.Delete();
        }

        // TODO: Figure out what to do about the "compile time" exceptions. SE.Redis is just throwing a RedisServerException
        //       which could be anything I suppose...

        // [Fact]
        // public void TestSyntaxErrorReporting()
        // {
        //     // Issue a query that causes a compile-time error
        //     var exception = Assert.Throws<NFalkorDBCompileTimeException>(() =>
        //     {
        //         _api.Query("social", "RETURN toUpper(5)");
        //     });

        //     Assert.Contains("Type mismatch: expected String but was Integer", exception.Message);
        // }

        // [Fact]
        // public void TestRuntimeErrorReporting()
        // {
        //     // Issue a query that causes a run-time error
        //     var exception = Assert.Throws<NFalkorDBRunTimeException>(() =>
        //     {
        //         _api.Query("social", "MATCH (p:person) RETURN toUpper(p.mixed_prop)");
        //     });

        //     Assert.Contains("Type mismatch: expected String but was Integer", exception.Message);
        // }

        // [Fact]
        // public void TestExceptionFlow()
        // {
        //     var compileTimeException = Assert.Throws<NFalkorDBCompileTimeException>(() =>
        //     {
        //         _api.Query("social", "RETURN toUpper(5)");
        //     });

        //     Assert.Contains("Type mismatch: expected String but was Integer", compileTimeException.Message);

        //     var runTimeException = Assert.Throws<NFalkorDBRunTimeException>(() =>
        //     {
        //         _api.Query("social", "MATCH (p:person) RETURN toUpper(p.mixed_prop)");
        //     });

        //     Assert.Contains("Type mismatch: expected String but was Integer", runTimeException.Message);
        // }

        // [Fact]
        // public void TestMissingParametersSyntaxErrorReporting()
        // {
        //     var exception = Assert.Throws<NFalkorDBCompileTimeException>(() =>
        //     {
        //         _api.Query("social", "RETURN $param");
        //     });

        //     Assert.Contains("Missing parameters", exception.Message);
        // }

        // [Fact]
        // public void TestMissingParametersSyntaxErrorReporting2()
        // {
        //     var exception = Assert.Throws<NFalkorDBCompileTimeException>(() =>
        //     {
        //         _api.Query("social", "RETURN $param", new Dictionary<string, object>());
        //     });

        //     Assert.Contains("Missing parameters", exception.Message);
        // }

    }
}