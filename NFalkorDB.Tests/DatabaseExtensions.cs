using StackExchange.Redis;

namespace NFalkorDB.Tests;

public class IDatabaseExtensionsTests : BaseTest
{
    private ConnectionMultiplexer _muxr;
    private IDatabase _db;
    protected override void BeforeTest()
    {
        _muxr = ConnectionMultiplexer.Connect(RedisConnectionString);
        _db = _muxr.GetDatabase(0);
    }

    

    protected override void AfterTest()
    {
        _muxr.Dispose();
        _muxr = null;
    }
}