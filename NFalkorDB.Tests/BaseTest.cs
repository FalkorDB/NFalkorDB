using System;

namespace NFalkorDB.Tests;

public abstract class BaseTest : IDisposable
{
    public string RedisConnectionString { get; } = Environment.GetEnvironmentVariable("REDIS_CONNECTION_STRING") ?? "localhost";

    /// <summary>
    /// Connection string with admin commands enabled. StackExchange.Redis 3.0
    /// refuses FLUSHDB (and other admin commands) unless allowAdmin is set,
    /// so test fixtures that reset the database must connect with this.
    /// </summary>
    public string AdminRedisConnectionString => $"{RedisConnectionString},allowAdmin=true";

    protected abstract void BeforeTest();

    protected abstract void AfterTest();

    public BaseTest()
    {
        BeforeTest();
    }

    public void Dispose()
    {
        AfterTest();
    }
}