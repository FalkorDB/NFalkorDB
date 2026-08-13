using System;
using StackExchange.Redis;

namespace NFalkorDB.Tests;

public abstract class BaseTest : IDisposable
{
    public string RedisConnectionString { get; } = Environment.GetEnvironmentVariable("REDIS_CONNECTION_STRING") ?? "localhost";

    /// <summary>
    /// Connection options with admin commands enabled. StackExchange.Redis 3.0
    /// refuses FLUSHDB (and other admin commands) unless AllowAdmin is set,
    /// so test fixtures that reset the database must connect with these.
    /// </summary>
    public ConfigurationOptions AdminConnectionOptions
    {
        get
        {
            var options = ConfigurationOptions.Parse(RedisConnectionString);
            options.AllowAdmin = true;
            return options;
        }
    }

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