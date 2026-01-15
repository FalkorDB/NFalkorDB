using System;
using System.Collections.Generic;
using System.Linq;
using StackExchange.Redis;

namespace NFalkorDB;

/// <summary>
/// FalkorDB client.
/// 
/// This class facilitates querying FalkorDB and parsing the results.
/// </summary>
public sealed class FalkorDB
{
    private readonly IDatabase _db;

    /// <summary>
    /// Creates a FalkorDB client that leverages a specified instance of <see cref="IDatabase"/>.
    /// </summary>
    /// <param name="db">Existing StackExchange.Redis database instance to use.</param>
    public FalkorDB(IDatabase db = null)
    {
        _db = db ?? ConnectionMultiplexer.Connect("localhost").GetDatabase();
    }

    /// <summary>
    /// Creates a FalkorDB client using a StackExchange.Redis configuration string.
    /// </summary>
    /// <param name="configuration">Configuration string or URL accepted by StackExchange.Redis.</param>
    public FalkorDB(string configuration)
    {
        if (string.IsNullOrWhiteSpace(configuration))
        {
            throw new ArgumentException("Configuration must be provided.", nameof(configuration));
        }

        var mux = ConnectionMultiplexer.Connect(configuration);
        _db = mux.GetDatabase();
    }

    /// <summary>
    /// Factory method that creates a FalkorDB client using a StackExchange.Redis configuration string.
    /// </summary>
    /// <param name="configuration">Configuration string or URL accepted by StackExchange.Redis.</param>
    /// <returns>A new <see cref="FalkorDB"/> instance.</returns>
    public static FalkorDB Connect(string configuration) => new FalkorDB(configuration);

    /// <summary>
    /// Selects a graph by its ID.
    /// </summary>
    /// <param name="graphId">The graph identifier.</param>
    /// <returns>A <see cref="Graph"/> instance bound to this client.</returns>
    public Graph SelectGraph(string graphId)
    {
        if (string.IsNullOrWhiteSpace(graphId))
        {
            throw new ArgumentException("Graph id must be provided.", nameof(graphId));
        }

        var graph = new Graph(graphId, _db);
        return graph;
    }

    /// <summary>
    /// Lists all graph names in the current database.
    /// </summary>
    public IReadOnlyList<string> ListGraphs(CommandFlags flags = CommandFlags.None)
    {
        try
        {
            var result = _db.Execute(Command.LIST, flags);

            if (result.Resp2Type != ResultType.Array)
            {
                return Array.Empty<string>();
            }

            return ((RedisResult[])result)
                .Select(x => (string)x)
                .ToArray();
        }
        catch (RedisConnectionException)
        {
            // GRAPH.LIST may not be supported or may return an unexpected shape; treat as no graphs.
            return Array.Empty<string>();
        }
        catch (InvalidCastException)
        {
            // Defensive against unexpected element types in the reply.
            return Array.Empty<string>();
        }
    }

    /// <summary>
    /// Lists all graph names in the current database asynchronously.
    /// </summary>
    public async System.Threading.Tasks.Task<IReadOnlyList<string>> ListGraphsAsync(CommandFlags flags = CommandFlags.None)
    {
        var result = await _db.ExecuteAsync(Command.LIST, flags).ConfigureAwait(false);

        if (result.Resp2Type != ResultType.Array)
        {
            return Array.Empty<string>();
        }

        return ((RedisResult[])result)
            .Select(x => (string)x)
            .ToArray();
    }

    /// <summary>
    /// Gets a FalkorDB configuration value using GRAPH.CONFIG GET.
    /// </summary>
    /// <param name="name">Configuration name.</param>
    public string GetConfig(string name, CommandFlags flags = CommandFlags.None)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("Configuration name must be provided.", nameof(name));
        }

        try
        {
            var result = _db.Execute(Command.CONFIG, new object[] { "GET", name }, flags);

            if (result.Resp2Type != ResultType.Array)
            {
                return null;
            }

            var arr = (RedisResult[])result;
            return arr.Length == 2 ? (string)arr[1] : null;
        }
        catch (RedisServerException)
        {
            // GRAPH.CONFIG may not be supported or may respond in an unexpected way; treat as no value.
            return null;
        }
        catch (RedisConnectionException)
        {
            // Connection-level issues should not bring down the caller; treat as no value.
            return null;
        }
    }

    /// <summary>
    /// Gets a FalkorDB configuration value asynchronously using GRAPH.CONFIG GET.
    /// </summary>
    /// <param name="name">Configuration name.</param>
    public async System.Threading.Tasks.Task<string> GetConfigAsync(string name, CommandFlags flags = CommandFlags.None)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("Configuration name must be provided.", nameof(name));
        }

        try
        {
            var result = await _db.ExecuteAsync(Command.CONFIG, new object[] { "GET", name }, flags).ConfigureAwait(false);

            if (result.Resp2Type != ResultType.Array)
            {
                return null;
            }

            var arr = (RedisResult[])result;
            return arr.Length == 2 ? (string)arr[1] : null;
        }
        catch (RedisServerException)
        {
            return null;
        }
        catch (RedisConnectionException)
        {
            return null;
        }
    }

    /// <summary>
    /// Sets a FalkorDB configuration value using GRAPH.CONFIG SET.
    /// </summary>
    /// <param name="name">Configuration name.</param>
    /// <param name="value">Configuration value.</param>
    public void SetConfig(string name, RedisValue value, CommandFlags flags = CommandFlags.None)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("Configuration name must be provided.", nameof(name));
        }

        _db.Execute(Command.CONFIG, new object[] { "SET", name, value }, flags);
    }

    /// <summary>
    /// Sets a FalkorDB configuration value asynchronously using GRAPH.CONFIG SET.
    /// </summary>
    /// <param name="name">Configuration name.</param>
    /// <param name="value">Configuration value.</param>
    public System.Threading.Tasks.Task SetConfigAsync(string name, RedisValue value, CommandFlags flags = CommandFlags.None)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("Configuration name must be provided.", nameof(name));
        }

        return _db.ExecuteAsync(Command.CONFIG, new object[] { "SET", name, value }, flags);
    }

    /// <summary>
    /// Loads a user-defined function (UDF) from the specified path using GRAPH.UDF LOAD.
    /// </summary>
    /// <param name="path">Path to the UDF file.</param>
    public void UdfLoad(string path, CommandFlags flags = CommandFlags.None)
    {
        if (string.IsNullOrWhiteSpace(path))
        {
            throw new ArgumentException("Path must be provided.", nameof(path));
        }

        _db.Execute(Command.UDF, new object[] { "LOAD", path }, flags);
    }

    /// <summary>
    /// Loads a user-defined function (UDF) from the specified path asynchronously using GRAPH.UDF LOAD.
    /// </summary>
    /// <param name="path">Path to the UDF file.</param>
    public System.Threading.Tasks.Task UdfLoadAsync(string path, CommandFlags flags = CommandFlags.None)
    {
        if (string.IsNullOrWhiteSpace(path))
        {
            throw new ArgumentException("Path must be provided.", nameof(path));
        }

        return _db.ExecuteAsync(Command.UDF, new object[] { "LOAD", path }, flags);
    }

    /// <summary>
    /// Lists all loaded user-defined functions (UDFs) using GRAPH.UDF LIST.
    /// </summary>
    public IReadOnlyList<string> UdfList(CommandFlags flags = CommandFlags.None)
    {
        try
        {
            var result = _db.Execute(Command.UDF, new object[] { "LIST" }, flags);

            if (result.Resp2Type != ResultType.Array)
            {
                return Array.Empty<string>();
            }

            return ((RedisResult[])result)
                .Select(x => (string)x)
                .ToArray();
        }
        catch (RedisConnectionException)
        {
            return Array.Empty<string>();
        }
        catch (InvalidCastException)
        {
            return Array.Empty<string>();
        }
    }

    /// <summary>
    /// Lists all loaded user-defined functions (UDFs) asynchronously using GRAPH.UDF LIST.
    /// </summary>
    public async System.Threading.Tasks.Task<IReadOnlyList<string>> UdfListAsync(CommandFlags flags = CommandFlags.None)
    {
        try
        {
            var result = await _db.ExecuteAsync(Command.UDF, new object[] { "LIST" }, flags).ConfigureAwait(false);

            if (result.Resp2Type != ResultType.Array)
            {
                return Array.Empty<string>();
            }

            return ((RedisResult[])result)
                .Select(x => (string)x)
                .ToArray();
        }
        catch (RedisConnectionException)
        {
            return Array.Empty<string>();
        }
        catch (InvalidCastException)
        {
            return Array.Empty<string>();
        }
    }

    /// <summary>
    /// Removes all loaded user-defined functions (UDFs) using GRAPH.UDF FLUSH.
    /// </summary>
    public void UdfFlush(CommandFlags flags = CommandFlags.None)
    {
        _db.Execute(Command.UDF, new object[] { "FLUSH" }, flags);
    }

    /// <summary>
    /// Removes all loaded user-defined functions (UDFs) asynchronously using GRAPH.UDF FLUSH.
    /// </summary>
    public System.Threading.Tasks.Task UdfFlushAsync(CommandFlags flags = CommandFlags.None)
    {
        return _db.ExecuteAsync(Command.UDF, new object[] { "FLUSH" }, flags);
    }

    /// <summary>
    /// Deletes a specific user-defined function (UDF) using GRAPH.UDF DELETE.
    /// </summary>
    /// <param name="functionName">Name of the function to delete.</param>
    public void UdfDelete(string functionName, CommandFlags flags = CommandFlags.None)
    {
        if (string.IsNullOrWhiteSpace(functionName))
        {
            throw new ArgumentException("Function name must be provided.", nameof(functionName));
        }

        _db.Execute(Command.UDF, new object[] { "DELETE", functionName }, flags);
    }

    /// <summary>
    /// Deletes a specific user-defined function (UDF) asynchronously using GRAPH.UDF DELETE.
    /// </summary>
    /// <param name="functionName">Name of the function to delete.</param>
    public System.Threading.Tasks.Task UdfDeleteAsync(string functionName, CommandFlags flags = CommandFlags.None)
    {
        if (string.IsNullOrWhiteSpace(functionName))
        {
            throw new ArgumentException("Function name must be provided.", nameof(functionName));
        }

        return _db.ExecuteAsync(Command.UDF, new object[] { "DELETE", functionName }, flags);
    }
}
