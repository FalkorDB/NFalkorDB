using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using StackExchange.Redis;
using static NFalkorDB.FalkorDBUtilities;

namespace NFalkorDB;

/// <summary>
/// A graph object that represents a graph in the database.
/// </summary>
public class Graph
{
    internal static readonly object CompactQueryFlag = "--COMPACT";

    string _graphId;
    IGraphCache _cache;
    private readonly IDatabase _db;

    /// <summary>
    /// Create a new graph instance.
    /// </summary>
    /// <param name="graphId"></param>
    /// <param name="db"></param>
    internal Graph(string graphId, IDatabase db)
    {
        _graphId = graphId;
        _cache = new GraphCache(graphId, this);
        _db = db;
    }

    /// <summary>
    /// Execute a Cypher query with parameters.
    /// </summary>
    /// <param name="query">The Cypher query.</param>
    /// <param name="parameters">Parameters map.</param>
    /// <param name="flags">[Optional] Command flags that are to be sent to the StackExchange.Redis connection multiplexer...</param>
    /// <param name="timeout">[Optional] Timeout in milliseconds.</param>
    /// <returns>A result set.</returns>
    public ResultSet Query(string query, IDictionary<string, object> parameters = null, CommandFlags flags = CommandFlags.None, long timeout = 0)
    {
        var preparedQuery = PrepareQuery(query, parameters);

        var commandArgs = new List<object>
        {
            _graphId,
            preparedQuery,
            CompactQueryFlag
        };

        if (timeout > 0)
        {
            commandArgs.Add("timeout");
            commandArgs.Add(timeout);
        }

        var rawResult = _db.Execute(Command.QUERY, commandArgs.ToArray(), flags);

        if (flags.HasFlag(CommandFlags.FireAndForget))
        {
            return default;
        }
        else
        {
            return new ResultSet(rawResult, _cache);
        }
    }

    /// <summary>
    /// Execute a Cypher query with parameters.
    /// </summary>
    /// <param name="query">The Cypher query.</param>
    /// <param name="parameters">Parameters map.</param>
    /// <param name="flags">[Optional] Command flags that are to be sent to the StackExchange.Redis connection multiplexer...</param>
    /// <param name="timeout">[Optional] Timeout in milliseconds.</param>
    /// <returns>A result set.</returns>
    public async Task<ResultSet> QueryAsync(string query, IDictionary<string, object> parameters = null, CommandFlags flags = CommandFlags.None, long timeout = 0)
    {
        var preparedQuery = PrepareQuery(query, parameters);

        var commandArgs = new List<object>
        {
            _graphId,
            preparedQuery,
            CompactQueryFlag
        };

        if (timeout > 0)
        {
            commandArgs.Add("timeout");
            commandArgs.Add(timeout);
        }

        var rawResult = await _db.ExecuteAsync(Command.QUERY, commandArgs.ToArray(), flags);

        if (flags.HasFlag(CommandFlags.FireAndForget))
        {
            return default;
        }
        else
        {
            return new ResultSet(rawResult, _cache);
        }
    }

    /// <summary>
    /// Execute a Cypher query, preferring a read-only node.
    /// </summary>
    /// <param name="query">The Cypher query.</param>
    /// <param name="parms">Parameters map.</param>
    /// <param name="flags">Optional command flags. `PreferReplica` is set for you here.</param>
    /// <param name="timeout">[Optional] Timeout in milliseconds.</param>
    /// <returns>A result set.</returns>
    public ResultSet ReadOnlyQuery(string query, IDictionary<string, object> parms = null, CommandFlags flags = CommandFlags.None, long timeout = 0)
    {
        var preparedQuery = PrepareQuery(query, parms);

        var parameters = new List<object>
        {
            _graphId,
            preparedQuery,
            CompactQueryFlag
        };

        if (timeout > 0)
        {
            parameters.Add("timeout");
            parameters.Add(timeout);
        }

        var result = _db.Execute(Command.RO_QUERY, parameters.ToArray(), (flags | CommandFlags.PreferReplica));

        return new ResultSet(result, _cache);
    }

    /// <summary>
    /// Execute a Cypher query, preferring a read-only node.
    /// </summary>
    /// <param name="query">The Cypher query.</param>
    /// <param name="parms">Parameters map.</param>
    /// <param name="flags">Optional command flags. `PreferReplica` is set for you here.</param>
    /// <param name="timeout">[Optional] Timeout in milliseconds.</param>
    /// <returns>A result set.</returns>
    public async Task<ResultSet> ReadOnlyQueryAsync(string query, IDictionary<string, object> parms = null, CommandFlags flags = CommandFlags.None, long timeout = 0)
    {
        var preparedQuery = PrepareQuery(query, parms);

        var parameters = new List<object>
        {
            _graphId,
            preparedQuery,
            CompactQueryFlag
        };

        if (timeout > 0)
        {
            parameters.Add("timeout");
            parameters.Add(timeout);
        }

        var result = await _db.ExecuteAsync(Command.RO_QUERY, parameters.ToArray(), (flags | CommandFlags.PreferReplica));

        return new ResultSet(result, _cache);
    }

    /// <summary>
    /// Call a saved procedure with parameters.
    /// </summary>
    /// <param name="procedure">The procedure name.</param>
    /// <param name="args">A collection of positional arguments.</param>
    /// <param name="kwargs">A collection of keyword arguments.</param>
    /// <param name="flags">[Optional] Command flags that are to be sent to the StackExchange.Redis connection multiplexer...</param>/// 
    /// <returns>A result set.</returns>
    public ResultSet CallProcedure(string procedure, IEnumerable<string> args = null, Dictionary<string, List<string>> kwargs = null, CommandFlags flags = CommandFlags.None)
    {
        args = args?.Select(a => QuoteString(a));

        var queryBody = new StringBuilder();

        queryBody.Append(args != null ? $"CALL {procedure}({string.Join(",", args)})" : $"CALL {procedure}()");

        if (kwargs != null && kwargs.TryGetValue("y", out var kwargsList))
        {
            queryBody.Append(string.Join(",", kwargsList));
        }

        return Query(queryBody.ToString(), flags: flags);
    }

    /// <summary>
    /// Call a saved procedure with parameters.
    /// </summary>
    /// <param name="procedure">The procedure name.</param>
    /// <param name="args">A collection of positional arguments.</param>
    /// <param name="kwargs">A collection of keyword arguments.</param>
    /// <param name="flags">[Optional] Command flags that are to be sent to the StackExchange.Redis connection multiplexer...</param>/// 
    /// <returns>A result set.</returns>
    public Task<ResultSet> CallProcedureAsync(string procedure, IEnumerable<string> args, Dictionary<string, List<string>> kwargs, CommandFlags flags = CommandFlags.None)
    {
        args = args.Select(a => QuoteString(a));

        var queryBody = new StringBuilder();

        queryBody.Append($"CALL {procedure}({string.Join(",", args)})");

        if (kwargs.TryGetValue("y", out var kwargsList))
        {
            queryBody.Append(string.Join(",", kwargsList));
        }

        return QueryAsync(queryBody.ToString(), flags: flags);
    }

    /// <summary>
    /// Call a saved procedure with parameters against a read-only node.
    /// </summary>
    /// <param name="procedure">The procedure name.</param>
    /// <param name="args">A collection of positional arguments.</param>
    /// <param name="kwargs">A collection of keyword arguments.</param>
    /// <param name="flags">[Optional] Command flags that are to be sent to the StackExchange.Redis connection multiplexer...</param>/// 
    /// <returns>A result set.</returns>
    public ResultSet CallProcedureReadOnly(string procedure, IEnumerable<string> args = null, Dictionary<string, List<string>> kwargs = null, CommandFlags flags = CommandFlags.None)
    {
        args = args.Select(a => QuoteString(a));

        var queryBody = new StringBuilder();

        queryBody.Append($"CALL {procedure}({string.Join(",", args)})");

        if (kwargs != null && kwargs.TryGetValue("y", out var kwargsList))
        {
            queryBody.Append(string.Join(",", kwargsList));
        }

        return ReadOnlyQuery(queryBody.ToString(), flags: flags);
    }

    /// <summary>
    /// Call a saved procedure with parameters against a read-only node.
    /// </summary>
    /// <param name="procedure">The procedure name.</param>
    /// <param name="args">A collection of positional arguments.</param>
    /// <param name="kwargs">A collection of keyword arguments.</param>
    /// <param name="flags">[Optional] Command flags that are to be sent to the StackExchange.Redis connection multiplexer...</param>/// 
    /// <returns>A result set.</returns>
    public Task<ResultSet> CallProcedureReadOnlyAsync(string procedure, IEnumerable<string> args = null, Dictionary<string, List<string>> kwargs = null, CommandFlags flags = CommandFlags.None)
    {
        args = args.Select(a => QuoteString(a));

        var queryBody = new StringBuilder();

        queryBody.Append($"CALL {procedure}({string.Join(",", args)})");

        if (kwargs.TryGetValue("y", out var kwargsList))
        {
            queryBody.Append(string.Join(",", kwargsList));
        }

        return ReadOnlyQueryAsync(queryBody.ToString(), flags: flags);
    }

    /// <summary>
    /// Creates a copy of the current graph.
    /// </summary>
    /// <param name="cloneGraphId">Identifier of the cloned graph.</param>
    /// <param name="flags">[Optional] Command flags that are to be sent to the StackExchange.Redis connection multiplexer...</param>
    /// <returns>The cloned <see cref="Graph"/>.</returns>
    public Graph Copy(string cloneGraphId, CommandFlags flags = CommandFlags.None)
    {
        var commandArgs = new object[]
        {
            _graphId,
            cloneGraphId
        };

        _db.Execute(Command.COPY, commandArgs, flags);

        if (flags.HasFlag(CommandFlags.FireAndForget))
        {
            return null;
        }

        return new Graph(cloneGraphId, _db);
    }

    /// <summary>
    /// Creates a copy of the current graph asynchronously.
    /// </summary>
    /// <param name="cloneGraphId">Identifier of the cloned graph.</param>
    /// <param name="flags">[Optional] Command flags that are to be sent to the StackExchange.Redis connection multiplexer...</param>
    /// <returns>The cloned <see cref="Graph"/>.</returns>
    public async Task<Graph> CopyAsync(string cloneGraphId, CommandFlags flags = CommandFlags.None)
    {
        var commandArgs = new object[]
        {
            _graphId,
            cloneGraphId
        };

        await _db.ExecuteAsync(Command.COPY, commandArgs, flags);

        if (flags.HasFlag(CommandFlags.FireAndForget))
        {
            return null;
        }

        return new Graph(cloneGraphId, _db);
    }

    /// <summary>
    /// Delete an existing graph.
    /// </summary>
    /// <param name="flags">[Optional] Command flags that are to be sent to the StackExchange.Redis connection multiplexer...</param>/// 
    /// <returns>A result set.</returns>
    public ResultSet Delete(CommandFlags flags = CommandFlags.None)
    {
        var commandArgs = new object[]
        {
            _graphId
        };

        var rawResult = _db.Execute(Command.DELETE, commandArgs, flags);

        if (flags.HasFlag(CommandFlags.FireAndForget))
        {
            return default;
        }
        else
        {
            return new ResultSet(rawResult, null);
        }
    }

    /// <summary>
    /// Delete an existing graph.
    /// </summary>
    /// <param name="flags">[Optional] Command flags that are to be sent to the StackExchange.Redis connection multiplexer...</param>/// 
    /// <returns>A result set.</returns>
    public async Task<ResultSet> DeleteAsync(CommandFlags flags = CommandFlags.None)
    {
        var commandArgs = new object[]
        {
            _graphId
        };

        var rawResult = await _db.ExecuteAsync(Command.DELETE, commandArgs, flags);

        if (flags.HasFlag(CommandFlags.FireAndForget))
        {
            return default;
        }
        else
        {
            return new ResultSet(rawResult, null);
        }
    }

    /// <summary>
    /// Returns the execution plan for the given query.
    /// </summary>
    public IReadOnlyList<string> Explain(string query, IDictionary<string, object> parameters = null, CommandFlags flags = CommandFlags.None)
    {
        var preparedQuery = PrepareQuery(query, parameters);

        var commandArgs = new object[]
        {
            _graphId,
            preparedQuery
        };

        var result = _db.Execute(Command.EXPLAIN, commandArgs, flags);

        if (result.Resp2Type != ResultType.Array)
        {
            return System.Array.Empty<string>();
        }

        return ((RedisResult[])result)
            .Select(x => (string)x)
            .ToArray();
    }

    /// <summary>
    /// Returns the execution plan for the given query asynchronously.
    /// </summary>
    public async Task<IReadOnlyList<string>> ExplainAsync(string query, IDictionary<string, object> parameters = null, CommandFlags flags = CommandFlags.None)
    {
        var preparedQuery = PrepareQuery(query, parameters);

        var commandArgs = new object[]
        {
            _graphId,
            preparedQuery
        };

        var result = await _db.ExecuteAsync(Command.EXPLAIN, commandArgs, flags);

        if (result.Resp2Type != ResultType.Array)
        {
            return System.Array.Empty<string>();
        }

        return ((RedisResult[])result)
            .Select(x => (string)x)
            .ToArray();
    }

    /// <summary>
    /// Profiles the execution of the given query.
    /// </summary>
    public IReadOnlyList<string> Profile(string query, IDictionary<string, object> parameters = null, CommandFlags flags = CommandFlags.None)
    {
        var preparedQuery = PrepareQuery(query, parameters);

        var commandArgs = new object[]
        {
            _graphId,
            preparedQuery
        };

        var result = _db.Execute(Command.PROFILE, commandArgs, flags);

        if (result.Resp2Type != ResultType.Array)
        {
            return System.Array.Empty<string>();
        }

        return ((RedisResult[])result)
            .Select(x => (string)x)
            .ToArray();
    }

    /// <summary>
    /// Profiles the execution of the given query asynchronously.
    /// </summary>
    public async Task<IReadOnlyList<string>> ProfileAsync(string query, IDictionary<string, object> parameters = null, CommandFlags flags = CommandFlags.None)
    {
        var preparedQuery = PrepareQuery(query, parameters);

        var commandArgs = new object[]
        {
            _graphId,
            preparedQuery
        };

        var result = await _db.ExecuteAsync(Command.PROFILE, commandArgs, flags);

        if (result.Resp2Type != ResultType.Array)
        {
            return System.Array.Empty<string>();
        }

        return ((RedisResult[])result)
            .Select(x => (string)x)
            .ToArray();
    }

    /// <summary>
    /// Returns the current slowlog entries for this graph.
    /// </summary>
    public IReadOnlyList<RedisResult> Slowlog(CommandFlags flags = CommandFlags.None)
    {
        var commandArgs = new object[]
        {
            _graphId
        };

        var result = _db.Execute(Command.SLOWLOG, commandArgs, flags);

        if (result.Resp2Type != ResultType.Array)
        {
            return System.Array.Empty<RedisResult>();
        }

        return (RedisResult[])result;
    }

    /// <summary>
    /// Returns the current slowlog entries for this graph asynchronously.
    /// </summary>
    public async Task<IReadOnlyList<RedisResult>> SlowlogAsync(CommandFlags flags = CommandFlags.None)
    {
        var commandArgs = new object[]
        {
            _graphId
        };

        var result = await _db.ExecuteAsync(Command.SLOWLOG, commandArgs, flags);

        if (result.Resp2Type != ResultType.Array)
        {
            return System.Array.Empty<RedisResult>();
        }

        return (RedisResult[])result;
    }

    /// <summary>
    /// Resets the slowlog for this graph.
    /// </summary>
    public void SlowlogReset(CommandFlags flags = CommandFlags.None)
    {
        var commandArgs = new object[]
        {
            _graphId,
            "RESET"
        };

        _db.Execute(Command.SLOWLOG, commandArgs, flags);
    }

    /// <summary>
    /// Resets the slowlog for this graph asynchronously.
    /// </summary>
    public Task SlowlogResetAsync(CommandFlags flags = CommandFlags.None)
    {
        var commandArgs = new object[]
        {
            _graphId,
            "RESET"
        };

        return _db.ExecuteAsync(Command.SLOWLOG, commandArgs, flags);
    }
}
