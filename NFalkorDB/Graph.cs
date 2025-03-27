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

        var commandArgs = new object[]
        {
                _graphId,
                preparedQuery,
                CompactQueryFlag
        };

        var rawResult = _db.Execute(Command.QUERY, commandArgs, flags);

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
    /// <returns>A result set.</returns>
    public async Task<ResultSet> QueryAsync(string query, IDictionary<string, object> parameters = null, CommandFlags flags = CommandFlags.None)
    {
        var preparedQuery = PrepareQuery(query, parameters);

        var commandArgs = new object[]
        {
                _graphId,
                preparedQuery,
                CompactQueryFlag
        };

        var rawResult = await _db.ExecuteAsync(Command.QUERY, commandArgs, flags);

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
    /// <returns>A result set.</returns>
    public ResultSet ReadOnlyQuery(string query, IDictionary<string, object> parms = null, CommandFlags flags = CommandFlags.None)
    {
        var preparedQuery = PrepareQuery(query, parms);

        var parameters = new object[]
        {
                _graphId,
                preparedQuery,
                CompactQueryFlag
        };

        var result = _db.Execute(Command.RO_QUERY, parameters, (flags | CommandFlags.PreferReplica));

        return new ResultSet(result, _cache);
    }

    /// <summary>
    /// Execute a Cypher query, preferring a read-only node.
    /// </summary>
    /// <param name="query">The Cypher query.</param>
    /// <param name="parms">Parameters map.</param>
    /// <param name="flags">Optional command flags. `PreferReplica` is set for you here.</param>
    /// <returns>A result set.</returns>
    public async Task<ResultSet> ReadOnlyQueryAsync(string query, IDictionary<string, object> parms = null, CommandFlags flags = CommandFlags.None)
    {
        var preparedQuery = PrepareQuery(query, parms);

        var parameters = new object[]
        {
                _graphId,
                preparedQuery,
                CompactQueryFlag
        };

        var result = await _db.ExecuteAsync(Command.RO_QUERY, parameters, (flags | CommandFlags.PreferReplica));

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
}