using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using StackExchange.Redis;
using static NFalkorDB.FalkorDBUtilities;

namespace NFalkorDB;

public class Graph
{
    internal static readonly object CompactQueryFlag = "--COMPACT";

    string _graphId;
    IGraphCache _cache;
    private readonly IDatabase _db;

    public Graph(string graphId, IDatabase db)
    {
        _graphId = graphId;
        _cache = new GraphCache(graphId, this);
        _db = db;
    }

    /// <summary>
    /// Execute a Cypher query with parameters.
    /// </summary>
    /// <param name="graphId">A graph to perform the query on.</param>
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
    /// <param name="graphId">A graph to perform the query on.</param>
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
    /// <param name="graphId">A graph to perform the query on.</param>
    /// <param name="query">The Cypher query.</param>
    /// <param name="parameters">Parameters map.</param>
    /// <param name="flags">Optional command flags. `PreferReplica` is set for you here.</param>
    /// <returns>A result set.</returns>
    public ResultSet GraphReadOnlyQuery(string query, IDictionary<string, object> parameters, CommandFlags flags = CommandFlags.None)
    {
        var preparedQuery = PrepareQuery(query, parameters);

        return GraphReadOnlyQuery(preparedQuery, flags);
    }

    /// <summary>
    /// Execute a Cypher query, preferring a read-only node.
    /// </summary>
    /// <param name="graphId">A graph to perform the query on.</param>
    /// <param name="query">The Cypher query.</param>
    /// <param name="flags">Optional command flags. `PreferReplica` is set for you here.</param>
    /// <returns>A result set.</returns>
    public ResultSet GraphReadOnlyQuery(string query, CommandFlags flags = CommandFlags.None)
    {
        var parameters = new object[]
        {
                _graphId,
                query,
                CompactQueryFlag
        };

        var result = _db.Execute(Command.RO_QUERY, parameters, (flags | CommandFlags.PreferReplica));

        return new ResultSet(result, _cache);
    }

    /// <summary>
    /// Execute a Cypher query, preferring a read-only node.
    /// </summary>
    /// <param name="graphId">A graph to perform the query on.</param>
    /// <param name="query">The Cypher query.</param>
    /// <param name="parameters">Parameters map.</param>
    /// <param name="flags">Optional command flags. `PreferReplica` is set for you here.</param>
    /// <returns>A result set.</returns>
    public Task<ResultSet> GraphReadOnlyQueryAsync(string query, IDictionary<string, object> parameters, CommandFlags flags = CommandFlags.None)
    {
        var preparedQuery = PrepareQuery(query, parameters);

        return GraphReadOnlyQueryAsync(preparedQuery, flags);
    }

    /// <summary>
    /// Execute a Cypher query, preferring a read-only node.
    /// </summary>
    /// <param name="graphId">A graph to perform the query on.</param>
    /// <param name="query">The Cypher query.</param>
    /// <param name="flags">Optional command flags. `PreferReplica` is set for you here.</param>
    /// <returns>A result set.</returns>
    public async Task<ResultSet> GraphReadOnlyQueryAsync(string query, CommandFlags flags = CommandFlags.None)
    {
        var parameters = new object[]
        {
                _graphId,
                query,
                CompactQueryFlag
        };

        var result = await _db.ExecuteAsync(Command.RO_QUERY, parameters, (flags | CommandFlags.PreferReplica));

        return new ResultSet(result, _cache);
    }

    internal static readonly Dictionary<string, List<string>> EmptyKwargsDictionary =
        [];

    /// <summary>
    /// Call a saved procedure.
    /// </summary>
    /// <param name="graphId">The graph containing the saved procedure.</param>
    /// <param name="procedure">The procedure name.</param>
    /// <param name="flags">[Optional] Command flags that are to be sent to the StackExchange.Redis connection multiplexer...</param>/// 
    /// <returns>A result set.</returns>
    public ResultSet CallProcedure(string procedure, CommandFlags flags = CommandFlags.None) =>
        CallProcedure(procedure, Enumerable.Empty<string>(), EmptyKwargsDictionary, flags);

    /// <summary>
    /// Call a saved procedure.
    /// </summary>
    /// <param name="graphId">The graph containing the saved procedure.</param>
    /// <param name="procedure">The procedure name.</param>
    /// <param name="flags">[Optional] Command flags that are to be sent to the StackExchange.Redis connection multiplexer...</param>/// 
    /// <returns>A result set.</returns>
    public Task<ResultSet> CallProcedureAsync(string procedure, CommandFlags flags = CommandFlags.None) =>
        CallProcedureAsync(procedure, Enumerable.Empty<string>(), EmptyKwargsDictionary, flags);

    /// <summary>
    /// Call a saved procedure with parameters.
    /// </summary>
    /// <param name="graphId">The graph containing the saved procedure.</param>
    /// <param name="procedure">The procedure name.</param>
    /// <param name="args">A collection of positional arguments.</param>
    /// <param name="flags">[Optional] Command flags that are to be sent to the StackExchange.Redis connection multiplexer...</param>/// 
    /// <returns>A result set.</returns>
    public ResultSet CallProcedure(string procedure, IEnumerable<string> args, CommandFlags flags = CommandFlags.None) =>
        CallProcedure(procedure, args, EmptyKwargsDictionary, flags);

    /// <summary>
    /// Call a saved procedure with parameters.
    /// </summary>
    /// <param name="graphId">The graph containing the saved procedure.</param>
    /// <param name="procedure">The procedure name.</param>
    /// <param name="args">A collection of positional arguments.</param>
    /// <param name="flags">[Optional] Command flags that are to be sent to the StackExchange.Redis connection multiplexer...</param>/// 
    /// <returns>A result set.</returns>
    public Task<ResultSet> CallProcedureAsync(string procedure, IEnumerable<string> args, CommandFlags flags = CommandFlags.None) =>
        CallProcedureAsync(procedure, args, EmptyKwargsDictionary);

    /// <summary>
    /// Call a saved procedure with parameters.
    /// </summary>
    /// <param name="graphId">The graph containing the saved procedure.</param>
    /// <param name="procedure">The procedure name.</param>
    /// <param name="args">A collection of positional arguments.</param>
    /// <param name="kwargs">A collection of keyword arguments.</param>
    /// <param name="flags">[Optional] Command flags that are to be sent to the StackExchange.Redis connection multiplexer...</param>/// 
    /// <returns>A result set.</returns>
    public ResultSet CallProcedure(string procedure, IEnumerable<string> args, Dictionary<string, List<string>> kwargs, CommandFlags flags = CommandFlags.None)
    {
        args = args.Select(a => QuoteString(a));

        var queryBody = new StringBuilder();

        queryBody.Append($"CALL {procedure}({string.Join(",", args)})");

        if (kwargs.TryGetValue("y", out var kwargsList))
        {
            queryBody.Append(string.Join(",", kwargsList));
        }

        return Query(queryBody.ToString(), flags: flags);
    }

    /// <summary>
    /// Call a saved procedure with parameters.
    /// </summary>
    /// <param name="graphId">The graph containing the saved procedure.</param>
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
    /// Call a saved procedure against a read-only node.
    /// </summary>
    /// <param name="graphId">The graph containing the saved procedure.</param>
    /// <param name="procedure">The procedure name.</param>
    /// <param name="flags">[Optional] Command flags that are to be sent to the StackExchange.Redis connection multiplexer...</param>/// 
    /// <returns>A result set.</returns>
    public ResultSet CallProcedureReadOnly(string procedure, CommandFlags flags = CommandFlags.None) =>
        CallProcedureReadOnly(procedure, Enumerable.Empty<string>(), EmptyKwargsDictionary, flags);

    /// <summary>
    /// Call a saved procedure against a read-only node.
    /// </summary>
    /// <param name="graphId">The graph containing the saved procedure.</param>
    /// <param name="procedure">The procedure name.</param>
    /// <param name="flags">[Optional] Command flags that are to be sent to the StackExchange.Redis connection multiplexer...</param>/// 
    /// <returns>A result set.</returns>
    public Task<ResultSet> CallProcedureReadOnlyAsync(string procedure, CommandFlags flags = CommandFlags.None) =>
        CallProcedureReadOnlyAsync(procedure, Enumerable.Empty<string>(), EmptyKwargsDictionary, flags);

    /// <summary>
    /// Call a saved procedure with parameters against a read-only node.
    /// </summary>
    /// <param name="graphId">The graph containing the saved procedure.</param>
    /// <param name="procedure">The procedure name.</param>
    /// <param name="args">A collection of positional arguments.</param>
    /// <param name="flags">[Optional] Command flags that are to be sent to the StackExchange.Redis connection multiplexer...</param>/// 
    /// <returns>A result set.</returns>
    public ResultSet CallProcedureReadOnly(string procedure, IEnumerable<string> args, CommandFlags flags = CommandFlags.None) =>
        CallProcedureReadOnly(procedure, args, EmptyKwargsDictionary, flags);

    /// <summary>
    /// Call a saved procedure with parameters against a read-only node.
    /// </summary>
    /// <param name="graphId">The graph containing the saved procedure.</param>
    /// <param name="procedure">The procedure name.</param>
    /// <param name="args">A collection of positional arguments.</param>
    /// <param name="flags">[Optional] Command flags that are to be sent to the StackExchange.Redis connection multiplexer...</param>/// 
    /// <returns>A result set.</returns>
    public Task<ResultSet> CallProcedureReadOnlyAsync(string procedure, IEnumerable<string> args, CommandFlags flags = CommandFlags.None) =>
        CallProcedureReadOnlyAsync(procedure, args, EmptyKwargsDictionary, flags);

    /// <summary>
    /// Call a saved procedure with parameters against a read-only node.
    /// </summary>
    /// <param name="graphId">The graph containing the saved procedure.</param>
    /// <param name="procedure">The procedure name.</param>
    /// <param name="args">A collection of positional arguments.</param>
    /// <param name="kwargs">A collection of keyword arguments.</param>
    /// <param name="flags">[Optional] Command flags that are to be sent to the StackExchange.Redis connection multiplexer...</param>/// 
    /// <returns>A result set.</returns>
    public ResultSet CallProcedureReadOnly(string procedure, IEnumerable<string> args, Dictionary<string, List<string>> kwargs, CommandFlags flags = CommandFlags.None)
    {
        args = args.Select(a => QuoteString(a));

        var queryBody = new StringBuilder();

        queryBody.Append($"CALL {procedure}({string.Join(",", args)})");

        if (kwargs.TryGetValue("y", out var kwargsList))
        {
            queryBody.Append(string.Join(",", kwargsList));
        }

        return GraphReadOnlyQuery(queryBody.ToString(), flags);
    }

    /// <summary>
    /// Call a saved procedure with parameters against a read-only node.
    /// </summary>
    /// <param name="graphId">The graph containing the saved procedure.</param>
    /// <param name="procedure">The procedure name.</param>
    /// <param name="args">A collection of positional arguments.</param>
    /// <param name="kwargs">A collection of keyword arguments.</param>
    /// <param name="flags">[Optional] Command flags that are to be sent to the StackExchange.Redis connection multiplexer...</param>/// 
    /// <returns>A result set.</returns>
    public Task<ResultSet> CallProcedureReadOnlyAsync(string procedure, IEnumerable<string> args, Dictionary<string, List<string>> kwargs, CommandFlags flags = CommandFlags.None)
    {
        args = args.Select(a => QuoteString(a));

        var queryBody = new StringBuilder();

        queryBody.Append($"CALL {procedure}({string.Join(",", args)})");

        if (kwargs.TryGetValue("y", out var kwargsList))
        {
            queryBody.Append(string.Join(",", kwargsList));
        }

        return GraphReadOnlyQueryAsync(queryBody.ToString(), flags);
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