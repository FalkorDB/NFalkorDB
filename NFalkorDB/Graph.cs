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

    RedisKey _graphId;
    IGraphCache _cache;
    private readonly IDatabase _db;

    /// <summary>
    /// Create a new graph instance.
    /// </summary>
    /// <param name="graphId"></param>
    /// <param name="db"></param>
    internal Graph(string graphId, IDatabase db)
    {
        _graphId = new RedisKey(graphId);
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

        try
        {
            var rawResult = _db.Execute(Command.QUERY, commandArgs.ToArray(), flags);

            if (flags.HasFlag(CommandFlags.FireAndForget))
            {
                return default;
            }

            return new ResultSet(rawResult, _cache);
        }
        catch (SchemaVersionMismatchException)
        {
            // refresh schema cache and retry once
            if (_cache is BaseGraphCache baseCache)
            {
                baseCache.Refresh();
            }

            var rawResult = _db.Execute(Command.QUERY, commandArgs.ToArray(), flags);

            if (flags.HasFlag(CommandFlags.FireAndForget))
            {
                return default;
            }

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

        try
        {
            var rawResult = await _db.ExecuteAsync(Command.QUERY, commandArgs.ToArray(), flags);

            if (flags.HasFlag(CommandFlags.FireAndForget))
            {
                return default;
            }

            return new ResultSet(rawResult, _cache);
        }
        catch (SchemaVersionMismatchException)
        {
            if (_cache is BaseGraphCache baseCache)
            {
                baseCache.Refresh();
            }

            var rawResult = await _db.ExecuteAsync(Command.QUERY, commandArgs.ToArray(), flags);

            if (flags.HasFlag(CommandFlags.FireAndForget))
            {
                return default;
            }

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

        try
        {
            var result = _db.Execute(Command.RO_QUERY, parameters.ToArray(), (flags | CommandFlags.PreferReplica));
            return new ResultSet(result, _cache);
        }
        catch (SchemaVersionMismatchException)
        {
            if (_cache is BaseGraphCache baseCache)
            {
                baseCache.Refresh();
            }

            var result = _db.Execute(Command.RO_QUERY, parameters.ToArray(), (flags | CommandFlags.PreferReplica));
            return new ResultSet(result, _cache);
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

        try
        {
            var result = await _db.ExecuteAsync(Command.RO_QUERY, parameters.ToArray(), (flags | CommandFlags.PreferReplica));
            return new ResultSet(result, _cache);
        }
        catch (SchemaVersionMismatchException)
        {
            if (_cache is BaseGraphCache baseCache)
            {
                baseCache.Refresh();
            }

            var result = await _db.ExecuteAsync(Command.RO_QUERY, parameters.ToArray(), (flags | CommandFlags.PreferReplica));
            return new ResultSet(result, _cache);
        }
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
    // Index helpers

    private ResultSet CreateTypedIndex(string idxType, string entityType, string labelOrRelation, IEnumerable<string> properties, IDictionary<string, object> options = null)
    {
        if (string.IsNullOrWhiteSpace(labelOrRelation))
        {
            throw new System.ArgumentException("Label or relation must be provided.", nameof(labelOrRelation));
        }

        if (properties == null)
        {
            throw new System.ArgumentNullException(nameof(properties));
        }

        var propsArray = properties as string[] ?? properties.ToArray();
        if (propsArray.Length == 0)
        {
            throw new System.ArgumentException("At least one property must be provided.", nameof(properties));
        }

        string pattern = entityType == "NODE"
            ? $"(e:{labelOrRelation})"
            : entityType == "EDGE" ? $"()-[e:{labelOrRelation}]->()" : throw new System.ArgumentException("Invalid entity type", nameof(entityType));

        if (idxType == "RANGE")
        {
            idxType = string.Empty;
        }

        var sb = new StringBuilder();
        sb.Append("CREATE ");
        if (!string.IsNullOrEmpty(idxType))
        {
            sb.Append(idxType).Append(' ');
        }

        sb.Append("INDEX FOR ")
          .Append(pattern)
          .Append(" ON (")
          .Append(string.Join(",", propsArray.Select(p => $"e.{p}")))
          .Append(')');

        if (options != null && options.Count > 0)
        {
            var optionParts = new List<string>();
            foreach (var kvp in options)
            {
                if (kvp.Value is string s)
                {
                    optionParts.Add($"{kvp.Key}:'{s}'");
                }
                else
                {
                    optionParts.Add($"{kvp.Key}:{kvp.Value}");
                }
            }

            sb.Append(" OPTIONS {")
              .Append(string.Join(",", optionParts))
              .Append('}');
        }

        return Query(sb.ToString());
    }

    private ResultSet DropIndex(string idxType, string entityType, string labelOrRelation, string attribute)
    {
        if (string.IsNullOrWhiteSpace(labelOrRelation))
        {
            throw new System.ArgumentException("Label or relation must be provided.", nameof(labelOrRelation));
        }

        if (string.IsNullOrWhiteSpace(attribute))
        {
            throw new System.ArgumentException("Attribute must be provided.", nameof(attribute));
        }

        string pattern = entityType == "NODE"
            ? $"(e:{labelOrRelation})"
            : entityType == "EDGE" ? $"()-[e:{labelOrRelation}]->()" : throw new System.ArgumentException("Invalid entity type", nameof(entityType));

        string query;
        switch (idxType)
        {
            case "RANGE":
                query = $"DROP INDEX FOR {pattern} ON (e.{attribute})";
                break;
            case "FULLTEXT":
                query = $"DROP FULLTEXT INDEX FOR {pattern} ON (e.{attribute})";
                break;
            case "VECTOR":
                query = $"DROP VECTOR INDEX FOR {pattern} ON (e.{attribute})";
                break;
            default:
                throw new System.ArgumentException("Invalid index type", nameof(idxType));
        }

        return Query(query);
    }

    public ResultSet CreateNodeRangeIndex(string label, params string[] properties) =>
        CreateTypedIndex("RANGE", "NODE", label, properties);

    public ResultSet CreateNodeFulltextIndex(string label, params string[] properties) =>
        CreateTypedIndex("FULLTEXT", "NODE", label, properties);

    public ResultSet CreateNodeVectorIndex(string label, int dimension, string similarityFunction = "euclidean", params string[] properties)
    {
        var options = new Dictionary<string, object>
        {
            ["dimension"] = dimension,
            ["similarityFunction"] = similarityFunction
        };

        return CreateTypedIndex("VECTOR", "NODE", label, properties, options);
    }

    public ResultSet CreateEdgeRangeIndex(string relation, params string[] properties) =>
        CreateTypedIndex("RANGE", "EDGE", relation, properties);

    public ResultSet CreateEdgeFulltextIndex(string relation, params string[] properties) =>
        CreateTypedIndex("FULLTEXT", "EDGE", relation, properties);

    public ResultSet CreateEdgeVectorIndex(string relation, int dimension, string similarityFunction = "euclidean", params string[] properties)
    {
        var options = new Dictionary<string, object>
        {
            ["dimension"] = dimension,
            ["similarityFunction"] = similarityFunction
        };

        return CreateTypedIndex("VECTOR", "EDGE", relation, properties, options);
    }

    public ResultSet DropNodeRangeIndex(string label, string attribute) =>
        DropIndex("RANGE", "NODE", label, attribute);

    public ResultSet DropNodeFulltextIndex(string label, string attribute) =>
        DropIndex("FULLTEXT", "NODE", label, attribute);

    public ResultSet DropNodeVectorIndex(string label, string attribute) =>
        DropIndex("VECTOR", "NODE", label, attribute);

    public ResultSet DropEdgeRangeIndex(string relation, string attribute) =>
        DropIndex("RANGE", "EDGE", relation, attribute);

    public ResultSet DropEdgeFulltextIndex(string relation, string attribute) =>
        DropIndex("FULLTEXT", "EDGE", relation, attribute);

    public ResultSet DropEdgeVectorIndex(string relation, string attribute) =>
        DropIndex("VECTOR", "EDGE", relation, attribute);

    /// <summary>
    /// Lists graph indices using the db.indexes procedure.
    /// </summary>
    public ResultSet ListIndices(CommandFlags flags = CommandFlags.None) =>
        CallProcedure("db.indexes", flags: flags);

    /// <summary>
    /// Lists graph indices using the db.indexes procedure asynchronously.
    /// </summary>
    public Task<ResultSet> ListIndicesAsync(CommandFlags flags = CommandFlags.None) =>
        CallProcedureAsync("db.indexes", null, null, flags);

    // Constraint helpers

    private ResultSet CreateConstraint(string constraintType, string entityType, string labelOrRelation, params string[] properties)
    {
        if (string.IsNullOrWhiteSpace(constraintType))
        {
            throw new System.ArgumentException("Constraint type must be provided.", nameof(constraintType));
        }

        if (string.IsNullOrWhiteSpace(entityType))
        {
            throw new System.ArgumentException("Entity type must be provided.", nameof(entityType));
        }

        if (string.IsNullOrWhiteSpace(labelOrRelation))
        {
            throw new System.ArgumentException("Label or relation must be provided.", nameof(labelOrRelation));
        }

        if (properties == null || properties.Length == 0)
        {
            throw new System.ArgumentException("At least one property must be provided.", nameof(properties));
        }

        var args = new List<object>
        {
            Command.CONSTRAINT,
            "CREATE",
            _graphId,
            constraintType,
            entityType,
            labelOrRelation,
            "PROPERTIES",
            properties.Length
        };

        args.AddRange(properties);

        var result = _db.Execute((string)args[0], args.Skip(1).ToArray());
        return new ResultSet(result, null);
    }

    private ResultSet DropConstraint(string constraintType, string entityType, string labelOrRelation, params string[] properties)
    {
        if (string.IsNullOrWhiteSpace(constraintType))
        {
            throw new System.ArgumentException("Constraint type must be provided.", nameof(constraintType));
        }

        if (string.IsNullOrWhiteSpace(entityType))
        {
            throw new System.ArgumentException("Entity type must be provided.", nameof(entityType));
        }

        if (string.IsNullOrWhiteSpace(labelOrRelation))
        {
            throw new System.ArgumentException("Label or relation must be provided.", nameof(labelOrRelation));
        }

        if (properties == null || properties.Length == 0)
        {
            throw new System.ArgumentException("At least one property must be provided.", nameof(properties));
        }

        var args = new List<object>
        {
            Command.CONSTRAINT,
            "DROP",
            _graphId,
            constraintType,
            entityType,
            labelOrRelation,
            "PROPERTIES",
            properties.Length
        };

        args.AddRange(properties);

        var result = _db.Execute((string)args[0], args.Skip(1).ToArray());
        return new ResultSet(result, null);
    }

    public ResultSet CreateNodeUniqueConstraint(string label, params string[] properties)
    {
        // ensure supporting range index exists (ignore errors if already present)
        try
        {
            CreateNodeRangeIndex(label, properties);
        }
        catch
        {
            // ignore index creation errors
        }

        return CreateConstraint("UNIQUE", "NODE", label, properties);
    }

    public ResultSet CreateEdgeUniqueConstraint(string relation, params string[] properties)
    {
        try
        {
            CreateEdgeRangeIndex(relation, properties);
        }
        catch
        {
        }

        return CreateConstraint("UNIQUE", "RELATIONSHIP", relation, properties);
    }

    public ResultSet CreateNodeMandatoryConstraint(string label, params string[] properties) =>
        CreateConstraint("MANDATORY", "NODE", label, properties);

    public ResultSet CreateEdgeMandatoryConstraint(string relation, params string[] properties) =>
        CreateConstraint("MANDATORY", "RELATIONSHIP", relation, properties);

    public ResultSet DropNodeUniqueConstraint(string label, params string[] properties) =>
        DropConstraint("UNIQUE", "NODE", label, properties);

    public ResultSet DropEdgeUniqueConstraint(string relation, params string[] properties) =>
        DropConstraint("UNIQUE", "RELATIONSHIP", relation, properties);

    public ResultSet DropNodeMandatoryConstraint(string label, params string[] properties) =>
        DropConstraint("MANDATORY", "NODE", label, properties);

    public ResultSet DropEdgeMandatoryConstraint(string relation, params string[] properties) =>
        DropConstraint("MANDATORY", "RELATIONSHIP", relation, properties);

    /// <summary>
    /// Lists graph constraints using the DB.CONSTRAINTS procedure.
    /// </summary>
    public ResultSet ListConstraints(CommandFlags flags = CommandFlags.None) =>
        CallProcedure("DB.CONSTRAINTS", flags: flags);

    /// <summary>
    /// Lists graph constraints using the DB.CONSTRAINTS procedure asynchronously.
    /// </summary>
    public Task<ResultSet> ListConstraintsAsync(CommandFlags flags = CommandFlags.None) =>
        CallProcedureAsync("DB.CONSTRAINTS", null, null, flags);

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
}
