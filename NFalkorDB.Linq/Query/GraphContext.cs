using System;
using System.Linq;
using NFalkorDB.Linq.Mapping;

namespace NFalkorDB.Linq;

/// <summary>
/// The entry point for LINQ queries against a FalkorDB graph.
/// <para>
/// A context is a thin, stateless wrapper around a <see cref="Graph"/>: it does not track changes
/// and holds no per-entity state, so it is cheap to create and safe to keep for the lifetime of the
/// underlying connection.
/// </para>
/// </summary>
/// <example>
/// <code>
/// var context = new GraphContext(new FalkorDB("localhost").SelectGraph("social"));
///
/// var adults = context.Nodes&lt;Person&gt;()
///     .Where(p =&gt; p.Age &gt;= 18)
///     .OrderBy(p =&gt; p.Name)
///     .Take(10)
///     .ToList();
/// </code>
/// </example>
public sealed class GraphContext
{
    /// <summary>
    /// Creates a context over an existing graph.
    /// </summary>
    /// <param name="graph">The graph to query.</param>
    public GraphContext(Graph graph)
    {
        Graph = graph ?? throw new ArgumentNullException(nameof(graph));
        Provider = new GraphQueryProvider(Graph);
    }

    /// <summary>
    /// Creates a context over a graph selected from a client.
    /// </summary>
    /// <param name="client">The FalkorDB client.</param>
    /// <param name="graphId">The key the graph is stored under.</param>
    public GraphContext(FalkorDB client, string graphId)
        : this(SelectGraph(client, graphId))
    {
    }

    private static Graph SelectGraph(FalkorDB client, string graphId)
    {
        if (client == null)
        {
            throw new ArgumentNullException(nameof(client));
        }

        if (string.IsNullOrEmpty(graphId))
        {
            throw new ArgumentException("A graph id is required.", nameof(graphId));
        }

        return client.SelectGraph(graphId);
    }

    /// <summary>
    /// The graph this context queries.
    /// </summary>
    public Graph Graph { get; }

    /// <summary>
    /// The provider that translates and runs queries built from this context.
    /// </summary>
    public GraphQueryProvider Provider { get; }

    /// <summary>
    /// Starts a query over the nodes mapped to <typeparamref name="T"/>, which becomes
    /// <c>MATCH (n0:Label)</c>.
    /// </summary>
    /// <typeparam name="T">A type mapped to a node, either by <see cref="NodeAttribute"/> or by
    /// convention on the CLR type name.</typeparam>
    /// <returns>A queryable over the matching nodes.</returns>
    public IQueryable<T> Nodes<T>() where T : class, new()
    {
        var metadata = EntityMetadataCache.Get<T>();

        if (metadata.Kind != EntityKind.Node)
        {
            throw new GraphMappingException(
                $"'{typeof(T).Name}' is annotated with [Relationship], so query it with Relationships<{typeof(T).Name}>().");
        }

        return new GraphQueryable<T>(Provider);
    }

    /// <summary>
    /// Starts a query over the relationships mapped to <typeparamref name="T"/>, which becomes
    /// <c>MATCH ()-[r0:TYPE]-&gt;()</c>.
    /// </summary>
    /// <typeparam name="T">A type annotated with <see cref="RelationshipAttribute"/>.</typeparam>
    /// <returns>A queryable over the matching relationships.</returns>
    public IQueryable<T> Relationships<T>() where T : class, new()
    {
        var metadata = EntityMetadataCache.Get<T>();

        if (metadata.Kind != EntityKind.Relationship)
        {
            throw new GraphMappingException(
                $"'{typeof(T).Name}' is not annotated with [Relationship], so query it with Nodes<{typeof(T).Name}>().");
        }

        return new GraphQueryable<T>(Provider);
    }
}
