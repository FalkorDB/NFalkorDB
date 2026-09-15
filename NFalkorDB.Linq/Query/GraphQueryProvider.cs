using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using NFalkorDB.Linq.Translation;

namespace NFalkorDB.Linq;

/// <summary>
/// Translates LINQ expression trees into Cypher and runs them against a <see cref="Graph"/>.
/// Every query the provider issues is a pure read, so it is sent with <c>GRAPH.RO_QUERY</c>.
/// </summary>
public class GraphQueryProvider : IQueryProvider
{
    private readonly Graph _graph;

    /// <summary>
    /// Creates a provider over a graph.
    /// </summary>
    /// <param name="graph">The graph to query.</param>
    public GraphQueryProvider(Graph graph) =>
        _graph = graph ?? throw new ArgumentNullException(nameof(graph));

    /// <summary>
    /// Creates a provider with no underlying graph. Only translation is available; any attempt to
    /// execute throws. Used by the translation tests.
    /// </summary>
    internal GraphQueryProvider()
    {
    }

    /// <summary>
    /// The graph the provider queries.
    /// </summary>
    public Graph Graph => _graph;

    /// <summary>
    /// Creates a queryable for <paramref name="expression"/>.
    /// </summary>
    /// <param name="expression">The composed expression tree.</param>
    /// <returns>A queryable over the expression.</returns>
    public IQueryable CreateQuery(Expression expression)
    {
        if (expression == null)
        {
            throw new ArgumentNullException(nameof(expression));
        }

        var elementType = GetElementType(expression.Type);

        try
        {
            return (IQueryable)Activator.CreateInstance(
                typeof(GraphQueryable<>).MakeGenericType(elementType),
                System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic,
                binder: null,
                args: new object[] { this, expression },
                culture: null);
        }
        catch (System.Reflection.TargetInvocationException ex) when (ex.InnerException != null)
        {
            throw ex.InnerException;
        }
    }

    /// <summary>
    /// Creates a queryable for <paramref name="expression"/>.
    /// </summary>
    /// <typeparam name="TElement">The element type of the new queryable.</typeparam>
    /// <param name="expression">The composed expression tree.</param>
    /// <returns>A queryable over the expression.</returns>
    public IQueryable<TElement> CreateQuery<TElement>(Expression expression) =>
        new GraphQueryable<TElement>(this, expression ?? throw new ArgumentNullException(nameof(expression)));

    /// <summary>
    /// Translates and runs <paramref name="expression"/>.
    /// </summary>
    /// <param name="expression">The expression tree to run.</param>
    /// <returns>The query result.</returns>
    public object Execute(Expression expression) =>
        ExecuteCompiled(QueryTranslator.Translate(expression));

    /// <summary>
    /// Translates and runs <paramref name="expression"/>.
    /// </summary>
    /// <typeparam name="TResult">The type the terminal operator produces.</typeparam>
    /// <param name="expression">The expression tree to run.</param>
    /// <returns>The query result.</returns>
    public TResult Execute<TResult>(Expression expression) =>
        (TResult)ExecuteCompiled(QueryTranslator.Translate(expression));

    internal async Task<TResult> ExecuteAsync<TResult>(Expression expression, TerminalOperator terminal, Type resultType, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var compiled = QueryTranslator.Translate(expression, terminal, resultType);
        var result = await ExecuteCompiledAsync(compiled, cancellationToken).ConfigureAwait(false);

        return (TResult)result;
    }

    internal CypherQuery TranslateToCypher(Expression expression)
    {
        var compiled = QueryTranslator.Translate(expression);

        return new CypherQuery(compiled.Cypher, compiled.Parameters);
    }

    internal virtual object ExecuteCompiled(CompiledQuery compiled)
    {
        var resultSet = RequireGraph().ReadOnlyQuery(compiled.Cypher, compiled.Parameters);

        return ResultAssembler.Assemble(compiled, resultSet);
    }

    internal virtual async Task<object> ExecuteCompiledAsync(CompiledQuery compiled, CancellationToken cancellationToken)
    {
        var resultSet = await RequireGraph()
            .ReadOnlyQueryAsync(compiled.Cypher, compiled.Parameters)
            .ConfigureAwait(false);

        cancellationToken.ThrowIfCancellationRequested();

        return ResultAssembler.Assemble(compiled, resultSet);
    }

    private Graph RequireGraph() =>
        _graph ?? throw new InvalidOperationException(
            "This GraphQueryProvider was created without a graph and can only translate queries, not run them.");

    private static Type GetElementType(Type queryableType)
    {
        if (queryableType.IsGenericType &&
            (queryableType.GetGenericTypeDefinition() == typeof(IQueryable<>) ||
             queryableType.GetGenericTypeDefinition() == typeof(IOrderedQueryable<>)))
        {
            return queryableType.GetGenericArguments()[0];
        }

        var queryable = queryableType.GetInterfaces()
            .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IQueryable<>));

        if (queryable != null)
        {
            return queryable.GetGenericArguments()[0];
        }

        throw new ArgumentException(
            $"'{queryableType.Name}' is not a queryable type.", nameof(queryableType));
    }
}
