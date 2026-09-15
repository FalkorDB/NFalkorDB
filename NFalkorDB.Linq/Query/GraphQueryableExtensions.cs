using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using NFalkorDB.Linq.Translation;

namespace NFalkorDB.Linq;

/// <summary>
/// Graph specific query operators: relationship traversal, and access to the Cypher a query
/// translates to.
/// </summary>
public static class GraphQueryableExtensions
{
    private static readonly MethodInfo[] TraverseOverloads = typeof(GraphQueryableExtensions)
        .GetMethods(BindingFlags.Public | BindingFlags.Static)
        .Where(m => m.Name == nameof(Traverse))
        .ToArray();

    private static readonly MethodInfo TraverseDefinition = FindTraverse(hasPredicate: false, hasDirection: false);
    private static readonly MethodInfo TraverseDirectionDefinition = FindTraverse(hasPredicate: false, hasDirection: true);
    private static readonly MethodInfo TraversePredicateDefinition = FindTraverse(hasPredicate: true, hasDirection: false);
    private static readonly MethodInfo TraversePredicateDirectionDefinition = FindTraverse(hasPredicate: true, hasDirection: true);

    /// <summary>
    /// Follows an outgoing relationship of type <typeparamref name="TEdge"/> and continues the
    /// query from the <typeparamref name="TTarget"/> nodes at the far end, extending the MATCH
    /// pattern to <c>(n0:Source)-[r0:TYPE]-&gt;(n1:Target)</c>.
    /// </summary>
    /// <typeparam name="TEdge">A type annotated with <see cref="RelationshipAttribute"/>.</typeparam>
    /// <typeparam name="TTarget">The node type at the far end of the relationship.</typeparam>
    /// <param name="source">The query to extend.</param>
    /// <returns>A query over the nodes at the far end of the relationship.</returns>
    public static IQueryable<TTarget> Traverse<TEdge, TTarget>(this IQueryable source) =>
        Continue<TTarget>(source, TraverseDefinition, typeof(TEdge), typeof(TTarget));

    /// <summary>
    /// Follows a relationship of type <typeparamref name="TEdge"/> in the given direction and
    /// continues the query from the <typeparamref name="TTarget"/> nodes at the far end.
    /// </summary>
    /// <typeparam name="TEdge">A type annotated with <see cref="RelationshipAttribute"/>.</typeparam>
    /// <typeparam name="TTarget">The node type at the far end of the relationship.</typeparam>
    /// <param name="source">The query to extend.</param>
    /// <param name="direction">The direction to traverse in.</param>
    /// <returns>A query over the nodes at the far end of the relationship.</returns>
    public static IQueryable<TTarget> Traverse<TEdge, TTarget>(this IQueryable source, TraversalDirection direction) =>
        Continue<TTarget>(source, TraverseDirectionDefinition, typeof(TEdge), typeof(TTarget), Expression.Constant(direction));

    /// <summary>
    /// Follows an outgoing relationship of type <typeparamref name="TEdge"/> whose properties match
    /// <paramref name="edgePredicate"/>, and continues the query from the nodes at the far end.
    /// </summary>
    /// <typeparam name="TEdge">A type annotated with <see cref="RelationshipAttribute"/>.</typeparam>
    /// <typeparam name="TTarget">The node type at the far end of the relationship.</typeparam>
    /// <param name="source">The query to extend.</param>
    /// <param name="edgePredicate">A filter over the relationship's own properties.</param>
    /// <returns>A query over the nodes at the far end of the relationship.</returns>
    public static IQueryable<TTarget> Traverse<TEdge, TTarget>(this IQueryable source, Expression<Func<TEdge, bool>> edgePredicate) =>
        Continue<TTarget>(source, TraversePredicateDefinition, typeof(TEdge), typeof(TTarget), Require(edgePredicate, nameof(edgePredicate)));

    /// <summary>
    /// Follows a relationship of type <typeparamref name="TEdge"/> in the given direction, keeping
    /// only the relationships matching <paramref name="edgePredicate"/>.
    /// </summary>
    /// <typeparam name="TEdge">A type annotated with <see cref="RelationshipAttribute"/>.</typeparam>
    /// <typeparam name="TTarget">The node type at the far end of the relationship.</typeparam>
    /// <param name="source">The query to extend.</param>
    /// <param name="edgePredicate">A filter over the relationship's own properties.</param>
    /// <param name="direction">The direction to traverse in.</param>
    /// <returns>A query over the nodes at the far end of the relationship.</returns>
    public static IQueryable<TTarget> Traverse<TEdge, TTarget>(this IQueryable source, Expression<Func<TEdge, bool>> edgePredicate, TraversalDirection direction) =>
        Continue<TTarget>(source, TraversePredicateDirectionDefinition, typeof(TEdge), typeof(TTarget), Require(edgePredicate, nameof(edgePredicate)), Expression.Constant(direction));

    /// <summary>
    /// Translates the query to Cypher without running it.
    /// </summary>
    /// <param name="source">The query to translate.</param>
    /// <returns>The generated Cypher and its parameters.</returns>
    public static CypherQuery ToCypherQuery(this IQueryable source)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        return RequireProvider(source).TranslateToCypher(source.Expression);
    }

    /// <summary>
    /// Runs <c>GRAPH.EXPLAIN</c> for the query and returns the execution plan.
    /// </summary>
    /// <param name="source">The query to explain.</param>
    /// <returns>The plan, one line per entry.</returns>
    public static IReadOnlyList<string> Explain(this IQueryable source)
    {
        var provider = RequireProvider(source);
        var query = provider.TranslateToCypher(source.Expression);

        return provider.Graph.Explain(query.Cypher, ToParameterDictionary(query));
    }

    /// <summary>
    /// Runs <c>GRAPH.PROFILE</c> for the query and returns the profiled execution plan.
    /// </summary>
    /// <param name="source">The query to profile.</param>
    /// <returns>The profiled plan, one line per entry.</returns>
    public static IReadOnlyList<string> Profile(this IQueryable source)
    {
        var provider = RequireProvider(source);
        var query = provider.TranslateToCypher(source.Expression);

        return provider.Graph.Profile(query.Cypher, ToParameterDictionary(query));
    }

    private static IDictionary<string, object> ToParameterDictionary(CypherQuery query) =>
        query.Parameters.ToDictionary(p => p.Key, p => p.Value, StringComparer.Ordinal);

    internal static GraphQueryProvider RequireProvider(IQueryable source)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (source.Provider is GraphQueryProvider provider)
        {
            return provider;
        }

        throw new NotSupportedException(
            "This operator only works on a query created from a GraphContext.");
    }

    private static Expression Require<T>(Expression<T> expression, string name)
    {
        if (expression == null)
        {
            throw new ArgumentNullException(name);
        }

        // The argument slot is typed Expression<Func<...>>, so the lambda has to be quoted rather
        // than passed as a bare lambda node.
        return Expression.Quote(expression);
    }

    private static IQueryable<TTarget> Continue<TTarget>(
        IQueryable source,
        MethodInfo definition,
        Type edgeType,
        Type targetType,
        params Expression[] extraArguments)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        var arguments = new Expression[extraArguments.Length + 1];

        arguments[0] = source.Expression;

        Array.Copy(extraArguments, 0, arguments, 1, extraArguments.Length);

        var call = Expression.Call(null, definition.MakeGenericMethod(edgeType, targetType), arguments);

        return source.Provider.CreateQuery<TTarget>(call);
    }

    private static MethodInfo FindTraverse(bool hasPredicate, bool hasDirection) =>
        TraverseOverloads.Single(m =>
        {
            var parameters = m.GetParameters();

            var predicate = parameters.Any(p =>
                p.ParameterType.IsGenericType &&
                p.ParameterType.GetGenericTypeDefinition() == typeof(Expression<>));

            var direction = parameters.Any(p => p.ParameterType == typeof(TraversalDirection));

            return predicate == hasPredicate && direction == hasDirection;
        });
}
