using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace NFalkorDB.Linq;

/// <summary>
/// An <see cref="IQueryable{T}"/> over a FalkorDB graph. Building on the queryable composes an
/// expression tree; the tree is translated to Cypher and sent to the server only when the query is
/// enumerated or a terminal operator runs.
/// </summary>
/// <typeparam name="T">The element type of the query.</typeparam>
public sealed class GraphQueryable<T> : IOrderedQueryable<T>
{
    internal GraphQueryable(GraphQueryProvider provider)
    {
        Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        Expression = Expression.Constant(this);
    }

    internal GraphQueryable(GraphQueryProvider provider, Expression expression)
    {
        Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        Expression = expression ?? throw new ArgumentNullException(nameof(expression));
    }

    /// <summary>
    /// The element type produced by the query.
    /// </summary>
    public Type ElementType => typeof(T);

    /// <summary>
    /// The expression tree built so far.
    /// </summary>
    public Expression Expression { get; }

    /// <summary>
    /// The provider that translates and runs the query.
    /// </summary>
    public IQueryProvider Provider { get; }

    /// <summary>
    /// Translates the query to Cypher, runs it, and enumerates the materialized results.
    /// </summary>
    /// <returns>An enumerator over the results.</returns>
    public IEnumerator<T> GetEnumerator() =>
        Provider.Execute<IEnumerable<T>>(Expression).GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    /// <summary>
    /// Returns the Cypher this query translates to. Useful when debugging a query.
    /// </summary>
    /// <returns>The generated Cypher.</returns>
    public override string ToString() => this.ToCypherQuery().ToString();
}
