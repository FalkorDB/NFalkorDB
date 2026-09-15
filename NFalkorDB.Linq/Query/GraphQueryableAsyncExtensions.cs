using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using NFalkorDB.Linq.Translation;

namespace NFalkorDB.Linq;

/// <summary>
/// Asynchronous terminal operators for FalkorDB LINQ queries. Each one translates the query in the
/// same way as its synchronous <see cref="Queryable"/> counterpart and sends it with
/// <c>GRAPH.RO_QUERY</c>.
/// <para>
/// FalkorDB has no way to abort a query that is already in flight, so the cancellation token is
/// observed before the command is sent and again once the reply arrives.
/// </para>
/// </summary>
public static class GraphQueryableAsyncExtensions
{
    /// <summary>Runs the query and returns every result.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The query to run.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The materialized results.</returns>
    public static async Task<List<T>> ToListAsync<T>(this IQueryable<T> source, CancellationToken cancellationToken = default)
    {
        var results = await Execute<T, IEnumerable<T>>(source, TerminalOperator.Sequence, typeof(T), cancellationToken).ConfigureAwait(false);

        return results as List<T> ?? results.ToList();
    }

    /// <summary>Runs the query and returns every result as an array.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The query to run.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The materialized results.</returns>
    public static async Task<T[]> ToArrayAsync<T>(this IQueryable<T> source, CancellationToken cancellationToken = default) =>
        (await source.ToListAsync(cancellationToken).ConfigureAwait(false)).ToArray();

    /// <summary>Returns the first result, throwing when the query matched nothing.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The query to run.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The first result.</returns>
    public static Task<T> FirstAsync<T>(this IQueryable<T> source, CancellationToken cancellationToken = default) =>
        Execute<T, T>(source, TerminalOperator.First, typeof(T), cancellationToken);

    /// <summary>Returns the first result matching the predicate, throwing when there is none.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The query to run.</param>
    /// <param name="predicate">An additional filter.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The first matching result.</returns>
    public static Task<T> FirstAsync<T>(this IQueryable<T> source, Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default) =>
        FirstAsync(Filter(source, predicate), cancellationToken);

    /// <summary>Returns the first result, or the element default when the query matched nothing.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The query to run.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The first result or the element default.</returns>
    public static Task<T> FirstOrDefaultAsync<T>(this IQueryable<T> source, CancellationToken cancellationToken = default) =>
        Execute<T, T>(source, TerminalOperator.FirstOrDefault, typeof(T), cancellationToken);

    /// <summary>Returns the first result matching the predicate, or the element default.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The query to run.</param>
    /// <param name="predicate">An additional filter.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The first matching result or the element default.</returns>
    public static Task<T> FirstOrDefaultAsync<T>(this IQueryable<T> source, Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default) =>
        FirstOrDefaultAsync(Filter(source, predicate), cancellationToken);

    /// <summary>Returns the only result, throwing when there is not exactly one.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The query to run.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The only result.</returns>
    public static Task<T> SingleAsync<T>(this IQueryable<T> source, CancellationToken cancellationToken = default) =>
        Execute<T, T>(source, TerminalOperator.Single, typeof(T), cancellationToken);

    /// <summary>Returns the only result matching the predicate, throwing when there is not exactly one.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The query to run.</param>
    /// <param name="predicate">An additional filter.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The only matching result.</returns>
    public static Task<T> SingleAsync<T>(this IQueryable<T> source, Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default) =>
        SingleAsync(Filter(source, predicate), cancellationToken);

    /// <summary>Returns the only result, or the element default when there is none.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The query to run.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The only result or the element default.</returns>
    public static Task<T> SingleOrDefaultAsync<T>(this IQueryable<T> source, CancellationToken cancellationToken = default) =>
        Execute<T, T>(source, TerminalOperator.SingleOrDefault, typeof(T), cancellationToken);

    /// <summary>Returns the only result matching the predicate, or the element default.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The query to run.</param>
    /// <param name="predicate">An additional filter.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The only matching result or the element default.</returns>
    public static Task<T> SingleOrDefaultAsync<T>(this IQueryable<T> source, Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default) =>
        SingleOrDefaultAsync(Filter(source, predicate), cancellationToken);

    /// <summary>Returns how many rows the query matched.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The query to run.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The row count.</returns>
    public static Task<int> CountAsync<T>(this IQueryable<T> source, CancellationToken cancellationToken = default) =>
        Execute<T, int>(source, TerminalOperator.Count, typeof(int), cancellationToken);

    /// <summary>Returns how many rows match the predicate.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The query to run.</param>
    /// <param name="predicate">An additional filter.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The row count.</returns>
    public static Task<int> CountAsync<T>(this IQueryable<T> source, Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default) =>
        CountAsync(Filter(source, predicate), cancellationToken);

    /// <summary>Returns how many rows the query matched, as a <see cref="long"/>.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The query to run.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The row count.</returns>
    public static Task<long> LongCountAsync<T>(this IQueryable<T> source, CancellationToken cancellationToken = default) =>
        Execute<T, long>(source, TerminalOperator.LongCount, typeof(long), cancellationToken);

    /// <summary>Returns how many rows match the predicate, as a <see cref="long"/>.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The query to run.</param>
    /// <param name="predicate">An additional filter.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The row count.</returns>
    public static Task<long> LongCountAsync<T>(this IQueryable<T> source, Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default) =>
        LongCountAsync(Filter(source, predicate), cancellationToken);

    /// <summary>Returns whether the query matched anything.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The query to run.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns><c>true</c> when at least one row matched.</returns>
    public static Task<bool> AnyAsync<T>(this IQueryable<T> source, CancellationToken cancellationToken = default) =>
        Execute<T, bool>(source, TerminalOperator.Any, typeof(bool), cancellationToken);

    /// <summary>Returns whether any row matches the predicate.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The query to run.</param>
    /// <param name="predicate">An additional filter.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns><c>true</c> when at least one row matched.</returns>
    public static Task<bool> AnyAsync<T>(this IQueryable<T> source, Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default) =>
        AnyAsync(Filter(source, predicate), cancellationToken);

    /// <summary>Returns whether every matched row satisfies the predicate.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The query to run.</param>
    /// <param name="predicate">The predicate every row must satisfy.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns><c>true</c> when no row violates the predicate.</returns>
    public static Task<bool> AllAsync<T>(this IQueryable<T> source, Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default)
    {
        if (predicate == null)
        {
            throw new ArgumentNullException(nameof(predicate));
        }

        var negated = Expression.Lambda<Func<T, bool>>(Expression.Not(predicate.Body), predicate.Parameters);

        return Execute<T, bool>(Filter(source, negated), TerminalOperator.None, typeof(bool), cancellationToken);
    }

    /// <summary>Sums the projected column.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The query to run.</param>
    /// <param name="selector">The column to sum.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The sum.</returns>
    public static Task<int> SumAsync<T>(this IQueryable<T> source, Expression<Func<T, int>> selector, CancellationToken cancellationToken = default) =>
        Execute<int, int>(Project(source, selector), TerminalOperator.Sum, typeof(int), cancellationToken);

    /// <summary>Sums the projected column.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The query to run.</param>
    /// <param name="selector">The column to sum.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The sum.</returns>
    public static Task<long> SumAsync<T>(this IQueryable<T> source, Expression<Func<T, long>> selector, CancellationToken cancellationToken = default) =>
        Execute<long, long>(Project(source, selector), TerminalOperator.Sum, typeof(long), cancellationToken);

    /// <summary>Sums the projected column.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The query to run.</param>
    /// <param name="selector">The column to sum.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The sum.</returns>
    public static Task<double> SumAsync<T>(this IQueryable<T> source, Expression<Func<T, double>> selector, CancellationToken cancellationToken = default) =>
        Execute<double, double>(Project(source, selector), TerminalOperator.Sum, typeof(double), cancellationToken);

    /// <summary>Sums the projected column.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The query to run.</param>
    /// <param name="selector">The column to sum.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The sum.</returns>
    public static Task<decimal> SumAsync<T>(this IQueryable<T> source, Expression<Func<T, decimal>> selector, CancellationToken cancellationToken = default) =>
        Execute<decimal, decimal>(Project(source, selector), TerminalOperator.Sum, typeof(decimal), cancellationToken);

    /// <summary>Returns the smallest projected value.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <typeparam name="TResult">The projected type.</typeparam>
    /// <param name="source">The query to run.</param>
    /// <param name="selector">The column to take the minimum of.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The minimum.</returns>
    public static Task<TResult> MinAsync<T, TResult>(this IQueryable<T> source, Expression<Func<T, TResult>> selector, CancellationToken cancellationToken = default) =>
        Execute<TResult, TResult>(Project(source, selector), TerminalOperator.Min, typeof(TResult), cancellationToken);

    /// <summary>Returns the smallest value of an already projected query.</summary>
    /// <typeparam name="T">The projected type.</typeparam>
    /// <param name="source">The query to run.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The minimum.</returns>
    public static Task<T> MinAsync<T>(this IQueryable<T> source, CancellationToken cancellationToken = default) =>
        Execute<T, T>(source, TerminalOperator.Min, typeof(T), cancellationToken);

    /// <summary>Returns the largest projected value.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <typeparam name="TResult">The projected type.</typeparam>
    /// <param name="source">The query to run.</param>
    /// <param name="selector">The column to take the maximum of.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The maximum.</returns>
    public static Task<TResult> MaxAsync<T, TResult>(this IQueryable<T> source, Expression<Func<T, TResult>> selector, CancellationToken cancellationToken = default) =>
        Execute<TResult, TResult>(Project(source, selector), TerminalOperator.Max, typeof(TResult), cancellationToken);

    /// <summary>Returns the largest value of an already projected query.</summary>
    /// <typeparam name="T">The projected type.</typeparam>
    /// <param name="source">The query to run.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The maximum.</returns>
    public static Task<T> MaxAsync<T>(this IQueryable<T> source, CancellationToken cancellationToken = default) =>
        Execute<T, T>(source, TerminalOperator.Max, typeof(T), cancellationToken);

    /// <summary>Averages the projected column.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The query to run.</param>
    /// <param name="selector">The column to average.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The mean.</returns>
    public static Task<double> AverageAsync<T>(this IQueryable<T> source, Expression<Func<T, int>> selector, CancellationToken cancellationToken = default) =>
        Execute<int, double>(Project(source, selector), TerminalOperator.Average, typeof(double), cancellationToken);

    /// <summary>Averages the projected column.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The query to run.</param>
    /// <param name="selector">The column to average.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The mean.</returns>
    public static Task<double> AverageAsync<T>(this IQueryable<T> source, Expression<Func<T, long>> selector, CancellationToken cancellationToken = default) =>
        Execute<long, double>(Project(source, selector), TerminalOperator.Average, typeof(double), cancellationToken);

    /// <summary>Averages the projected column.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The query to run.</param>
    /// <param name="selector">The column to average.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The mean.</returns>
    public static Task<double> AverageAsync<T>(this IQueryable<T> source, Expression<Func<T, double>> selector, CancellationToken cancellationToken = default) =>
        Execute<double, double>(Project(source, selector), TerminalOperator.Average, typeof(double), cancellationToken);

    /// <summary>Averages the projected column.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The query to run.</param>
    /// <param name="selector">The column to average.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The mean.</returns>
    public static Task<decimal> AverageAsync<T>(this IQueryable<T> source, Expression<Func<T, decimal>> selector, CancellationToken cancellationToken = default) =>
        Execute<decimal, decimal>(Project(source, selector), TerminalOperator.Average, typeof(decimal), cancellationToken);

    private static IQueryable<T> Filter<T>(IQueryable<T> source, Expression<Func<T, bool>> predicate)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (predicate == null)
        {
            throw new ArgumentNullException(nameof(predicate));
        }

        return source.Where(predicate);
    }

    private static IQueryable<TResult> Project<T, TResult>(IQueryable<T> source, Expression<Func<T, TResult>> selector)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (selector == null)
        {
            throw new ArgumentNullException(nameof(selector));
        }

        return source.Select(selector);
    }

    private static Task<TResult> Execute<TElement, TResult>(
        IQueryable<TElement> source,
        TerminalOperator terminal,
        Type resultType,
        CancellationToken cancellationToken)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        return GraphQueryableExtensions
            .RequireProvider(source)
            .ExecuteAsync<TResult>(source.Expression, terminal, resultType, cancellationToken);
    }
}
