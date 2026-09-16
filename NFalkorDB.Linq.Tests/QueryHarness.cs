using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NFalkorDB.Linq.Translation;

namespace NFalkorDB.Linq.Tests;

/// <summary>
/// A provider that translates queries but never talks to a server. It records the compiled query
/// so terminal operators (which do not return an <see cref="IQueryable"/> and therefore cannot be
/// inspected with <c>ToCypherQuery()</c>) can still be asserted on.
/// </summary>
internal sealed class CapturingProvider : GraphQueryProvider
{
    internal CompiledQuery Captured { get; private set; }

    internal override object ExecuteCompiled(CompiledQuery compiled)
    {
        Captured = compiled;

        return EmptyResultFor(compiled);
    }

    internal override Task<object> ExecuteCompiledAsync(CompiledQuery compiled, CancellationToken cancellationToken) =>
        Task.FromResult(ExecuteCompiled(compiled));

    private static object EmptyResultFor(CompiledQuery compiled)
    {
        if (compiled.Terminal == TerminalOperator.Sequence)
        {
            return Activator.CreateInstance(typeof(List<>).MakeGenericType(compiled.ResultType));
        }

        var type = compiled.ResultType;

        return type.IsValueType && Nullable.GetUnderlyingType(type) == null
            ? Activator.CreateInstance(type)
            : null;
    }
}

/// <summary>
/// Builds server-free queries for the translation tests.
/// </summary>
internal sealed class QueryHarness
{
    private readonly CapturingProvider _provider = new CapturingProvider();

    internal IQueryable<T> Nodes<T>() where T : class, new() => new GraphQueryable<T>(_provider);

    internal IQueryable<T> Relationships<T>() where T : class, new() => new GraphQueryable<T>(_provider);

    internal CompiledQuery Captured =>
        _provider.Captured ?? throw new InvalidOperationException("No query was executed.");

    internal string CapturedCypher => Captured.Cypher;

    internal IDictionary<string, object> CapturedParameters => Captured.Parameters;
}

internal static class QueryHarnessExtensions
{
    /// <summary>
    /// Shorthand for the very common "start a query over T" in the translation tests.
    /// </summary>
    internal static IQueryable<T> Nodes<T>() where T : class, new() =>
        new QueryHarness().Nodes<T>();

    internal static IQueryable<T> Relationships<T>() where T : class, new() =>
        new QueryHarness().Relationships<T>();

    internal static string Cypher(this IQueryable source) => source.ToCypherQuery().Cypher;

    internal static IReadOnlyDictionary<string, object> Parameters(this IQueryable source) =>
        source.ToCypherQuery().Parameters;
}
