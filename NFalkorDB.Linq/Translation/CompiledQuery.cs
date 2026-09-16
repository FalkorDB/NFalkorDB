using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using NFalkorDB.Linq.Materialization;

namespace NFalkorDB.Linq.Translation;

/// <summary>
/// The terminal operation a translated query represents.
/// </summary>
internal enum TerminalOperator
{
    /// <summary>Return every matching row.</summary>
    Sequence,

    /// <summary>Return every matching row as an array, sized from the result set.</summary>
    Array,

    /// <summary>Return the first row, throwing when there is none.</summary>
    First,

    /// <summary>Return the first row or the element default.</summary>
    FirstOrDefault,

    /// <summary>Return the only row, throwing when there is not exactly one.</summary>
    Single,

    /// <summary>Return the only row or the element default.</summary>
    SingleOrDefault,

    /// <summary>Return whether any row matched.</summary>
    Any,

    /// <summary>Return whether no row matched. Used to express <c>All</c> over a negated predicate.</summary>
    None,

    /// <summary>Return the row count as an <see cref="int"/>.</summary>
    Count,

    /// <summary>Return the row count as a <see cref="long"/>.</summary>
    LongCount,

    /// <summary>Return the sum of the projected column.</summary>
    Sum,

    /// <summary>Return the minimum of the projected column.</summary>
    Min,

    /// <summary>Return the maximum of the projected column.</summary>
    Max,

    /// <summary>Return the mean of the projected column.</summary>
    Average
}

/// <summary>
/// A translated query: the Cypher text, the parameters it refers to, and the instructions for
/// turning the result rows back into CLR values.
/// </summary>
internal sealed class CompiledQuery
{
    internal CompiledQuery(
        string cypher,
        IDictionary<string, object> parameters,
        ProjectionShape projection,
        TerminalOperator terminal,
        Type resultType)
    {
        Cypher = cypher;
        Parameters = parameters;
        Projection = projection;
        Terminal = terminal;
        ResultType = resultType;
    }

    internal string Cypher { get; }

    internal IDictionary<string, object> Parameters { get; }

    internal ProjectionShape Projection { get; }

    internal TerminalOperator Terminal { get; }

    internal Type ResultType { get; }
}
