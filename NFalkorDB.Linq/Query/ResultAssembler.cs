using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using NFalkorDB.Linq.Materialization;
using NFalkorDB.Linq.Translation;

namespace NFalkorDB.Linq;

/// <summary>
/// Turns a <see cref="ResultSet"/> into the CLR value the terminal operator promised.
/// </summary>
internal static class ResultAssembler
{
    internal static object Assemble(CompiledQuery compiled, ResultSet resultSet)
    {
        switch (compiled.Terminal)
        {
            case TerminalOperator.Sequence:
                return AssembleSequence(compiled, resultSet);

            case TerminalOperator.Array:
                return AssembleArray(compiled, resultSet);

            case TerminalOperator.First:
            case TerminalOperator.FirstOrDefault:
            case TerminalOperator.Single:
            case TerminalOperator.SingleOrDefault:
                return AssembleElement(compiled, resultSet);

            default:
                return AssembleScalar(compiled, resultSet);
        }
    }

    /// <summary>
    /// Materializes straight into a right-sized array.
    /// </summary>
    /// <remarks>
    /// The result set knows its own row count, so the array is allocated once and filled in place.
    /// Going through a list and copying it held both buffers at once at peak.
    /// </remarks>
    private static object AssembleArray(CompiledQuery compiled, ResultSet resultSet)
    {
        var elementType = compiled.Projection.ResultType;
        var results = System.Array.CreateInstance(elementType, resultSet?.Count ?? 0);

        if (resultSet != null)
        {
            var index = 0;

            foreach (var record in resultSet)
            {
                results.SetValue(compiled.Projection.Materialize(record), index++);
            }
        }

        return results;
    }

    private static object AssembleSequence(CompiledQuery compiled, ResultSet resultSet)
    {
        var elementType = compiled.Projection.ResultType;
        // Sizing from the row count up front avoids the list growing by repeated doubling.
        var results = (IList)Activator.CreateInstance(
            typeof(List<>).MakeGenericType(elementType), resultSet?.Count ?? 0);

        if (resultSet != null)
        {
            foreach (var record in resultSet)
            {
                results.Add(compiled.Projection.Materialize(record));
            }
        }

        return results;
    }

    private static object AssembleElement(CompiledQuery compiled, ResultSet resultSet)
    {
        var elementType = compiled.Projection.ResultType;
        var records = resultSet?.Take(2).ToList() ?? new List<Record>();

        var allowsMultiple = compiled.Terminal == TerminalOperator.First ||
                             compiled.Terminal == TerminalOperator.FirstOrDefault;

        if (!allowsMultiple && records.Count > 1)
        {
            throw new InvalidOperationException("Sequence contains more than one element");
        }

        if (records.Count == 0)
        {
            var allowsEmpty = compiled.Terminal == TerminalOperator.FirstOrDefault ||
                              compiled.Terminal == TerminalOperator.SingleOrDefault;

            if (!allowsEmpty)
            {
                throw new InvalidOperationException("Sequence contains no elements");
            }

            return DefaultOf(elementType);
        }

        return compiled.Projection.Materialize(records[0]);
    }

    private static object AssembleScalar(CompiledQuery compiled, ResultSet resultSet)
    {
        var record = resultSet?.FirstOrDefault();

        if (record == null)
        {
            throw new NFalkorDBRunTimeException(
                $"FalkorDB returned no rows for the aggregate query '{compiled.Cypher}'.");
        }

        var requiresElements = compiled.Terminal == TerminalOperator.Min ||
                               compiled.Terminal == TerminalOperator.Max ||
                               compiled.Terminal == TerminalOperator.Average;

        if (requiresElements && record.Values.Count > 0 && record.Values[0] == null &&
            compiled.ResultType != null && compiled.ResultType.IsValueType &&
            Nullable.GetUnderlyingType(compiled.ResultType) == null)
        {
            // Cypher yields NULL where LINQ throws for an empty sequence.
            throw new InvalidOperationException("Sequence contains no elements");
        }

        return compiled.Projection.Materialize(record);
    }

    internal static object DefaultOf(Type type) =>
        type.IsValueType ? Activator.CreateInstance(type) : null;
}
