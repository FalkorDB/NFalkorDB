using System.Collections.Generic;

namespace NFalkorDB.Linq.Translation;

/// <summary>
/// A single element of a Cypher MATCH pattern.
/// </summary>
internal interface IPatternElement
{
}

/// <summary>
/// A node in a MATCH pattern, for example <c>(n0:Person)</c>.
/// </summary>
internal sealed class NodePatternElement : IPatternElement
{
    internal NodePatternElement(string alias, IReadOnlyList<string> labels)
    {
        Alias = alias;
        Labels = labels ?? new string[0];
    }

    internal string Alias { get; }

    internal IReadOnlyList<string> Labels { get; }
}

/// <summary>
/// A relationship in a MATCH pattern, for example <c>-[r0:KNOWS]-&gt;</c>.
/// </summary>
internal sealed class RelationshipPatternElement : IPatternElement
{
    internal RelationshipPatternElement(string alias, string relationshipType, TraversalDirection direction)
    {
        Alias = alias;
        RelationshipType = relationshipType;
        Direction = direction;
    }

    internal string Alias { get; }

    internal string RelationshipType { get; }

    internal TraversalDirection Direction { get; }
}

/// <summary>
/// One projected column of a RETURN (or WITH) clause.
/// </summary>
internal sealed class ReturnItem
{
    internal ReturnItem(string expression, string alias)
    {
        Expression = expression;
        Alias = alias;
    }

    internal string Expression { get; }

    internal string Alias { get; }
}

/// <summary>
/// One term of an ORDER BY clause.
/// </summary>
internal sealed class OrderByTerm
{
    internal OrderByTerm(string expression, bool descending)
    {
        Expression = expression;
        Descending = descending;
    }

    internal string Expression { get; }

    internal bool Descending { get; }
}

/// <summary>
/// The intermediate, renderer-agnostic shape of a translated LINQ query. Keeping the model
/// separate from the rendered string is what makes the translation unit testable without a server.
/// </summary>
internal sealed class CypherQueryModel
{
    internal List<IPatternElement> Pattern { get; } = new List<IPatternElement>();

    internal List<string> WhereClauses { get; } = new List<string>();

    internal List<ReturnItem> ReturnItems { get; } = new List<ReturnItem>();

    internal List<OrderByTerm> OrderByTerms { get; } = new List<OrderByTerm>();

    internal bool Distinct { get; set; }

    internal long? Skip { get; set; }

    internal long? Limit { get; set; }

    /// <summary>
    /// The fully rendered aggregate expression, for example <c>count(*)</c> or <c>sum(n0.age) </c>.
    /// <c>null</c> when the query returns rows rather than a single aggregate value.
    /// </summary>
    internal string AggregateExpression { get; set; }

    /// <summary>
    /// Set when an aggregate has to be applied to an already paged or de-duplicated row set, which
    /// Cypher expresses as a WITH clause between the MATCH and the RETURN.
    /// </summary>
    internal bool UseWithClause { get; set; }
}
