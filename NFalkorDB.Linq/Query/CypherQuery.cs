using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;

namespace NFalkorDB.Linq;

/// <summary>
/// The Cypher a LINQ query translates to, together with the parameters it refers to.
/// </summary>
public sealed class CypherQuery
{
    internal CypherQuery(string cypher, IDictionary<string, object> parameters)
    {
        Cypher = cypher;
        Parameters = new Dictionary<string, object>(parameters);
    }

    /// <summary>
    /// The generated Cypher. Caller supplied values never appear here; they are referenced as
    /// <c>$p0</c>, <c>$p1</c>, and so on.
    /// </summary>
    public string Cypher { get; }

    /// <summary>
    /// The parameter values the Cypher refers to, keyed without the leading <c>$</c>.
    /// </summary>
    public IReadOnlyDictionary<string, object> Parameters { get; }

    /// <summary>
    /// Renders the query and its parameters for logging.
    /// </summary>
    /// <returns>A human readable description of the query.</returns>
    public override string ToString()
    {
        if (Parameters.Count == 0)
        {
            return Cypher;
        }

        var text = new StringBuilder(Cypher);

        text.Append(" -- ");
        text.Append(string.Join(", ", Parameters
            .OrderBy(p => p.Key, StringComparer.Ordinal)
            .Select(p => p.Key + "=" + System.Convert.ToString(p.Value, CultureInfo.InvariantCulture))
            .ToArray()));

        return text.ToString();
    }
}
