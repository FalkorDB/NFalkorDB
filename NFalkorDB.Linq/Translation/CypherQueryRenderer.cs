using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;

namespace NFalkorDB.Linq.Translation;

/// <summary>
/// Renders a <see cref="CypherQueryModel"/> into a single line of Cypher.
/// </summary>
internal static class CypherQueryRenderer
{
    internal static string Render(CypherQueryModel model)
    {
        var cypher = new StringBuilder();

        cypher.Append("MATCH ");
        cypher.Append(RenderPattern(model.Pattern));

        if (model.WhereClauses.Count > 0)
        {
            cypher.Append(" WHERE ");
            cypher.Append(string.Join(" AND ", model.WhereClauses.ToArray()));
        }

        if (model.AggregateExpression != null)
        {
            if (model.UseWithClause)
            {
                var withItems = RenderWithItems(model.ReturnItems).ToList();
                var orderTerms = RenderWithOrderTerms(model, withItems);

                cypher.Append(" WITH ");

                if (model.Distinct)
                {
                    cypher.Append("DISTINCT ");
                }

                cypher.Append(string.Join(", ", withItems.ToArray()));

                if (orderTerms.Count > 0)
                {
                    cypher.Append(" ORDER BY ");
                    cypher.Append(string.Join(", ", orderTerms.ToArray()));
                }

                AppendSkipLimit(cypher, model);
            }

            cypher.Append(" RETURN ");
            cypher.Append(model.AggregateExpression);

            return cypher.ToString();
        }

        cypher.Append(" RETURN ");

        if (model.Distinct)
        {
            cypher.Append("DISTINCT ");
        }

        cypher.Append(string.Join(", ", model.ReturnItems.Select(RenderReturnItem).ToArray()));

        AppendOrderSkipLimit(cypher, model);

        return cypher.ToString();
    }

    private static void AppendOrderSkipLimit(StringBuilder cypher, CypherQueryModel model)
    {
        if (model.OrderByTerms.Count > 0)
        {
            cypher.Append(" ORDER BY ");
            cypher.Append(string.Join(", ", model.OrderByTerms
                .Select(t => t.Expression + (t.Descending ? " DESC" : " ASC"))
                .ToArray()));
        }

        AppendSkipLimit(cypher, model);
    }

    private static void AppendSkipLimit(StringBuilder cypher, CypherQueryModel model)
    {
        if (model.Skip.HasValue)
        {
            cypher.Append(" SKIP ");
            cypher.Append(model.Skip.Value.ToString(CultureInfo.InvariantCulture));
        }

        if (model.Limit.HasValue)
        {
            cypher.Append(" LIMIT ");
            cypher.Append(model.Limit.Value.ToString(CultureInfo.InvariantCulture));
        }
    }

    /// <summary>
    /// A WITH clause narrows what is in scope, so an ORDER BY attached to it can only mention the
    /// columns the WITH carries. Terms that survive are rewritten to the WITH alias; anything else
    /// is carried through as an extra column so the ordering still happens server side.
    /// </summary>
    private static List<string> RenderWithOrderTerms(CypherQueryModel model, List<string> withItems)
    {
        var terms = new List<string>(model.OrderByTerms.Count);
        var carried = new HashSet<string>(
            model.ReturnItems.Select((item, index) => WithAlias(model.ReturnItems, index)),
            StringComparer.Ordinal);

        foreach (var term in model.OrderByTerms)
        {
            var expression = ResolveOrderExpression(model, withItems, carried, term.Expression);

            terms.Add(expression + (term.Descending ? " DESC" : " ASC"));
        }

        return terms;
    }

    private static string ResolveOrderExpression(
        CypherQueryModel model,
        List<string> withItems,
        HashSet<string> carried,
        string expression)
    {
        for (var i = 0; i < model.ReturnItems.Count; i++)
        {
            if (string.Equals(model.ReturnItems[i].Expression, expression, StringComparison.Ordinal))
            {
                return WithAlias(model.ReturnItems, i);
            }
        }

        if (carried.Contains(RootIdentifier(expression)))
        {
            return expression;
        }

        var alias = "o" + withItems.Count.ToString(CultureInfo.InvariantCulture);

        withItems.Add(expression + " AS " + alias);
        carried.Add(alias);

        return alias;
    }

    /// <summary>
    /// The variable a property access hangs off, so <c>n0.age</c> yields <c>n0</c>.
    /// </summary>
    private static string RootIdentifier(string expression)
    {
        var dot = expression.IndexOf('.');

        return dot < 0 ? expression : expression.Substring(0, dot);
    }

    private static string RenderReturnItem(ReturnItem item) =>
        item.Alias == null ? item.Expression : item.Expression + " AS " + item.Alias;

    /// <summary>
    /// A WITH clause has to name every column so the RETURN after it can refer to them, whereas a
    /// RETURN is free to leave a projection unnamed.
    /// </summary>
    internal static IEnumerable<string> RenderWithItems(IReadOnlyList<ReturnItem> items)
    {
        for (var i = 0; i < items.Count; i++)
        {
            var item = items[i];

            if (item.Alias != null)
            {
                yield return item.Expression + " AS " + item.Alias;
            }
            else if (IsIdentifier(item.Expression))
            {
                yield return item.Expression;
            }
            else
            {
                yield return item.Expression + " AS c" + i.ToString(CultureInfo.InvariantCulture);
            }
        }
    }

    /// <summary>
    /// The name a column carries after a WITH clause, which is what an aggregate placed after the
    /// WITH has to reference.
    /// </summary>
    internal static string WithAlias(IReadOnlyList<ReturnItem> items, int index)
    {
        var item = items[index];

        if (item.Alias != null)
        {
            return item.Alias;
        }

        return IsIdentifier(item.Expression)
            ? item.Expression
            : "c" + index.ToString(CultureInfo.InvariantCulture);
    }

    private static bool IsIdentifier(string expression)
    {
        if (string.IsNullOrEmpty(expression))
        {
            return false;
        }

        if (!char.IsLetter(expression[0]) && expression[0] != '_')
        {
            return false;
        }

        for (var i = 1; i < expression.Length; i++)
        {
            if (!char.IsLetterOrDigit(expression[i]) && expression[i] != '_')
            {
                return false;
            }
        }

        return true;
    }

    private static string RenderPattern(IReadOnlyList<IPatternElement> pattern)
    {
        var rendered = new StringBuilder();

        foreach (var element in pattern)
        {
            switch (element)
            {
                case NodePatternElement node:
                    rendered.Append('(');
                    rendered.Append(node.Alias);

                    foreach (var label in node.Labels)
                    {
                        rendered.Append(':');
                        rendered.Append(label);
                    }

                    rendered.Append(')');
                    break;

                case RelationshipPatternElement relationship:
                    if (relationship.Direction == TraversalDirection.Incoming)
                    {
                        rendered.Append("<-[");
                    }
                    else
                    {
                        rendered.Append("-[");
                    }

                    rendered.Append(relationship.Alias);

                    if (!string.IsNullOrEmpty(relationship.RelationshipType))
                    {
                        rendered.Append(':');
                        rendered.Append(relationship.RelationshipType);
                    }

                    if (relationship.Direction == TraversalDirection.Outgoing)
                    {
                        rendered.Append("]->");
                    }
                    else
                    {
                        rendered.Append("]-");
                    }

                    break;

                default:
                    throw new InvalidOperationException($"Unknown pattern element '{element.GetType().Name}'.");
            }
        }

        return rendered.ToString();
    }
}
