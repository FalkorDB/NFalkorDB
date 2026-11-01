using System.Text;

namespace NFalkorDB.Linq.Translation;

/// <summary>
/// Renders schema names — labels, relationship types, property keys and map keys — as Cypher
/// identifiers.
/// </summary>
/// <remarks>
/// Mapped names come from attributes and CLR member names rather than from end-user input, but they
/// are still the only strings the provider writes into the query text verbatim. Escaping them keeps
/// the structural guarantee intact and makes names that are not bare identifiers — such as
/// <c>[Property("first-name")]</c> — render correctly instead of being parsed as an expression.
/// </remarks>
internal static class CypherIdentifier
{
    /// <summary>
    /// Returns <paramref name="name"/> unchanged when it is a bare Cypher identifier, and otherwise
    /// wraps it in backticks, doubling any backticks it contains.
    /// </summary>
    public static string Escape(string name)
    {
        if (IsBareIdentifier(name))
        {
            return name;
        }

        var escaped = new StringBuilder(name.Length + 2);

        escaped.Append('`');

        foreach (var character in name)
        {
            if (character == '`')
            {
                escaped.Append('`');
            }

            escaped.Append(character);
        }

        escaped.Append('`');

        return escaped.ToString();
    }

    private static bool IsBareIdentifier(string name)
    {
        if (string.IsNullOrEmpty(name))
        {
            return false;
        }

        var first = name[0];

        if (!char.IsLetter(first) && first != '_')
        {
            return false;
        }

        for (var index = 1; index < name.Length; index++)
        {
            var character = name[index];

            if (!char.IsLetterOrDigit(character) && character != '_')
            {
                return false;
            }
        }

        return true;
    }
}
