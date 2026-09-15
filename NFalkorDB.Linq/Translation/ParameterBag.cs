using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace NFalkorDB.Linq.Translation;

/// <summary>
/// Collects the literal values pulled out of an expression tree and hands back the
/// <c>$pN</c> placeholder that refers to them.
/// <para>
/// Every value a caller supplies goes through here. Nothing a caller supplies is ever
/// concatenated into the Cypher text, which is what keeps the provider injection-safe: the
/// generated Cypher only ever contains identifiers the provider itself produced.
/// </para>
/// </summary>
internal sealed class ParameterBag
{
    private readonly Dictionary<string, object> _values = new Dictionary<string, object>(StringComparer.Ordinal);

    private int _next;

    /// <summary>
    /// The parameter dictionary to hand to <c>Graph.Query</c> / <c>Graph.ReadOnlyQuery</c>.
    /// </summary>
    internal IDictionary<string, object> Values => _values;

    /// <summary>
    /// Registers a value and returns the placeholder that refers to it, for example <c>$p0</c>.
    /// </summary>
    internal string Add(object value)
    {
        var name = "p" + _next.ToString(CultureInfo.InvariantCulture);

        _next++;

        _values[name] = Normalize(value);

        return "$" + name;
    }

    /// <summary>
    /// Maps a CLR value onto one of the shapes <c>FalkorDBUtilities.ValueToString</c> can render
    /// safely. Anything it would fall through to <c>object.ToString()</c> for has to be converted
    /// here, because that fallback emits unquoted text.
    /// </summary>
    internal static object Normalize(object value)
    {
        switch (value)
        {
            case null:
                return null;
            case string _:
            case bool _:
            case long _:
            case double _:
                return value;
            case char c:
                return c.ToString();
            case Enum e:
                // Enums round-trip as their member name: a graph is schema-less, so a readable
                // name survives a refactor of the underlying numeric values.
                return e.ToString();
            case sbyte _:
            case byte _:
            case short _:
            case ushort _:
            case int _:
            case uint _:
                return Convert.ToInt64(value, CultureInfo.InvariantCulture);
            case ulong u:
                return u <= long.MaxValue
                    ? (object)Convert.ToInt64(u)
                    : Convert.ToDouble(u, CultureInfo.InvariantCulture);
            case float _:
            case decimal _:
                return Convert.ToDouble(value, CultureInfo.InvariantCulture);
            case DateTime dateTime:
                return dateTime.ToString("o", CultureInfo.InvariantCulture);
            case DateTimeOffset dateTimeOffset:
                return dateTimeOffset.ToString("o", CultureInfo.InvariantCulture);
            case TimeSpan timeSpan:
                return (long)timeSpan.TotalMilliseconds;
            case Guid guid:
                return guid.ToString("D", CultureInfo.InvariantCulture);
            case Point point:
                throw new NotSupportedException(
                    $"A Point value ({point}) cannot be used as a query parameter. Compare the individual coordinates, or use distance() through a raw Cypher query.");
        }

        // Any dictionary keyed by string is a Cypher map, not a collection of key/value pairs.
        // Matching the non-generic interface catches Dictionary<string, int> and friends, which
        // would otherwise fall through to the IEnumerable branch and fail on KeyValuePair.
        if (value is IDictionary map && IsStringKeyed(value.GetType()))
        {
            var normalizedMap = new Dictionary<string, object>(map.Count, StringComparer.Ordinal);

            foreach (DictionaryEntry entry in map)
            {
                if (entry.Key == null)
                {
                    throw new NotSupportedException("A map used as a query parameter cannot have a null key.");
                }

                normalizedMap[CypherIdentifier.Escape((string)entry.Key)] = Normalize(entry.Value);
            }

            return normalizedMap;
        }

        if (value is IEnumerable enumerable)
        {
            return enumerable.Cast<object>().Select(Normalize).ToArray();
        }

        throw new NotSupportedException(
            $"Values of type '{value.GetType().FullName}' cannot be used as a FalkorDB query parameter.");
    }

    private static bool IsStringKeyed(Type type)
    {
        foreach (var contract in type.GetInterfaces())
        {
            if (contract.IsGenericType &&
                contract.GetGenericTypeDefinition() == typeof(IDictionary<,>) &&
                contract.GetGenericArguments()[0] == typeof(string))
            {
                return true;
            }
        }

        return false;
    }
}
