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
                // FalkorDB integers are signed 64-bit. There is no representation for a larger
                // value: sent as an integer literal the server silently clamps it to long.MaxValue,
                // and sent as a double it loses precision, so both would bind a different number.
                if (u > long.MaxValue)
                {
                    throw new NotSupportedException(
                        $"The value {u.ToString(CultureInfo.InvariantCulture)} cannot be used as a query parameter, because it does not fit in the signed 64-bit integer FalkorDB stores and would be bound as a different number. Use a smaller value, or map the property as a string.");
                }

                return Convert.ToInt64(u);
            case float _:
            case decimal _:
                return Convert.ToDouble(value, CultureInfo.InvariantCulture);
            case DateTime dateTime:
                // Every temporal value is bound as UTC so the stored strings are all the same shape
                // and sort lexically in chronological order. Left alone, a local DateTime renders
                // with a "+02:00" suffix and an unspecified one with no suffix at all, so the three
                // kinds would not compare against each other. An unspecified kind is read as UTC,
                // because there is nothing else to read it as.
                return ToUtcText(
                    dateTime.Kind == DateTimeKind.Local ? dateTime.ToUniversalTime() : dateTime);
            case DateTimeOffset dateTimeOffset:
                return ToUtcText(dateTimeOffset.UtcDateTime);
            case TimeSpan timeSpan:
                // Whole milliseconds is the documented representation, but the CLR compares a
                // TimeSpan at tick precision. Truncating turned a one-tick difference into zero, so
                // a predicate or a sort silently changed. There is no wider integer representation
                // to fall back on, so anything finer has to be refused.
                if (timeSpan.Ticks % TimeSpan.TicksPerMillisecond != 0)
                {
                    throw new NotSupportedException(
                        $"The TimeSpan {timeSpan} cannot be used as a query parameter, because it is stored as whole milliseconds and {timeSpan.Ticks % TimeSpan.TicksPerMillisecond} tick(s) would be lost, which can change which rows match. Round the value first, for example with TimeSpan.FromMilliseconds(Math.Round(value.TotalMilliseconds)).");
                }

                return timeSpan.Ticks / TimeSpan.TicksPerMillisecond;
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

        // Enumerating a queryable here would run a second query and pull its rows to the client in
        // the middle of translating this one, which is exactly the silent client-side fallback the
        // provider promises never to do.
        if (value is IQueryable)
        {
            throw new NotSupportedException(
                $"An IQueryable of '{value.GetType().GetGenericArguments().FirstOrDefault()?.Name ?? "?"}' cannot be used as a query parameter, because evaluating it would execute a second query and transfer its results to the client. Materialize it first with ToList() if you intend to send the values as a parameter.");
        }

        if (value is IEnumerable enumerable)
        {
            return enumerable.Cast<object>().Select(Normalize).ToArray();
        }

        throw new NotSupportedException(
            $"Values of type '{value.GetType().FullName}' cannot be used as a FalkorDB query parameter.");
    }

    /// <summary>
    /// Formats an instant as a fixed-width round-trip ISO-8601 string in UTC, which is the one shape
    /// that sorts lexically in the same order it sorts chronologically.
    /// </summary>
    private static string ToUtcText(DateTime value) =>
        DateTime.SpecifyKind(value, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture);

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
