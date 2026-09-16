using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using NFalkorDB.Linq.Mapping;

namespace NFalkorDB.Linq.Materialization;

/// <summary>
/// Converts the loosely typed values FalkorDB returns (FalkorDB widens every integer to
/// <see cref="long"/> and every real to <see cref="double"/>) into the CLR types a query asked for.
/// </summary>
internal static class ValueConverter
{
    internal static object Convert(object value, Type targetType, string context)
    {
        if (targetType == typeof(object))
        {
            return value;
        }

        var underlying = Nullable.GetUnderlyingType(targetType);
        var isNullable = underlying != null;
        var target = underlying ?? targetType;

        if (value == null)
        {
            if (isNullable || !targetType.IsValueType)
            {
                return null;
            }

            // A graph is schema-less: an absent property is a missing value, not a type error.
            return Activator.CreateInstance(targetType);
        }

        if (target.IsInstanceOfType(value))
        {
            return value;
        }

        if (target.IsEnum)
        {
            return ConvertEnum(value, target, context);
        }

        if (target == typeof(string))
        {
            return ConvertString(value, context);
        }

        if (target == typeof(char))
        {
            var text = ConvertString(value, context);

            if (text.Length != 1)
            {
                throw Mismatch(value, targetType, context);
            }

            return text[0];
        }

        if (target == typeof(bool))
        {
            if (value is bool b)
            {
                return b;
            }

            throw Mismatch(value, targetType, context);
        }

        if (target == typeof(DateTime))
        {
            return ConvertDateTime(value, targetType, context);
        }

        if (target == typeof(DateTimeOffset))
        {
            return ConvertDateTimeOffset(value, targetType, context);
        }

        if (target == typeof(TimeSpan))
        {
            return ConvertTimeSpan(value, targetType, context);
        }

        if (target == typeof(Guid))
        {
            if (value is string guidText && Guid.TryParse(guidText, out var guid))
            {
                return guid;
            }

            throw Mismatch(value, targetType, context);
        }

        if (IsNumeric(target))
        {
            if (!(value is IConvertible) || value is string || value is bool)
            {
                throw Mismatch(value, targetType, context);
            }

            RejectFractionalNarrowing(value, target, targetType, context);

            try
            {
                return System.Convert.ChangeType(value, target, CultureInfo.InvariantCulture);
            }
            catch (Exception ex) when (ex is OverflowException || ex is InvalidCastException || ex is FormatException)
            {
                throw Mismatch(value, targetType, context, ex);
            }
        }

        if (ScalarTypes.TryGetDictionaryValueType(target, out var dictionaryValueType))
        {
            return ConvertDictionary(value, target, dictionaryValueType, context);
        }

        if (ScalarTypes.TryGetElementType(target, out var elementType))
        {
            return ConvertCollection(value, target, elementType, context);
        }

        throw Mismatch(value, targetType, context);
    }

    /// <summary>
    /// Rejects a real value that would lose its fractional part on the way into an integral property.
    /// </summary>
    /// <remarks>
    /// <see cref="System.Convert.ChangeType(object, Type, IFormatProvider)"/> rounds rather than
    /// failing, so a stored <c>1.5</c> read into an <c>int</c> would silently materialize as <c>2</c>.
    /// A whole real such as <c>2.0</c> is still accepted, because FalkorDB returns a double for
    /// aggregates such as <c>avg()</c> and for any property written through a real-valued expression.
    /// </remarks>
    private static void RejectFractionalNarrowing(object value, Type target, Type targetType, string context)
    {
        if (!IsIntegral(target))
        {
            return;
        }

        double real;

        switch (value)
        {
            case double d:
                real = d;
                break;
            case float f:
                real = f;
                break;
            case decimal m:
                // A decimal must be tested in its own precision. Casting 9007199254740992.5m to a
                // double rounds the fraction away first, and the truncation test below would then
                // see a whole number and let ChangeType round the original value silently.
                if (m == decimal.Truncate(m))
                {
                    return;
                }

                throw FractionLost(m.ToString(CultureInfo.InvariantCulture), targetType, context);
            default:
                return;
        }

        if (real == Math.Truncate(real))
        {
            return;
        }

        throw FractionLost(real.ToString(CultureInfo.InvariantCulture), targetType, context);
    }

    private static GraphMappingException FractionLost(string value, Type targetType, string context) =>
        new GraphMappingException(
            $"Cannot convert the value '{value}' to {targetType} for {context}, because it has a fractional part that the conversion would silently round away. Map the property as a floating point type.");

    private static bool IsIntegral(Type type) =>
        type == typeof(byte) || type == typeof(sbyte) ||
        type == typeof(short) || type == typeof(ushort) ||
        type == typeof(int) || type == typeof(uint) ||
        type == typeof(long) || type == typeof(ulong);

    private static object ConvertEnum(object value, Type target, string context)
    {
        if (value is string name)
        {
            try
            {
                return Enum.Parse(target, name, ignoreCase: true);
            }
            catch (ArgumentException ex)
            {
                throw new GraphMappingException(
                    $"Cannot convert the value '{name}' into enum '{target.Name}' while materializing {context}.", ex);
            }
        }

        // Only an integral value can name an enum member. Accepting every IConvertible would turn
        // `true` into the member with value 1, and round 2.5 into the member with value 2, hiding
        // a genuine type mismatch.
        if (IsIntegral(value))
        {
            return Enum.ToObject(target, System.Convert.ToInt64(value, CultureInfo.InvariantCulture));
        }

        throw Mismatch(value, target, context);
    }

    private static bool IsIntegral(object value) =>
        value is byte || value is sbyte ||
        value is short || value is ushort ||
        value is int || value is uint ||
        value is long || value is ulong;

    private static string ConvertString(object value, string context)
    {
        switch (value)
        {
            case string text:
                return text;
            case char c:
                return c.ToString();
            case bool b:
                return b ? "true" : "false";
            case IConvertible convertible:
                return convertible.ToString(CultureInfo.InvariantCulture);
        }

        throw Mismatch(value, typeof(string), context);
    }

    /// <summary>
    /// Reads a stored value as a <see cref="DateTimeOffset"/>, keeping the offset it was written
    /// with.
    /// </summary>
    /// <remarks>
    /// The parameter binder writes these with the round-trip "o" format, which carries the offset,
    /// so parsing as <see cref="DateTime"/> first would reinterpret the instant in local time and
    /// then fail to pair it with a zero offset.
    /// </remarks>
    private static DateTimeOffset ConvertDateTimeOffset(object value, Type targetType, string context)
    {
        switch (value)
        {
            case DateTimeOffset offset:
                return offset;

            // FalkorDB has no offset-aware temporal type, so an unqualified value is read as UTC
            // rather than as the reading machine's local time.
            case DateTime dateTime:
                return new DateTimeOffset(dateTime.Kind == DateTimeKind.Unspecified
                    ? DateTime.SpecifyKind(dateTime, DateTimeKind.Utc)
                    : dateTime);

            case long epochMilliseconds:
                return DateTimeOffset.FromUnixTimeMilliseconds(epochMilliseconds);

            case string text when DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var parsed):
                return parsed;
        }

        throw Mismatch(value, targetType, context);
    }

    private static DateTime ConvertDateTime(object value, Type targetType, string context)
    {
        switch (value)
        {
            case DateTime dateTime:
                return dateTime;
            case long epochMilliseconds:
                return DateTimeOffset.FromUnixTimeMilliseconds(epochMilliseconds).UtcDateTime;
            case string text when DateTime.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var parsed):
                return parsed;
        }

        throw Mismatch(value, targetType, context);
    }

    private static TimeSpan ConvertTimeSpan(object value, Type targetType, string context)
    {
        switch (value)
        {
            case TimeSpan timeSpan:
                return timeSpan;
            case long milliseconds:
                return TimeSpan.FromMilliseconds(milliseconds);
            case double milliseconds:
                return TimeSpan.FromMilliseconds(milliseconds);
            case string text when TimeSpan.TryParse(text, CultureInfo.InvariantCulture, out var parsed):
                return parsed;
        }

        throw Mismatch(value, targetType, context);
    }

    private static object ConvertDictionary(object value, Type target, Type valueType, string context)
    {
        if (!(value is IDictionary map))
        {
            throw Mismatch(value, target, context);
        }

        var result = (IDictionary)Activator.CreateInstance(typeof(Dictionary<,>).MakeGenericType(typeof(string), valueType));

        foreach (DictionaryEntry entry in map)
        {
            result[System.Convert.ToString(entry.Key, CultureInfo.InvariantCulture)] =
                Convert(entry.Value, valueType, context);
        }

        return result;
    }

    private static object ConvertCollection(object value, Type target, Type elementType, string context)
    {
        if (!(value is IEnumerable source) || value is string)
        {
            throw Mismatch(value, target, context);
        }

        var listType = typeof(List<>).MakeGenericType(elementType);
        var list = (IList)Activator.CreateInstance(listType);

        foreach (var item in source)
        {
            list.Add(Convert(item, elementType, context));
        }

        if (target.IsArray)
        {
            var array = Array.CreateInstance(elementType, list.Count);

            list.CopyTo(array, 0);

            return array;
        }

        if (target.IsAssignableFrom(listType))
        {
            return list;
        }

        // A set interface cannot be constructed, but it is a legal property type that
        // ScalarTypes.TryGetElementType accepts, so pick the obvious concrete implementation.
        var setType = typeof(HashSet<>).MakeGenericType(elementType);

        if (target.IsInterface && target.IsAssignableFrom(setType))
        {
            return Activator.CreateInstance(setType, list);
        }

        var enumerableConstructor = target.GetConstructor(new[] { typeof(IEnumerable<>).MakeGenericType(elementType) });

        if (enumerableConstructor != null)
        {
            return enumerableConstructor.Invoke(new object[] { list });
        }

        throw Mismatch(value, target, context);
    }

    private static bool IsNumeric(Type type) =>
        type == typeof(byte) || type == typeof(sbyte) ||
        type == typeof(short) || type == typeof(ushort) ||
        type == typeof(int) || type == typeof(uint) ||
        type == typeof(long) || type == typeof(ulong) ||
        type == typeof(float) || type == typeof(double) || type == typeof(decimal);

    private static GraphMappingException Mismatch(object value, Type targetType, string context, Exception inner = null) =>
        new GraphMappingException(
            $"Cannot convert a value of type '{value?.GetType().Name ?? "null"}' into '{targetType.Name}' while materializing {context}.",
            inner);
}
