using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace NFalkorDB.Linq.Mapping;

/// <summary>
/// Classifies CLR types into the values FalkorDB can store as graph properties.
/// </summary>
internal static class ScalarTypes
{
    private static readonly HashSet<Type> Supported = new HashSet<Type>
    {
        typeof(string),
        typeof(char),
        typeof(bool),
        typeof(byte),
        typeof(sbyte),
        typeof(short),
        typeof(ushort),
        typeof(int),
        typeof(uint),
        typeof(long),
        typeof(ulong),
        typeof(float),
        typeof(double),
        typeof(decimal),
        typeof(DateTime),
        typeof(DateTimeOffset),
        typeof(TimeSpan),
        typeof(Guid),
        typeof(Point),
        typeof(object)
    };

    internal static Type Unwrap(Type type) => Nullable.GetUnderlyingType(type) ?? type;

    internal static bool IsScalar(Type type)
    {
        var unwrapped = Unwrap(type);

        return Supported.Contains(unwrapped) || unwrapped.IsEnum;
    }

    /// <summary>
    /// True for a scalar the CLR can also sort.
    /// </summary>
    /// <remarks>
    /// Storable and orderable are not the same set. A <see cref="Point"/> is a perfectly good
    /// property value, but it is a record with no <see cref="IComparable"/>, so
    /// <c>Comparer&lt;Point&gt;.Default</c> throws. Cypher will happily order one, so ordering by a
    /// point returned rows in an arbitrary order where LINQ raised an error.
    /// </remarks>
    internal static bool IsOrderable(Type type) => IsScalar(type) && Unwrap(type) != typeof(Point);

    /// <summary>
    /// True for a scalar, or for a collection/map whose elements are themselves scalars.
    /// </summary>
    internal static bool IsStorable(Type type)
    {
        if (IsScalar(type))
        {
            return true;
        }

        if (TryGetDictionaryValueType(type, out var dictionaryValue))
        {
            return IsStorable(dictionaryValue);
        }

        if (TryGetElementType(type, out var elementType))
        {
            return IsScalar(elementType) || TryGetDictionaryValueType(elementType, out var nested) && IsStorable(nested);
        }

        return false;
    }

    internal static bool TryGetElementType(Type type, out Type elementType)
    {
        elementType = null;

        if (type == typeof(string))
        {
            return false;
        }

        if (type.IsArray)
        {
            elementType = type.GetElementType();
            return true;
        }

        if (type.IsGenericType)
        {
            var definition = type.GetGenericTypeDefinition();

            if (definition == typeof(IEnumerable<>) ||
                definition == typeof(ICollection<>) ||
                definition == typeof(IList<>) ||
                definition == typeof(IReadOnlyCollection<>) ||
                definition == typeof(IReadOnlyList<>) ||
                definition == typeof(List<>) ||
                definition == typeof(HashSet<>))
            {
                elementType = type.GetGenericArguments()[0];
                return true;
            }
        }

        var enumerable = type.GetInterfaces()
            .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>));

        if (enumerable != null)
        {
            elementType = enumerable.GetGenericArguments()[0];
            return true;
        }

        return false;
    }

    internal static bool TryGetDictionaryValueType(Type type, out Type valueType)
    {
        valueType = null;

        if (!type.IsGenericType)
        {
            return false;
        }

        var definition = type.GetGenericTypeDefinition();

        if (definition != typeof(IDictionary<,>) && definition != typeof(Dictionary<,>) && definition != typeof(IReadOnlyDictionary<,>))
        {
            return false;
        }

        var args = type.GetGenericArguments();

        if (args[0] != typeof(string))
        {
            return false;
        }

        valueType = args[1];

        return true;
    }
}
