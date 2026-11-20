using System;
using System.Collections;
using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("NFalkorDB.Tests")]

namespace NFalkorDB;

internal static class Objects
{
    public static bool AreEqual(object obj1, object obj2)
    {
        if (ReferenceEquals(obj1, obj2))
        {
            return true;
        }

        if (obj1 == null || obj2 == null)
        {
            return false;
        }

        if (obj1.GetType() != obj2.GetType())
        {
            return false;
        }

        switch (obj1)
        {
            case string o1:
                return o1 == (string)obj2;
            case byte o1:
                return o1 == (byte)obj2;
            case sbyte o1:
                return o1 == (sbyte)obj2;
            case short o1:
                return o1 == (short)obj2;
            case ushort o1:
                return o1 == (ushort)obj2;
            case int o1:
                return o1 == (int)obj2;
            case uint o1:
                return o1 == (uint)obj2;
            case long o1:
                return o1 == (long)obj2;
            case ulong o1:
                return o1 == (ulong)obj2;
            case float o1:
                // `Equals` rather than `==` so that NaN equals NaN, matching both the hash codes
                // produced by GetValueHashCode and the equality used for `Point` and for values
                // nested inside collections.
                return o1.Equals((float)obj2);
            case double o1:
                return o1.Equals((double)obj2);
            case decimal o1:
                return o1 == (decimal)obj2;
            case char o1:
                return o1 == (char)obj2;
            case bool o1:
                return o1 == (bool)obj2;
            case DateTime o1:
                return o1 == (DateTime)obj2;
            case DateTimeOffset o1:
                return o1 == (DateTimeOffset)obj2;
            case TimeSpan o1:
                return o1 == (TimeSpan)obj2;
        }

        if (obj1 is IDictionary map1 && obj2 is IDictionary map2)
        {
            return MapsAreEqual(map1, map2);
        }

        if (obj1 is IEnumerable sequence1 && obj2 is IEnumerable sequence2)
        {
            return SequencesAreEqual(sequence1, sequence2);
        }

        return obj1.Equals(obj2);
    }

    /// <summary>
    /// Computes a hash code that is consistent with <see cref="AreEqual"/>: null tolerant, structural for
    /// sequences, and insertion-order independent for maps.
    /// </summary>
    public static int GetValueHashCode(object value)
    {
        if (value == null)
        {
            return 0;
        }

        if (value is string stringValue)
        {
            return stringValue.GetHashCode();
        }

        unchecked
        {
            if (value is IDictionary map)
            {
                int mapHash = map.Count;

                foreach (DictionaryEntry entry in map)
                {
                    mapHash += (GetValueHashCode(entry.Key) * 397) ^ GetValueHashCode(entry.Value);
                }

                return mapHash;
            }

            if (value is IEnumerable sequence)
            {
                int sequenceHash = 17;

                foreach (var item in sequence)
                {
                    sequenceHash = sequenceHash * 31 + GetValueHashCode(item);
                }

                return sequenceHash;
            }

            return value.GetHashCode();
        }
    }

    private static bool MapsAreEqual(IDictionary map1, IDictionary map2)
    {
        if (map1.Count != map2.Count)
        {
            return false;
        }

        foreach (DictionaryEntry entry in map1)
        {
            if (!map2.Contains(entry.Key) || !AreEqual(entry.Value, map2[entry.Key]))
            {
                return false;
            }
        }

        return true;
    }

    private static bool SequencesAreEqual(IEnumerable sequence1, IEnumerable sequence2)
    {
        var enumerator1 = sequence1.GetEnumerator();
        var enumerator2 = sequence2.GetEnumerator();

        try
        {
            while (enumerator1.MoveNext())
            {
                if (!enumerator2.MoveNext() || !AreEqual(enumerator1.Current, enumerator2.Current))
                {
                    return false;
                }
            }

            return !enumerator2.MoveNext();
        }
        finally
        {
            (enumerator1 as IDisposable)?.Dispose();
            (enumerator2 as IDisposable)?.Dispose();
        }
    }
}
