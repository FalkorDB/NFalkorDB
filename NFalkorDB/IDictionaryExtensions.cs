using System.Collections.Generic;
using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("NFalkorDB.Tests")]

namespace NFalkorDB;

internal static class IDictionaryExtensions
{
    internal static bool SequenceEqual<TKey, TValue>(this IDictionary<TKey, TValue> @this, IDictionary<TKey, TValue> that)
    {
        if (@this == default(IDictionary<TKey, TValue>) || that == default(IDictionary<TKey, TValue>))
        {
            return false;
        }

        if (@this.Count != that.Count)
        {
            return false;
        }

        foreach (var key in @this.Keys)
        {
            var thisValue = @this[key];
            var thatValue = that[key];

            if (!thisValue.Equals(thatValue))
            {
                return false;
            }
        }

        return true;
    }
}