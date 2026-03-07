using System.Collections.Generic;
using System.Linq;

namespace DataVo.Core.Utils;

/// <summary>
/// A utility comparer for dictionaries containing object values, primarily used
/// to determine uniqueness or equality between rows in a dataset.
/// </summary>
public sealed class DictionaryComparer : IEqualityComparer<Dictionary<string, object?>>
{
    /// <summary>
    /// Determines whether two dictionaries contain the exact same key-value pairs.
    /// Values represented as the string "null" are treated as equivalent to literal nulls.
    /// </summary>
    /// <param name="x">The first dictionary to compare.</param>
    /// <param name="y">The second dictionary to compare.</param>
    /// <returns>True if the dictionaries are matching, false otherwise.</returns>
    public bool Equals(Dictionary<string, object?>? x, Dictionary<string, object?>? y)
    {
        if (x == null || y == null) return x == y;
        if (x.Count != y.Count) return false;

        foreach (var kvp in x)
        {
            if (!y.TryGetValue(kvp.Key, out var yVal)) return false;
            
            if (kvp.Value == null || kvp.Value.ToString() == "null")
            {
                if (yVal != null && yVal.ToString() != "null") return false;
            }
            else if (!kvp.Value.Equals(yVal))
            {
                return false;
            }
        }
        return true;
    }

    /// <summary>
    /// Gets the hash code for the specified dictionary based on its contents.
    /// </summary>
    /// <param name="obj">The dictionary to get the hash code for.</param>
    /// <returns>The computed hash code.</returns>
    public int GetHashCode(Dictionary<string, object?> obj)
    {
        int hash = 17;
        foreach (var kvp in obj.OrderBy(k => k.Key))
        {
            hash = hash * 31 + kvp.Key.GetHashCode();
            if (kvp.Value != null && kvp.Value.ToString() != "null")
            {
                hash = hash * 31 + kvp.Value.GetHashCode();
            }
        }
        return hash;
    }
}
