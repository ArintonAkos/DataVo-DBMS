using System;
using System.Collections.Generic;

namespace DataVo.Core.Utils;

/// <summary>
/// A utility comparer for dynamically comparing objects, handling nulls,
/// and falling back to string comparison when types differ.
/// </summary>
public sealed class DynamicObjectComparer : IComparer<object?>
{
    /// <summary>
    /// A singleton instance of the <see cref="DynamicObjectComparer"/>.
    /// </summary>
    public static readonly DynamicObjectComparer Instance = new();

    /// <summary>
    /// Compares two objects dynamically.
    /// </summary>
    /// <param name="x">The first object to compare.</param>
    /// <param name="y">The second object to compare.</param>
    /// <returns>A signed integer indicating their relative order.</returns>
    public int Compare(object? x, object? y)
    {
        if (ReferenceEquals(x, y)) return 0;
        if (x is null) return -1;
        if (y is null) return 1;

        if (x is IComparable comparableX)
        {
            try
            {
                return comparableX.CompareTo(y);
            }
            catch
            {
                // Fallback to string comparison
            }
        }

        return string.CompareOrdinal(x.ToString(), y.ToString());
    }
}
