namespace DataVo.Core.Utils;

/// <summary>
/// A set of distinct 64-bit integers kept as one bit per value of the range they span. Membership is one
/// subtraction and one bit test, with no hashing, and 100,000 consecutive keys take 12.5 KB instead of the
/// megabytes a hash table needs. It suits keys that are unique and close together, such as the integer primary
/// keys of a table; <see cref="TryCreate"/> declines anything else.
/// </summary>
/// <example>
/// <code>
/// DenseInt64Set? customers = DenseInt64Set.TryCreate(customerIds);
/// bool known = customers is not null &amp;&amp; customers.Contains(order.CustomerId);
/// </code>
/// </example>
internal sealed class DenseInt64Set
{
    /// <summary>A range may hold this many times more values than there are keys before the set is declined.</summary>
    private const int MaxValuesPerKey = 64;

    /// <summary>Ranges up to this many values are always accepted, however few keys they hold.</summary>
    private const int SmallRange = 4096;

    private readonly long _min;
    private readonly ulong _range;
    private readonly ulong[] _bits;

    private DenseInt64Set(long min, ulong range, ulong[] bits, int count)
    {
        _min = min;
        _range = range;
        _bits = bits;
        Count = count;
    }

    /// <summary>Gets the number of keys in the set.</summary>
    public int Count { get; }

    /// <summary>
    /// Builds the set of <paramref name="keys"/>. Returns null when a key occurs twice (the keys are not a
    /// set) or when the keys are spread over a range more than 64 times their number, where a bit per value
    /// would cost more than a hash table.
    /// </summary>
    public static DenseInt64Set? TryCreate(ReadOnlySpan<long> keys)
    {
        if (keys.IsEmpty)
        {
            return new DenseInt64Set(0, 0, [], 0);
        }

        long min = keys[0];
        long max = keys[0];
        foreach (long key in keys)
        {
            min = Math.Min(min, key);
            max = Math.Max(max, key);
        }

        // max - min can exceed long.MaxValue; as an unsigned difference it is exact.
        ulong last = unchecked((ulong)(max - min));
        if (last >= (ulong)Math.Max(SmallRange, (long)keys.Length * MaxValuesPerKey))
        {
            return null;
        }

        var bits = new ulong[(int)(last >> 6) + 1];
        foreach (long key in keys)
        {
            ulong offset = unchecked((ulong)(key - min));
            ulong bit = 1UL << (int)(offset & 63);
            ref ulong word = ref bits[(int)(offset >> 6)];
            if ((word & bit) != 0)
            {
                return null;
            }

            word |= bit;
        }

        return new DenseInt64Set(min, last + 1, bits, keys.Length);
    }

    /// <summary>Whether <paramref name="key"/> is in the set.</summary>
    public bool Contains(long key)
    {
        // A key below the minimum wraps to a huge offset, so one comparison covers both ends of the range.
        ulong offset = unchecked((ulong)(key - _min));
        return offset < _range && (_bits[(int)(offset >> 6)] & (1UL << (int)(offset & 63))) != 0;
    }

    /// <summary>The keys in ascending order.</summary>
    public IEnumerable<long> Keys()
    {
        for (int word = 0; word < _bits.Length; word++)
        {
            ulong bits = _bits[word];
            for (int bit = 0; bits != 0; bit++, bits >>= 1)
            {
                if ((bits & 1) != 0)
                {
                    yield return unchecked(_min + ((long)word << 6) + bit);
                }
            }
        }
    }
}
