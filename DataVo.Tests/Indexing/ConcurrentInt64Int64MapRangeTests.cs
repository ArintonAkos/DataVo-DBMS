using DataVo.Core.Indexing;

namespace DataVo.Tests.Indexing;

/// <summary>
/// Key ranges of the integer primary-key map: read from a sorted copy of the keys plus a log of the keys inserted
/// since, with removed keys dropped by looking them up. Each case is checked against a plain sorted dictionary.
/// </summary>
public sealed class ConcurrentInt64Int64MapRangeTests
{
    [Fact]
    public void Ranges_MatchASortedDictionary_ThroughInsertsRemovesAndResorts()
    {
        var random = new Random(20261116);
        var map = new ConcurrentInt64Int64Map();
        var reference = new SortedDictionary<long, long>();
        void Set(long key, long value)
        {
            map.Set(key, value);
            reference[key] = value;
        }

        // Unordered keys, negative ones included.
        foreach (long key in Enumerable.Range(-2_000, 6_000).Select(i => (long)i * 7).OrderBy(_ => random.Next()))
        {
            Set(key, key * 10);
        }

        for (int round = 0; round < 40; round++)
        {
            AssertRanges(map, reference, random);

            // Between reads: inserts (logged), removals, re-inserts of removed keys, and value updates.
            for (int i = 0; i < 400; i++)
            {
                long key = random.Next(-20_000, 60_000);
                switch (random.Next(4))
                {
                    case 0:
                        map.TryRemove(key);
                        reference.Remove(key);
                        break;
                    default:
                        Set(key, random.Next());
                        break;
                }
            }
        }
    }

    [Fact]
    public void RangeLargerThanTheLimit_ReturnsFalseAndAppendsNothing()
    {
        var map = new ConcurrentInt64Int64Map();
        for (long key = 1; key <= 1_000; key++)
        {
            map.Set(key, key);
        }

        var values = new List<long> { -1 };
        Assert.False(map.TryCollectValuesInKeyRange(1, 501, 499, values));
        Assert.Equal([-1L], values);
        Assert.True(map.TryCollectValuesInKeyRange(1, 501, 500, values));
        Assert.Equal(501, values.Count);
    }

    [Fact]
    public void OpenAndEmptyRanges()
    {
        var map = new ConcurrentInt64Int64Map();
        foreach (long key in new long[] { long.MinValue, -5, 0, 5, long.MaxValue })
        {
            map.Set(key, key);
        }

        Assert.Equal([long.MinValue, -5L, 0L, 5L, long.MaxValue], Collect(map, null, null));
        Assert.Equal([0L, 5L, long.MaxValue], Collect(map, 0, null));
        Assert.Equal([long.MinValue, -5L], Collect(map, null, 0));
        Assert.Empty(Collect(map, 6, 6));
        Assert.Empty(Collect(map, 1, 5));
    }

    private static void AssertRanges(ConcurrentInt64Int64Map map, SortedDictionary<long, long> reference, Random random)
    {
        for (int i = 0; i < 20; i++)
        {
            long? lower = random.Next(5) == 0 ? null : random.Next(-25_000, 65_000);
            long? upper = random.Next(5) == 0 ? null : (lower ?? -25_000) + random.Next(0, 5_000);
            long[] expected = [.. reference.Where(entry => (lower is null || entry.Key >= lower) && (upper is null || entry.Key < upper)).Select(entry => entry.Value)];
            Assert.Equal(expected, Collect(map, lower, upper));
        }
    }

    private static long[] Collect(ConcurrentInt64Int64Map map, long? lower, long? upper)
    {
        var values = new List<long>();
        Assert.True(map.TryCollectValuesInKeyRange(lower, upper, int.MaxValue, values));
        return [.. values];
    }
}
