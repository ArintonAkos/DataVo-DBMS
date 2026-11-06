using DataVo.Core;

namespace DataVo.Tests.Keys;

public sealed class RowKeyTests
{
    [Theory]
    [InlineData(long.MinValue)]
    [InlineData(-1L)]
    [InlineData(0L)]
    [InlineData(1L)]
    [InlineData(long.MaxValue)]
    public void Integers_RoundTrip(long value)
    {
        RowKey key = value;
        Assert.True(key.IsInt64);
        Assert.Equal(value, (long)key);
        Assert.Equal(value.ToString(System.Globalization.CultureInfo.InvariantCulture), key.ToString());
    }

    [Fact]
    public void Integers_SortNumerically_IncludingNegatives()
    {
        long[] values = [5, -3, long.MaxValue, 0, long.MinValue, -1, 42];
        long[] sorted = [.. values.Select(v => (RowKey)v).Order().Select(k => (long)k)];
        Assert.Equal(values.Order(), sorted);
    }

    [Fact]
    public void Guids_RoundTrip_AndVersion7SortsByTime()
    {
        Guid guid = Guid.NewGuid();
        Assert.Equal(guid, RowKey.FromGuid(guid).AsGuid());

        Guid[] byTime = [.. Enumerable.Range(0, 100).Select(i => Guid.CreateVersion7(DateTimeOffset.UnixEpoch.AddMilliseconds(1_700_000_000_000L + (i * 1000L))))];
        RowKey[] keys = [.. byTime.Select(RowKey.FromGuid)];
        Assert.Equal(keys, keys.Order());
    }

    [Fact]
    public void BytesPreserveOrder()
    {
        RowKey[] keys = [-7, 3, RowKey.FromGuid(Guid.NewGuid())];
        foreach (RowKey left in keys)
        {
            foreach (RowKey right in keys)
            {
                byte[] a = new byte[16], b = new byte[16];
                left.WriteTo(a);
                right.WriteTo(b);
                Assert.Equal(Math.Sign(left.CompareTo(right)), Math.Sign(a.AsSpan().SequenceCompareTo(b)));
                Assert.Equal(left, RowKey.Read(a));
            }
        }
    }

    [Fact]
    public void GuidKey_DoesNotConvertToAnInteger()
    {
        RowKey key = RowKey.FromGuid(Guid.Parse("018bcfe5-6800-7000-8000-000000000001"));
        Assert.False(key.IsInt64);
        Assert.Throws<InvalidCastException>(() => (long)key);
    }

    [Fact]
    public void Text_RoundTrips_ForIntegersAndGuids()
    {
        RowKey[] keys = [-42, 0, long.MaxValue, RowKey.FromGuid(Guid.NewGuid())];
        foreach (RowKey key in keys)
        {
            Assert.Equal(key, RowKey.Parse(key.ToString()));
        }

        Assert.False(RowKey.TryParse("not a key", out _));
    }

    [Fact]
    public void EqualKeys_HashAlike()
    {
        Guid guid = Guid.NewGuid();
        Assert.Equal(RowKey.FromGuid(guid).GetHashCode(), RowKey.FromGuid(guid).GetHashCode());
        Assert.Equal(((RowKey)99).GetHashCode(), ((RowKey)99).GetHashCode());
        Assert.NotEqual((RowKey)1, (RowKey)2);
    }
}
