using DataVo.Core.StorageEngine.Lsm;

namespace DataVo.Tests.Lsm;

public class MemTableTests
{
    private static byte[] Key(long pk)
    {
        var buf = new byte[8];
        InternalKey.EncodeInt64UserKey(buf, pk);
        return buf;
    }

    private static byte[] Val(string s) => System.Text.Encoding.UTF8.GetBytes(s);

    [Fact]
    public void Put_Then_TryGet_ReturnsValue()
    {
        using var table = new MemTable();
        table.Put(Key(1), seqno: 5, LsmValueType.Put, Val("hello"));

        bool found = table.TryGet(Key(1), snapshotSeqno: 10, out ReadOnlySpan<byte> value, out bool tomb);

        Assert.True(found);
        Assert.False(tomb);
        Assert.Equal("hello", System.Text.Encoding.UTF8.GetString(value));
        Assert.Equal(1, table.Count);
    }

    [Fact]
    public void TryGet_MissingKey_ReturnsFalse()
    {
        using var table = new MemTable();
        table.Put(Key(1), 5, LsmValueType.Put, Val("a"));

        Assert.False(table.TryGet(Key(2), 10, out _, out _));
    }

    [Fact]
    public void TryGet_ReturnsNewestVersionAtOrBelowSnapshot()
    {
        using var table = new MemTable();
        table.Put(Key(1), 3, LsmValueType.Put, Val("old"));
        table.Put(Key(1), 7, LsmValueType.Put, Val("new"));

        // Snapshot above both → newest (seqno 7).
        Assert.True(table.TryGet(Key(1), 10, out ReadOnlySpan<byte> v1, out _));
        Assert.Equal("new", System.Text.Encoding.UTF8.GetString(v1));

        // Snapshot between them → only the older version is visible.
        Assert.True(table.TryGet(Key(1), 5, out ReadOnlySpan<byte> v2, out _));
        Assert.Equal("old", System.Text.Encoding.UTF8.GetString(v2));

        // Snapshot below both → not visible.
        Assert.False(table.TryGet(Key(1), 2, out _, out _));
    }

    [Fact]
    public void TryGet_ManyKeys_AllRetrievable()
    {
        using var table = new MemTable();
        for (int i = 0; i < 1000; i++)
        {
            table.Put(Key(i), (ulong)(i + 1), LsmValueType.Put, Val($"v{i}"));
        }

        Assert.Equal(1000, table.Count);
        for (int i = 0; i < 1000; i++)
        {
            Assert.True(table.TryGet(Key(i), 2000, out ReadOnlySpan<byte> v, out _), $"missing key {i}");
            Assert.Equal($"v{i}", System.Text.Encoding.UTF8.GetString(v));
        }
    }

    [Fact]
    public void ApproximateBytes_GrowsWithInserts()
    {
        using var table = new MemTable();
        long before = table.ApproximateBytes;
        table.Put(Key(1), 1, LsmValueType.Put, Val("xyz"));
        Assert.True(table.ApproximateBytes > before);
    }

    [Fact]
    public void Delete_ShadowsOlderPut_AtNewerSnapshot()
    {
        using var table = new MemTable();
        table.Put(Key(1), 3, LsmValueType.Put, Val("alive"));
        table.Delete(Key(1), 7);

        bool found = table.TryGet(Key(1), 10, out _, out bool tomb);

        Assert.False(found);
        Assert.True(tomb);
    }

    [Fact]
    public void Delete_DoesNotAffectReadsBelowTombstoneSeqno()
    {
        using var table = new MemTable();
        table.Put(Key(1), 3, LsmValueType.Put, Val("alive"));
        table.Delete(Key(1), 7);

        // A snapshot below the tombstone still sees the live value.
        Assert.True(table.TryGet(Key(1), 5, out ReadOnlySpan<byte> v, out bool tomb));
        Assert.False(tomb);
        Assert.Equal("alive", System.Text.Encoding.UTF8.GetString(v));
    }

    [Fact]
    public void Put_AfterDelete_Resurrects()
    {
        using var table = new MemTable();
        table.Put(Key(1), 3, LsmValueType.Put, Val("v3"));
        table.Delete(Key(1), 7);
        table.Put(Key(1), 9, LsmValueType.Put, Val("v9"));

        Assert.True(table.TryGet(Key(1), 10, out ReadOnlySpan<byte> v, out bool tomb));
        Assert.False(tomb);
        Assert.Equal("v9", System.Text.Encoding.UTF8.GetString(v));
    }
}
