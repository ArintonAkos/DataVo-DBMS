using System.Text;
using DataVo.Core.StorageEngine.Lsm;

namespace DataVo.Tests.Lsm;

public sealed class LsmFlushManagerTests
{
    private static byte[] Key(long pk)
    {
        var buf = new byte[8];
        InternalKey.EncodeInt64UserKey(buf, pk);
        return buf;
    }

    private static byte[] Val(string value) => Encoding.UTF8.GetBytes(value);

    [Fact]
    public void FreezeAndFlush_FreezesTableAndRejectsSubsequentWrites()
    {
        using var table = new MemTable();
        table.Put(Key(1), seqno: 1, LsmValueType.Put, Val("one"));
        var manager = new LsmFlushManager();

        byte[] bytes = manager.FreezeAndFlush(table);

        Assert.NotEmpty(bytes);
        Assert.True(table.IsFrozen);
        Assert.Throws<InvalidOperationException>(() => table.Put(Key(2), seqno: 2, LsmValueType.Put, Val("two")));
    }

    [Fact]
    public void FreezeAndFlush_WritesAllVersionsAndTombstonesReadableByReader()
    {
        using var table = new MemTable();
        table.Put(Key(7), seqno: 10, LsmValueType.Put, Val("old"));
        table.Put(Key(7), seqno: 20, LsmValueType.Put, Val("new"));
        table.Put(Key(3), seqno: 4, LsmValueType.Put, Val("alive"));
        table.Delete(Key(3), seqno: 8);
        var manager = new LsmFlushManager();

        byte[] bytes = manager.FreezeAndFlush(table);
        SsTableReader reader = SsTableReader.Load(bytes);

        Assert.True(reader.TryGet(Key(7), snapshotSeqno: 20, out byte[] newest, out bool newestIsTombstone));
        Assert.False(newestIsTombstone);
        Assert.Equal(Val("new"), newest);

        Assert.True(reader.TryGet(Key(7), snapshotSeqno: 15, out byte[] older, out bool olderIsTombstone));
        Assert.False(olderIsTombstone);
        Assert.Equal(Val("old"), older);

        Assert.False(reader.TryGet(Key(3), snapshotSeqno: 8, out byte[] deleted, out bool deletedIsTombstone));
        Assert.True(deletedIsTombstone);
        Assert.Empty(deleted);
    }

    [Fact]
    public void FreezeAndFlush_CopiesBytesBeforeMemTableDisposal()
    {
        byte[] bytes;
        var manager = new LsmFlushManager();
        using (var table = new MemTable())
        {
            table.Put(Key(5), seqno: 1, LsmValueType.Put, Val("five"));

            bytes = manager.FreezeAndFlush(table);
        }

        SsTableReader reader = SsTableReader.Load(bytes);
        Assert.True(reader.TryGet(Key(5), snapshotSeqno: 1, out byte[] value, out bool isTombstone));
        Assert.False(isTombstone);
        Assert.Equal(Val("five"), value);
    }

    [Fact]
    public void FreezeAndFlush_EmptyMemTableThrowsClearException()
    {
        using var table = new MemTable();
        var manager = new LsmFlushManager();

        var ex = Assert.Throws<InvalidOperationException>(() => manager.FreezeAndFlush(table));

        Assert.Contains("empty", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.False(table.IsFrozen);
        table.Put(Key(1), seqno: 1, LsmValueType.Put, Val("still-mutable"));
        Assert.True(table.TryGet(Key(1), snapshotSeqno: 1, out _, out _));
    }
}
