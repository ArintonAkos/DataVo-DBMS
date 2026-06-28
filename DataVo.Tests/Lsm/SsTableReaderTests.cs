using System.Buffers.Binary;
using System.Text;
using DataVo.Core.StorageEngine.Lsm;

namespace DataVo.Tests.Lsm;

public class SsTableReaderTests
{
    private static byte[] Key(long pk)
    {
        var buf = new byte[8];
        InternalKey.EncodeInt64UserKey(buf, pk);
        return buf;
    }

    private static byte[] Internal(long pk, ulong seqno, LsmValueType valueType = LsmValueType.Put)
    {
        var key = new byte[InternalKey.MeasureSize(8)];
        InternalKey.Write(key, Key(pk), seqno, valueType);
        return key;
    }

    private static byte[] Val(string value) => Encoding.UTF8.GetBytes(value);

    [Fact]
    public void TryGet_ReturnsNewestVisibleValue()
    {
        var writer = new SsTableWriter(expectedEntries: 2);
        writer.Add(Internal(7, 10), Val("old"));
        writer.Add(Internal(7, 20), Val("new"));
        SsTableReader reader = SsTableReader.Load(writer.Finish());

        bool found = reader.TryGet(Key(7), snapshotSeqno: 20, out byte[]? value, out bool isTombstone);

        Assert.True(found);
        Assert.False(isTombstone);
        Assert.Equal(Val("new"), value);
    }

    [Fact]
    public void TryGet_SnapshotBelowNewestReturnsOlderVisibleValue()
    {
        var writer = new SsTableWriter(expectedEntries: 2);
        writer.Add(Internal(7, 10), Val("old"));
        writer.Add(Internal(7, 20), Val("new"));
        SsTableReader reader = SsTableReader.Load(writer.Finish());

        bool found = reader.TryGet(Key(7), snapshotSeqno: 15, out byte[]? value, out bool isTombstone);

        Assert.True(found);
        Assert.False(isTombstone);
        Assert.Equal(Val("old"), value);
    }

    [Fact]
    public void TryGet_TombstoneShadowsOlderPut()
    {
        var writer = new SsTableWriter(expectedEntries: 2);
        writer.Add(Internal(3, 4), Val("alive"));
        writer.Add(Internal(3, 8, LsmValueType.Deletion), []);
        SsTableReader reader = SsTableReader.Load(writer.Finish());

        bool found = reader.TryGet(Key(3), snapshotSeqno: 8, out byte[]? value, out bool isTombstone);

        Assert.False(found);
        Assert.True(isTombstone);
        Assert.Empty(value);
    }

    [Fact]
    public void TryGet_MissingKeySkippedByFilterDoesNotScanDataBlock()
    {
        var writer = new SsTableWriter(expectedEntries: 1);
        writer.Add(Internal(1, 1), Val("one"));
        SsTableReader reader = SsTableReader.Load(writer.Finish());

        bool found = reader.TryGet(Key(99), snapshotSeqno: 1, out byte[]? value, out bool isTombstone);

        Assert.False(found);
        Assert.False(isTombstone);
        Assert.Empty(value);
        Assert.Equal(0, reader.DataBlocksScanned);
    }

    [Fact]
    public void Load_RejectsTruncatedFooter()
    {
        Assert.Throws<InvalidDataException>(() => SsTableReader.Load(new byte[SsTableFormat.FooterSize - 1]));
    }

    [Fact]
    public void Load_RejectsCorruptFilterBlock()
    {
        var writer = new SsTableWriter(expectedEntries: 1);
        writer.Add(Internal(1, 1), Val("one"));
        byte[] sstable = writer.Finish();
        Assert.True(SsTableFormat.TryReadFooter(sstable, out _, out SsTableBlockHandle filterBlock));

        BinaryPrimitives.WriteInt32LittleEndian(sstable.AsSpan((int)filterBlock.Offset, sizeof(int)), 100_000);

        Assert.Throws<InvalidDataException>(() => SsTableReader.Load(sstable));
    }

    [Fact]
    public void Load_RejectsValidShapedFilterWithFalseNegative()
    {
        var writer = new SsTableWriter(expectedEntries: 1);
        writer.Add(Internal(1, 1), Val("one"));
        byte[] sstable = writer.Finish();
        Assert.True(SsTableFormat.TryReadFooter(sstable, out _, out SsTableBlockHandle filterBlock));

        sstable.AsSpan((int)filterBlock.Offset + 8, filterBlock.Length - 8).Clear();

        Assert.Throws<InvalidDataException>(() => SsTableReader.Load(sstable));
    }

    [Fact]
    public void Load_RejectsIndexKeyBoundsThatDisagreeWithDataBlock()
    {
        var writer = new SsTableWriter(expectedEntries: 2);
        writer.Add(Internal(1, 1), Val("one"));
        writer.Add(Internal(2, 1), Val("two"));
        byte[] sstable = writer.Finish();
        Assert.True(SsTableFormat.TryReadFooter(sstable, out SsTableBlockHandle indexBlock, out _));

        int firstKeyLength = BinaryPrimitives.ReadInt32LittleEndian(
            sstable.AsSpan((int)indexBlock.Offset + sizeof(int) + SsTableFormat.BlockHandleSize, sizeof(int)));
        int lastKeyLengthOffset = (int)indexBlock.Offset + sizeof(int) + SsTableFormat.BlockHandleSize + sizeof(int);
        int firstKeyOffset = lastKeyLengthOffset + sizeof(int);
        byte[] wrongFirstKey = Internal(1, 0);
        Assert.Equal(firstKeyLength, wrongFirstKey.Length);
        wrongFirstKey.CopyTo(sstable.AsSpan(firstKeyOffset, wrongFirstKey.Length));

        Assert.Throws<InvalidDataException>(() => SsTableReader.Load(sstable));
    }

    [Fact]
    public void Load_RejectsTruncatedIndexBlock()
    {
        var writer = new SsTableWriter(expectedEntries: 1);
        writer.Add(Internal(1, 1), Val("one"));
        byte[] sstable = writer.Finish();
        Assert.True(SsTableFormat.TryReadFooter(sstable, out SsTableBlockHandle indexBlock, out SsTableBlockHandle filterBlock));

        byte[] corrupt = RewriteFooter(
            sstable,
            new SsTableBlockHandle(indexBlock.Offset, indexBlock.Length - 1),
            filterBlock);

        Assert.Throws<InvalidDataException>(() => SsTableReader.Load(corrupt));
    }

    [Fact]
    public void Load_RejectsTruncatedDataBlock()
    {
        var writer = new SsTableWriter(expectedEntries: 1);
        writer.Add(Internal(1, 1), Val("one"));
        byte[] sstable = writer.Finish();
        Assert.True(SsTableFormat.TryReadFooter(sstable, out SsTableBlockHandle indexBlock, out _));

        int dataLengthOffset = (int)indexBlock.Offset + sizeof(int) + sizeof(long);
        BinaryPrimitives.WriteInt32LittleEndian(
            sstable.AsSpan(dataLengthOffset, sizeof(int)),
            BinaryPrimitives.ReadInt32LittleEndian(sstable.AsSpan(dataLengthOffset, sizeof(int))) - 1);

        Assert.Throws<InvalidDataException>(() => SsTableReader.Load(sstable));
    }

    [Fact]
    public void Load_RejectsOutOfOrderDataBlock()
    {
        var writer = new SsTableWriter(expectedEntries: 2);
        writer.Add(Internal(1, 1), Val("one"));
        writer.Add(Internal(2, 1), Val("two"));
        byte[] sstable = writer.Finish();
        Assert.True(SsTableFormat.TryReadFooter(sstable, out SsTableBlockHandle indexBlock, out _));

        int dataLength = BinaryPrimitives.ReadInt32LittleEndian(
            sstable.AsSpan((int)indexBlock.Offset + sizeof(int) + sizeof(long), sizeof(int)));
        Span<byte> dataBlock = sstable.AsSpan(0, dataLength);
        int firstLength = EntryLength(dataBlock);
        byte[] first = dataBlock[..firstLength].ToArray();
        byte[] second = dataBlock.Slice(firstLength, dataBlock.Length - firstLength).ToArray();
        second.CopyTo(dataBlock);
        first.CopyTo(dataBlock[second.Length..]);

        Assert.Throws<InvalidDataException>(() => SsTableReader.Load(sstable));
    }

    private static byte[] RewriteFooter(
        byte[] sstable,
        SsTableBlockHandle indexBlock,
        SsTableBlockHandle filterBlock)
    {
        byte[] copy = sstable.ToArray();
        SsTableFormat.WriteFooter(copy.AsSpan(copy.Length - SsTableFormat.FooterSize), indexBlock, filterBlock);
        return copy;
    }

    private static int EntryLength(ReadOnlySpan<byte> dataBlock)
    {
        int keyLength = BinaryPrimitives.ReadInt32LittleEndian(dataBlock[..sizeof(int)]);
        int valueLength = BinaryPrimitives.ReadInt32LittleEndian(dataBlock.Slice(sizeof(int), sizeof(int)));
        return sizeof(int) + sizeof(int) + keyLength + valueLength;
    }
}
