using System.Buffers.Binary;
using System.Text;
using DataVo.Core.StorageEngine.Lsm;

namespace DataVo.Tests.Lsm;

public class SsTableWriterTests
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
    public void Finish_WritesDeterministicBytesWithReadableFooterHandles()
    {
        var firstRun = new SsTableWriter(expectedEntries: 2);
        firstRun.Add(Internal(1, 10), Val("one"));
        firstRun.Add(Internal(2, 20), Val("two"));

        var secondRun = new SsTableWriter(expectedEntries: 2);
        secondRun.Add(Internal(1, 10), Val("one"));
        secondRun.Add(Internal(2, 20), Val("two"));

        byte[] firstBytes = firstRun.Finish();
        byte[] secondBytes = secondRun.Finish();

        Assert.Equal(firstBytes, secondBytes);
        Assert.True(SsTableFormat.TryReadFooter(firstBytes, out SsTableBlockHandle indexBlock, out SsTableBlockHandle filterBlock));
        Assert.True(indexBlock.Offset > 0);
        Assert.True(indexBlock.Length > 0);
        Assert.True(filterBlock.Offset > indexBlock.Offset);
        Assert.True(filterBlock.Length > 0);
        Assert.Equal(firstBytes.Length - SsTableFormat.FooterSize, filterBlock.Offset + filterBlock.Length);
    }

    [Fact]
    public void Write_PreservesMemTableInternalKeyOrderInDataBlock()
    {
        using var table = new MemTable();
        table.Put(Key(5), 2, LsmValueType.Put, Val("five-old"));
        table.Put(Key(1), 1, LsmValueType.Put, Val("one"));
        table.Put(Key(5), 8, LsmValueType.Put, Val("five-new"));
        table.Put(Key(3), 4, LsmValueType.Put, Val("three"));
        table.Freeze();

        byte[] sstable = SsTableWriter.Write(table);
        List<byte[]> writtenKeys = ReadDataEntries(sstable).Select(entry => entry.InternalKey).ToList();

        Assert.Equal(table.Count, writtenKeys.Count);
        for (int i = 1; i < writtenKeys.Count; i++)
        {
            Assert.True(InternalKey.Compare(writtenKeys[i - 1], writtenKeys[i]) <= 0, $"out of order at {i}");
        }

        var fiveSeqnos = writtenKeys
            .Where(k => InternalKey.UserKey(k).SequenceEqual(Key(5)))
            .Select(k => InternalKey.Sequence(k))
            .ToArray();
        Assert.Equal([8UL, 2UL], fiveSeqnos);
    }

    [Fact]
    public void Write_PreservesTombstoneWithEmptyValue()
    {
        using var table = new MemTable();
        table.Put(Key(1), 3, LsmValueType.Put, Val("alive"));
        table.Delete(Key(1), 7);
        table.Freeze();

        byte[] sstable = SsTableWriter.Write(table);

        var tombstone = Assert.Single(
            ReadDataEntries(sstable),
            entry => InternalKey.ValueType(entry.InternalKey) == LsmValueType.Deletion);
        Assert.Empty(tombstone.Value);
        Assert.Equal(7UL, InternalKey.Sequence(tombstone.InternalKey));
        Assert.True(InternalKey.UserKey(tombstone.InternalKey).SequenceEqual(Key(1)));
    }

    [Fact]
    public void FilterBlock_ContainsUserKeysForMultipleVersions()
    {
        byte[] userKey = Key(42);
        byte[] oldInternalKey = Internal(42, 2);
        byte[] newInternalKey = Internal(42, 9);
        var writer = new SsTableWriter(expectedEntries: 2);
        writer.Add(oldInternalKey, Val("old"));
        writer.Add(newInternalKey, Val("new"));

        byte[] sstable = writer.Finish();
        Assert.True(SsTableFormat.TryReadFooter(sstable, out _, out SsTableBlockHandle filterBlock));
        byte[] filterBytes = sstable.AsSpan((int)filterBlock.Offset, filterBlock.Length).ToArray();
        BloomFilter filter = BloomFilter.FromBytes(filterBytes);
        BloomFilter expectedUserKeyFilter = BloomFilter.Create(expectedKeys: 2);
        expectedUserKeyFilter.Add(userKey);
        expectedUserKeyFilter.Add(userKey);
        BloomFilter incorrectInternalKeyFilter = BloomFilter.Create(expectedKeys: 2);
        incorrectInternalKeyFilter.Add(newInternalKey);
        incorrectInternalKeyFilter.Add(oldInternalKey);

        Assert.Equal(expectedUserKeyFilter.ToBytes(), filterBytes);
        Assert.NotEqual(incorrectInternalKeyFilter.ToBytes(), filterBytes);
        Assert.True(filter.MightContain(userKey));
    }

    [Fact]
    public void IndexBlock_RecordsOneDataBlockHandleAndFirstAndLastInternalKeys()
    {
        byte[] firstKey = Internal(1, 10);
        byte[] lastKey = Internal(3, 30);
        var writer = new SsTableWriter(expectedEntries: 2);
        writer.Add(firstKey, Val("first"));
        writer.Add(lastKey, Val("last"));

        byte[] sstable = writer.Finish();
        Assert.True(SsTableFormat.TryReadFooter(sstable, out SsTableBlockHandle indexBlock, out _));
        IndexEntry indexEntry = ReadSingleIndexEntry(sstable.AsSpan((int)indexBlock.Offset, indexBlock.Length));

        Assert.Equal(1, indexEntry.Count);
        Assert.Equal(0, indexEntry.DataBlock.Offset);
        Assert.True(indexEntry.DataBlock.Length > 0);
        Assert.Equal(firstKey, indexEntry.FirstInternalKey);
        Assert.Equal(lastKey, indexEntry.LastInternalKey);
    }

    private static List<DataEntry> ReadDataEntries(byte[] sstable)
    {
        Assert.True(SsTableFormat.TryReadFooter(sstable, out SsTableBlockHandle indexBlock, out _));
        IndexEntry indexEntry = ReadSingleIndexEntry(sstable.AsSpan((int)indexBlock.Offset, indexBlock.Length));
        ReadOnlySpan<byte> dataBlock = sstable.AsSpan((int)indexEntry.DataBlock.Offset, indexEntry.DataBlock.Length);

        var entries = new List<DataEntry>();
        int offset = 0;
        while (offset < dataBlock.Length)
        {
            int keyLen = BinaryPrimitives.ReadInt32LittleEndian(dataBlock.Slice(offset, sizeof(int)));
            offset += sizeof(int);
            int valueLen = BinaryPrimitives.ReadInt32LittleEndian(dataBlock.Slice(offset, sizeof(int)));
            offset += sizeof(int);
            byte[] internalKey = dataBlock.Slice(offset, keyLen).ToArray();
            offset += keyLen;
            byte[] value = dataBlock.Slice(offset, valueLen).ToArray();
            offset += valueLen;
            entries.Add(new DataEntry(internalKey, value));
        }

        Assert.Equal(dataBlock.Length, offset);
        return entries;
    }

    private static IndexEntry ReadSingleIndexEntry(ReadOnlySpan<byte> indexBlock)
    {
        int offset = 0;
        int count = BinaryPrimitives.ReadInt32LittleEndian(indexBlock.Slice(offset, sizeof(int)));
        offset += sizeof(int);
        long dataOffset = BinaryPrimitives.ReadInt64LittleEndian(indexBlock.Slice(offset, sizeof(long)));
        offset += sizeof(long);
        int dataLength = BinaryPrimitives.ReadInt32LittleEndian(indexBlock.Slice(offset, sizeof(int)));
        offset += sizeof(int);
        int firstKeyLength = BinaryPrimitives.ReadInt32LittleEndian(indexBlock.Slice(offset, sizeof(int)));
        offset += sizeof(int);
        int lastKeyLength = BinaryPrimitives.ReadInt32LittleEndian(indexBlock.Slice(offset, sizeof(int)));
        offset += sizeof(int);
        byte[] firstKey = indexBlock.Slice(offset, firstKeyLength).ToArray();
        offset += firstKeyLength;
        byte[] lastKey = indexBlock.Slice(offset, lastKeyLength).ToArray();
        offset += lastKeyLength;

        Assert.Equal(indexBlock.Length, offset);
        return new IndexEntry(count, new SsTableBlockHandle(dataOffset, dataLength), firstKey, lastKey);
    }

    private sealed record DataEntry(byte[] InternalKey, byte[] Value);

    private sealed record IndexEntry(
        int Count,
        SsTableBlockHandle DataBlock,
        byte[] FirstInternalKey,
        byte[] LastInternalKey);
}
