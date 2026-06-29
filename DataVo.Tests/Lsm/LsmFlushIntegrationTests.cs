using System.Buffers.Binary;
using System.Text;
using DataVo.Core.StorageEngine.Lsm;

namespace DataVo.Tests.Lsm;

public sealed class LsmFlushIntegrationTests
{
    [Fact]
    public void WalReplayThenFreezeAndFlush_ProducesReaderVisibleValuesAndTombstones()
    {
        using var table = new MemTable();
        byte[][] persisted =
        [
            Payload(10, seqno: 1, LsmValueType.Put, "ten-v1"),
            Payload(20, seqno: 2, LsmValueType.Put, "twenty-live"),
            Payload(10, seqno: 3, LsmValueType.Put, "ten-v3"),
            Payload(20, seqno: 4, LsmValueType.Deletion),
        ];

        foreach (byte[] payload in persisted)
        {
            Assert.True(LsmWalRecordCodec.Replay(payload, table));
        }

        byte[] sstable = new LsmFlushManager().FreezeAndFlush(table);
        SsTableReader reader = SsTableReader.Load(sstable);

        Assert.True(reader.TryGet(Key(10), snapshotSeqno: 3, out byte[] latest, out bool latestIsTombstone));
        Assert.False(latestIsTombstone);
        Assert.Equal(Val("ten-v3"), latest);

        Assert.True(reader.TryGet(Key(10), snapshotSeqno: 1, out byte[] older, out bool olderIsTombstone));
        Assert.False(olderIsTombstone);
        Assert.Equal(Val("ten-v1"), older);

        Assert.False(reader.TryGet(Key(20), snapshotSeqno: 4, out byte[] deleted, out bool deletedIsTombstone));
        Assert.True(deletedIsTombstone);
        Assert.Empty(deleted);
    }

    [Fact]
    public void RestartStyleRecovery_ReplaysPersistedPayloadsIntoFreshMemTableAndFlushesLatestAndOlderSnapshots()
    {
        byte[][] persistedBeforeRestart =
        [
            Payload(1, seqno: 1, LsmValueType.Put, "one-v1"),
            Payload(2, seqno: 2, LsmValueType.Put, "two-v2"),
            Payload(1, seqno: 5, LsmValueType.Put, "one-v5"),
            Payload(3, seqno: 6, LsmValueType.Put, "three-v6"),
            Payload(2, seqno: 7, LsmValueType.Deletion),
            Payload(1, seqno: 9, LsmValueType.Put, "one-v9"),
        ];

        using var recovered = new MemTable();
        foreach (byte[] payload in persistedBeforeRestart)
        {
            Assert.True(LsmWalRecordCodec.Replay(payload, recovered));
        }

        byte[] sstable = new LsmFlushManager().FreezeAndFlush(recovered);
        SsTableReader reader = SsTableReader.Load(sstable);

        Assert.True(reader.TryGet(Key(1), snapshotSeqno: 9, out byte[] oneLatest, out bool oneLatestIsTombstone));
        Assert.False(oneLatestIsTombstone);
        Assert.Equal(Val("one-v9"), oneLatest);

        Assert.True(reader.TryGet(Key(1), snapshotSeqno: 5, out byte[] oneOlder, out bool oneOlderIsTombstone));
        Assert.False(oneOlderIsTombstone);
        Assert.Equal(Val("one-v5"), oneOlder);

        Assert.True(reader.TryGet(Key(1), snapshotSeqno: 1, out byte[] oneOldest, out bool oneOldestIsTombstone));
        Assert.False(oneOldestIsTombstone);
        Assert.Equal(Val("one-v1"), oneOldest);

        Assert.False(reader.TryGet(Key(2), snapshotSeqno: 7, out byte[] twoDeleted, out bool twoDeletedIsTombstone));
        Assert.True(twoDeletedIsTombstone);
        Assert.Empty(twoDeleted);

        Assert.True(reader.TryGet(Key(2), snapshotSeqno: 2, out byte[] twoOlder, out bool twoOlderIsTombstone));
        Assert.False(twoOlderIsTombstone);
        Assert.Equal(Val("two-v2"), twoOlder);

        Assert.False(reader.TryGet(Key(4), snapshotSeqno: 10, out _, out bool missingIsTombstone));
        Assert.False(missingIsTombstone);
    }

    [Fact]
    public void FreezeAndFlush_FilterBlockIsBuiltFromUserKeysNotInternalKeys()
    {
        using var table = new MemTable();
        table.Put(Key(42), seqno: 1, LsmValueType.Put, Val("forty-two-v1"));
        table.Put(Key(42), seqno: 5, LsmValueType.Put, Val("forty-two-v5"));
        table.Delete(Key(42), seqno: 8);
        table.Put(Key(99), seqno: 2, LsmValueType.Put, Val("ninety-nine"));

        byte[] sstable = new LsmFlushManager().FreezeAndFlush(table);
        List<DataEntry> entries = ReadDataEntries(sstable);
        byte[] filterBytes = ReadFilterBlock(sstable);

        BloomFilter expectedUserKeyFilter = BloomFilter.Create(expectedKeys: entries.Count);
        BloomFilter incorrectInternalKeyFilter = BloomFilter.Create(expectedKeys: entries.Count);
        foreach (DataEntry entry in entries)
        {
            expectedUserKeyFilter.Add(InternalKey.UserKey(entry.InternalKey));
            incorrectInternalKeyFilter.Add(entry.InternalKey);
        }

        Assert.Equal(expectedUserKeyFilter.ToBytes(), filterBytes);
        Assert.NotEqual(incorrectInternalKeyFilter.ToBytes(), filterBytes);
        Assert.True(BloomFilter.FromBytes(filterBytes).MightContain(Key(42)));
        Assert.True(BloomFilter.FromBytes(filterBytes).MightContain(Key(99)));
    }

    [Fact]
    public void RecoveryFlushBytesRemainReadableAfterMemTableDisposal()
    {
        byte[][] persisted =
        [
            Payload(7, seqno: 11, LsmValueType.Put, "seven"),
            Payload(8, seqno: 12, LsmValueType.Put, "eight"),
            Payload(8, seqno: 13, LsmValueType.Deletion),
        ];

        byte[] sstable;
        using (var recovered = new MemTable())
        {
            foreach (byte[] payload in persisted)
            {
                Assert.True(LsmWalRecordCodec.Replay(payload, recovered));
            }

            sstable = new LsmFlushManager().FreezeAndFlush(recovered);
        }

        SsTableReader reader = SsTableReader.Load(sstable);
        Assert.True(reader.TryGet(Key(7), snapshotSeqno: 11, out byte[] seven, out bool sevenIsTombstone));
        Assert.False(sevenIsTombstone);
        Assert.Equal(Val("seven"), seven);

        Assert.False(reader.TryGet(Key(8), snapshotSeqno: 13, out byte[] eight, out bool eightIsTombstone));
        Assert.True(eightIsTombstone);
        Assert.Empty(eight);
    }

    private static byte[] Payload(long pk, ulong seqno, LsmValueType valueType, string? value = null)
    {
        byte[] userKey = Key(pk);
        byte[] valueBytes = value is null ? [] : Val(value);
        byte[] payload = new byte[LsmWalRecordCodec.MeasureSize(userKey, valueBytes)];
        int written = LsmWalRecordCodec.Write(payload, userKey, seqno, valueType, valueBytes);
        Assert.Equal(payload.Length, written);
        return payload;
    }

    private static byte[] Key(long pk)
    {
        var buf = new byte[8];
        InternalKey.EncodeInt64UserKey(buf, pk);
        return buf;
    }

    private static byte[] Val(string value) => Encoding.UTF8.GetBytes(value);

    private static byte[] ReadFilterBlock(byte[] sstable)
    {
        Assert.True(SsTableFormat.TryReadFooter(sstable, out _, out SsTableBlockHandle filterBlock));
        return sstable.AsSpan((int)filterBlock.Offset, filterBlock.Length).ToArray();
    }

    private static List<DataEntry> ReadDataEntries(byte[] sstable)
    {
        Assert.True(SsTableFormat.TryReadFooter(sstable, out SsTableBlockHandle indexBlock, out _));
        SsTableBlockHandle dataBlock = ReadSingleDataBlockHandle(sstable.AsSpan((int)indexBlock.Offset, indexBlock.Length));
        ReadOnlySpan<byte> data = sstable.AsSpan((int)dataBlock.Offset, dataBlock.Length);

        var entries = new List<DataEntry>();
        int offset = 0;
        while (offset < data.Length)
        {
            int keyLength = BinaryPrimitives.ReadInt32LittleEndian(data.Slice(offset, sizeof(int)));
            offset += sizeof(int);
            int valueLength = BinaryPrimitives.ReadInt32LittleEndian(data.Slice(offset, sizeof(int)));
            offset += sizeof(int);
            byte[] internalKey = data.Slice(offset, keyLength).ToArray();
            offset += keyLength;
            byte[] value = data.Slice(offset, valueLength).ToArray();
            offset += valueLength;
            entries.Add(new DataEntry(internalKey, value));
        }

        Assert.Equal(data.Length, offset);
        return entries;
    }

    private static SsTableBlockHandle ReadSingleDataBlockHandle(ReadOnlySpan<byte> indexBlock)
    {
        int offset = 0;
        int count = BinaryPrimitives.ReadInt32LittleEndian(indexBlock.Slice(offset, sizeof(int)));
        offset += sizeof(int);
        Assert.Equal(1, count);

        long dataOffset = BinaryPrimitives.ReadInt64LittleEndian(indexBlock.Slice(offset, sizeof(long)));
        offset += sizeof(long);
        int dataLength = BinaryPrimitives.ReadInt32LittleEndian(indexBlock.Slice(offset, sizeof(int)));
        offset += sizeof(int);
        int firstKeyLength = BinaryPrimitives.ReadInt32LittleEndian(indexBlock.Slice(offset, sizeof(int)));
        offset += sizeof(int);
        int lastKeyLength = BinaryPrimitives.ReadInt32LittleEndian(indexBlock.Slice(offset, sizeof(int)));
        offset += sizeof(int);
        offset += firstKeyLength + lastKeyLength;
        Assert.Equal(indexBlock.Length, offset);

        return new SsTableBlockHandle(dataOffset, dataLength);
    }

    private sealed record DataEntry(byte[] InternalKey, byte[] Value);
}
