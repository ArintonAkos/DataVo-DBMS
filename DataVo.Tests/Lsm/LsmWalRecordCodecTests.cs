using DataVo.Core.StorageEngine.Lsm;

namespace DataVo.Tests.Lsm;

public sealed class LsmWalRecordCodecTests
{
    private static byte[] Key(long pk)
    {
        var buf = new byte[8];
        InternalKey.EncodeInt64UserKey(buf, pk);
        return buf;
    }

    private static byte[] Val(string value) => System.Text.Encoding.UTF8.GetBytes(value);

    [Fact]
    public void PutPayload_RoundTripsUserKeySeqnoValueTypeAndValue()
    {
        byte[] userKey = Key(42);
        byte[] value = Val("payload");
        byte[] payload = new byte[LsmWalRecordCodec.MeasureSize(userKey, value)];

        int written = LsmWalRecordCodec.Write(payload, userKey, seqno: 11, LsmValueType.Put, value);

        Assert.Equal(payload.Length, written);
        Assert.True(LsmWalRecordCodec.TryRead(payload, out LsmWalRecord record));
        Assert.Equal(userKey, record.UserKey);
        Assert.Equal(11UL, record.Seqno);
        Assert.Equal(LsmValueType.Put, record.ValueType);
        Assert.Equal(value, record.Value);
    }

    [Fact]
    public void DeletePayload_RoundTripsWithEmptyValue()
    {
        byte[] userKey = Key(7);
        byte[] payload = new byte[LsmWalRecordCodec.MeasureSize(userKey, ReadOnlySpan<byte>.Empty)];

        int written = LsmWalRecordCodec.Write(payload, userKey, seqno: 12, LsmValueType.Deletion, ReadOnlySpan<byte>.Empty);

        Assert.Equal(payload.Length, written);
        Assert.True(LsmWalRecordCodec.TryRead(payload, out LsmWalRecord record));
        Assert.Equal(userKey, record.UserKey);
        Assert.Equal(12UL, record.Seqno);
        Assert.Equal(LsmValueType.Deletion, record.ValueType);
        Assert.Empty(record.Value);
    }

    [Fact]
    public void TryRead_RejectsTruncatedPayload()
    {
        byte[] userKey = Key(1);
        byte[] value = Val("value");
        byte[] payload = new byte[LsmWalRecordCodec.MeasureSize(userKey, value)];
        LsmWalRecordCodec.Write(payload, userKey, seqno: 1, LsmValueType.Put, value);

        Assert.False(LsmWalRecordCodec.TryRead(payload.AsSpan(0, payload.Length - 1), out _));
    }

    [Fact]
    public void TryRead_RejectsInvalidValueType()
    {
        byte[] userKey = Key(1);
        byte[] value = Val("value");
        byte[] payload = new byte[LsmWalRecordCodec.MeasureSize(userKey, value)];
        LsmWalRecordCodec.Write(payload, userKey, seqno: 1, LsmValueType.Put, value);
        payload[2] = 0xFE;

        Assert.False(LsmWalRecordCodec.TryRead(payload, out _));
    }

    [Fact]
    public void TryRead_RejectsDeletionWithValue()
    {
        byte[] userKey = Key(1);
        byte[] value = Val("not-empty");
        byte[] payload = new byte[LsmWalRecordCodec.MeasureSize(userKey, value)];
        LsmWalRecordCodec.Write(payload, userKey, seqno: 1, LsmValueType.Put, value);
        payload[2] = (byte)LsmValueType.Deletion;

        Assert.False(LsmWalRecordCodec.TryRead(payload, out _));
    }

    [Fact]
    public void ReplayPut_InsertsValueVisibleAtSnapshot()
    {
        using var table = new MemTable();
        byte[] userKey = Key(5);
        byte[] value = Val("alive");
        byte[] payload = new byte[LsmWalRecordCodec.MeasureSize(userKey, value)];
        LsmWalRecordCodec.Write(payload, userKey, seqno: 9, LsmValueType.Put, value);

        Assert.True(LsmWalRecordCodec.Replay(payload, table));

        Assert.True(table.TryGet(userKey, snapshotSeqno: 9, out ReadOnlySpan<byte> actual, out bool tombstone));
        Assert.False(tombstone);
        Assert.Equal(value, actual.ToArray());
    }

    [Fact]
    public void ReplayDelete_ShadowsOlderPutAtNewerSnapshotButPreservesOlderSnapshot()
    {
        using var table = new MemTable();
        byte[] userKey = Key(5);
        byte[] olderValue = Val("older");
        table.Put(userKey, seqno: 3, LsmValueType.Put, olderValue);

        byte[] payload = new byte[LsmWalRecordCodec.MeasureSize(userKey, ReadOnlySpan<byte>.Empty)];
        LsmWalRecordCodec.Write(payload, userKey, seqno: 8, LsmValueType.Deletion, ReadOnlySpan<byte>.Empty);

        Assert.True(LsmWalRecordCodec.Replay(payload, table));

        Assert.False(table.TryGet(userKey, snapshotSeqno: 8, out _, out bool tombstone));
        Assert.True(tombstone);
        Assert.True(table.TryGet(userKey, snapshotSeqno: 7, out ReadOnlySpan<byte> visibleOlder, out bool olderTombstone));
        Assert.False(olderTombstone);
        Assert.Equal(olderValue, visibleOlder.ToArray());
    }

    [Fact]
    public void Apply_RejectsDefaultRecord()
    {
        using var table = new MemTable();

        Assert.False(LsmWalRecordCodec.Apply(default, table));
    }

    [Fact]
    public void Apply_RejectsNullKeyOrValueArrays()
    {
        using var table = new MemTable();

        Assert.False(LsmWalRecordCodec.Apply(
            new LsmWalRecord(null!, Seqno: 1, LsmValueType.Put, Val("value")),
            table));
        Assert.False(LsmWalRecordCodec.Apply(
            new LsmWalRecord(Key(1), Seqno: 1, LsmValueType.Put, null!),
            table));
    }

    [Fact]
    public void Apply_RejectsDeletionWithNonEmptyValue()
    {
        using var table = new MemTable();

        Assert.False(LsmWalRecordCodec.Apply(
            new LsmWalRecord(Key(1), Seqno: 1, LsmValueType.Deletion, Val("invalid")),
            table));
    }
}
