using System.Buffers.Binary;
using System.Text;
using DataVo.Core;
using DataVo.Core.StorageEngine.Lsm;

namespace DataVo.Tests.Lsm;

public sealed class LsmCommitLogTests : IDisposable
{
    private readonly string _directory = TestStorageDirectories.Create("commit_log");

    private string LogPath => Path.Combine(_directory, "commit.wal");

    [Fact]
    public void Codec_RoundTripsPutsAndDeletesAcrossTables()
    {
        var batch = new LsmCommitBatch();
        batch.Put("Db", "Players", 1, [1, 2, 3]);
        batch.Delete("Db", "Players", 2);
        batch.Put("Db", "Équipes", 7, []);
        batch.Put("Other", "T", long.MaxValue, Enumerable.Range(0, 2_000_000).Select(i => (byte)i).ToArray());
        batch.DropTable("Db", "Old");
        batch.DropDatabase("Gone");

        byte[] payload = new byte[LsmCommitBatchCodec.MeasureSize(batch)];
        int written = LsmCommitBatchCodec.Write(payload, batch);

        Assert.Equal(payload.Length, written);
        Assert.True(LsmCommitBatchCodec.TryRead(payload, out List<LsmCommitOperation> operations));
        AssertSameOperations(batch.Operations, operations);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(5)]
    [InlineData(20)]
    public void Codec_RejectsTruncatedPayload(int bytesRemoved)
    {
        var batch = new LsmCommitBatch();
        batch.Put("Db", "T", 1, Encoding.UTF8.GetBytes("value"));
        batch.Delete("Db", "T", 2);
        byte[] payload = new byte[LsmCommitBatchCodec.MeasureSize(batch)];
        LsmCommitBatchCodec.Write(payload, batch);

        Assert.False(LsmCommitBatchCodec.TryRead(payload.AsSpan(0, payload.Length - bytesRemoved), out _));
    }

    [Fact]
    public void Codec_RejectsTrailingBytesAndUnknownOperationKinds()
    {
        var batch = new LsmCommitBatch();
        batch.Delete("Db", "T", 2);
        byte[] payload = new byte[LsmCommitBatchCodec.MeasureSize(batch) + 1];
        LsmCommitBatchCodec.Write(payload, batch);
        Assert.False(LsmCommitBatchCodec.TryRead(payload, out _));

        byte[] exact = payload[..^1];
        exact[LsmCommitBatchCodec.FirstOperationOffset] = 99; // operation kind byte
        Assert.False(LsmCommitBatchCodec.TryRead(exact, out _));
    }

    [Fact]
    public void Batch_CollapsesRepeatedRowOperations_KeepingTheLast()
    {
        var batch = new LsmCommitBatch();
        batch.Put("Db", "T", 1, [1]);
        batch.Put("Db", "T", 2, [2]);
        batch.Delete("Db", "T", 1);
        batch.Put("Db", "U", 1, [9]);
        batch.Put("Db", "T", 2, [3]);

        batch.CollapseToLatestPerRow();

        Assert.Equal(
            [(LsmCommitOperationKind.Delete, "T", 1L), (LsmCommitOperationKind.Put, "U", 1L), (LsmCommitOperationKind.Put, "T", 2L)],
            batch.Operations.Select(o => (o.Kind, o.Table, o.RowId)));
        Assert.Equal([3], batch.Operations[^1].Value);
    }

    [Fact]
    public void Batch_RejectsInvalidOperations()
    {
        var batch = new LsmCommitBatch();
        Assert.Throws<ArgumentException>(() => batch.Put("", "T", 1, []));
        Assert.Throws<ArgumentException>(() => batch.Put("Db", "", 1, []));
    }

    [Fact]
    public void Codec_RoundTripsSixteenByteKeys_IncludingNegativeIntegersAndGuids()
    {
        var batch = new LsmCommitBatch();
        RowKey guid = RowKey.FromGuid(Guid.Parse("018f2c1e-7b3a-7cde-8f00-123456789abc"));
        batch.Put("Db", "T", -42, "negative"u8.ToArray());
        batch.Put("Db", "T", long.MaxValue, "max"u8.ToArray());
        batch.Delete("Db", "T", guid);
        batch.IndexPut("Db", "T", "ix", "k"u8, guid);

        byte[] payload = new byte[LsmCommitBatchCodec.MeasureSize(batch)];
        LsmCommitBatchCodec.Write(payload, batch);

        Assert.True(LsmCommitBatchCodec.TryRead(payload, out List<LsmCommitOperation> operations));
        Assert.Equal([(RowKey)(-42), long.MaxValue, guid, guid], operations.Select(o => o.RowId));
    }

    [Fact]
    public void Codec_ReadsVersionOneFrames_WithEightByteRowIds()
    {
        // A frame as written before row keys were widened: i64 row id in an 18-byte operation header.
        byte[] database = "Db"u8.ToArray();
        byte[] table = "T"u8.ToArray();
        byte[] value = "row"u8.ToArray();
        byte[] payload = new byte[8 + 18 + database.Length + table.Length + value.Length];
        BinaryPrimitives.WriteUInt16LittleEndian(payload, 1);
        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(4), 1);
        Span<byte> header = payload.AsSpan(8, 18);
        header[0] = (byte)LsmCommitOperationKind.Put;
        BinaryPrimitives.WriteUInt16LittleEndian(header[2..], (ushort)database.Length);
        BinaryPrimitives.WriteUInt16LittleEndian(header[4..], (ushort)table.Length);
        BinaryPrimitives.WriteInt64LittleEndian(header[6..], 77);
        BinaryPrimitives.WriteInt32LittleEndian(header[14..], value.Length);
        database.CopyTo(payload.AsSpan(26));
        table.CopyTo(payload.AsSpan(26 + database.Length));
        value.CopyTo(payload.AsSpan(26 + database.Length + table.Length));

        Assert.True(LsmCommitBatchCodec.TryRead(payload, out List<LsmCommitOperation> operations));
        LsmCommitOperation operation = Assert.Single(operations);
        Assert.Equal((RowKey)77, operation.RowId);
        Assert.Equal("T", operation.Table);
        Assert.Equal(value, operation.Value);

        // Version 1 only ever held engine row ids: a negative one is corruption.
        BinaryPrimitives.WriteInt64LittleEndian(header[6..], -1);
        Assert.False(LsmCommitBatchCodec.TryRead(payload, out _));
    }

    [Theory]
    [InlineData(LsmWalDurabilityMode.StrictFsync)]
    [InlineData(LsmWalDurabilityMode.RelaxedOsBuffer)]
    public void AppendedFrames_AreReadBackInOrder_AfterReopen(LsmWalDurabilityMode mode)
    {
        using (var log = new LsmCommitLog(LogPath, mode))
        {
            Assert.Equal(1, AppendAndWait(log, "a", 1));
            Assert.Equal(2, AppendAndWait(log, "b", 2));
            Assert.Equal(3, AppendAndWait(log, "c", 3));
        }

        using var reopened = new LsmCommitLog(LogPath, mode);
        IReadOnlyList<LsmCommitFrame> frames = reopened.ReadAll();

        Assert.Equal([1L, 2L, 3L], frames.Select(f => f.Lsn));
        Assert.Equal(["a", "b", "c"], frames.Select(f => f.Operations.Single().Table));
        Assert.Equal(4, AppendAndWait(reopened, "d", 4));
    }

    [Fact]
    public void TornTail_IsTruncatedOnOpen_SoLaterAppendsStayReadable()
    {
        using (var log = new LsmCommitLog(LogPath, LsmWalDurabilityMode.StrictFsync))
        {
            AppendAndWait(log, "a", 1);
            AppendAndWait(log, "b", 2);
            AppendAndWait(log, "c", 3);
        }

        using (FileStream stream = File.Open(LogPath, FileMode.Open))
        {
            stream.SetLength(stream.Length - 3); // the last frame was only partly written
        }

        using (var recovered = new LsmCommitLog(LogPath, LsmWalDurabilityMode.StrictFsync))
        {
            Assert.Equal([1L, 2L], recovered.ReadAll().Select(f => f.Lsn));
            Assert.Equal(3, AppendAndWait(recovered, "d", 4));
        }

        using var reopened = new LsmCommitLog(LogPath, LsmWalDurabilityMode.StrictFsync);
        Assert.Equal(["a", "b", "d"], reopened.ReadAll().Select(f => f.Operations.Single().Table));
    }

    [Fact]
    public void ChecksumMismatch_EndsTheValidPrefix()
    {
        using (var log = new LsmCommitLog(LogPath, LsmWalDurabilityMode.StrictFsync))
        {
            AppendAndWait(log, "a", 1);
            AppendAndWait(log, "b", 2);
        }

        byte[] bytes = File.ReadAllBytes(LogPath);
        bytes[^1] ^= 0xFF; // corrupt the second frame's payload
        File.WriteAllBytes(LogPath, bytes);

        using var recovered = new LsmCommitLog(LogPath, LsmWalDurabilityMode.StrictFsync);
        Assert.Equal([1L], recovered.ReadAll().Select(f => f.Lsn));
    }

    [Fact]
    public void Prune_DropsCoveredFrames_AndLsnFloorKeepsLsnsIncreasing()
    {
        using (var log = new LsmCommitLog(LogPath, LsmWalDurabilityMode.StrictFsync))
        {
            AppendAndWait(log, "a", 1);
            AppendAndWait(log, "b", 2);
            AppendAndWait(log, "c", 3);

            log.PruneThrough(2);
            Assert.Equal([3L], log.ReadAll().Select(f => f.Lsn));

            log.PruneThrough(3);
            Assert.Empty(log.ReadAll());
            Assert.Equal(4, AppendAndWait(log, "d", 4));
            log.PruneThrough(4);
        }

        // Every frame is pruned, so the file alone no longer knows the last LSN; the owner supplies it.
        using var reopened = new LsmCommitLog(LogPath, LsmWalDurabilityMode.StrictFsync);
        reopened.EnsureLsnAtLeast(4);
        Assert.Equal(5, AppendAndWait(reopened, "e", 5));
    }

    [Fact]
    public void ConcurrentStrictAppends_AreAllDurable_WithUniqueLsns()
    {
        const int threads = 8;
        const int perThread = 100;
        using (var log = new LsmCommitLog(LogPath, LsmWalDurabilityMode.StrictFsync))
        {
            Parallel.For(0, threads, new ParallelOptions { MaxDegreeOfParallelism = threads }, t =>
            {
                for (int i = 0; i < perThread; i++)
                {
                    AppendAndWait(log, $"t{t}", t * perThread + i);
                }
            });

            Assert.True(log.DurableFlushCount <= threads * perThread);
        }

        using var reopened = new LsmCommitLog(LogPath, LsmWalDurabilityMode.StrictFsync);
        IReadOnlyList<LsmCommitFrame> frames = reopened.ReadAll();
        Assert.Equal(Enumerable.Range(1, threads * perThread).Select(i => (long)i), frames.Select(f => f.Lsn));
    }

    [Fact]
    public void EmptyBatch_IsRejected()
    {
        using var log = new LsmCommitLog(LogPath, LsmWalDurabilityMode.StrictFsync);
        Assert.Throws<ArgumentException>(() => log.Append(new LsmCommitBatch()));
    }

    public void Dispose()
    {
    }

    private static long AppendAndWait(LsmCommitLog log, string table, long rowId)
    {
        var batch = new LsmCommitBatch();
        batch.Put("Db", table, rowId, BitConverter.GetBytes(rowId));
        LsmCommitTicket ticket = log.Append(batch);
        ticket.Wait();
        return ticket.Lsn;
    }

    private static void AssertSameOperations(IReadOnlyList<LsmCommitOperation> expected, IReadOnlyList<LsmCommitOperation> actual)
    {
        Assert.Equal(expected.Count, actual.Count);
        for (int i = 0; i < expected.Count; i++)
        {
            Assert.Equal(expected[i].Kind, actual[i].Kind);
            Assert.Equal(expected[i].Database, actual[i].Database);
            Assert.Equal(expected[i].Table, actual[i].Table);
            Assert.Equal(expected[i].RowId, actual[i].RowId);
            Assert.Equal(expected[i].Value, actual[i].Value);
        }
    }
}
