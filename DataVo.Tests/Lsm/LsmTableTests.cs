using System.Text;
using DataVo.Core.StorageEngine.Lsm;
using DataVo.Core.Transactions;

namespace DataVo.Tests.Lsm;

public sealed class LsmTableTests
{
    [Fact]
    public void PutDelete_UpdateActiveCountAndEmptyFlushIsNoOp()
    {
        using var fixture = new TableFixture();
        var table = new LsmTable(fixture.TableDirectory, fixture.Manifest);

        Assert.Equal(0, table.ActiveCount);
        Assert.Null(table.FlushActiveMemTable());
        Assert.Empty(fixture.Manifest.GetLiveFiles(0));

        table.Put(Key(1), seqno: 1, Val("one"));
        table.Delete(Key(1), seqno: 2);

        Assert.Equal(2, table.ActiveCount);
    }

    [Fact]
    public void Put_WithWalEnabled_AppendsBinaryWalFrameBeforeActiveWrite()
    {
        using var fixture = new TableFixture();
        var table = new LsmTable(
            fixture.TableDirectory,
            fixture.Manifest,
            fixture.WalPath,
            LsmWalDurabilityMode.StrictFsync);
        byte[] userKey = Key(1);
        byte[] value = Val("one");

        table.Put(userKey, seqno: 7, value);

        WalFrameRecord frame = Assert.Single(new WalFileStore(fixture.WalPath).ReadBinaryFrames());
        Assert.Equal(WalFrameOperationType.Insert, frame.Header.OpType);
        Assert.True(LsmWalRecordCodec.TryRead(frame.Payload, out LsmWalRecord record));
        Assert.Equal(userKey, record.UserKey);
        Assert.Equal(7UL, record.Seqno);
        Assert.Equal(LsmValueType.Put, record.ValueType);
        Assert.Equal(value, record.Value);
        Assert.Equal(1, table.ActiveCount);
    }

    [Fact]
    public void Delete_WithWalEnabled_AppendsTombstoneWalFrame()
    {
        using var fixture = new TableFixture();
        var table = new LsmTable(
            fixture.TableDirectory,
            fixture.Manifest,
            fixture.WalPath,
            LsmWalDurabilityMode.StrictFsync);
        byte[] userKey = Key(3);

        table.Delete(userKey, seqno: 9);

        WalFrameRecord frame = Assert.Single(new WalFileStore(fixture.WalPath).ReadBinaryFrames());
        Assert.Equal(WalFrameOperationType.Delete, frame.Header.OpType);
        Assert.True(LsmWalRecordCodec.TryRead(frame.Payload, out LsmWalRecord record));
        Assert.Equal(userKey, record.UserKey);
        Assert.Equal(9UL, record.Seqno);
        Assert.Equal(LsmValueType.Deletion, record.ValueType);
        Assert.Empty(record.Value);
        Assert.Equal(1, table.ActiveCount);
    }

    [Fact]
    public void Put_WhenWalAppendFails_DoesNotMutateActiveMemTable()
    {
        using var fixture = new TableFixture();
        Directory.CreateDirectory(fixture.WalPath);
        var table = new LsmTable(
            fixture.TableDirectory,
            fixture.Manifest,
            fixture.WalPath,
            LsmWalDurabilityMode.StrictFsync);

        Assert.Throws<UnauthorizedAccessException>(() => table.Put(Key(1), seqno: 1, Val("one")));
        Assert.Equal(0, table.ActiveCount);
    }

    [Fact]
    public void Constructor_WithWalEnabled_ReplaysUnflushedPutIntoActiveMemTable()
    {
        using var fixture = new TableFixture();
        byte[] userKey = Key(4);
        byte[] value = Val("wal-only");
        using (var table = new LsmTable(
            fixture.TableDirectory,
            fixture.Manifest,
            fixture.WalPath,
            LsmWalDurabilityMode.StrictFsync))
        {
            table.Put(userKey, seqno: 11, value);
        }

        using var reopened = new LsmTable(
            fixture.TableDirectory,
            fixture.Manifest,
            fixture.WalPath,
            LsmWalDurabilityMode.StrictFsync);

        LsmFlushResult result = reopened.FlushActiveMemTable()!.Value;
        SsTableReader reader = SsTableReader.Load(File.ReadAllBytes(result.FilePath));
        Assert.True(reader.TryGet(userKey, snapshotSeqno: 11, out byte[] actual, out bool tombstone));
        Assert.False(tombstone);
        Assert.Equal(value, actual);
    }

    [Fact]
    public void Constructor_WithWalEnabled_StopsReplayAtTornTail()
    {
        using var fixture = new TableFixture();
        byte[] userKey = Key(6);
        byte[] value = Val("valid-prefix");
        using (var table = new LsmTable(
            fixture.TableDirectory,
            fixture.Manifest,
            fixture.WalPath,
            LsmWalDurabilityMode.StrictFsync))
        {
            table.Put(userKey, seqno: 13, value);
        }

        using (var stream = new FileStream(fixture.WalPath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite | FileShare.Delete))
        {
            stream.Write([0x01, 0x02, 0x03]);
            stream.Flush(flushToDisk: true);
        }

        using var reopened = new LsmTable(
            fixture.TableDirectory,
            fixture.Manifest,
            fixture.WalPath,
            LsmWalDurabilityMode.StrictFsync);

        LsmFlushResult result = reopened.FlushActiveMemTable()!.Value;
        SsTableReader reader = SsTableReader.Load(File.ReadAllBytes(result.FilePath));
        Assert.True(reader.TryGet(userKey, snapshotSeqno: 13, out byte[] actual, out bool tombstone));
        Assert.False(tombstone);
        Assert.Equal(value, actual);
    }

    [Fact]
    public void WalWriter_StrictModeFlushesToDiskAndRelaxedModeOnlyAppends()
    {
        using var fixture = new TableFixture();
        byte[] key = Key(8);
        var strict = new LsmWalWriter(Path.Combine(fixture.TableDirectory, "strict.wal"), LsmWalDurabilityMode.StrictFsync);
        var relaxed = new LsmWalWriter(Path.Combine(fixture.TableDirectory, "relaxed.wal"), LsmWalDurabilityMode.RelaxedOsBuffer);

        strict.AppendMutation(key, seqno: 1, LsmValueType.Put, Val("strict"));
        relaxed.AppendMutation(key, seqno: 1, LsmValueType.Put, Val("relaxed"));

        Assert.Equal(1, strict.DurableFlushCount);
        Assert.Equal(0, relaxed.DurableFlushCount);
        Assert.Single(new WalFileStore(Path.Combine(fixture.TableDirectory, "strict.wal")).ReadBinaryFrames());
        Assert.Single(new WalFileStore(Path.Combine(fixture.TableDirectory, "relaxed.wal")).ReadBinaryFrames());
    }

    [Theory]
    [InlineData((int)LsmCrashPoint.AfterSstableTempFileFsyncBeforeRename, false, true)]
    [InlineData((int)LsmCrashPoint.AfterSstableRenameBeforeDirectoryFsync, true, false)]
    [InlineData((int)LsmCrashPoint.AfterSstableDirectoryFsyncBeforeManifest, true, false)]
    [InlineData((int)LsmCrashPoint.AfterManifestTempFileFsyncBeforeRename, true, true)]
    [InlineData((int)LsmCrashPoint.AfterManifestRenameBeforeDirectoryFsync, true, false)]
    public void FlushActiveMemTable_WhenCrashBeforeWalClear_ReopenIgnoresOrphansAndReplaysWal(
        int crashPointValue,
        bool expectOrphanSstable,
        bool expectTempFile)
    {
        using var fixture = new TableFixture();
        var crashPoint = (LsmCrashPoint)crashPointValue;
        byte[] userKey = Key(12);
        byte[] value = Val("wal-survives");
        using var table = LsmTable.CreateForTesting(
            fixture.TableDirectory,
            fixture.Manifest,
            fixture.WalPath,
            LsmWalDurabilityMode.StrictFsync,
            point =>
            {
                if (point == crashPoint)
                {
                    throw new LsmCrashSimulationException(point);
                }
            });
        table.Put(userKey, seqno: 21, value);

        LsmCrashSimulationException ex = Assert.Throws<LsmCrashSimulationException>(() => table.FlushActiveMemTable());
        Assert.Equal(crashPoint, ex.CrashPoint);
        Assert.True(File.Exists(fixture.WalPath));
        Assert.Equal(expectOrphanSstable, File.Exists(Path.Combine(fixture.TableDirectory, "000001.sst")));
        Assert.Equal(expectTempFile, Directory.EnumerateFiles(fixture.TableDirectory, "*.tmp-*").Any());

        using var reopened = new LsmTable(
            fixture.TableDirectory,
            new LsmManifest(Path.Combine(fixture.TableDirectory, "MANIFEST")),
            fixture.WalPath,
            LsmWalDurabilityMode.StrictFsync);

        LsmFlushResult recovered = reopened.FlushActiveMemTable()!.Value;
        SsTableReader reader = SsTableReader.Load(File.ReadAllBytes(recovered.FilePath));
        Assert.True(reader.TryGet(userKey, snapshotSeqno: 21, out byte[] actual, out bool tombstone));
        Assert.False(tombstone);
        Assert.Equal(value, actual);
    }

    [Fact]
    public void FlushActiveMemTable_WhenManifestCommittedBeforeWalClear_ReopenKeepsManifestAndReplaysIdempotently()
    {
        using var fixture = new TableFixture();
        byte[] userKey = Key(14);
        byte[] value = Val("manifest-committed");
        using var table = LsmTable.CreateForTesting(
            fixture.TableDirectory,
            fixture.Manifest,
            fixture.WalPath,
            LsmWalDurabilityMode.StrictFsync,
            point =>
            {
                if (point == LsmCrashPoint.AfterManifestDirectoryFsyncBeforeWalClear)
                {
                    throw new LsmCrashSimulationException(point);
                }
            });
        table.Put(userKey, seqno: 23, value);

        Assert.Throws<LsmCrashSimulationException>(() => table.FlushActiveMemTable());
        Assert.True(File.Exists(fixture.WalPath));
        LsmTableFileMetadata committed = Assert.Single(new LsmManifest(Path.Combine(fixture.TableDirectory, "MANIFEST")).GetLiveFiles(0));

        using var reopened = new LsmTable(
            fixture.TableDirectory,
            new LsmManifest(Path.Combine(fixture.TableDirectory, "MANIFEST")),
            fixture.WalPath,
            LsmWalDurabilityMode.StrictFsync);

        LsmFlushResult replayed = reopened.FlushActiveMemTable()!.Value;
        Assert.True(File.Exists(Path.Combine(fixture.TableDirectory, committed.FileName)));
        Assert.True(File.Exists(replayed.FilePath));
    }

    [Fact]
    public void FlushActiveMemTable_WritesSstableAndRegistersOneLevelZeroFile()
    {
        using var fixture = new TableFixture();
        var table = new LsmTable(fixture.TableDirectory, fixture.Manifest);
        table.Put(Key(1), seqno: 1, Val("one"));
        table.Put(Key(2), seqno: 2, Val("two"));

        LsmFlushResult? result = table.FlushActiveMemTable();

        Assert.NotNull(result);
        Assert.Equal(0, table.ActiveCount);
        Assert.Equal(Path.Combine(fixture.TableDirectory, "000001.sst"), result.Value.FilePath);
        Assert.True(File.Exists(result.Value.FilePath));
        Assert.Equal(new FileInfo(result.Value.FilePath).Length, result.Value.ByteCount);

        LsmTableFileMetadata live = Assert.Single(fixture.Manifest.GetLiveFiles(0));
        Assert.Equal(1, live.FileNumber);
        Assert.Equal(0, live.Level);
        Assert.Equal("000001.sst", live.FileName);
        Assert.Equal(result.Value.ByteCount, live.FileSize);
        Assert.Equal(result.Value.Metadata.FileNumber, live.FileNumber);
        Assert.Equal(result.Value.Metadata.SmallestInternalKey, live.SmallestInternalKey);
        Assert.Equal(result.Value.Metadata.LargestInternalKey, live.LargestInternalKey);
    }

    [Fact]
    public void FlushActiveMemTable_WithWalEnabled_ClearsWalOnlyAfterManifestCommit()
    {
        using var fixture = new TableFixture();
        var table = new LsmTable(
            fixture.TableDirectory,
            fixture.Manifest,
            fixture.WalPath,
            LsmWalDurabilityMode.StrictFsync);
        table.Put(Key(1), seqno: 1, Val("one"));
        Assert.True(File.Exists(fixture.WalPath));

        LsmFlushResult? result = table.FlushActiveMemTable();

        Assert.NotNull(result);
        Assert.Single(fixture.Manifest.GetLiveFiles(0));
        Assert.False(File.Exists(fixture.WalPath));
    }

    [Fact]
    public void FlushActiveMemTable_ReplacesActiveTableAndAllocatesIncreasingFiles()
    {
        using var fixture = new TableFixture();
        var table = new LsmTable(fixture.TableDirectory, fixture.Manifest);
        table.Put(Key(1), seqno: 1, Val("one"));

        LsmFlushResult? first = table.FlushActiveMemTable();
        table.Put(Key(2), seqno: 2, Val("two"));
        LsmFlushResult? second = table.FlushActiveMemTable();

        Assert.NotNull(first);
        Assert.NotNull(second);
        Assert.Equal(0, table.ActiveCount);
        Assert.Equal(1, first.Value.Metadata.FileNumber);
        Assert.Equal(2, second.Value.Metadata.FileNumber);
        Assert.Equal("000001.sst", first.Value.Metadata.FileName);
        Assert.Equal("000002.sst", second.Value.Metadata.FileName);
        Assert.True(File.Exists(first.Value.FilePath));
        Assert.True(File.Exists(second.Value.FilePath));
        Assert.Equal([1L, 2L], fixture.Manifest.GetLiveFiles(0).Select(file => file.FileNumber));
    }

    [Fact]
    public void FlushedSstable_IsReadableAndPreservesVersionAndTombstoneSemantics()
    {
        using var fixture = new TableFixture();
        var table = new LsmTable(fixture.TableDirectory, fixture.Manifest);
        table.Put(Key(7), seqno: 10, Val("old"));
        table.Put(Key(7), seqno: 20, Val("new"));
        table.Put(Key(3), seqno: 4, Val("alive"));
        table.Delete(Key(3), seqno: 8);

        LsmFlushResult result = table.FlushActiveMemTable()!.Value;
        SsTableReader reader = SsTableReader.Load(File.ReadAllBytes(result.FilePath));

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
    public void FlushActiveMemTable_MetadataBoundsMatchInternalKeyOrder()
    {
        using var fixture = new TableFixture();
        var table = new LsmTable(fixture.TableDirectory, fixture.Manifest);
        table.Put(Key(5), seqno: 2, Val("five-old"));
        table.Put(Key(1), seqno: 1, Val("one"));
        table.Put(Key(5), seqno: 8, Val("five-new"));
        table.Put(Key(3), seqno: 4, Val("three"));

        LsmFlushResult result = table.FlushActiveMemTable()!.Value;
        byte[] expectedSmallest = Internal(1, seqno: 1, LsmValueType.Put);
        byte[] expectedLargest = Internal(5, seqno: 2, LsmValueType.Put);

        Assert.Equal(expectedSmallest, result.Metadata.SmallestInternalKey);
        Assert.Equal(expectedLargest, result.Metadata.LargestInternalKey);
        Assert.True(InternalKey.Compare(result.Metadata.SmallestInternalKey, result.Metadata.LargestInternalKey) <= 0);

        LsmTableFileMetadata live = Assert.Single(fixture.Manifest.GetLiveFiles(0));
        Assert.Equal(expectedSmallest, live.SmallestInternalKey);
        Assert.Equal(expectedLargest, live.LargestInternalKey);
    }

    [Fact]
    public void FlushActiveMemTable_ReturnedBytesAndFileRemainValidAfterNewActiveWrites()
    {
        using var fixture = new TableFixture();
        var table = new LsmTable(fixture.TableDirectory, fixture.Manifest);
        table.Put(Key(1), seqno: 1, Val("one"));

        LsmFlushResult result = table.FlushActiveMemTable()!.Value;
        byte[] originalBytes = File.ReadAllBytes(result.FilePath);

        table.Put(Key(2), seqno: 2, Val("two"));
        table.Put(Key(1), seqno: 3, Val("one-new-active"));

        Assert.Equal(originalBytes, File.ReadAllBytes(result.FilePath));
        SsTableReader reader = SsTableReader.Load(result.Bytes);
        Assert.True(reader.TryGet(Key(1), snapshotSeqno: 3, out byte[] value, out bool isTombstone));
        Assert.False(isTombstone);
        Assert.Equal(Val("one"), value);
    }

    [Fact]
    public void FlushActiveMemTable_WhenSstableMoveFails_KeepsActiveGenerationRetryable()
    {
        using var fixture = new TableFixture();
        var table = new LsmTable(fixture.TableDirectory, fixture.Manifest);
        table.Put(Key(1), seqno: 1, Val("one"));
        Directory.CreateDirectory(Path.Combine(fixture.TableDirectory, "000001.sst"));

        Assert.Throws<IOException>(() => table.FlushActiveMemTable());

        Assert.Equal(1, table.ActiveCount);
        Assert.Empty(fixture.Manifest.GetLiveFiles(0));

        Directory.Delete(Path.Combine(fixture.TableDirectory, "000001.sst"));
        LsmFlushResult retried = table.FlushActiveMemTable()!.Value;
        SsTableReader reader = SsTableReader.Load(File.ReadAllBytes(retried.FilePath));
        Assert.True(reader.TryGet(Key(1), snapshotSeqno: 1, out byte[] value, out bool isTombstone));
        Assert.False(isTombstone);
        Assert.Equal(Val("one"), value);
    }

    [Fact]
    public void FlushActiveMemTable_WhenManifestApplyFails_RemovesOrphanAndKeepsActiveGenerationRetryable()
    {
        using var fixture = new TableFixture();
        var table = LsmTable.CreateForTesting(
            fixture.TableDirectory,
            fixture.Manifest,
            applyEdit: _ => throw new InvalidDataException("manifest failure"));
        table.Put(Key(1), seqno: 1, Val("one"));

        Assert.Throws<InvalidDataException>(() => { _ = table.FlushActiveMemTable(); });

        Assert.Equal(1, table.ActiveCount);
        Assert.Empty(fixture.Manifest.GetLiveFiles(0));
        Assert.False(File.Exists(Path.Combine(fixture.TableDirectory, "000001.sst")));

        using var retryTable = new LsmTable(fixture.TableDirectory, fixture.Manifest);
        retryTable.Put(Key(2), seqno: 2, Val("two"));
        LsmFlushResult retried = retryTable.FlushActiveMemTable()!.Value;
        Assert.Equal("000002.sst", retried.Metadata.FileName);
        Assert.Single(fixture.Manifest.GetLiveFiles(0));
    }

    private static byte[] Key(long pk)
    {
        var buf = new byte[8];
        InternalKey.EncodeInt64UserKey(buf, pk);
        return buf;
    }

    private static byte[] Internal(long pk, ulong seqno, LsmValueType valueType)
    {
        byte[] userKey = Key(pk);
        var internalKey = new byte[InternalKey.MeasureSize(userKey.Length)];
        InternalKey.Write(internalKey, userKey, seqno, valueType);
        return internalKey;
    }

    private static byte[] Val(string value) => Encoding.UTF8.GetBytes(value);

    private sealed class TableFixture : IDisposable
    {
        public TableFixture()
        {
            TableDirectory = Path.Combine(Path.GetTempPath(), "datavo-lsm-table-tests", Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(TableDirectory);
            Manifest = new LsmManifest(Path.Combine(TableDirectory, "MANIFEST"));
        }

        public string TableDirectory { get; }

        public LsmManifest Manifest { get; }

        public string WalPath => Path.Combine(TableDirectory, "active.wal");

        public void Dispose()
        {
            if (Directory.Exists(TableDirectory))
            {
                Directory.Delete(TableDirectory, recursive: true);
            }
        }
    }
}
