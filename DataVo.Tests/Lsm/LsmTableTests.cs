using System.Text;
using DataVo.Core.StorageEngine.Lsm;

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

    [Theory]
    [InlineData((int)LsmCrashPoint.AfterSstableTempFileFsyncBeforeRename, false, true)]
    [InlineData((int)LsmCrashPoint.AfterSstableRenameBeforeDirectoryFsync, true, false)]
    [InlineData((int)LsmCrashPoint.AfterSstableDirectoryFsyncBeforeManifest, true, false)]
    [InlineData((int)LsmCrashPoint.AfterManifestTempFileFsyncBeforeRename, true, true)]
    [InlineData((int)LsmCrashPoint.AfterManifestRenameBeforeDirectoryFsync, true, false)]
    public void FlushActiveMemTable_WhenCrashMidPublish_KeepsGenerationAndReopenIgnoresOrphans(
        int crashPointValue,
        bool expectOrphanSstable,
        bool expectTempFile)
    {
        using var fixture = new TableFixture();
        var crashPoint = (LsmCrashPoint)crashPointValue;
        byte[] userKey = Key(12);
        byte[] value = Val("flushed-after-reopen");
        using var table = LsmTable.CreateForTesting(
            fixture.TableDirectory,
            fixture.Manifest,
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
        Assert.Equal(1, table.ActiveCount);
        Assert.Equal(expectOrphanSstable, File.Exists(Path.Combine(fixture.TableDirectory, "000001.sst")));
        Assert.Equal(expectTempFile, Directory.EnumerateFiles(fixture.TableDirectory, "*.tmp-*").Any());

        // The owner's commit log would replay the lost rows; here the test writes them again.
        using var reopened = new LsmTable(
            fixture.TableDirectory,
            new LsmManifest(Path.Combine(fixture.TableDirectory, "MANIFEST")));
        reopened.Put(userKey, seqno: 21, value);

        LsmFlushResult recovered = reopened.FlushActiveMemTable()!.Value;
        TestSsTable reader = TestSsTable.Open(recovered.FilePath);
        Assert.True(reader.TryGet(userKey, snapshotSeqno: 21, out byte[] actual, out bool tombstone));
        Assert.False(tombstone);
        Assert.Equal(value, actual);
    }

    [Fact]
    public void FlushActiveMemTable_WhenCrashAfterManifestCommit_ReopenKeepsCommittedFile()
    {
        using var fixture = new TableFixture();
        byte[] userKey = Key(14);
        byte[] value = Val("manifest-committed");
        using var table = LsmTable.CreateForTesting(
            fixture.TableDirectory,
            fixture.Manifest,
            point =>
            {
                if (point == LsmCrashPoint.AfterManifestDirectoryFsync)
                {
                    throw new LsmCrashSimulationException(point);
                }
            });
        table.Put(userKey, seqno: 23, value);

        Assert.Throws<LsmCrashSimulationException>(() => table.FlushActiveMemTable());
        LsmTableFileMetadata committed = Assert.Single(new LsmManifest(Path.Combine(fixture.TableDirectory, "MANIFEST")).GetLiveFiles(0));

        using var reopened = new LsmTable(
            fixture.TableDirectory,
            new LsmManifest(Path.Combine(fixture.TableDirectory, "MANIFEST")));

        Assert.Equal(0, reopened.ActiveCount);
        TestSsTable reader = TestSsTable.Open(Path.Combine(fixture.TableDirectory, committed.FileName));
        Assert.True(reader.TryGet(userKey, snapshotSeqno: 23, out byte[] actual, out bool tombstone));
        Assert.False(tombstone);
        Assert.Equal(value, actual);
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
        TestSsTable reader = TestSsTable.Open(result.FilePath);

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
        TestSsTable reader = TestSsTable.Load(originalBytes);
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
        TestSsTable reader = TestSsTable.Open(retried.FilePath);
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

    [Fact]
    public void WaitForBackgroundWork_DrainsScheduledCompactionTriggeredByFlush()
    {
        using var fixture = new TableFixture();
        var registry = new LsmFileRegistry(fixture.TableDirectory, fixture.Manifest);
        using var table = new LsmTable(fixture.TableDirectory, fixture.Manifest)
        {
            CompactionRegistry = registry,
            Level0CompactionThreshold = 2,
        };

        table.Put(Key(1), seqno: 1, Val("v1"));
        Assert.NotNull(table.FlushActiveMemTable());

        table.Put(Key(1), seqno: 2, Val("v2"));
        Assert.NotNull(table.FlushActiveMemTable());

        table.WaitForBackgroundWork();

        Assert.Empty(fixture.Manifest.GetLiveFiles(0));
        Assert.Single(fixture.Manifest.GetLiveFiles(1));
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

        public void Dispose()
        {
            if (Directory.Exists(TableDirectory))
            {
                Directory.Delete(TableDirectory, recursive: true);
            }
        }
    }
}
