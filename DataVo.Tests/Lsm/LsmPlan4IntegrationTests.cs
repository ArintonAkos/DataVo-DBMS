using System.Text;
using DataVo.Core.StorageEngine.Lsm;

namespace DataVo.Tests.Lsm;

public sealed class LsmPlan4IntegrationTests
{
    [Fact]
    public void FlushesMultipleMemTablesThenCompactsToLevelOneAndReloadsManifest()
    {
        using var fixture = new Plan4Fixture();
        using var table = new LsmTable(fixture.TableDirectory, fixture.Manifest);

        table.Put(Key(1), seqno: 1, Val("one-v1"));
        table.Put(Key(2), seqno: 2, Val("two-v2"));
        LsmFlushResult firstFlush = table.FlushActiveMemTable()!.Value;

        table.Put(Key(1), seqno: 5, Val("one-v5"));
        table.Delete(Key(2), seqno: 6);
        table.Put(Key(3), seqno: 7, Val("three-v7"));
        LsmFlushResult secondFlush = table.FlushActiveMemTable()!.Value;

        Assert.Equal([firstFlush.Metadata.FileNumber, secondFlush.Metadata.FileNumber],
            fixture.Manifest.GetLiveFiles(0).Select(file => file.FileNumber));

        var compactor = new LsmCompactor(fixture.TableDirectory, fixture.Manifest);
        LsmCompactionResult compaction = compactor.CompactLevel(0, 1)!.Value;

        Assert.Equal([firstFlush.Metadata.FileNumber, secondFlush.Metadata.FileNumber],
            compaction.DeletedFiles.Select(file => file.FileNumber).Order());
        Assert.Empty(fixture.Manifest.GetLiveFiles(0));
        LsmTableFileMetadata levelOne = Assert.Single(fixture.Manifest.GetLiveFiles(1));
        Assert.Equal(compaction.OutputFile!.Value.FileNumber, levelOne.FileNumber);

        var reloaded = new LsmManifest(fixture.ManifestPath);
        LsmTableFileMetadata reloadedLevelOne = Assert.Single(reloaded.GetLiveFiles(1));
        Assert.Equal(levelOne.FileNumber, reloadedLevelOne.FileNumber);
        Assert.Empty(reloaded.GetLiveFiles(0));

        SsTableReader reader = fixture.LoadReader(reloadedLevelOne);
        AssertValue(reader, pk: 1, snapshotSeqno: 5, "one-v5");
        Assert.False(reader.TryGet(Key(2), snapshotSeqno: 6, out byte[] deleted, out bool isTombstone));
        Assert.True(isTombstone);
        Assert.Empty(deleted);
        AssertValue(reader, pk: 3, snapshotSeqno: 7, "three-v7");
        Assert.False(reader.TryGet(Key(1), snapshotSeqno: 1, out _, out bool olderSnapshotTombstone));
        Assert.False(olderSnapshotTombstone);
    }

    [Fact]
    public void BottomLevelCompactionDropsTombstoneOnlyOutputAndPersistsDeletion()
    {
        using var fixture = new Plan4Fixture();
        using var table = new LsmTable(fixture.TableDirectory, fixture.Manifest);

        table.Delete(Key(9), seqno: 10);
        LsmFlushResult flushed = table.FlushActiveMemTable()!.Value;

        var compactor = new LsmCompactor(fixture.TableDirectory, fixture.Manifest);
        LsmCompactionResult compaction = compactor.CompactLevel(0, 1, dropTombstonesAtBottomLevel: true)!.Value;

        Assert.Null(compaction.OutputFile);
        Assert.Equal([flushed.Metadata.FileNumber], compaction.DeletedFiles.Select(file => file.FileNumber));
        Assert.Empty(fixture.Manifest.GetLiveFiles(0));
        Assert.Empty(fixture.Manifest.GetLiveFiles(1));

        var reloaded = new LsmManifest(fixture.ManifestPath);
        Assert.Empty(reloaded.GetLiveFiles(0));
        Assert.Empty(reloaded.GetLiveFiles(1));
        Assert.Empty(Directory.EnumerateFiles(fixture.TableDirectory, "*.sst"));
    }

    private static void AssertValue(SsTableReader reader, long pk, ulong snapshotSeqno, string expectedValue)
    {
        Assert.True(reader.TryGet(Key(pk), snapshotSeqno, out byte[] value, out bool isTombstone));
        Assert.False(isTombstone);
        Assert.Equal(Val(expectedValue), value);
    }

    private static byte[] Key(long pk)
    {
        var buf = new byte[8];
        InternalKey.EncodeInt64UserKey(buf, pk);
        return buf;
    }

    private static byte[] Val(string value) => Encoding.UTF8.GetBytes(value);

    private sealed class Plan4Fixture : IDisposable
    {
        public Plan4Fixture()
        {
            TableDirectory = Path.Combine(Path.GetTempPath(), "datavo-lsm-plan4-integration-tests", Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(TableDirectory);
            ManifestPath = Path.Combine(TableDirectory, "MANIFEST");
            Manifest = new LsmManifest(ManifestPath);
        }

        public string TableDirectory { get; }

        public string ManifestPath { get; }

        public LsmManifest Manifest { get; }

        public SsTableReader LoadReader(LsmTableFileMetadata metadata) =>
            SsTableReader.Load(File.ReadAllBytes(Path.Combine(TableDirectory, metadata.FileName)));

        public void Dispose()
        {
            if (Directory.Exists(TableDirectory))
            {
                Directory.Delete(TableDirectory, recursive: true);
            }
        }
    }
}
