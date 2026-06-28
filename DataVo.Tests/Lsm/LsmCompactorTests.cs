using System.Buffers.Binary;
using System.Text;
using DataVo.Core.StorageEngine.Lsm;

namespace DataVo.Tests.Lsm;

public sealed class LsmCompactorTests
{
    [Fact]
    public void CompactLevel_TwoLevelZeroFilesWithSameUserKeyKeepsNewestValueInLevelOne()
    {
        using var fixture = new CompactorFixture();
        fixture.AddSstable(level: 0, [(Internal(7, 10, LsmValueType.Put), Val("old"))]);
        fixture.AddSstable(level: 0, [(Internal(7, 20, LsmValueType.Put), Val("new"))]);
        var compactor = new LsmCompactor(fixture.TableDirectory, fixture.Manifest);

        LsmCompactionResult result = compactor.CompactLevel(0, 1)!.Value;

        Assert.Empty(fixture.Manifest.GetLiveFiles(0));
        LsmTableFileMetadata output = Assert.Single(fixture.Manifest.GetLiveFiles(1));
        Assert.Equal(output.FileNumber, result.OutputFile!.Value.FileNumber);
        Assert.Equal(2, result.DeletedFiles.Count);
        SsTableReader reader = fixture.LoadReader(output);
        Assert.True(reader.TryGet(Key(7), snapshotSeqno: 20, out byte[] value, out bool isTombstone));
        Assert.False(isTombstone);
        Assert.Equal(Val("new"), value);
    }

    [Fact]
    public void CompactLevel_DropsOlderVersionsSoOlderSnapshotNoLongerFindsOldValue()
    {
        using var fixture = new CompactorFixture();
        fixture.AddSstable(level: 0, [(Internal(7, 10, LsmValueType.Put), Val("old"))]);
        fixture.AddSstable(level: 0, [(Internal(7, 20, LsmValueType.Put), Val("new"))]);
        var compactor = new LsmCompactor(fixture.TableDirectory, fixture.Manifest);

        LsmTableFileMetadata output = compactor.CompactLevel(0, 1)!.Value.OutputFile!.Value;

        SsTableReader reader = fixture.LoadReader(output);
        Assert.False(reader.TryGet(Key(7), snapshotSeqno: 10, out byte[] value, out bool isTombstone));
        Assert.False(isTombstone);
        Assert.Empty(value);
    }

    [Fact]
    public void CompactLevel_KeepsTombstoneWhenNotDroppingBottomLevelTombstones()
    {
        using var fixture = new CompactorFixture();
        fixture.AddSstable(level: 0, [(Internal(3, 4, LsmValueType.Put), Val("alive"))]);
        fixture.AddSstable(level: 0, [(Internal(3, 8, LsmValueType.Deletion), [])]);
        var compactor = new LsmCompactor(fixture.TableDirectory, fixture.Manifest);

        LsmTableFileMetadata output = compactor.CompactLevel(0, 1)!.Value.OutputFile!.Value;

        SsTableReader reader = fixture.LoadReader(output);
        Assert.False(reader.TryGet(Key(3), snapshotSeqno: 8, out byte[] value, out bool isTombstone));
        Assert.True(isTombstone);
        Assert.Empty(value);
    }

    [Fact]
    public void CompactLevel_DropsBottomLevelTombstoneAndDeletesInputsWithoutOutputWhenAllEntriesDropped()
    {
        using var fixture = new CompactorFixture();
        fixture.AddSstable(level: 0, [(Internal(3, 8, LsmValueType.Deletion), [])]);
        var compactor = new LsmCompactor(fixture.TableDirectory, fixture.Manifest);

        LsmCompactionResult result = compactor.CompactLevel(0, 1, dropTombstonesAtBottomLevel: true)!.Value;

        Assert.Null(result.OutputFile);
        Assert.Single(result.DeletedFiles);
        Assert.Empty(fixture.Manifest.GetLiveFiles(0));
        Assert.Empty(fixture.Manifest.GetLiveFiles(1));
        Assert.Empty(Directory.EnumerateFiles(fixture.TableDirectory, "*.sst"));
    }

    [Fact]
    public void CompactLevel_IncludesOverlappingTargetFilesAndPreservesNonOverlappingTargetFiles()
    {
        using var fixture = new CompactorFixture();
        fixture.AddSstable(level: 0, [(Internal(10, 6, LsmValueType.Put), Val("ten-new"))]);
        LsmTableFileMetadata overlapping = fixture.AddSstable(level: 1, [(Internal(9, 3, LsmValueType.Put), Val("nine")), (Internal(10, 2, LsmValueType.Put), Val("ten-old"))]);
        LsmTableFileMetadata nonOverlapping = fixture.AddSstable(level: 1, [(Internal(30, 1, LsmValueType.Put), Val("thirty"))]);
        var compactor = new LsmCompactor(fixture.TableDirectory, fixture.Manifest);

        LsmCompactionResult result = compactor.CompactLevel(0, 1)!.Value;

        Assert.Contains(result.DeletedFiles, file => file.FileNumber == overlapping.FileNumber);
        Assert.DoesNotContain(result.DeletedFiles, file => file.FileNumber == nonOverlapping.FileNumber);
        Assert.Contains(fixture.Manifest.GetLiveFiles(1), file => file.FileNumber == nonOverlapping.FileNumber);

        LsmTableFileMetadata output = result.OutputFile!.Value;
        SsTableReader reader = fixture.LoadReader(output);
        Assert.True(reader.TryGet(Key(9), snapshotSeqno: 3, out byte[] nine, out bool nineTombstone));
        Assert.False(nineTombstone);
        Assert.Equal(Val("nine"), nine);
        Assert.True(reader.TryGet(Key(10), snapshotSeqno: 6, out byte[] ten, out bool tenTombstone));
        Assert.False(tenTombstone);
        Assert.Equal(Val("ten-new"), ten);
    }

    [Fact]
    public void CompactLevel_ExpandsTargetOverlapSelectionUntilNoLevelOneFilesOverlapOutputRange()
    {
        using var fixture = new CompactorFixture();
        fixture.AddSstable(level: 0, [(Internal(20, 10, LsmValueType.Put), Val("twenty"))]);
        LsmTableFileMetadata targetA = fixture.AddSstable(
            level: 1,
            [
                (Internal(15, 7, LsmValueType.Put), Val("fifteen")),
                (Internal(25, 7, LsmValueType.Put), Val("twenty-five"))
            ]);
        LsmTableFileMetadata targetB = fixture.AddSstable(
            level: 1,
            [
                (Internal(24, 6, LsmValueType.Put), Val("twenty-four")),
                (Internal(30, 6, LsmValueType.Put), Val("thirty"))
            ]);
        var compactor = new LsmCompactor(fixture.TableDirectory, fixture.Manifest);

        LsmCompactionResult result = compactor.CompactLevel(0, 1)!.Value;

        Assert.Contains(result.DeletedFiles, file => file.FileNumber == targetA.FileNumber);
        Assert.Contains(result.DeletedFiles, file => file.FileNumber == targetB.FileNumber);
        LsmTableFileMetadata output = Assert.Single(fixture.Manifest.GetLiveFiles(1));
        Assert.Equal(output.FileNumber, result.OutputFile!.Value.FileNumber);
        Assert.True(InternalKey.UserKey(output.SmallestInternalKey).SequenceCompareTo(Key(15)) <= 0);
        Assert.True(InternalKey.UserKey(output.LargestInternalKey).SequenceCompareTo(Key(30)) >= 0);

        SsTableReader reader = fixture.LoadReader(output);
        AssertValue(reader, pk: 15, snapshotSeqno: 7, "fifteen");
        AssertValue(reader, pk: 20, snapshotSeqno: 10, "twenty");
        AssertValue(reader, pk: 24, snapshotSeqno: 6, "twenty-four");
        AssertValue(reader, pk: 25, snapshotSeqno: 7, "twenty-five");
        AssertValue(reader, pk: 30, snapshotSeqno: 6, "thirty");
    }

    [Fact]
    public void CompactLevel_WhenInputSstableReaderValidationFailsThrowsAndLeavesManifestUnchanged()
    {
        using var fixture = new CompactorFixture();
        LsmTableFileMetadata corrupt = fixture.AddSstable(
            level: 0,
            [
                (Internal(1, 1, LsmValueType.Put), Val("one")),
                (Internal(2, 1, LsmValueType.Put), Val("two"))
            ]);
        byte[] bytes = fixture.ReadSstable(corrupt);
        CorruptBySwappingDataEntries(bytes);
        fixture.WriteSstable(corrupt, bytes);
        var compactor = new LsmCompactor(fixture.TableDirectory, fixture.Manifest);

        Assert.Throws<InvalidDataException>(() => compactor.CompactLevel(0, 1));

        LsmTableFileMetadata liveFile = Assert.Single(fixture.Manifest.GetLiveFiles(0));
        Assert.Equal(corrupt.FileNumber, liveFile.FileNumber);
        Assert.Empty(fixture.Manifest.GetLiveFiles(1));
        Assert.Equal(["000001.sst"], Directory.EnumerateFiles(fixture.TableDirectory, "*.sst").Select(Path.GetFileName).Order());
    }

    [Fact]
    public void CompactLevel_WhenManifestApplyFailsCleansOutputAndLeavesInputsLive()
    {
        using var fixture = new CompactorFixture();
        LsmTableFileMetadata input = fixture.AddSstable(level: 0, [(Internal(1, 1, LsmValueType.Put), Val("one"))]);
        var compactor = LsmCompactor.CreateForTesting(
            fixture.TableDirectory,
            fixture.Manifest,
            applyEdit: _ => throw new InvalidDataException("manifest failure"));

        Assert.Throws<InvalidDataException>(() => compactor.CompactLevel(0, 1));

        Assert.Equal([input.FileNumber], fixture.Manifest.GetLiveFiles(0).Select(file => file.FileNumber));
        Assert.Empty(fixture.Manifest.GetLiveFiles(1));
        Assert.Equal(["000001.sst"], Directory.EnumerateFiles(fixture.TableDirectory, "*.sst").Select(Path.GetFileName).Order());
    }

    [Fact]
    public void CompactLevel_DefersPhysicalDeleteUntilSnapshotDisposed()
    {
        using var fixture = new CompactorFixture();
        LsmTableFileMetadata first = fixture.AddSstable(level: 0, [(Internal(1, 1, LsmValueType.Put), Val("one"))]);
        LsmTableFileMetadata second = fixture.AddSstable(level: 0, [(Internal(2, 2, LsmValueType.Put), Val("two"))]);
        var registry = new LsmFileRegistry(fixture.TableDirectory, fixture.Manifest);
        using LsmReadSnapshot snapshot = registry.OpenSnapshot();
        var compactor = new LsmCompactor(fixture.TableDirectory, fixture.Manifest, registry);

        LsmCompactionResult result = compactor.CompactLevel(0, 1)!.Value;

        Assert.Equal([first.FileNumber, second.FileNumber], result.DeletedFiles.Select(file => file.FileNumber).Order());
        Assert.Empty(fixture.Manifest.GetLiveFiles(0));
        Assert.Single(fixture.Manifest.GetLiveFiles(1));
        Assert.True(File.Exists(Path.Combine(fixture.TableDirectory, first.FileName)));
        Assert.True(File.Exists(Path.Combine(fixture.TableDirectory, second.FileName)));

        byte[] bytes = snapshot.ReadAllBytes(first.FileNumber);
        SsTableReader reader = SsTableReader.Load(bytes);
        Assert.True(reader.TryGet(Key(1), snapshotSeqno: 1, out byte[] value, out bool tombstone));
        Assert.False(tombstone);
        Assert.Equal(Val("one"), value);

        snapshot.Dispose();

        Assert.False(File.Exists(Path.Combine(fixture.TableDirectory, first.FileName)));
        Assert.False(File.Exists(Path.Combine(fixture.TableDirectory, second.FileName)));
    }

    [Fact]
    public void CompactLevel_DeletesUnreferencedInputsImmediately()
    {
        using var fixture = new CompactorFixture();
        LsmTableFileMetadata input = fixture.AddSstable(level: 0, [(Internal(1, 1, LsmValueType.Put), Val("one"))]);
        var registry = new LsmFileRegistry(fixture.TableDirectory, fixture.Manifest);
        var compactor = new LsmCompactor(fixture.TableDirectory, fixture.Manifest, registry);

        compactor.CompactLevel(0, 1);

        Assert.Empty(fixture.Manifest.GetLiveFiles(0));
        Assert.False(File.Exists(Path.Combine(fixture.TableDirectory, input.FileName)));
    }

    [Fact]
    public async Task SnapshotReadSurvivesConcurrentCompactionAndDeletesAfterDispose()
    {
        using var fixture = new CompactorFixture();
        LsmTableFileMetadata input = fixture.AddSstable(level: 0, [(Internal(1, 1, LsmValueType.Put), Val("one"))]);
        var registry = new LsmFileRegistry(fixture.TableDirectory, fixture.Manifest);
        using LsmReadSnapshot snapshot = registry.OpenSnapshot();
        var readerCanContinue = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var readerFinished = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        Task readTask = Task.Run(() =>
        {
            readerCanContinue.Task.GetAwaiter().GetResult();
            byte[] bytes = snapshot.ReadAllBytes(input.FileNumber);
            SsTableReader reader = SsTableReader.Load(bytes);
            Assert.True(reader.TryGet(Key(1), snapshotSeqno: 1, out byte[] value, out bool tombstone));
            Assert.False(tombstone);
            Assert.Equal(Val("one"), value);
            readerFinished.SetResult();
        });

        var compactor = new LsmCompactor(fixture.TableDirectory, fixture.Manifest, registry);
        compactor.CompactLevel(0, 1);

        Assert.DoesNotContain(fixture.Manifest.GetLiveFiles(0), file => file.FileNumber == input.FileNumber);
        Assert.True(File.Exists(Path.Combine(fixture.TableDirectory, input.FileName)));
        readerCanContinue.SetResult();
        await readerFinished.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await readTask;

        snapshot.Dispose();

        Assert.False(File.Exists(Path.Combine(fixture.TableDirectory, input.FileName)));
    }

    [Fact]
    public async Task OpenSnapshot_ConcurrentWithCompaction_NeverPinsDeletedHandle()
    {
        using var fixture = new CompactorFixture();
        fixture.AddSstable(level: 0, [(Internal(1, 1, LsmValueType.Put), Val("one"))]);
        fixture.AddSstable(level: 0, [(Internal(2, 2, LsmValueType.Put), Val("two"))]);
        var registry = new LsmFileRegistry(fixture.TableDirectory, fixture.Manifest);
        Exception? failure = null;

        Task snapshotLoop = Task.Run(() =>
        {
            try
            {
                for (int i = 0; i < 500; i++)
                {
                    using LsmReadSnapshot snapshot = registry.OpenSnapshot();
                    foreach (LsmTableFileMetadata file in snapshot.Files)
                    {
                        byte[] bytes = snapshot.ReadAllBytes(file.FileNumber);
                        _ = SsTableReader.Load(bytes);
                    }
                }
            }
            catch (Exception ex)
            {
                failure = ex;
            }
        });

        var compactor = new LsmCompactor(fixture.TableDirectory, fixture.Manifest, registry);
        compactor.CompactLevel(0, 1);
        await snapshotLoop;

        if (failure is not null)
        {
            throw failure;
        }
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

    private static void AssertValue(SsTableReader reader, long pk, ulong snapshotSeqno, string expectedValue)
    {
        Assert.True(reader.TryGet(Key(pk), snapshotSeqno, out byte[] value, out bool isTombstone));
        Assert.False(isTombstone);
        Assert.Equal(Val(expectedValue), value);
    }

    private static void CorruptBySwappingDataEntries(byte[] sstable)
    {
        Assert.True(SsTableFormat.TryReadFooter(sstable, out SsTableBlockHandle indexBlock, out _));
        int dataLength = BinaryPrimitives.ReadInt32LittleEndian(
            sstable.AsSpan((int)indexBlock.Offset + sizeof(int) + sizeof(long), sizeof(int)));
        Span<byte> dataBlock = sstable.AsSpan(0, dataLength);
        int firstLength = EntryLength(dataBlock);
        byte[] first = dataBlock[..firstLength].ToArray();
        byte[] second = dataBlock.Slice(firstLength, dataBlock.Length - firstLength).ToArray();
        second.CopyTo(dataBlock);
        first.CopyTo(dataBlock[second.Length..]);
    }

    private static int EntryLength(ReadOnlySpan<byte> dataBlock)
    {
        int keyLength = BinaryPrimitives.ReadInt32LittleEndian(dataBlock[..sizeof(int)]);
        int valueLength = BinaryPrimitives.ReadInt32LittleEndian(dataBlock.Slice(sizeof(int), sizeof(int)));
        return sizeof(int) + sizeof(int) + keyLength + valueLength;
    }

    private sealed class CompactorFixture : IDisposable
    {
        public CompactorFixture()
        {
            TableDirectory = Path.Combine(Path.GetTempPath(), "datavo-lsm-compactor-tests", Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(TableDirectory);
            Manifest = new LsmManifest(Path.Combine(TableDirectory, "MANIFEST"));
        }

        public string TableDirectory { get; }

        public LsmManifest Manifest { get; }

        public LsmTableFileMetadata AddSstable(int level, IReadOnlyList<(byte[] InternalKey, byte[] Value)> entries)
        {
            var writer = new SsTableWriter(entries.Count);
            foreach ((byte[] internalKey, byte[] value) in entries)
            {
                writer.Add(internalKey, value);
            }

            byte[] bytes = writer.Finish();
            long fileNumber = Manifest.AllocateFileNumber();
            string fileName = $"{fileNumber:D6}.sst";
            File.WriteAllBytes(Path.Combine(TableDirectory, fileName), bytes);

            byte[] smallest = entries.Select(entry => entry.InternalKey).Order(InternalKeyComparer.Instance).First();
            byte[] largest = entries.Select(entry => entry.InternalKey).Order(InternalKeyComparer.Instance).Last();
            var metadata = new LsmTableFileMetadata(fileNumber, level, smallest, largest, bytes.LongLength, fileName);
            var edit = new LsmVersionEdit();
            edit.AddFile(metadata);
            Manifest.ApplyEdit(edit);
            return metadata;
        }

        public SsTableReader LoadReader(LsmTableFileMetadata metadata) =>
            SsTableReader.Load(File.ReadAllBytes(Path.Combine(TableDirectory, metadata.FileName)));

        public byte[] ReadSstable(LsmTableFileMetadata metadata) =>
            File.ReadAllBytes(Path.Combine(TableDirectory, metadata.FileName));

        public void WriteSstable(LsmTableFileMetadata metadata, byte[] bytes) =>
            File.WriteAllBytes(Path.Combine(TableDirectory, metadata.FileName), bytes);

        public void Dispose()
        {
            if (Directory.Exists(TableDirectory))
            {
                Directory.Delete(TableDirectory, recursive: true);
            }
        }
    }

    private sealed class InternalKeyComparer : IComparer<byte[]>
    {
        public static readonly InternalKeyComparer Instance = new();

        public int Compare(byte[]? x, byte[]? y) => InternalKey.Compare(x!, y!);
    }
}
