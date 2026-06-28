using DataVo.Core.StorageEngine.Lsm;

namespace DataVo.Tests.Lsm;

public sealed class LsmManifestTests
{
    [Fact]
    public void ApplyEdit_AddsFilesToLevelsAndReturnsDeterministicSnapshots()
    {
        string path = NewManifestPath();
        var manifest = new LsmManifest(path);
        var edit = new LsmVersionEdit();
        edit.AddFile(File(3, level: 0, smallest: 30, largest: 39));
        edit.AddFile(File(1, level: 0, smallest: 10, largest: 19));
        edit.AddFile(File(2, level: 1, smallest: 20, largest: 29));

        manifest.ApplyEdit(edit);

        Assert.Equal([1L, 3L], manifest.GetLiveFiles(0).Select(file => file.FileNumber));
        Assert.Equal([2L], manifest.GetLiveFiles(1).Select(file => file.FileNumber));

        IReadOnlyList<LsmTableFileMetadata> snapshot = manifest.GetLiveFiles(0);
        edit = new LsmVersionEdit();
        edit.DeleteFile(level: 0, fileNumber: 1);
        manifest.ApplyEdit(edit);

        Assert.Equal([1L, 3L], snapshot.Select(file => file.FileNumber));
        Assert.Equal([3L], manifest.GetLiveFiles(0).Select(file => file.FileNumber));
    }

    [Fact]
    public void ApplyEdit_DeletesFilesAtomically()
    {
        string path = NewManifestPath();
        var manifest = new LsmManifest(path);
        var seed = new LsmVersionEdit();
        seed.AddFile(File(1, level: 0, smallest: 10, largest: 19));
        seed.AddFile(File(2, level: 0, smallest: 20, largest: 29));
        manifest.ApplyEdit(seed);

        var delete = new LsmVersionEdit();
        delete.DeleteFile(level: 0, fileNumber: 1);

        manifest.ApplyEdit(delete);

        Assert.Equal([2L], manifest.GetLiveFiles(0).Select(file => file.FileNumber));
    }

    [Fact]
    public void ApplyEdit_InvalidEditDoesNotPartiallyMutateExistingState()
    {
        string path = NewManifestPath();
        var manifest = new LsmManifest(path);
        var seed = new LsmVersionEdit();
        seed.AddFile(File(1, level: 0, smallest: 10, largest: 19));
        manifest.ApplyEdit(seed);

        var invalid = new LsmVersionEdit();
        invalid.DeleteFile(level: 0, fileNumber: 1);
        invalid.AddFile(File(2, level: -1, smallest: 20, largest: 29));

        Assert.Throws<ArgumentException>(() => manifest.ApplyEdit(invalid));
        Assert.Equal([1L], manifest.GetLiveFiles(0).Select(file => file.FileNumber));
    }

    [Fact]
    public void Manifest_PersistsAndReloadsLiveFilesAndNextFileNumber()
    {
        string path = NewManifestPath();
        var manifest = new LsmManifest(path);
        Assert.Equal(1, manifest.AllocateFileNumber());
        long persistedNumber = manifest.AllocateFileNumber();
        var edit = new LsmVersionEdit();
        edit.AddFile(File(10, level: 0, smallest: 10, largest: 19));
        edit.AddFile(File(3, level: 1, smallest: 30, largest: 39));
        manifest.ApplyEdit(edit);

        var reloaded = new LsmManifest(path);

        Assert.Equal([10L], reloaded.GetLiveFiles(0).Select(file => file.FileNumber));
        Assert.Equal([3L], reloaded.GetLiveFiles(1).Select(file => file.FileNumber));
        Assert.True(reloaded.AllocateFileNumber() > persistedNumber);
        Assert.True(reloaded.AllocateFileNumber() > 10);
    }

    [Fact]
    public void AllocateFileNumber_IsMonotonicAcrossReload()
    {
        string path = NewManifestPath();
        var manifest = new LsmManifest(path);
        long first = manifest.AllocateFileNumber();
        long second = manifest.AllocateFileNumber();

        var reloaded = new LsmManifest(path);
        long third = reloaded.AllocateFileNumber();

        Assert.True(first < second);
        Assert.True(second < third);
    }

    [Fact]
    public void FailedInvalidEdit_DoesNotCorruptPersistedReloadState()
    {
        string path = NewManifestPath();
        var manifest = new LsmManifest(path);
        var seed = new LsmVersionEdit();
        seed.AddFile(File(1, level: 0, smallest: 10, largest: 19));
        manifest.ApplyEdit(seed);

        var invalid = new LsmVersionEdit();
        invalid.AddFile(File(2, level: 0, smallest: 29, largest: 20));

        Assert.Throws<ArgumentException>(() => manifest.ApplyEdit(invalid));

        var reloaded = new LsmManifest(path);
        Assert.Equal([1L], reloaded.GetLiveFiles(0).Select(file => file.FileNumber));
        Assert.Empty(reloaded.GetLiveFiles(1));
    }

    [Fact]
    public void ApplyEdit_AcceptsInternalKeyRangeWhenInternalComparatorOrdersBounds()
    {
        string path = NewManifestPath();
        var manifest = new LsmManifest(path);
        byte[] newest = InternalKey(pk: 42, seqno: 9);
        byte[] older = InternalKey(pk: 42, seqno: 4);
        Assert.True(newest.AsSpan().SequenceCompareTo(older) > 0);
        Assert.True(DataVo.Core.StorageEngine.Lsm.InternalKey.Compare(newest, older) < 0);

        var edit = new LsmVersionEdit();
        edit.AddFile(new LsmTableFileMetadata(1, 0, newest, older, fileSize: 128, fileName: "000001.sst"));

        manifest.ApplyEdit(edit);

        LsmTableFileMetadata file = Assert.Single(manifest.GetLiveFiles(0));
        Assert.Equal(1, file.FileNumber);
    }

    [Fact]
    public void ApplyEdit_RejectsEmptyKeyBounds()
    {
        string path = NewManifestPath();
        var manifest = new LsmManifest(path);
        var edit = new LsmVersionEdit();
        edit.AddFile(new LsmTableFileMetadata(1, 0, [], [1], fileSize: 128, fileName: "000001.sst"));

        Assert.Throws<ArgumentException>(() => manifest.ApplyEdit(edit));
        Assert.Empty(manifest.GetLiveFiles(0));
    }

    [Fact]
    public void ApplyEdit_RejectsDuplicateLiveFileNumberAtSameLevelAtomically()
    {
        string path = NewManifestPath();
        var manifest = new LsmManifest(path);
        var seed = new LsmVersionEdit();
        seed.AddFile(File(1, level: 0, smallest: 10, largest: 19));
        manifest.ApplyEdit(seed);

        var duplicate = new LsmVersionEdit();
        duplicate.AddFile(File(1, level: 0, smallest: 20, largest: 29));

        Assert.Throws<ArgumentException>(() => manifest.ApplyEdit(duplicate));
        LsmTableFileMetadata file = Assert.Single(manifest.GetLiveFiles(0));
        Assert.Equal(1, file.FileNumber);
        Assert.Equal("000001.sst", file.FileName);
        Assert.Equal(128, file.FileSize);
    }

    [Fact]
    public void ApplyEdit_RejectsDuplicateFileNumberWithinSingleEdit()
    {
        string path = NewManifestPath();
        var manifest = new LsmManifest(path);
        var edit = new LsmVersionEdit();
        edit.AddFile(File(1, level: 0, smallest: 10, largest: 19));
        edit.AddFile(File(1, level: 0, smallest: 20, largest: 29));

        Assert.Throws<ArgumentException>(() => manifest.ApplyEdit(edit));
        Assert.Empty(manifest.GetLiveFiles(0));
    }

    [Fact]
    public void ApplyEdit_RejectsDuplicateFileNumberAcrossLevelsAtomically()
    {
        string path = NewManifestPath();
        var manifest = new LsmManifest(path);
        var seed = new LsmVersionEdit();
        seed.AddFile(File(1, level: 0, smallest: 10, largest: 19));
        manifest.ApplyEdit(seed);

        var duplicate = new LsmVersionEdit();
        duplicate.AddFile(File(1, level: 1, smallest: 20, largest: 29));

        Assert.Throws<ArgumentException>(() => manifest.ApplyEdit(duplicate));
        Assert.Equal([1L], manifest.GetLiveFiles(0).Select(file => file.FileNumber));
        Assert.Empty(manifest.GetLiveFiles(1));
    }

    [Fact]
    public void ApplyEdit_RejectsDuplicateFileNumberAcrossLevelsWithinSingleEdit()
    {
        string path = NewManifestPath();
        var manifest = new LsmManifest(path);
        var edit = new LsmVersionEdit();
        edit.AddFile(File(1, level: 0, smallest: 10, largest: 19));
        edit.AddFile(File(1, level: 1, smallest: 20, largest: 29));

        Assert.Throws<ArgumentException>(() => manifest.ApplyEdit(edit));
        Assert.Empty(manifest.GetLiveFiles(0));
        Assert.Empty(manifest.GetLiveFiles(1));
    }

    [Fact]
    public void GetLiveFiles_ReturnsDefensiveCopiesOfKeyBounds()
    {
        string path = NewManifestPath();
        var manifest = new LsmManifest(path);
        var edit = new LsmVersionEdit();
        edit.AddFile(File(1, level: 0, smallest: 10, largest: 19));
        manifest.ApplyEdit(edit);

        LsmTableFileMetadata snapshot = Assert.Single(manifest.GetLiveFiles(0));
        snapshot.SmallestInternalKey[0] ^= 0xFF;

        LsmTableFileMetadata fresh = Assert.Single(manifest.GetLiveFiles(0));
        Assert.NotEqual(snapshot.SmallestInternalKey, fresh.SmallestInternalKey);
        Assert.True(DataVo.Core.StorageEngine.Lsm.InternalKey.UserKey(fresh.SmallestInternalKey)
            .SequenceEqual(Key(10)));
    }

    [Fact]
    public void Manifest_ReloadPreservesFileSizeAndFileName()
    {
        string path = NewManifestPath();
        var manifest = new LsmManifest(path);
        var edit = new LsmVersionEdit();
        edit.AddFile(new LsmTableFileMetadata(
            7,
            2,
            InternalKey(pk: 10, seqno: 1),
            InternalKey(pk: 19, seqno: 1),
            fileSize: 4096,
            fileName: "level-2-000007.sst"));
        manifest.ApplyEdit(edit);

        var reloaded = new LsmManifest(path);

        LsmTableFileMetadata file = Assert.Single(reloaded.GetLiveFiles(2));
        Assert.Equal(7, file.FileNumber);
        Assert.Equal(4096, file.FileSize);
        Assert.Equal("level-2-000007.sst", file.FileName);
    }

    [Fact]
    public void Manifest_EquivalentStatePersistsDeterministicBytes()
    {
        string firstPath = NewManifestPath();
        string secondPath = NewManifestPath();

        WriteEquivalentState(firstPath);
        WriteEquivalentState(secondPath);

        Assert.Equal(System.IO.File.ReadAllBytes(firstPath), System.IO.File.ReadAllBytes(secondPath));
    }

    [Fact]
    public void ApplyEdit_WhenPersistenceFails_DoesNotMutateLiveState()
    {
        string parentFile = NewParentFilePath();
        string path = Path.Combine(parentFile, "MANIFEST");
        var manifest = new LsmManifest(path);
        var edit = new LsmVersionEdit();
        edit.AddFile(File(1, level: 0, smallest: 10, largest: 19));

        Assert.Throws<IOException>(() => manifest.ApplyEdit(edit));

        Assert.Empty(manifest.GetLiveFiles(0));
    }

    [Fact]
    public void AllocateFileNumber_WhenPersistenceFails_DoesNotAdvanceNextFileNumber()
    {
        string parentFile = NewParentFilePath();
        string path = Path.Combine(parentFile, "MANIFEST");
        var manifest = new LsmManifest(path);

        Assert.Throws<IOException>(() => manifest.AllocateFileNumber());
        System.IO.File.Delete(parentFile);
        Directory.CreateDirectory(parentFile);

        Assert.Equal(1, manifest.AllocateFileNumber());
    }

    private static void WriteEquivalentState(string path)
    {
        var manifest = new LsmManifest(path);
        var edit = new LsmVersionEdit();
        edit.AddFile(File(3, level: 1, smallest: 30, largest: 39));
        edit.AddFile(File(1, level: 0, smallest: 10, largest: 19));
        edit.AddFile(new LsmTableFileMetadata(
            2,
            0,
            InternalKey(pk: 20, seqno: 1),
            InternalKey(pk: 29, seqno: 1),
            fileSize: 512,
            fileName: "custom-name.sst"));
        manifest.ApplyEdit(edit);
    }

    private static byte[] InternalKey(long pk, ulong seqno)
    {
        byte[] user = Key(pk);
        var key = new byte[DataVo.Core.StorageEngine.Lsm.InternalKey.MeasureSize(user.Length)];
        DataVo.Core.StorageEngine.Lsm.InternalKey.Write(key, user, seqno, LsmValueType.Put);
        return key;
    }

    private static byte[] Key(long pk)
    {
        var user = new byte[8];
        DataVo.Core.StorageEngine.Lsm.InternalKey.EncodeInt64UserKey(user, pk);
        return user;
    }

    private static LsmTableFileMetadata File(long fileNumber, int level, byte smallest, byte largest)
    {
        return new LsmTableFileMetadata(
            fileNumber,
            level,
            InternalKey(pk: smallest, seqno: 1),
            InternalKey(pk: largest, seqno: 1),
            fileSize: 128,
            fileName: $"{fileNumber:D6}.sst");
    }

    private static string NewManifestPath()
    {
        string directory = Path.Combine(Path.GetTempPath(), "datavo-lsm-manifest-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        return Path.Combine(directory, "MANIFEST");
    }

    private static string NewParentFilePath()
    {
        string directory = Path.Combine(Path.GetTempPath(), "datavo-lsm-manifest-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        string parentFile = Path.Combine(directory, "not-a-directory");
        System.IO.File.WriteAllText(parentFile, "blocks manifest directory creation");
        return parentFile;
    }
}
