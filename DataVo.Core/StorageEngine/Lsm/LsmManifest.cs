namespace DataVo.Core.StorageEngine.Lsm;

/// <summary>Persistent metadata for one SSTable file in an LSM version.</summary>
public readonly record struct LsmTableFileMetadata
{
    /// <summary>Creates immutable SSTable metadata and copies key bounds defensively.</summary>
    public LsmTableFileMetadata(
        long fileNumber,
        int level,
        byte[] smallestInternalKey,
        byte[] largestInternalKey,
        long fileSize,
        string fileName)
    {
        FileNumber = fileNumber;
        Level = level;
        SmallestInternalKey = smallestInternalKey.ToArray();
        LargestInternalKey = largestInternalKey.ToArray();
        FileSize = fileSize;
        FileName = fileName;
    }

    /// <summary>Monotonic SSTable file number.</summary>
    public long FileNumber { get; }

    /// <summary>LSM level containing the SSTable. Level zero is represented as <c>0</c>.</summary>
    public int Level { get; }

    /// <summary>Inclusive smallest internal key contained by the SSTable.</summary>
    public byte[] SmallestInternalKey { get; }

    /// <summary>Inclusive largest internal key contained by the SSTable.</summary>
    public byte[] LargestInternalKey { get; }

    /// <summary>Size of the SSTable file in bytes.</summary>
    public long FileSize { get; }

    /// <summary>File name of the SSTable relative to its owning LSM directory.</summary>
    public string FileName { get; }

    internal LsmTableFileMetadata Copy()
    {
        return new LsmTableFileMetadata(
            FileNumber,
            Level,
            SmallestInternalKey,
            LargestInternalKey,
            FileSize,
            FileName);
    }
}

/// <summary>A set of additions and deletions to apply atomically to an LSM manifest version.</summary>
public sealed class LsmVersionEdit
{
    private readonly List<LsmTableFileMetadata> _addedFiles = [];
    private readonly List<LsmDeletedFile> _deletedFiles = [];

    /// <summary>SSTables to add to the version.</summary>
    public IReadOnlyList<LsmTableFileMetadata> AddedFiles => _addedFiles;

    /// <summary>SSTables to delete from the version, identified by level and file number.</summary>
    public IReadOnlyList<LsmDeletedFile> DeletedFiles => _deletedFiles;

    /// <summary>Adds an SSTable to this version edit.</summary>
    public void AddFile(LsmTableFileMetadata file)
    {
        _addedFiles.Add(file.Copy());
    }

    /// <summary>Deletes an SSTable from this version edit by level and file number.</summary>
    public void DeleteFile(int level, long fileNumber)
    {
        _deletedFiles.Add(new LsmDeletedFile(level, fileNumber));
    }
}

/// <summary>Identity of a deleted SSTable in a version edit.</summary>
public readonly record struct LsmDeletedFile(int Level, long FileNumber);

/// <summary>
/// Tracks the live LSM SSTable set by level and persists complete version snapshots atomically.
/// Files returned within a level are sorted by file number for deterministic callers.
/// </summary>
public sealed class LsmManifest
{
    private const uint Magic = 0x31464D4CU;
    private const ushort Version = 1;

    private readonly string _manifestPath;
    private readonly Dictionary<int, List<LsmTableFileMetadata>> _filesByLevel = [];
    private long _nextFileNumber = 1;

    /// <summary>Loads an existing manifest from <paramref name="manifestPath"/> or starts an empty version.</summary>
    public LsmManifest(string manifestPath)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfNullOrWhiteSpace(manifestPath);
        _manifestPath = manifestPath;
        Load();
    }

    internal Action<LsmCrashPoint>? CrashHook { get; set; }

    /// <summary>Allocates and persists the next monotonically increasing SSTable file number.</summary>
    public long AllocateFileNumber()
    {
        long allocated = _nextFileNumber;
        long candidateNextFileNumber = allocated + 1;
        Persist(_filesByLevel, candidateNextFileNumber);
        _nextFileNumber = candidateNextFileNumber;
        return allocated;
    }

    /// <summary>Validates and atomically applies a version edit to the live manifest state.</summary>
    public void ApplyEdit(LsmVersionEdit edit)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(edit);

        Dictionary<int, List<LsmTableFileMetadata>> candidate = CopyLevels(_filesByLevel);
        ApplyDeletes(candidate, edit.DeletedFiles);
        ApplyAdds(candidate, edit.AddedFiles);
        Normalize(candidate);
        ValidateLevels(candidate);

        long candidateNextFileNumber = _nextFileNumber;
        long highestLiveFileNumber = HighestLiveFileNumber(candidate);
        if (highestLiveFileNumber >= candidateNextFileNumber)
        {
            candidateNextFileNumber = highestLiveFileNumber + 1;
        }

        Persist(candidate, candidateNextFileNumber);

        _filesByLevel.Clear();
        foreach ((int level, List<LsmTableFileMetadata> files) in candidate)
        {
            _filesByLevel[level] = files;
        }

        _nextFileNumber = candidateNextFileNumber;
    }

    /// <summary>Returns a deterministic snapshot of live files in <paramref name="level"/>, sorted by file number.</summary>
    public IReadOnlyList<LsmTableFileMetadata> GetLiveFiles(int level)
    {
        if (level < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(level));
        }

        if (!_filesByLevel.TryGetValue(level, out List<LsmTableFileMetadata>? files))
        {
            return [];
        }

        return files
            .OrderBy(file => file.FileNumber)
            .Select(file => file.Copy())
            .ToArray();
    }

    private static void ApplyDeletes(
        Dictionary<int, List<LsmTableFileMetadata>> levels,
        IReadOnlyList<LsmDeletedFile> deletedFiles)
    {
        foreach (LsmDeletedFile deleted in deletedFiles)
        {
            if (deleted.Level < 0)
            {
                throw new ArgumentException("Deleted file level must be non-negative.", nameof(deletedFiles));
            }

            if (!levels.TryGetValue(deleted.Level, out List<LsmTableFileMetadata>? files))
            {
                continue;
            }

            files.RemoveAll(file => file.FileNumber == deleted.FileNumber);
            if (files.Count == 0)
            {
                levels.Remove(deleted.Level);
            }
        }
    }

    private static void ApplyAdds(
        Dictionary<int, List<LsmTableFileMetadata>> levels,
        IReadOnlyList<LsmTableFileMetadata> addedFiles)
    {
        foreach (LsmTableFileMetadata file in addedFiles)
        {
            ValidateFile(file);
            if (!levels.TryGetValue(file.Level, out List<LsmTableFileMetadata>? files))
            {
                files = [];
                levels.Add(file.Level, files);
            }

            if (files.Any(live => live.FileNumber == file.FileNumber))
            {
                throw new ArgumentException("A live file already exists at the same level with this file number.", nameof(addedFiles));
            }

            files.Add(file.Copy());
        }
    }

    private static void ValidateLevels(Dictionary<int, List<LsmTableFileMetadata>> levels)
    {
        var seenFileNumbers = new HashSet<long>();
        foreach ((int level, List<LsmTableFileMetadata> files) in levels)
        {
            if (level < 0)
            {
                throw new ArgumentException("Level must be non-negative.", nameof(levels));
            }

            foreach (LsmTableFileMetadata file in files)
            {
                ValidateFile(file);
                if (!seenFileNumbers.Add(file.FileNumber))
                {
                    throw new ArgumentException("Duplicate live file number.", nameof(levels));
                }
            }
        }
    }

    private static void ValidateFile(LsmTableFileMetadata file)
    {
        if (file.Level < 0)
        {
            throw new ArgumentException("File level must be non-negative.", nameof(file));
        }

        if (file.FileNumber <= 0)
        {
            throw new ArgumentException("File number must be positive.", nameof(file));
        }

        if (file.SmallestInternalKey.Length == 0 || file.LargestInternalKey.Length == 0)
        {
            throw new ArgumentException("File key bounds must be non-empty.", nameof(file));
        }

        if (InternalKey.Compare(file.SmallestInternalKey, file.LargestInternalKey) > 0)
        {
            throw new ArgumentException("Smallest internal key must be less than or equal to largest internal key.", nameof(file));
        }

        if (file.FileSize < 0)
        {
            throw new ArgumentException("File size must be non-negative.", nameof(file));
        }

        DataVo.Core.Compat.ThrowHelper.ThrowIfNullOrWhiteSpace(file.FileName);
    }

    private static void Normalize(Dictionary<int, List<LsmTableFileMetadata>> levels)
    {
        foreach (List<LsmTableFileMetadata> files in levels.Values)
        {
            files.Sort(static (left, right) => left.FileNumber.CompareTo(right.FileNumber));
        }
    }

    private void Load()
    {
        if (!File.Exists(_manifestPath))
        {
            return;
        }

        using var stream = new FileStream(_manifestPath, FileMode.Open, FileAccess.Read, FileShare.Read);
        using var reader = new BinaryReader(stream);

        uint magic = reader.ReadUInt32();
        ushort version = reader.ReadUInt16();
        if (magic != Magic || version != Version)
        {
            throw new InvalidDataException("Unsupported LSM manifest format.");
        }

        _nextFileNumber = reader.ReadInt64();
        int fileCount = reader.ReadInt32();
        var loaded = new Dictionary<int, List<LsmTableFileMetadata>>();

        for (int i = 0; i < fileCount; i++)
        {
            long fileNumber = reader.ReadInt64();
            int level = reader.ReadInt32();
            byte[] smallest = ReadBytes(reader);
            byte[] largest = ReadBytes(reader);
            long fileSize = reader.ReadInt64();
            string fileName = reader.ReadString();
            var file = new LsmTableFileMetadata(fileNumber, level, smallest, largest, fileSize, fileName);
            ValidateFile(file);

            if (!loaded.TryGetValue(level, out List<LsmTableFileMetadata>? files))
            {
                files = [];
                loaded.Add(level, files);
            }

            files.Add(file);
        }

        Normalize(loaded);
        ValidateLevels(loaded);

        _filesByLevel.Clear();
        foreach ((int level, List<LsmTableFileMetadata> files) in loaded)
        {
            _filesByLevel[level] = files;
        }

        long minimumNextFileNumber = HighestLiveFileNumber(_filesByLevel) + 1;
        if (_nextFileNumber < minimumNextFileNumber)
        {
            _nextFileNumber = minimumNextFileNumber;
        }
    }

    private void Persist(Dictionary<int, List<LsmTableFileMetadata>> levels, long nextFileNumber)
    {
        string? directory = Path.GetDirectoryName(_manifestPath);
        if (!string.IsNullOrEmpty(directory))
        {
            Directory.CreateDirectory(directory);
        }

        LsmDurableFileOperations.WriteFileAtomically(
            _manifestPath,
            overwrite: true,
            stream =>
        {
            using var writer = new BinaryWriter(stream, System.Text.Encoding.UTF8, leaveOpen: true);
            writer.Write(Magic);
            writer.Write(Version);
            writer.Write(nextFileNumber);

            LsmTableFileMetadata[] files = levels
                .OrderBy(pair => pair.Key)
                .SelectMany(pair => pair.Value.OrderBy(file => file.FileNumber))
                .ToArray();

            writer.Write(files.Length);
            foreach (LsmTableFileMetadata file in files)
            {
                writer.Write(file.FileNumber);
                writer.Write(file.Level);
                WriteBytes(writer, file.SmallestInternalKey);
                WriteBytes(writer, file.LargestInternalKey);
                writer.Write(file.FileSize);
                writer.Write(file.FileName);
            }

            writer.Flush();
        },
            CrashHook,
            LsmCrashPoint.AfterManifestTempFileFsyncBeforeRename,
            LsmCrashPoint.AfterManifestRenameBeforeDirectoryFsync,
            LsmCrashPoint.AfterManifestDirectoryFsyncBeforeWalClear);
    }

    private static Dictionary<int, List<LsmTableFileMetadata>> CopyLevels(
        Dictionary<int, List<LsmTableFileMetadata>> source)
    {
        var copy = new Dictionary<int, List<LsmTableFileMetadata>>(source.Count);
        foreach ((int level, List<LsmTableFileMetadata> files) in source)
        {
            copy[level] = files.Select(file => file.Copy()).ToList();
        }

        return copy;
    }

    private static long HighestLiveFileNumber(Dictionary<int, List<LsmTableFileMetadata>> levels)
    {
        long highest = 0;
        foreach (List<LsmTableFileMetadata> files in levels.Values)
        {
            foreach (LsmTableFileMetadata file in files)
            {
                if (file.FileNumber > highest)
                {
                    highest = file.FileNumber;
                }
            }
        }

        return highest;
    }

    private static byte[] ReadBytes(BinaryReader reader)
    {
        int length = reader.ReadInt32();
        if (length < 0)
        {
            throw new InvalidDataException("Negative byte array length in LSM manifest.");
        }

        return reader.ReadBytes(length);
    }

    private static void WriteBytes(BinaryWriter writer, byte[] value)
    {
        writer.Write(value.Length);
        writer.Write(value);
    }
}
