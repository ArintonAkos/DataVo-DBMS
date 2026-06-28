using System.Buffers.Binary;

namespace DataVo.Core.StorageEngine.Lsm;

/// <summary>Result of one level compaction attempt.</summary>
public readonly record struct LsmCompactionResult(
    LsmTableFileMetadata? OutputFile,
    IReadOnlyList<LsmTableFileMetadata> DeletedFiles);

/// <summary>Compacts selected SSTables from one LSM level into another.</summary>
public sealed class LsmCompactor
{
    private readonly string _tableDirectory;
    private readonly LsmManifest _manifest;
    private readonly LsmFileRegistry? _fileRegistry;
    private readonly Action<LsmVersionEdit> _applyEdit;

    /// <summary>Creates a compactor rooted at <paramref name="tableDirectory"/> and backed by <paramref name="manifest"/>.</summary>
    public LsmCompactor(string tableDirectory, LsmManifest manifest)
        : this(tableDirectory, manifest, fileRegistry: null, applyEdit: null)
    {
    }

    internal LsmCompactor(string tableDirectory, LsmManifest manifest, LsmFileRegistry fileRegistry)
        : this(tableDirectory, manifest, fileRegistry, applyEdit: null)
    {
    }

    private LsmCompactor(
        string tableDirectory,
        LsmManifest manifest,
        LsmFileRegistry? fileRegistry,
        Action<LsmVersionEdit>? applyEdit)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tableDirectory);
        ArgumentNullException.ThrowIfNull(manifest);

        _tableDirectory = tableDirectory;
        _manifest = manifest;
        _fileRegistry = fileRegistry;
        _applyEdit = applyEdit ?? manifest.ApplyEdit;
        Directory.CreateDirectory(_tableDirectory);
    }

    internal static LsmCompactor CreateForTesting(
        string tableDirectory,
        LsmManifest manifest,
        Action<LsmVersionEdit> applyEdit) =>
        new(tableDirectory, manifest, fileRegistry: null, applyEdit);

    /// <summary>
    /// Compacts all files in <paramref name="sourceLevel"/> plus overlapping files in
    /// <paramref name="targetLevel"/>. Entries are collapsed to the newest version per user key.
    /// </summary>
    public LsmCompactionResult? CompactLevel(
        int sourceLevel,
        int targetLevel,
        bool dropTombstonesAtBottomLevel = false)
    {
        if (sourceLevel < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(sourceLevel));
        }

        if (targetLevel < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(targetLevel));
        }

        LsmTableFileMetadata[] sourceFiles = _manifest.GetLiveFiles(sourceLevel).ToArray();
        if (sourceFiles.Length == 0)
        {
            return null;
        }

        LsmTableFileMetadata[] targetOverlaps = SelectTargetOverlapsClosed(
            sourceFiles,
            _manifest.GetLiveFiles(targetLevel));

        LsmTableFileMetadata[] selectedFiles = sourceFiles.Concat(targetOverlaps).ToArray();
        LsmSstableHandle[] selectedHandles = _fileRegistry?.AddRefs(selectedFiles) ?? [];
        try
        {
            List<SstableEntry> mergedEntries = MergeSelectedEntries(selectedFiles, selectedHandles, dropTombstonesAtBottomLevel);

            LsmTableFileMetadata? outputMetadata = null;
            string? outputPath = null;
            if (mergedEntries.Count > 0)
            {
                var writer = new SsTableWriter(mergedEntries.Count);
                foreach (SstableEntry entry in mergedEntries)
                {
                    writer.Add(entry.InternalKey, entry.Value);
                }

                byte[] bytes = writer.Finish();
                long fileNumber = _manifest.AllocateFileNumber();
                string fileName = $"{fileNumber:D6}.sst";
                outputPath = Path.Combine(_tableDirectory, fileName);
                WriteSstableAtomically(outputPath, bytes);

                outputMetadata = new LsmTableFileMetadata(
                    fileNumber,
                    targetLevel,
                    mergedEntries[0].InternalKey,
                    mergedEntries[^1].InternalKey,
                    bytes.LongLength,
                    fileName);
            }

            var edit = new LsmVersionEdit();
            if (outputMetadata is { } metadata)
            {
                edit.AddFile(metadata);
            }

            foreach (LsmTableFileMetadata file in selectedFiles)
            {
                edit.DeleteFile(file.Level, file.FileNumber);
            }

            try
            {
                if (_fileRegistry is null)
                {
                    _applyEdit(edit);
                }
                else
                {
                    _fileRegistry.ExecuteVersionEdit(() =>
                    {
                        _applyEdit(edit);
                        if (outputMetadata is { } output)
                        {
                            _fileRegistry.Register(output);
                        }

                        _fileRegistry.MarkDeleted(selectedFiles);
                        return true;
                    });
                }
            }
            catch
            {
                if (outputPath is not null)
                {
                    DeleteFileIfExists(outputPath);
                }

                throw;
            }

            if (_fileRegistry is null)
            {
                foreach (LsmTableFileMetadata file in selectedFiles)
                {
                    DeleteFileIfExists(Path.Combine(_tableDirectory, file.FileName));
                }
            }

            return new LsmCompactionResult(outputMetadata, selectedFiles);
        }
        finally
        {
            _fileRegistry?.Release(selectedHandles);
        }
    }

    private List<SstableEntry> MergeSelectedEntries(
        IReadOnlyList<LsmTableFileMetadata> files,
        IReadOnlyList<LsmSstableHandle> handles,
        bool dropTombstonesAtBottomLevel)
    {
        var queue = new PriorityQueue<SstableCursor, SstableCursorPriority>(SstableCursorPriorityComparer.Instance);
        for (int i = 0; i < files.Count; i++)
        {
            LsmTableFileMetadata file = files[i];
            byte[] bytes = handles.Count == files.Count
                ? handles[i].ReadAllBytes()
                : File.ReadAllBytes(Path.Combine(_tableDirectory, file.FileName));
            SsTableReader.Load(bytes);
            List<SstableEntry> entries = ReadEntries(bytes);
            entries.Sort(InternalKeyEntryComparer.Instance);
            if (entries.Count == 0)
            {
                continue;
            }

            var cursor = new SstableCursor(i, entries);
            queue.Enqueue(cursor, SstableCursorPriority.From(cursor));
        }

        var mergedEntries = new List<SstableEntry>();
        byte[]? previousUserKey = null;
        while (queue.TryDequeue(out SstableCursor? cursor, out _))
        {
            SstableEntry entry = cursor.Current;
            if (previousUserKey is null || !InternalKey.UserKey(entry.InternalKey).SequenceEqual(previousUserKey))
            {
                previousUserKey = InternalKey.UserKey(entry.InternalKey).ToArray();
                if (!dropTombstonesAtBottomLevel || InternalKey.ValueType(entry.InternalKey) != LsmValueType.Deletion)
                {
                    mergedEntries.Add(entry);
                }
            }

            cursor.MoveNext();
            if (!cursor.IsDone)
            {
                queue.Enqueue(cursor, SstableCursorPriority.From(cursor));
            }
        }

        return mergedEntries;
    }

    private static LsmTableFileMetadata[] SelectTargetOverlapsClosed(
        IReadOnlyList<LsmTableFileMetadata> sourceFiles,
        IReadOnlyList<LsmTableFileMetadata> targetFiles)
    {
        (byte[] smallestUserKey, byte[] largestUserKey) = UserKeyBounds(sourceFiles);
        var selectedTargets = new List<LsmTableFileMetadata>();
        var selectedTargetFileNumbers = new HashSet<long>();

        bool expanded;
        do
        {
            expanded = false;
            foreach (LsmTableFileMetadata target in targetFiles)
            {
                if (selectedTargetFileNumbers.Contains(target.FileNumber)
                    || !UserKeyRangeOverlaps(smallestUserKey, largestUserKey, target))
                {
                    continue;
                }

                selectedTargets.Add(target);
                selectedTargetFileNumbers.Add(target.FileNumber);
                if (InternalKey.UserKey(target.SmallestInternalKey).SequenceCompareTo(smallestUserKey) < 0)
                {
                    smallestUserKey = InternalKey.UserKey(target.SmallestInternalKey).ToArray();
                    expanded = true;
                }

                if (InternalKey.UserKey(target.LargestInternalKey).SequenceCompareTo(largestUserKey) > 0)
                {
                    largestUserKey = InternalKey.UserKey(target.LargestInternalKey).ToArray();
                    expanded = true;
                }
            }
        }
        while (expanded);

        return selectedTargets.ToArray();
    }

    private static (byte[] SmallestUserKey, byte[] LargestUserKey) UserKeyBounds(
        IReadOnlyList<LsmTableFileMetadata> files)
    {
        ReadOnlySpan<byte> smallest = InternalKey.UserKey(files[0].SmallestInternalKey);
        ReadOnlySpan<byte> largest = InternalKey.UserKey(files[0].LargestInternalKey);
        byte[] smallestUserKey = smallest.ToArray();
        byte[] largestUserKey = largest.ToArray();

        for (int i = 1; i < files.Count; i++)
        {
            ReadOnlySpan<byte> candidateSmallest = InternalKey.UserKey(files[i].SmallestInternalKey);
            if (candidateSmallest.SequenceCompareTo(smallestUserKey) < 0)
            {
                smallestUserKey = candidateSmallest.ToArray();
            }

            ReadOnlySpan<byte> candidateLargest = InternalKey.UserKey(files[i].LargestInternalKey);
            if (candidateLargest.SequenceCompareTo(largestUserKey) > 0)
            {
                largestUserKey = candidateLargest.ToArray();
            }
        }

        return (smallestUserKey, largestUserKey);
    }

    private static List<SstableEntry> ReadEntries(byte[] bytes)
    {
        if (!SsTableFormat.TryReadFooter(bytes, out SsTableBlockHandle indexBlock, out _))
        {
            throw new InvalidDataException("SSTable footer is missing, corrupt, or points outside the table.");
        }

        var entries = new List<SstableEntry>();
        SsTableBlockHandle dataBlock = ReadDataBlockHandle(SliceBlock(bytes, indexBlock));
        ReadOnlySpan<byte> data = SliceBlock(bytes, dataBlock);
        int offset = 0;
        while (offset < data.Length)
        {
            int keyLength = ReadInt32(data, ref offset);
            int valueLength = ReadInt32(data, ref offset);
            if (keyLength < InternalKey.TagSize || valueLength < 0)
            {
                throw new InvalidDataException("SSTable data block contains a malformed entry length.");
            }

            byte[] internalKey = ReadSpan(data, ref offset, keyLength).ToArray();
            byte[] value = ReadSpan(data, ref offset, valueLength).ToArray();
            entries.Add(new SstableEntry(internalKey, value));
        }

        return entries;
    }

    private static SsTableBlockHandle ReadDataBlockHandle(ReadOnlySpan<byte> indexBlock)
    {
        int offset = 0;
        int count = ReadInt32(indexBlock, ref offset);
        if (count != 1)
        {
            throw new InvalidDataException($"SSTable v1 compactor expects exactly one data block, found {count}.");
        }

        long dataOffset = ReadInt64(indexBlock, ref offset);
        int dataLength = ReadInt32(indexBlock, ref offset);
        return new SsTableBlockHandle(dataOffset, dataLength);
    }

    private static bool UserKeyRangeOverlaps(
        ReadOnlySpan<byte> leftSmallest,
        ReadOnlySpan<byte> leftLargest,
        LsmTableFileMetadata right)
    {
        ReadOnlySpan<byte> rightSmallest = InternalKey.UserKey(right.SmallestInternalKey);
        ReadOnlySpan<byte> rightLargest = InternalKey.UserKey(right.LargestInternalKey);

        return leftSmallest.SequenceCompareTo(rightLargest) <= 0
            && rightSmallest.SequenceCompareTo(leftLargest) <= 0;
    }

    private static ReadOnlySpan<byte> SliceBlock(byte[] source, SsTableBlockHandle handle)
    {
        if (handle.Offset < 0
            || handle.Length <= 0
            || handle.Offset > int.MaxValue
            || handle.Offset + handle.Length > source.Length)
        {
            throw new InvalidDataException("SSTable block handle points outside the table.");
        }

        return source.AsSpan((int)handle.Offset, handle.Length);
    }

    private static int ReadInt32(ReadOnlySpan<byte> source, ref int offset)
    {
        ReadOnlySpan<byte> span = ReadSpan(source, ref offset, sizeof(int));
        return BinaryPrimitives.ReadInt32LittleEndian(span);
    }

    private static long ReadInt64(ReadOnlySpan<byte> source, ref int offset)
    {
        ReadOnlySpan<byte> span = ReadSpan(source, ref offset, sizeof(long));
        return BinaryPrimitives.ReadInt64LittleEndian(span);
    }

    private static ReadOnlySpan<byte> ReadSpan(ReadOnlySpan<byte> source, ref int offset, int length)
    {
        if (length < 0 || offset < 0 || offset > source.Length - length)
        {
            throw new InvalidDataException("SSTable block is truncated.");
        }

        ReadOnlySpan<byte> span = source.Slice(offset, length);
        offset += length;
        return span;
    }

    private static void WriteSstableAtomically(string filePath, byte[] bytes)
    {
        LsmDurableFileOperations.WriteFileAtomically(
            filePath,
            overwrite: false,
            stream => stream.Write(bytes),
            crashHook: null,
            LsmCrashPoint.AfterSstableTempFileFsyncBeforeRename,
            LsmCrashPoint.AfterSstableRenameBeforeDirectoryFsync,
            LsmCrashPoint.AfterSstableDirectoryFsyncBeforeManifest);
    }

    private static void DeleteFileIfExists(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }

    private sealed record SstableEntry(byte[] InternalKey, byte[] Value);

    private sealed class SstableCursor
    {
        private readonly IReadOnlyList<SstableEntry> _entries;
        private int _index;

        public SstableCursor(int ordinal, IReadOnlyList<SstableEntry> entries)
        {
            Ordinal = ordinal;
            _entries = entries;
        }

        public int Ordinal { get; }

        public bool IsDone => _index >= _entries.Count;

        public SstableEntry Current => _entries[_index];

        public void MoveNext()
        {
            _index++;
        }
    }

    private readonly record struct SstableCursorPriority(byte[] InternalKey, int CursorOrdinal)
    {
        public static SstableCursorPriority From(SstableCursor cursor) =>
            new(cursor.Current.InternalKey, cursor.Ordinal);
    }

    private sealed class SstableCursorPriorityComparer : IComparer<SstableCursorPriority>
    {
        public static readonly SstableCursorPriorityComparer Instance = new();

        public int Compare(SstableCursorPriority x, SstableCursorPriority y)
        {
            int byInternalKey = InternalKey.Compare(x.InternalKey, y.InternalKey);
            if (byInternalKey != 0)
            {
                return byInternalKey;
            }

            return x.CursorOrdinal.CompareTo(y.CursorOrdinal);
        }
    }

    private sealed class InternalKeyEntryComparer : IComparer<SstableEntry>
    {
        public static readonly InternalKeyEntryComparer Instance = new();

        public int Compare(SstableEntry? x, SstableEntry? y) => InternalKey.Compare(x!.InternalKey, y!.InternalKey);
    }
}
