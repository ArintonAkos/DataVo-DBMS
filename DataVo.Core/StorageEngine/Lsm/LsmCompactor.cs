using System.Buffers.Binary;
using DataVo.Core.Compat;
using DataVo.Core.StorageEngine.Lsm.V2;

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
        DataVo.Core.Compat.ThrowHelper.ThrowIfNullOrWhiteSpace(tableDirectory);
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(manifest);

        _tableDirectory = tableDirectory;
        _manifest = manifest;
        _fileRegistry = fileRegistry;
        _applyEdit = applyEdit ?? manifest.ApplyEdit;
        Directory.CreateDirectory(_tableDirectory);
    }

    /// <summary>Format of the output (inputs may be any format).</summary>
    internal LsmSstableFormat Format { get; set; }

    private bool WritesV2 => Format == LsmSstableFormat.RowKeyed;

    /// <summary>How v2 inputs are read when no registry handle provides a reader.</summary>
    internal LsmReadMode ReadMode { get; set; }

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
            _manifest.GetLiveFiles(targetLevel),
            rowKeyed: WritesV2);

        LsmTableFileMetadata[] selectedFiles = sourceFiles.Concat(targetOverlaps).ToArray();
        LsmSstableHandle[] selectedHandles = _fileRegistry?.AddRefs(selectedFiles) ?? [];
        try
        {
            List<SstableEntry> mergedEntries = MergeSelectedEntries(selectedFiles, selectedHandles, dropTombstonesAtBottomLevel);

            LsmTableFileMetadata? outputMetadata = null;
            string? outputPath = null;
            if (mergedEntries.Count > 0 && WritesV2)
            {
                (outputMetadata, outputPath) = WriteV2Output(mergedEntries, targetLevel);
            }
            else if (mergedEntries.Count > 0 && Format == LsmSstableFormat.Keyed)
            {
                (outputMetadata, outputPath) = WriteKeyedOutput(mergedEntries, targetLevel);
            }
            else if (mergedEntries.Count > 0)
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
        var queue = new DataVoPriorityQueue<SstableCursor, SstableCursorPriority>(SstableCursorPriorityComparer.Instance);
        for (int i = 0; i < files.Count; i++)
        {
            LsmTableFileMetadata file = files[i];
            List<SstableEntry> entries = ReadAnyFormat(file, handles.Count == files.Count ? handles[i] : null);
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
        SstableEntry? lastDroppedRowTombstone = null;
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
                else if (LsmRowKeyCodec.IsRowKey(InternalKey.UserKey(entry.InternalKey)))
                {
                    lastDroppedRowTombstone = entry;
                }
            }

            cursor.MoveNext();
            if (!cursor.IsDone)
            {
                queue.Enqueue(cursor, SstableCursorPriority.From(cursor));
            }
        }

        // A row-keyed file needs at least one row entry to carry the table metadata (next row id and live
        // count): when every row was deleted and its tombstone dropped, keep the last tombstone.
        if (WritesV2
            && lastDroppedRowTombstone is not null
            && !mergedEntries.Exists(static e => LsmRowKeyCodec.IsRowKey(InternalKey.UserKey(e.InternalKey)))
            && mergedEntries.Count > 0)
        {
            mergedEntries.Add(lastDroppedRowTombstone);
            mergedEntries.Sort(InternalKeyEntryComparer.Instance);
        }

        return mergedEntries;
    }

    /// <summary>Reads every entry of a v1 or v2 file as internal-key entries.</summary>
    private List<SstableEntry> ReadAnyFormat(LsmTableFileMetadata file, LsmSstableHandle? handle)
    {
        string path = Path.Combine(_tableDirectory, file.FileName);
        if (SsTableKeyedFormat.IsKeyedFile(path))
        {
            SsTableKeyedReader? ownedKeyed = handle is null ? SsTableKeyedReader.Open(path, ReadMode) : null;
            try
            {
                SsTableKeyedReader keyed = ownedKeyed ?? handle!.GetKeyedReader(ReadMode);
                var keyedEntries = new List<SstableEntry>((int)Math.Min(keyed.Properties.EntryCount, int.MaxValue));
                keyed.ForEach(keyedEntries, static (userKey, sequence, type, value, list) =>
                {
                    var internalKey = new byte[userKey.Length + InternalKey.TagSize];
                    InternalKey.Write(internalKey, userKey, sequence, type);
                    list.Add(new SstableEntry(internalKey, value.ToArray()));
                });
                return keyedEntries;
            }
            finally
            {
                ownedKeyed?.Dispose();
            }
        }

        bool isV2 = handle?.IsV2() ?? SsTableV2Format.IsV2File(path);
        if (!isV2)
        {
            byte[] bytes = handle?.ReadAllBytes() ?? File.ReadAllBytes(path);
            SsTableReader.Load(bytes);
            List<SstableEntry> legacy = ReadEntries(bytes);
            if (WritesV2)
            {
                // A v1 row file holds 8-byte row keys; merge them in their 16-byte form so they order
                // correctly against rows read from newer files.
                for (int i = 0; i < legacy.Count; i++)
                {
                    legacy[i] = legacy[i] with { InternalKey = WidenRowInternalKey(legacy[i].InternalKey) };
                }
            }

            return legacy;
        }

        SsTableV2Reader? owned = handle is null ? SsTableV2Reader.Open(path, ReadMode) : null;
        try
        {
            SsTableV2Reader reader = owned ?? handle!.GetV2Reader(ReadMode);
            var entries = new List<SstableEntry>((int)Math.Min(reader.Properties.EntryCount + 1, int.MaxValue));
            SsTableV2Cursor cursor = reader.OpenCursor();
            while (cursor.MoveNext())
            {
                entries.Add(new SstableEntry(
                    LsmRowKeyCodec.RowInternalKey(cursor.Key, cursor.Sequence, cursor.Type),
                    cursor.Type == LsmValueType.Put ? cursor.Value.ToArray() : []));
            }

            // The properties carry the metadata version this file held; merge it like any other entry.
            SsTableV2Properties properties = reader.Properties;
            if (properties.NextRowId > 0)
            {
                entries.Add(new SstableEntry(
                    LsmRowKeyCodec.MetadataInternalKey(properties.NextRowIdSequence),
                    LsmRowKeyCodec.EncodeTableMetadata(properties.NextRowId, properties.LiveRowCount)));
            }

            return entries;
        }
        finally
        {
            owned?.Dispose();
        }
    }

    private (LsmTableFileMetadata Metadata, string Path) WriteKeyedOutput(List<SstableEntry> entries, int targetLevel)
    {
        long fileNumber = _manifest.AllocateFileNumber();
        string fileName = $"{fileNumber:D6}.sst";
        string outputPath = Path.Combine(_tableDirectory, fileName);
        long length = 0;
        LsmDurableFileOperations.WriteFileAtomically(
            outputPath,
            overwrite: false,
            stream =>
            {
                using var writer = new SsTableKeyedWriter(stream, LsmIndexKeyCodec.BloomSuffix);
                foreach (SstableEntry entry in entries)
                {
                    writer.Add(InternalKey.UserKey(entry.InternalKey), InternalKey.Sequence(entry.InternalKey), InternalKey.ValueType(entry.InternalKey), entry.Value);
                }

                length = writer.Finish();
            },
            crashHook: null,
            LsmCrashPoint.AfterSstableTempFileFsyncBeforeRename,
            LsmCrashPoint.AfterSstableRenameBeforeDirectoryFsync,
            LsmCrashPoint.AfterSstableDirectoryFsyncBeforeManifest);

        return (new LsmTableFileMetadata(fileNumber, targetLevel, entries[0].InternalKey, entries[^1].InternalKey, length, fileName), outputPath);
    }

    private (LsmTableFileMetadata Metadata, string Path) WriteV2Output(List<SstableEntry> entries, int targetLevel)
    {
        byte[]? first = null;
        byte[]? last = null;
        foreach (SstableEntry entry in entries)
        {
            if (LsmRowKeyCodec.IsRowKey(InternalKey.UserKey(entry.InternalKey)))
            {
                first ??= entry.InternalKey;
                last = entry.InternalKey;
            }
        }

        if (first is null || last is null)
        {
            throw new InvalidOperationException("Compaction output for a row-keyed table has no row entries.");
        }

        long fileNumber = _manifest.AllocateFileNumber();
        string fileName = $"{fileNumber:D6}.sst";
        string outputPath = Path.Combine(_tableDirectory, fileName);
        long length = 0;
        LsmDurableFileOperations.WriteFileAtomically(
            outputPath,
            overwrite: false,
            stream =>
            {
                using var writer = new SsTableV2Writer(stream, alignment: 0, entries.Count);
                foreach (SstableEntry entry in entries)
                {
                    LsmRowKeyCodec.AddInternalEntry(writer, entry.InternalKey, entry.Value);
                }

                length = writer.Finish();
            },
            crashHook: null,
            LsmCrashPoint.AfterSstableTempFileFsyncBeforeRename,
            LsmCrashPoint.AfterSstableRenameBeforeDirectoryFsync,
            LsmCrashPoint.AfterSstableDirectoryFsyncBeforeManifest);

        return (new LsmTableFileMetadata(fileNumber, targetLevel, first, last, length, fileName), outputPath);
    }

    private static byte[] WidenRowInternalKey(byte[] internalKey)
    {
        ReadOnlySpan<byte> userKey = InternalKey.UserKey(internalKey);
        if (userKey.Length != LsmRowKeyCodec.LegacyRowKeySize)
        {
            return internalKey;
        }

        var widened = new byte[LsmRowKeyCodec.RowKeySize + InternalKey.TagSize];
        userKey.CopyTo(widened.AsSpan(LsmRowKeyCodec.RowKeySize - LsmRowKeyCodec.LegacyRowKeySize));
        internalKey.AsSpan(userKey.Length).CopyTo(widened.AsSpan(LsmRowKeyCodec.RowKeySize));
        return widened;
    }

    /// <summary>
    /// The user key a file's bounds are compared by. Row tables' manifests may still record 8-byte keys
    /// of files written before row keys were widened; those are compared in their 16-byte form.
    /// </summary>
    private static byte[] BoundUserKey(byte[] internalKey, bool rowKeyed) =>
        rowKeyed
            ? LsmRowKeyCodec.NormalizeUserKey(InternalKey.UserKey(internalKey))
            : InternalKey.UserKey(internalKey).ToArray();

    private static LsmTableFileMetadata[] SelectTargetOverlapsClosed(
        IReadOnlyList<LsmTableFileMetadata> sourceFiles,
        IReadOnlyList<LsmTableFileMetadata> targetFiles,
        bool rowKeyed)
    {
        (byte[] smallestUserKey, byte[] largestUserKey) = UserKeyBounds(sourceFiles, rowKeyed);
        var selectedTargets = new List<LsmTableFileMetadata>();
        var selectedTargetFileNumbers = new HashSet<long>();

        bool expanded;
        do
        {
            expanded = false;
            foreach (LsmTableFileMetadata target in targetFiles)
            {
                if (selectedTargetFileNumbers.Contains(target.FileNumber))
                {
                    continue;
                }

                byte[] targetSmallest = BoundUserKey(target.SmallestInternalKey, rowKeyed);
                byte[] targetLargest = BoundUserKey(target.LargestInternalKey, rowKeyed);
                if (!UserKeyRangeOverlaps(smallestUserKey, largestUserKey, targetSmallest, targetLargest))
                {
                    continue;
                }

                selectedTargets.Add(target);
                selectedTargetFileNumbers.Add(target.FileNumber);
                if (targetSmallest.AsSpan().SequenceCompareTo(smallestUserKey) < 0)
                {
                    smallestUserKey = targetSmallest;
                    expanded = true;
                }

                if (targetLargest.AsSpan().SequenceCompareTo(largestUserKey) > 0)
                {
                    largestUserKey = targetLargest;
                    expanded = true;
                }
            }
        }
        while (expanded);

        return selectedTargets.ToArray();
    }

    private static (byte[] SmallestUserKey, byte[] LargestUserKey) UserKeyBounds(
        IReadOnlyList<LsmTableFileMetadata> files,
        bool rowKeyed)
    {
        byte[] smallestUserKey = BoundUserKey(files[0].SmallestInternalKey, rowKeyed);
        byte[] largestUserKey = BoundUserKey(files[0].LargestInternalKey, rowKeyed);

        for (int i = 1; i < files.Count; i++)
        {
            byte[] candidateSmallest = BoundUserKey(files[i].SmallestInternalKey, rowKeyed);
            if (candidateSmallest.AsSpan().SequenceCompareTo(smallestUserKey) < 0)
            {
                smallestUserKey = candidateSmallest;
            }

            byte[] candidateLargest = BoundUserKey(files[i].LargestInternalKey, rowKeyed);
            if (candidateLargest.AsSpan().SequenceCompareTo(largestUserKey) > 0)
            {
                largestUserKey = candidateLargest;
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
        ReadOnlySpan<byte> rightSmallest,
        ReadOnlySpan<byte> rightLargest)
    {
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
