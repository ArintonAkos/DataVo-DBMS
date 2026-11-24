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

    /// <summary>The columns a row-keyed output file carries as a columnar segment, or null for none.</summary>
    internal DataVo.Core.Indexing.Columnar.ColumnarSpec? Columnar { get; set; }

    /// <summary>
    /// The bytes a row-keyed output keeps in memory per buffer (Bloom keys, column groups, deleted keys) before
    /// moving it to a temporary file in the table directory.
    /// </summary>
    internal long SpillMemoryLimit { get; set; } = SsTableSpillOptions.DefaultMemoryLimit;

    /// <summary>How v2 inputs are read when no registry handle provides a reader.</summary>
    internal LsmReadMode ReadMode { get; set; }

    /// <summary>The engine's block cache for positional reads, or null.</summary>
    internal LsmBlockCache? BlockCache { get; set; }

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
            LsmTableFileMetadata? outputMetadata = null;
            string? outputPath = null;
            LsmMergeSource[] sources = OpenSources(selectedFiles, selectedHandles);
            try
            {
                var merge = new LsmCompactionMerge(sources, dropTombstonesAtBottomLevel, keepLastRowTombstone: WritesV2);

                // The output file is only created once there is something to write.
                if (merge.MoveNext())
                {
                    (outputMetadata, outputPath) = WritesV2
                        ? WriteV2Output(merge, targetLevel)
                        : Format == LsmSstableFormat.Keyed
                            ? WriteKeyedOutput(merge, targetLevel)
                            : WriteLegacyOutput(merge, targetLevel);
                }
            }
            finally
            {
                foreach (LsmMergeSource source in sources)
                {
                    source.Dispose();
                }
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

    // One merge input per file, in the order of `files` (which breaks ties between equal internal keys). A
    // row-keyed file's table metadata lives in its properties and becomes an input of its own, with the file's
    // ordinal, as it is merged like any other entry.
    private LsmMergeSource[] OpenSources(IReadOnlyList<LsmTableFileMetadata> files, IReadOnlyList<LsmSstableHandle> handles)
    {
        var sources = new List<LsmMergeSource>(files.Count + 1);
        try
        {
            for (int i = 0; i < files.Count; i++)
            {
                AddSources(sources, i, files[i], handles.Count == files.Count ? handles[i] : null);
            }
        }
        catch
        {
            foreach (LsmMergeSource source in sources)
            {
                source.Dispose();
            }

            throw;
        }

        return sources.ToArray();
    }

    private void AddSources(List<LsmMergeSource> sources, int ordinal, LsmTableFileMetadata file, LsmSstableHandle? handle)
    {
        string path = Path.Combine(_tableDirectory, file.FileName);
        if (SsTableKeyedFormat.IsKeyedFile(path))
        {
            SsTableKeyedReader keyed = handle?.GetKeyedReader(ReadMode, BlockCache) ?? SsTableKeyedReader.Open(path, ReadMode);
            sources.Add(new LsmKeyedFileMergeSource(ordinal, keyed, ownsReader: handle is null));
            return;
        }

        bool isV2 = handle?.IsV2() ?? SsTableV2Format.IsV2File(path);
        if (!isV2)
        {
            sources.Add(new LsmListMergeSource(ordinal, ReadLegacyEntries(handle?.ReadAllBytes() ?? File.ReadAllBytes(path))));
            return;
        }

        SsTableV2Reader reader = handle?.GetV2Reader(ReadMode, BlockCache) ?? SsTableV2Reader.Open(path, ReadMode);
        sources.Add(new LsmRowFileMergeSource(ordinal, reader, ownsReader: handle is null));
        SsTableV2Properties properties = reader.Properties;
        if (properties.NextRowId > 0)
        {
            sources.Add(new LsmListMergeSource(ordinal, [(
                LsmRowKeyCodec.MetadataInternalKey(properties.NextRowIdSequence),
                LsmRowKeyCodec.EncodeTableMetadata(properties.NextRowId, properties.LiveRowCount))]));
        }
    }

    // A v1 file is one block, read whole. In a row-keyed table its 8-byte row keys are merged in their
    // 16-byte form so they order correctly against rows read from newer files.
    private List<(byte[] InternalKey, byte[] Value)> ReadLegacyEntries(byte[] bytes)
    {
        SsTableReader.Load(bytes);
        List<(byte[] InternalKey, byte[] Value)> entries = ReadEntries(bytes);
        if (WritesV2)
        {
            for (int i = 0; i < entries.Count; i++)
            {
                entries[i] = (WidenRowInternalKey(entries[i].InternalKey), entries[i].Value);
            }
        }

        entries.Sort(static (x, y) => InternalKey.Compare(x.InternalKey, y.InternalKey));
        return entries;
    }

    private (LsmTableFileMetadata Metadata, string Path) WriteLegacyOutput(LsmCompactionMerge merge, int targetLevel)
    {
        var writer = new SsTableWriter();
        byte[] first = merge.InternalKey.ToArray();
        byte[] last;
        do
        {
            writer.Add(merge.InternalKey, merge.Value);
            last = merge.InternalKey.ToArray();
        }
        while (merge.MoveNext());

        byte[] bytes = writer.Finish();
        long fileNumber = _manifest.AllocateFileNumber();
        string fileName = $"{fileNumber:D6}.sst";
        string outputPath = Path.Combine(_tableDirectory, fileName);
        WriteSstableAtomically(outputPath, bytes);
        return (new LsmTableFileMetadata(fileNumber, targetLevel, first, last, bytes.LongLength, fileName), outputPath);
    }

    private (LsmTableFileMetadata Metadata, string Path) WriteKeyedOutput(LsmCompactionMerge merge, int targetLevel)
    {
        long fileNumber = _manifest.AllocateFileNumber();
        string fileName = $"{fileNumber:D6}.sst";
        string outputPath = Path.Combine(_tableDirectory, fileName);
        long length = 0;
        byte[] first = merge.InternalKey.ToArray();
        var last = new KeyBuffer();
        LsmDurableFileOperations.WriteFileAtomically(
            outputPath,
            overwrite: false,
            stream =>
            {
                using var writer = new SsTableKeyedWriter(stream, LsmIndexKeyCodec.BloomSuffix);
                do
                {
                    ReadOnlySpan<byte> key = merge.InternalKey;
                    writer.Add(InternalKey.UserKey(key), InternalKey.Sequence(key), InternalKey.ValueType(key), merge.Value);
                    last.Set(key);
                }
                while (merge.MoveNext());

                length = writer.Finish();
            },
            crashHook: null,
            LsmCrashPoint.AfterSstableTempFileFsyncBeforeRename,
            LsmCrashPoint.AfterSstableRenameBeforeDirectoryFsync,
            LsmCrashPoint.AfterSstableDirectoryFsyncBeforeManifest);

        return (new LsmTableFileMetadata(fileNumber, targetLevel, first, last.ToArray(), length, fileName), outputPath);
    }

    [System.Runtime.CompilerServices.MethodImpl(DataVo.Core.Compat.MethodImplCompat.AggressiveOptimization)]
    private (LsmTableFileMetadata Metadata, string Path) WriteV2Output(LsmCompactionMerge merge, int targetLevel)
    {
        long fileNumber = _manifest.AllocateFileNumber();
        string fileName = $"{fileNumber:D6}.sst";
        string outputPath = Path.Combine(_tableDirectory, fileName);
        long length = 0;
        byte[]? first = null;
        var last = new KeyBuffer();
        LsmDurableFileOperations.WriteFileAtomically(
            outputPath,
            overwrite: false,
            stream =>
            {
                using var writer = new SsTableV2Writer(stream, alignment: 0, columnar: Columnar, spill: new SsTableSpillOptions(_tableDirectory, SpillMemoryLimit));
                do
                {
                    ReadOnlySpan<byte> key = merge.InternalKey;
                    if (LsmRowKeyCodec.IsRowKey(InternalKey.UserKey(key)))
                    {
                        first ??= key.ToArray();
                        last.Set(key);
                    }

                    LsmRowKeyCodec.AddInternalEntry(writer, key, merge.Value);
                }
                while (merge.MoveNext());

                if (first is null)
                {
                    throw new InvalidOperationException("Compaction output for a row-keyed table has no row entries.");
                }

                length = writer.Finish();
            },
            crashHook: null,
            LsmCrashPoint.AfterSstableTempFileFsyncBeforeRename,
            LsmCrashPoint.AfterSstableRenameBeforeDirectoryFsync,
            LsmCrashPoint.AfterSstableDirectoryFsyncBeforeManifest);

        return (new LsmTableFileMetadata(fileNumber, targetLevel, first!, last.ToArray(), length, fileName), outputPath);
    }

    /// <summary>The last of a run of internal keys, kept without allocating per key.</summary>
    private sealed class KeyBuffer
    {
        private byte[] _bytes = new byte[32];
        private int _length;

        public void Set(ReadOnlySpan<byte> key)
        {
            if (_bytes.Length < key.Length)
            {
                _bytes = new byte[Math.Max(key.Length, _bytes.Length * 2)];
            }

            key.CopyTo(_bytes);
            _length = key.Length;
        }

        public byte[] ToArray() => _bytes.AsSpan(0, _length).ToArray();
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

    private static List<(byte[] InternalKey, byte[] Value)> ReadEntries(byte[] bytes)
    {
        if (!SsTableFormat.TryReadFooter(bytes, out SsTableBlockHandle indexBlock, out _))
        {
            throw new InvalidDataException("SSTable footer is missing, corrupt, or points outside the table.");
        }

        var entries = new List<(byte[] InternalKey, byte[] Value)>();
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
            entries.Add((internalKey, value));
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
}
