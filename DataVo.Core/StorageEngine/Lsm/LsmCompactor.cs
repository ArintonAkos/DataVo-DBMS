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
    internal LsmSstableFormat Format { get; set; } = LsmSstableFormat.Keyed;

    private bool WritesV2 => Format == LsmSstableFormat.RowKeyed;

    /// <summary>Bloom setting of keyed output files (see <see cref="SsTableKeyedFormat.BloomKeyLength"/>).</summary>
    internal int BloomSuffixLength { get; set; }

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
                        : WriteKeyedOutput(merge, targetLevel);
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

        if (!(handle?.IsV2() ?? SsTableV2Format.IsV2File(path)))
        {
            throw new InvalidDataException($"SSTable '{file.FileName}' is neither a row-keyed nor a keyed file.");
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
                using var writer = new SsTableKeyedWriter(stream, BloomSuffixLength);
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

    /// <summary>
    /// The user key a file's bounds are compared by.
    /// </summary>
    private static byte[] BoundUserKey(byte[] internalKey, bool rowKeyed) => InternalKey.UserKey(internalKey).ToArray();

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

    private static bool UserKeyRangeOverlaps(
        ReadOnlySpan<byte> leftSmallest,
        ReadOnlySpan<byte> leftLargest,
        ReadOnlySpan<byte> rightSmallest,
        ReadOnlySpan<byte> rightLargest)
    {
        return leftSmallest.SequenceCompareTo(rightLargest) <= 0
            && rightSmallest.SequenceCompareTo(leftLargest) <= 0;
    }

    private static void DeleteFileIfExists(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }
}
