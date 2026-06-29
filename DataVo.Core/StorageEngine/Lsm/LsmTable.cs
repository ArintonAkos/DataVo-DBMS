namespace DataVo.Core.StorageEngine.Lsm;

/// <summary>Result of flushing one MemTable generation to an SSTable file.</summary>
public readonly record struct LsmFlushResult(
    LsmTableFileMetadata Metadata,
    long ByteCount,
    string FilePath,
    byte[] Bytes);

internal readonly record struct LsmBatchPutEntry(byte[] UserKey, ulong Seqno, byte[] Value);

/// <summary>Coordinates one active MemTable generation with SSTable flushes and manifest registration.</summary>
public sealed class LsmTable : IDisposable
{
    private readonly object _gate = new();
    private readonly string _tableDirectory;
    private readonly LsmManifest _manifest;
    private readonly Action<LsmVersionEdit> _applyEdit;
    private readonly Action<LsmCrashPoint>? _crashHook;
    private readonly LsmWalWriter? _walWriter;
    private MemTable _active = new();
    private bool _disposed;

    /// <summary>Creates an LSM table rooted at <paramref name="tableDirectory"/> and backed by <paramref name="manifest"/>.</summary>
    public LsmTable(string tableDirectory, LsmManifest manifest)
        : this(tableDirectory, manifest, walWriter: null, applyEdit: null)
    {
    }

    /// <summary>
    /// Creates an LSM table with an active MemTable WAL rooted at <paramref name="walPath"/>.
    /// Strict fsync is the default production durability mode.
    /// </summary>
    public LsmTable(
        string tableDirectory,
        LsmManifest manifest,
        string walPath,
        LsmWalDurabilityMode durabilityMode = LsmWalDurabilityMode.StrictFsync)
        : this(tableDirectory, manifest, new LsmWalWriter(walPath, durabilityMode), applyEdit: null)
    {
    }

    private LsmTable(
        string tableDirectory,
        LsmManifest manifest,
        LsmWalWriter? walWriter,
        Action<LsmVersionEdit>? applyEdit,
        Action<LsmCrashPoint>? crashHook = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tableDirectory);
        ArgumentNullException.ThrowIfNull(manifest);

        _tableDirectory = tableDirectory;
        _manifest = manifest;
        _crashHook = crashHook;
        _walWriter = walWriter;
        _applyEdit = applyEdit ?? manifest.ApplyEdit;
        Directory.CreateDirectory(_tableDirectory);
        _walWriter?.ReplayInto(_active);
    }

    internal static LsmTable CreateForTesting(
        string tableDirectory,
        LsmManifest manifest,
        Action<LsmVersionEdit> applyEdit) =>
        new(tableDirectory, manifest, walWriter: null, applyEdit);

    internal static LsmTable CreateForTesting(
        string tableDirectory,
        LsmManifest manifest,
        string walPath,
        LsmWalDurabilityMode durabilityMode,
        Action<LsmCrashPoint> crashHook) =>
        new(tableDirectory, manifest, new LsmWalWriter(walPath, durabilityMode), applyEdit: null, crashHook);

    /// <summary>Number of version entries currently buffered in the active mutable MemTable.</summary>
    public int ActiveCount
    {
        get
        {
            lock (_gate)
            {
                ObjectDisposedException.ThrowIf(_disposed, this);
                return _active.Count;
            }
        }
    }

    /// <summary>Adds a value version to the active MemTable.</summary>
    public void Put(ReadOnlySpan<byte> userKey, ulong seqno, ReadOnlySpan<byte> value)
    {
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            _walWriter?.AppendMutation(userKey, seqno, LsmValueType.Put, value);
            _active.Put(userKey, seqno, LsmValueType.Put, value);
        }
    }

    internal void PutBatch(IReadOnlyList<LsmBatchPutEntry> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);

        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (entries.Count == 0)
            {
                return;
            }

            if (_walWriter is not null)
            {
                foreach (LsmBatchPutEntry entry in entries)
                {
                    _walWriter.AppendMutationBuffered(entry.UserKey, entry.Seqno, LsmValueType.Put, entry.Value);
                }

                _walWriter.FlushBufferedMutations();
            }

            foreach (LsmBatchPutEntry entry in entries)
            {
                _active.Put(entry.UserKey, entry.Seqno, LsmValueType.Put, entry.Value);
            }
        }
    }

    /// <summary>Adds a tombstone version to the active MemTable.</summary>
    public void Delete(ReadOnlySpan<byte> userKey, ulong seqno)
    {
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            _walWriter?.AppendMutation(userKey, seqno, LsmValueType.Deletion, ReadOnlySpan<byte>.Empty);
            _active.Delete(userKey, seqno);
        }
    }

    /// <summary>
    /// Serializes the active MemTable flush, writes it to disk, registers the new level-0 SSTable
    /// in the manifest, and then installs a fresh active table. Empty active tables are a no-op.
    /// </summary>
    public LsmFlushResult? FlushActiveMemTable()
    {
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_active.Count == 0)
            {
                return null;
            }

            (byte[] smallest, byte[] largest) = ComputeBounds(_active);
            byte[] bytes = SsTableWriter.Write(_active);

            long fileNumber = _manifest.AllocateFileNumber();
            string fileName = $"{fileNumber:D6}.sst";
            string filePath = Path.Combine(_tableDirectory, fileName);
            WriteSstableAtomically(filePath, bytes, _crashHook);

            var metadata = new LsmTableFileMetadata(
                fileNumber,
                level: 0,
                smallest,
                largest,
                bytes.LongLength,
                fileName);

            var edit = new LsmVersionEdit();
            edit.AddFile(metadata);
            try
            {
                _manifest.CrashHook = _crashHook;
                _applyEdit(edit);
            }
            catch (LsmCrashSimulationException)
            {
                throw;
            }
            catch
            {
                DeleteFileIfExists(filePath);
                throw;
            }
            finally
            {
                _manifest.CrashHook = null;
            }

            MemTable flushed = _active;
            flushed.Freeze();
            _active = new MemTable();
            flushed.Dispose();
            _walWriter?.Clear();

            return new LsmFlushResult(metadata, bytes.LongLength, filePath, bytes);
        }
    }

    /// <summary>Releases the current active MemTable generation.</summary>
    public void Dispose()
    {
        MemTable active;
        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            active = _active;
            _active = null!;
        }

        active.Dispose();
    }

    private static (byte[] Smallest, byte[] Largest) ComputeBounds(MemTable memTable)
    {
        byte[]? smallest = null;
        byte[]? largest = null;
        foreach (MemTableEntry entry in memTable)
        {
            byte[] key = entry.InternalKey.ToArray();
            if (smallest is null || InternalKey.Compare(key, smallest) < 0)
            {
                smallest = key;
            }

            if (largest is null || InternalKey.Compare(key, largest) > 0)
            {
                largest = key;
            }
        }

        if (smallest is null || largest is null)
        {
            throw new InvalidOperationException("Cannot flush an empty MemTable.");
        }

        return (smallest, largest);
    }

    private static void WriteSstableAtomically(
        string filePath,
        byte[] bytes,
        Action<LsmCrashPoint>? crashHook)
    {
        LsmDurableFileOperations.WriteFileAtomically(
            filePath,
            overwrite: false,
            stream => stream.Write(bytes),
            crashHook,
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
