using DataVo.Core.StorageEngine.Lsm.V2;

namespace DataVo.Core.StorageEngine.Lsm;

/// <summary>Reference-counted physical SSTable ownership for snapshots and compaction deletion.</summary>
internal sealed class LsmSstableHandle
{
    private readonly object _deleteGate = new();
    private int _refCount;
    private int _deleteWhenUnreferenced;
    private int _deleted;

    public LsmSstableHandle(string tableDirectory, LsmTableFileMetadata metadata)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfNullOrWhiteSpace(tableDirectory);
        TableDirectory = tableDirectory;
        Metadata = metadata.Copy();
        FilePath = Path.Combine(tableDirectory, Metadata.FileName);
    }

    public string TableDirectory { get; }

    public LsmTableFileMetadata Metadata { get; }

    public string FilePath { get; }

    public long FileNumber => Metadata.FileNumber;

    public int RefCount => Volatile.Read(ref _refCount);

    public void AddRef()
    {
        if (Volatile.Read(ref _deleted) != 0)
        {
            throw new ObjectDisposedException(nameof(LsmSstableHandle));
        }

        Interlocked.Increment(ref _refCount);
        if (Volatile.Read(ref _deleted) != 0)
        {
            Release();
            throw new ObjectDisposedException(nameof(LsmSstableHandle));
        }
    }

    public void Release()
    {
        int count = Interlocked.Decrement(ref _refCount);
        if (count < 0)
        {
            throw new InvalidOperationException("SSTable handle reference count became negative.");
        }

        if (count == 0 && Volatile.Read(ref _deleteWhenUnreferenced) != 0)
        {
            DeletePhysicalFile();
        }
    }

    public void MarkDeletedOrDeleteNow()
    {
        Volatile.Write(ref _deleteWhenUnreferenced, 1);
        if (Volatile.Read(ref _refCount) == 0)
        {
            DeletePhysicalFile();
        }
    }

    public byte[] ReadAllBytes() => File.ReadAllBytes(FilePath);

    private readonly object _readerGate = new();
    private SsTableV2Reader? _reader;

    /// <summary>
    /// The file's v2 reader, opened (memory-mapped or positional) on first use and kept until the file
    /// is deleted or <see cref="CloseReader"/> runs. Callers must hold a reference (<see cref="AddRef"/>).
    /// </summary>
    public SsTableV2Reader GetV2Reader(LsmReadMode mode)
    {
        SsTableV2Reader? reader = Volatile.Read(ref _reader);
        if (reader is not null)
        {
            return reader;
        }

        lock (_readerGate)
        {
            if (Volatile.Read(ref _deleted) != 0)
            {
                throw new ObjectDisposedException(nameof(LsmSstableHandle));
            }

            _reader ??= SsTableV2Reader.Open(FilePath, mode);
            return _reader;
        }
    }

    private SsTableKeyedReader? _keyedReader;

    /// <summary>The file's keyed (v3) reader, opened on first use; see <see cref="GetV2Reader"/>.</summary>
    public SsTableKeyedReader GetKeyedReader(LsmReadMode mode)
    {
        SsTableKeyedReader? reader = Volatile.Read(ref _keyedReader);
        if (reader is not null)
        {
            return reader;
        }

        lock (_readerGate)
        {
            if (Volatile.Read(ref _deleted) != 0)
            {
                throw new ObjectDisposedException(nameof(LsmSstableHandle));
            }

            _keyedReader ??= SsTableKeyedReader.Open(FilePath, mode);
            return _keyedReader;
        }
    }

    /// <summary>Whether the file on disk is a v2 SSTable (reads only the footer).</summary>
    public bool IsV2() => SsTableV2Format.IsV2File(FilePath);

    /// <summary>Unmaps the reader (shutdown, table drop); a later <see cref="GetV2Reader"/> reopens it.</summary>
    public void CloseReader()
    {
        lock (_readerGate)
        {
            _reader?.Dispose();
            _reader = null;
            _keyedReader?.Dispose();
            _keyedReader = null;
        }
    }

    private void DeletePhysicalFile()
    {
        lock (_deleteGate)
        {
            if (Volatile.Read(ref _deleted) != 0 || Volatile.Read(ref _refCount) != 0)
            {
                return;
            }

            // Unmap first: Windows refuses to delete a file that is still mapped.
            CloseReader();
            if (File.Exists(FilePath))
            {
                File.Delete(FilePath);
            }

            Volatile.Write(ref _deleted, 1);
        }
    }
}
