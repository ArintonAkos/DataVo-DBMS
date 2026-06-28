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
        ArgumentException.ThrowIfNullOrWhiteSpace(tableDirectory);
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

    private void DeletePhysicalFile()
    {
        lock (_deleteGate)
        {
            if (Volatile.Read(ref _deleted) != 0 || Volatile.Read(ref _refCount) != 0)
            {
                return;
            }

            if (File.Exists(FilePath))
            {
                File.Delete(FilePath);
            }

            Volatile.Write(ref _deleted, 1);
        }
    }
}
