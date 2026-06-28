namespace DataVo.Core.StorageEngine.Lsm;

/// <summary>Disposable set of referenced SSTables visible to one read snapshot.</summary>
internal sealed class LsmReadSnapshot : IDisposable
{
    private readonly LsmSstableHandle[] _handles;
    private bool _disposed;

    internal LsmReadSnapshot(LsmSstableHandle[] handles)
    {
        _handles = handles;
    }

    public IReadOnlyList<LsmTableFileMetadata> Files => _handles.Select(static handle => handle.Metadata).ToArray();

    public byte[] ReadAllBytes(long fileNumber)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        foreach (LsmSstableHandle handle in _handles)
        {
            if (handle.FileNumber == fileNumber)
            {
                return handle.ReadAllBytes();
            }
        }

        throw new FileNotFoundException($"SSTable {fileNumber} is not part of this snapshot.");
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        foreach (LsmSstableHandle handle in _handles)
        {
            handle.Release();
        }
    }
}
