using System.Collections.Concurrent;

namespace DataVo.Core.StorageEngine.Lsm;

/// <summary>Tracks physical SSTable references and deferred deletion for one table directory.</summary>
internal sealed class LsmFileRegistry
{
    private readonly string _tableDirectory;
    private readonly LsmManifest _manifest;
    private readonly ConcurrentDictionary<long, LsmSstableHandle> _handles = new();
    private readonly object _versionGate = new();

    public LsmFileRegistry(string tableDirectory, LsmManifest manifest)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tableDirectory);
        ArgumentNullException.ThrowIfNull(manifest);
        _tableDirectory = tableDirectory;
        _manifest = manifest;
        RefreshFromManifest();
    }

    public LsmReadSnapshot OpenSnapshot()
    {
        lock (_versionGate)
        {
            RefreshFromManifest();
            LsmTableFileMetadata[] liveFiles = GetAllLiveFiles();
            var handles = new List<LsmSstableHandle>(liveFiles.Length);
            try
            {
                foreach (LsmTableFileMetadata file in liveFiles)
                {
                    LsmSstableHandle handle = GetOrAdd(file);
                    handle.AddRef();
                    handles.Add(handle);
                }
            }
            catch
            {
                foreach (LsmSstableHandle handle in handles)
                {
                    handle.Release();
                }

                throw;
            }

            return new LsmReadSnapshot(handles.ToArray());
        }
    }

    public LsmSstableHandle[] AddRefs(IReadOnlyList<LsmTableFileMetadata> files)
    {
        var handles = new List<LsmSstableHandle>(files.Count);
        try
        {
            foreach (LsmTableFileMetadata file in files)
            {
                LsmSstableHandle handle = GetOrAdd(file);
                handle.AddRef();
                handles.Add(handle);
            }
        }
        catch
        {
            foreach (LsmSstableHandle handle in handles)
            {
                handle.Release();
            }

            throw;
        }

        return handles.ToArray();
    }

    public void Release(IReadOnlyList<LsmSstableHandle> handles)
    {
        foreach (LsmSstableHandle handle in handles)
        {
            handle.Release();
        }
    }

    public void Register(LsmTableFileMetadata metadata)
    {
        _ = GetOrAdd(metadata);
    }

    public void MarkDeleted(IReadOnlyList<LsmTableFileMetadata> files)
    {
        foreach (LsmTableFileMetadata file in files)
        {
            if (_handles.TryGetValue(file.FileNumber, out LsmSstableHandle? handle))
            {
                handle.MarkDeletedOrDeleteNow();
            }
            else
            {
                string path = Path.Combine(_tableDirectory, file.FileName);
                if (File.Exists(path))
                {
                    File.Delete(path);
                }
            }
        }
    }

    public T ExecuteVersionEdit<T>(Func<T> action)
    {
        ArgumentNullException.ThrowIfNull(action);
        lock (_versionGate)
        {
            return action();
        }
    }

    private void RefreshFromManifest()
    {
        foreach (LsmTableFileMetadata file in GetAllLiveFiles())
        {
            Register(file);
        }
    }

    private LsmSstableHandle GetOrAdd(LsmTableFileMetadata metadata) =>
        _handles.GetOrAdd(metadata.FileNumber, _ => new LsmSstableHandle(_tableDirectory, metadata));

    private LsmTableFileMetadata[] GetAllLiveFiles()
    {
        var files = new List<LsmTableFileMetadata>();
        for (int level = 0; level <= 7; level++)
        {
            files.AddRange(_manifest.GetLiveFiles(level));
        }

        return files.ToArray();
    }
}
