using Microsoft.Win32.SafeHandles;

namespace DataVo.Core.StorageEngine.Disk;

internal sealed class FileHandlePool : IDisposable
{
    private readonly object _sync = new();
    private readonly int _capacity;
    private readonly Dictionary<string, Entry> _entries = new(StringComparer.Ordinal);
    private bool _disposed;

    public FileHandlePool(int capacity = 1024)
    {
        if (capacity <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(capacity), capacity, "Capacity must be greater than zero.");
        }

        _capacity = capacity;
    }

    public FileHandleLease Acquire(string path)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);

        string normalizedPath = NormalizePath(path);

        lock (_sync)
        {
            ThrowIfDisposed();

            if (_entries.TryGetValue(normalizedPath, out Entry? existing))
            {
                existing.RefCount++;
                existing.LastUsedTicks = Environment.TickCount64;
                return new FileHandleLease(this, normalizedPath, existing.Handle);
            }

            EvictIdleEntriesIfNeeded();

            Directory.CreateDirectory(Path.GetDirectoryName(normalizedPath) ?? ".");
            SafeFileHandle handle = File.OpenHandle(
                normalizedPath,
                FileMode.OpenOrCreate,
                FileAccess.ReadWrite,
                FileShare.ReadWrite | FileShare.Delete,
                FileOptions.RandomAccess);

            var entry = new Entry(handle)
            {
                RefCount = 1,
                LastUsedTicks = Environment.TickCount64
            };
            _entries.Add(normalizedPath, entry);

            return new FileHandleLease(this, normalizedPath, handle);
        }
    }

    /// <summary>
    /// Forces every open pooled handle to the physical device (<c>fsync</c>). Because a handle only ever
    /// leaves the pool through <see cref="Close"/> — which already fsyncs — flushing the open handles is
    /// sufficient to make all buffered data-file writes durable.
    /// </summary>
    public void FlushToDisk()
    {
        lock (_sync)
        {
            if (_disposed)
            {
                return;
            }

            foreach (Entry entry in _entries.Values)
            {
                if (entry.Handle.IsClosed)
                {
                    continue;
                }

                try
                {
                    RandomAccess.FlushToDisk(entry.Handle);
                }
                catch (IOException)
                {
                }
                catch (UnauthorizedAccessException)
                {
                }
                catch (ObjectDisposedException)
                {
                }
            }
        }
    }

    public void Remove(string path)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);

        string normalizedPath = NormalizePath(path);

        lock (_sync)
        {
            if (!_entries.TryGetValue(normalizedPath, out Entry? entry))
            {
                return;
            }

            entry.RemoveWhenIdle = true;
            if (entry.RefCount == 0)
            {
                _entries.Remove(normalizedPath);
                Close(entry);
            }
        }
    }

    public void Dispose()
    {
        lock (_sync)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            foreach (Entry entry in _entries.Values)
            {
                Close(entry);
            }

            _entries.Clear();
        }
    }

    private void Release(string normalizedPath)
    {
        lock (_sync)
        {
            if (!_entries.TryGetValue(normalizedPath, out Entry? entry))
            {
                return;
            }

            entry.RefCount--;
            entry.LastUsedTicks = Environment.TickCount64;

            if (entry.RefCount == 0 && entry.RemoveWhenIdle)
            {
                _entries.Remove(normalizedPath);
                Close(entry);
                return;
            }

            EvictIdleEntriesIfNeeded();
        }
    }

    private void EvictIdleEntriesIfNeeded()
    {
        while (_entries.Count >= _capacity)
        {
            KeyValuePair<string, Entry>? candidate = null;
            foreach (var entry in _entries)
            {
                if (entry.Value.RefCount != 0)
                {
                    continue;
                }

                if (candidate is null || entry.Value.LastUsedTicks < candidate.Value.Value.LastUsedTicks)
                {
                    candidate = entry;
                }
            }

            if (candidate is null)
            {
                return;
            }

            _entries.Remove(candidate.Value.Key);
            Close(candidate.Value.Value);
        }
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(FileHandlePool));
        }
    }

    private static string NormalizePath(string path) => Path.GetFullPath(path);

    private static void Close(Entry entry)
    {
        if (entry.Handle.IsClosed)
        {
            return;
        }

        try
        {
            RandomAccess.FlushToDisk(entry.Handle);
        }
        catch (IOException)
        {
        }
        catch (UnauthorizedAccessException)
        {
        }
        catch (ObjectDisposedException)
        {
        }

        entry.Handle.Dispose();
    }

    private sealed class Entry(SafeFileHandle handle)
    {
        public SafeFileHandle Handle { get; } = handle;
        public int RefCount { get; set; }
        public long LastUsedTicks { get; set; }
        public bool RemoveWhenIdle { get; set; }
    }

    public sealed class FileHandleLease : IDisposable
    {
        private readonly FileHandlePool? _owner;
        private readonly string? _normalizedPath;
        private bool _disposed;

        internal FileHandleLease(FileHandlePool owner, string normalizedPath, SafeFileHandle handle)
        {
            _owner = owner;
            _normalizedPath = normalizedPath;
            Handle = handle;
        }

        public SafeFileHandle Handle { get; }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            if (_owner is not null && _normalizedPath is not null)
            {
                _owner.Release(_normalizedPath);
            }
        }
    }
}
