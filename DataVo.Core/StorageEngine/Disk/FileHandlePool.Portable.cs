#if NETSTANDARD2_1
using Microsoft.Win32.SafeHandles;

namespace DataVo.Core.StorageEngine.Disk;

internal sealed class FileHandlePool : IDisposable
{
    private const string UnsupportedMessage =
        "File-handle durable storage is not supported by the experimental netstandard2.1 target. " +
        "Use StorageMode.InMemory.";

    public FileHandlePool(int capacity = 1024)
    {
        if (capacity <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(capacity), capacity, "Capacity must be greater than zero.");
        }
    }

    public FileHandleLease Acquire(string path) => throw CreateUnsupportedException();

    public void FlushToDisk() => throw CreateUnsupportedException();

    public void Remove(string path) => throw CreateUnsupportedException();

    public void Dispose()
    {
    }

    private static PlatformNotSupportedException CreateUnsupportedException() => new(UnsupportedMessage);

    public sealed class FileHandleLease : IDisposable
    {
        public SafeFileHandle Handle => throw CreateUnsupportedException();

        public void Dispose()
        {
        }
    }
}
#endif
