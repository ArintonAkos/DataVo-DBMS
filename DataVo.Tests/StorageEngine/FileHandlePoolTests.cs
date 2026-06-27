using DataVo.Core.StorageEngine.Disk;
using Microsoft.Win32.SafeHandles;

namespace DataVo.Tests.StorageEngine;

public sealed class FileHandlePoolTests
{
    [Fact]
    public void Acquire_ReusesSameHandle_ForSamePath()
    {
        string root = CreateTempDirectory();
        string path = Path.Combine(root, "table.dat");

        try
        {
            using var pool = new FileHandlePool(capacity: 8);

            SafeFileHandle firstHandle;
            using (var first = pool.Acquire(path))
            {
                firstHandle = first.Handle;
                Assert.False(firstHandle.IsClosed);
            }

            using var second = pool.Acquire(path);

            Assert.Same(firstHandle, second.Handle);
            Assert.False(second.Handle.IsClosed);
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public void Remove_ClosesExistingHandle_AndNextAcquireOpensReplacement()
    {
        string root = CreateTempDirectory();
        string path = Path.Combine(root, "table.dat");

        try
        {
            using var pool = new FileHandlePool(capacity: 8);

            SafeFileHandle removedHandle;
            using (var lease = pool.Acquire(path))
            {
                removedHandle = lease.Handle;
            }

            pool.Remove(path);

            Assert.True(removedHandle.IsClosed);

            using var replacement = pool.Acquire(path);
            Assert.NotSame(removedHandle, replacement.Handle);
            Assert.False(replacement.Handle.IsClosed);
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public void Acquire_EvictsLeastRecentlyUsedIdleHandle_WhenCapacityIsExceeded()
    {
        string root = CreateTempDirectory();
        string firstPath = Path.Combine(root, "first.dat");
        string secondPath = Path.Combine(root, "second.dat");

        try
        {
            using var pool = new FileHandlePool(capacity: 1);

            SafeFileHandle evictedHandle;
            using (var first = pool.Acquire(firstPath))
            {
                evictedHandle = first.Handle;
            }

            using (pool.Acquire(secondPath))
            {
            }

            Assert.True(evictedHandle.IsClosed);
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public void Remove_DefersCloseUntilActiveLeaseIsDisposed()
    {
        string root = CreateTempDirectory();
        string path = Path.Combine(root, "table.dat");

        try
        {
            using var pool = new FileHandlePool(capacity: 8);
            using var lease = pool.Acquire(path);
            SafeFileHandle removedHandle = lease.Handle;

            pool.Remove(path);

            Assert.False(removedHandle.IsClosed);

            lease.Dispose();

            Assert.True(removedHandle.IsClosed);
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    private static string CreateTempDirectory()
    {
        string root = Path.Combine(Path.GetTempPath(), $"datavo_file_handle_pool_{Guid.NewGuid():N}");
        Directory.CreateDirectory(root);
        return root;
    }
}
