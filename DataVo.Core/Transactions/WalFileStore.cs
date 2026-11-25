using System.Collections.Concurrent;
using DataVo.Core.Compat;
using DataVo.Core.Utils;

namespace DataVo.Core.Transactions;

/// <summary>
/// A decoded binary WAL frame: its fixed-size header plus the application payload bytes that follow it.
/// </summary>
internal readonly record struct WalFrameRecord(WalFrameHeader Header, byte[] Payload);

/// <summary>
/// Synchronized access to one binary write-ahead log file of the LSM engine (a table WAL or the commit log):
/// appending framed records, forcing them to disk, reading the valid frames back and truncating through a
/// checkpoint LSN. Appends go through a pooled file handle; a per-path lock orders appends, reads and rewrites.
/// </summary>
internal sealed class WalFileStore
{
    private static readonly ConcurrentDictionary<string, object> FileLocks =
        new(StringComparer.OrdinalIgnoreCase);

    private static readonly FileHandlePool BinaryFrameHandlePool = new(capacity: 128);

    private int _durableFlushCount;

    /// <summary>
    /// Initializes a new file store for the specified WAL path.
    /// </summary>
    /// <param name="filePath">The absolute or relative WAL file path.</param>
    public WalFileStore(string filePath)
    {
        FilePath = filePath;
    }

    /// <summary>
    /// Gets the path of the WAL file managed by this store.
    /// </summary>
    public string FilePath { get; }

    internal int DurableFlushCount => Volatile.Read(ref _durableFlushCount);

    internal void AppendFrameBytes(ReadOnlySpan<byte> bytes, bool flushToDisk)
    {
        lock (GetLock())
        {
            EnsureDirectoryExists();
            AppendFrameBytesCore(bytes, flushToDisk);
        }
    }

    internal void FlushToDisk()
    {
        ExecuteLocked(() =>
        {
            EnsureDirectoryExists();
            using FileHandlePool.FileHandleLease lease = BinaryFrameHandlePool.Acquire(FilePath);
            RandomAccessCompat.FlushToDisk(lease.Handle);
            Interlocked.Increment(ref _durableFlushCount);
        });
    }

    /// <summary>
    /// Forces buffered frames to disk WITHOUT holding the store lock across the device flush, so
    /// concurrent appends proceed while the fsync is in flight. This is what makes WAL group commit
    /// group: the leader's fsync no longer blocks follower appends, so the next batch accumulates
    /// during the current flush. Correctness is unaffected because the caller captures its durable
    /// watermark BEFORE invoking this — frames appended during the fsync are simply not yet claimed
    /// durable. The handle lease is acquired under the lock (so it matches the current file across
    /// delete/recreate cycles); a file deleted mid-flush by a completed generation flush is benign,
    /// because that generation's data is already durable in its SSTable.
    /// </summary>
    internal void FlushToDiskConcurrent()
    {
        FileHandlePool.FileHandleLease lease;
        lock (GetLock())
        {
            EnsureDirectoryExists();
            lease = BinaryFrameHandlePool.Acquire(FilePath);
        }

        using (lease)
        {
            try
            {
                RandomAccessCompat.FlushToDisk(lease.Handle);
            }
            catch (ObjectDisposedException)
            {
                // The segment was deleted after its SSTable became durable; nothing left to flush.
                return;
            }

            Interlocked.Increment(ref _durableFlushCount);
        }
    }

    private void AppendFrameBytesCore(ReadOnlySpan<byte> bytes, bool flushToDisk)
    {
        using FileHandlePool.FileHandleLease lease = BinaryFrameHandlePool.Acquire(FilePath);
        long offset = RandomAccessCompat.GetLength(lease.Handle);
        RandomAccessCompat.Write(lease.Handle, bytes, offset);
        if (flushToDisk)
        {
            RandomAccessCompat.FlushToDisk(lease.Handle);
            Interlocked.Increment(ref _durableFlushCount);
        }
    }

    /// <summary>
    /// Sequentially scans the binary WAL, returning every frame that passes <c>FrameLen</c> and
    /// <c>Crc32C</c> validation. Scanning stops at the first torn tail — a partial header, a frame whose
    /// declared length runs past the file, or a checksum mismatch — so a crash mid-append is tolerated.
    /// </summary>
    /// <returns>The ordered, validated frames preceding the first torn tail.</returns>
    public List<WalFrameRecord> ReadBinaryFrames()
    {
        return ExecuteLocked(() =>
        {
            var records = new List<WalFrameRecord>();
            if (!File.Exists(FilePath))
            {
                return records;
            }

            byte[] bytes = ReadAllBytesShared(FilePath);
            int offset = 0;
            while (TryReadValidFrame(bytes, offset, out WalFrameHeader header))
            {
                ReadOnlySpan<byte> payload = bytes.AsSpan(
                    offset + WalFrameFormat.FrameHeaderSize,
                    header.FrameLength - WalFrameFormat.FrameHeaderSize);
                records.Add(new WalFrameRecord(header, payload.ToArray()));
                offset += header.FrameLength;
            }

            return records;
        });
    }

    /// <summary>
    /// Rewrites the binary WAL so only frames with an LSN greater than <paramref name="checkpointLsn"/>
    /// survive, discarding any torn tail. When no frames remain the file is removed entirely. The rewrite
    /// runs under the shared file lock, so it is atomic with respect to concurrent appends.
    /// </summary>
    /// <param name="checkpointLsn">The highest LSN whose effects are already durable in the data files.</param>
    public void TruncateThroughLsn(long checkpointLsn)
    {
        ExecuteLocked(() =>
        {
            if (!File.Exists(FilePath))
            {
                return;
            }

            byte[] bytes = ReadAllBytesShared(FilePath);
            int offset = 0;
            int keepStart = -1;
            int keepEnd = 0;
            while (TryReadValidFrame(bytes, offset, out WalFrameHeader header))
            {
                if (header.Lsn > checkpointLsn && keepStart < 0)
                {
                    keepStart = offset;
                }

                offset += header.FrameLength;
                keepEnd = offset;
            }

            BinaryFrameHandlePool.Remove(FilePath);

            if (keepStart < 0)
            {
                DeleteIfExistsCore();
                return;
            }

            byte[] retained = bytes.AsSpan(keepStart, keepEnd - keepStart).ToArray();

            string tmpPath = FilePath + ".tmp";
            using (var stream = new FileStream(tmpPath, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                stream.Write(retained, 0, retained.Length);
                stream.Flush(true);
            }

            AtomicFileOperations.ReplaceFromTemp(tmpPath, FilePath);
        });
    }

    /// <summary>
    /// Attempts to decode and CRC-validate the frame starting at <paramref name="offset"/>.
    /// </summary>
    private static bool TryReadValidFrame(byte[] bytes, int offset, out WalFrameHeader header)
    {
        header = default;
        if (offset < 0 || offset + WalFrameFormat.FrameHeaderSize > bytes.Length)
        {
            return false;
        }

        ReadOnlySpan<byte> remaining = bytes.AsSpan(offset);
        if (!WalFrameFormat.TryReadFrameHeader(remaining, out header))
        {
            return false;
        }

        if (header.FrameLength < WalFrameFormat.FrameHeaderSize || offset + header.FrameLength > bytes.Length)
        {
            return false;
        }

        ReadOnlySpan<byte> frame = bytes.AsSpan(offset, header.FrameLength);
        return WalFrameFormat.ValidateFrame(frame, header);
    }

    /// <summary>
    /// Deletes the WAL file if it exists.
    /// </summary>
    public void DeleteIfExists()
    {
        ExecuteLocked(DeleteIfExistsCore);
    }

    /// <summary>
    /// Executes an operation while holding the file-specific synchronization lock.
    /// </summary>
    /// <param name="action">The action to execute.</param>
    public void ExecuteLocked(Action action)
    {
        lock (GetLock())
        {
            action();
        }
    }

    /// <summary>
    /// Executes a function while holding the file-specific synchronization lock.
    /// </summary>
    /// <typeparam name="T">The function return type.</typeparam>
    /// <param name="func">The function to execute.</param>
    /// <returns>The function result.</returns>
    public T ExecuteLocked<T>(Func<T> func)
    {
        lock (GetLock())
        {
            return func();
        }
    }

    /// <summary>
    /// Reads the whole file while the pooled append handle may still be open. On Windows a plain
    /// File.ReadAllBytes (FileShare.Read) is refused while another handle holds write access.
    /// </summary>
    private static byte[] ReadAllBytesShared(string path)
    {
        using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
        var bytes = new byte[stream.Length];
        int offset = 0;
        while (offset < bytes.Length)
        {
            int read = stream.Read(bytes, offset, bytes.Length - offset);
            if (read == 0)
            {
                Array.Resize(ref bytes, offset); // the file shrank while reading
                break;
            }

            offset += read;
        }

        return bytes;
    }

    /// <summary>
    /// Closes this file's pooled append handle. Owners call it when they are done with the log, so the
    /// file is not held open (on Windows an open handle also blocks deleting or replacing the file).
    /// </summary>
    internal void ReleaseFileHandle() => ExecuteLocked(() => BinaryFrameHandlePool.Remove(FilePath));

    private void EnsureDirectoryExists()
    {
        // Runs on every append; compute the directory once and only create it when it is missing
        // (it can be deleted underneath us, so the existence check stays).
        string? directory = _directory ??= Path.GetDirectoryName(FilePath) ?? string.Empty;
        if (directory.Length > 0 && !Directory.Exists(directory))
        {
            Directory.CreateDirectory(directory);
        }
    }

    private string? _directory;

    private void DeleteIfExistsCore()
    {
        BinaryFrameHandlePool.Remove(FilePath);
        if (File.Exists(FilePath))
        {
            File.Delete(FilePath);
        }
    }

    private object GetLock()
    {
        return FileLocks.GetOrAdd(FilePath, _ => new object());
    }
}
