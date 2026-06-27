using System.Collections.Concurrent;
using System.Buffers.Binary;
using DataVo.Core.Exceptions;
using DataVo.Core.StorageEngine.Config;
using Microsoft.Win32.SafeHandles;

namespace DataVo.Core.StorageEngine.Disk;

/// <summary>
/// Disk-backed storage engine that persists table rows into .dat files.
/// </summary>
public class DiskStorageEngine : IStorageEngine, IDisposable
{
    private readonly string _storageDirectory;
    private readonly bool _syncWrites;
    private readonly IoSchedulerMode _ioSchedulerMode;
    private readonly FileHandlePool? _handlePool;
    private static readonly ConcurrentDictionary<string, object> GlobalFileLocks = new();

    // 8-byte file header: [4 bytes magic "DaVo"] + [4 bytes version].
    // This ensures the first row's byte offset is >= 8, never 0.
    // Row ID 0 is reserved as the B+Tree empty-slot sentinel.
    private static readonly byte[] FileHeaderMagic = "DaVo"u8.ToArray();
    private const int FileHeaderVersion = 1;
    private const int FileHeaderSize = 8; // 4 magic + 4 version

    /// <summary>
    /// Initializes a disk storage engine rooted at the given directory.
    /// </summary>
    /// <param name="storageDirectory">The root directory used for database and table files.</param>
    /// <param name="syncWrites">
    /// When <see langword="true"/>, every row append/tombstone/compaction is flushed to the physical
    /// device (<c>fsync</c>) before returning, making writes power-crash durable.
    /// </param>
    /// <param name="ioSchedulerMode">The disk I/O scheduler mode used by this engine.</param>
    public DiskStorageEngine(string storageDirectory, bool syncWrites = false, IoSchedulerMode ioSchedulerMode = IoSchedulerMode.Off)
    {
        _storageDirectory = storageDirectory;
        _syncWrites = syncWrites;
        _ioSchedulerMode = ioSchedulerMode;
        _handlePool = UsesHandlePooling ? new FileHandlePool() : null;
        if (!Directory.Exists(_storageDirectory))
        {
            Directory.CreateDirectory(_storageDirectory);
        }
    }

    private bool UsesHandlePooling => _ioSchedulerMode != IoSchedulerMode.Off;

    private string GetFilePath(string databaseName, string tableName)
    {
        string dbPath = Path.Combine(_storageDirectory, databaseName);
        if (!Directory.Exists(dbPath)) Directory.CreateDirectory(dbPath);
        return Path.Combine(dbPath, $"{tableName}.dat");
    }

    /// <summary>
    /// Forces buffered writes to the physical device when durable writes are enabled. The
    /// <see cref="BinaryWriter"/> is flushed first so its bytes reach the <see cref="FileStream"/>
    /// buffer, then <see cref="FileStream.Flush(bool)"/> with <c>flushToDisk: true</c> issues the fsync.
    /// </summary>
    private void FlushIfDurable(BinaryWriter writer, FileStream fileStream)
    {
        if (!_syncWrites)
        {
            return;
        }

        writer.Flush();
        fileStream.Flush(flushToDisk: true);
    }

    private void FlushIfDurable(SafeFileHandle handle)
    {
        if (_syncWrites)
        {
            RandomAccess.FlushToDisk(handle);
        }
    }

    private object GetFileLock(string filePath)
    {
        string normalizedPath = Path.GetFullPath(filePath);
        return GlobalFileLocks.GetOrAdd(normalizedPath, _ => new object());
    }

    private static void RemoveFileLock(string filePath)
    {
        string normalizedPath = Path.GetFullPath(filePath);
        GlobalFileLocks.TryRemove(normalizedPath, out _);
    }

    /// <summary>
    /// Ensures the .dat file has the required header. Writes it if the file is new/empty.
    /// </summary>
    private void EnsureFileHeader(string filePath)
    {
        if (!File.Exists(filePath) || new FileInfo(filePath).Length < FileHeaderSize)
        {
            using var fs = new FileStream(filePath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None);
            using var writer = new BinaryWriter(fs);
            writer.Write(FileHeaderMagic);
            writer.Write(FileHeaderVersion);
        }
    }

    private void EnsureFileHeader(SafeFileHandle handle)
    {
        if (RandomAccess.GetLength(handle) >= FileHeaderSize)
        {
            return;
        }

        Span<byte> header = stackalloc byte[FileHeaderSize];
        FileHeaderMagic.CopyTo(header);
        BinaryPrimitives.WriteInt32LittleEndian(header[4..], FileHeaderVersion);
        RandomAccess.Write(handle, header, fileOffset: 0);
    }

    /// <summary>
    /// Creates a physical table file and writes a file header if needed.
    /// </summary>
    /// <param name="databaseName">The owning database name.</param>
    /// <param name="tableName">The table name.</param>
    public void CreateTable(string databaseName, string tableName)
    {
        string filePath = GetFilePath(databaseName, tableName);
        lock (GetFileLock(filePath))
        {
            if (UsesHandlePooling)
            {
                using var lease = Acquire(filePath);
                EnsureFileHeader(lease.Handle);
            }
            else
            {
                EnsureFileHeader(filePath);
            }
        }
    }

    /// <summary>
    /// Appends a serialized row to a table file.
    /// </summary>
    /// <param name="databaseName">The owning database name.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="rowBytes">The serialized row payload.</param>
    /// <returns>The byte-offset RowId where the row was written.</returns>
    public long InsertRow(string databaseName, string tableName, byte[] rowBytes)
    {
        string filePath = GetFilePath(databaseName, tableName);

        lock (GetFileLock(filePath))
        {
            if (UsesHandlePooling)
            {
                using var lease = Acquire(filePath);
                EnsureFileHeader(lease.Handle);

                long rowId = RandomAccess.GetLength(lease.Handle);
                WriteRowRecord(lease.Handle, rowId, rowBytes);
                FlushIfDurable(lease.Handle);

                return rowId;
            }

            EnsureFileHeader(filePath);

            using var fileStream = new FileStream(filePath, FileMode.Append, FileAccess.Write, FileShare.None);

            // The RowId is the exact byte offset into the physical file.
            // With header, first row starts at offset 8 (never 0).
            long rawByteOffsetRowId = fileStream.Position;

            // Write length prefix (4 bytes) and then the payload
            using var writer = new BinaryWriter(fileStream);
            writer.Write(rowBytes.Length);
            writer.Write(rowBytes);

            FlushIfDurable(writer, fileStream);

            return rawByteOffsetRowId;
        }
    }

    /// <summary>
    /// Appends multiple serialized rows to a table file.
    /// </summary>
    /// <param name="databaseName">The owning database name.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="rowsBytes">The serialized row payloads.</param>
    /// <returns>The byte-offset RowIds written in insertion order.</returns>
    public List<long> InsertRows(string databaseName, string tableName, List<byte[]> rowsBytes)
    {
        var rowIds = new List<long>();
        string filePath = GetFilePath(databaseName, tableName);

        lock (GetFileLock(filePath))
        {
            if (UsesHandlePooling)
            {
                using var lease = Acquire(filePath);
                EnsureFileHeader(lease.Handle);

                long offset = RandomAccess.GetLength(lease.Handle);
                foreach (var bytes in rowsBytes)
                {
                    rowIds.Add(offset);
                    offset = WriteRowRecord(lease.Handle, offset, bytes);
                }

                // One fsync amortizes the whole batch — the rows are durable as a unit on return.
                FlushIfDurable(lease.Handle);
                return rowIds;
            }

            EnsureFileHeader(filePath);

            using var fileStream = new FileStream(filePath, FileMode.Append, FileAccess.Write, FileShare.None);
            using var writer = new BinaryWriter(fileStream);

            foreach (var bytes in rowsBytes)
            {
                long rowId = fileStream.Position;
                writer.Write(bytes.Length);
                writer.Write(bytes);
                rowIds.Add(rowId);
            }

            // One fsync amortizes the whole batch — the rows are durable as a unit on return.
            FlushIfDurable(writer, fileStream);
        }
        return rowIds;
    }

    /// <summary>
    /// Reads a row payload by byte-offset RowId.
    /// </summary>
    /// <param name="databaseName">The owning database name.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="rowId">The byte-offset RowId.</param>
    /// <returns>The serialized row payload.</returns>
    public byte[] ReadRow(string databaseName, string tableName, long rowId)
    {
        string filePath = GetFilePath(databaseName, tableName);

        if (!File.Exists(filePath))
            throw new FileNotFoundException($"DiskStorageEngine: Data file for {tableName} does not exist.");

        lock (GetFileLock(filePath))
        {
            if (UsesHandlePooling)
            {
                using var lease = Acquire(filePath);
                int pooledLength = ReadInt32(lease.Handle, rowId);

                // Tombstone check: A negative length means this row was deleted
                if (pooledLength < 0)
                    throw new RowDeletedException(rowId, tableName);

                var data = new byte[pooledLength];
                ReadExactly(lease.Handle, data, rowId + sizeof(int));
                return data;
            }

            using var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
            using var reader = new BinaryReader(fileStream);

            // O(1) instant seek using the RowId coordinate
            fileStream.Seek(rowId, SeekOrigin.Begin);

            int length = reader.ReadInt32();

            // Tombstone check: A negative length means this row was deleted
            if (length < 0)
                throw new RowDeletedException(rowId, tableName);

            return reader.ReadBytes(length);
        }
    }

    /// <summary>
    /// Enumerates all non-deleted rows in a table file.
    /// </summary>
    /// <param name="databaseName">The owning database name.</param>
    /// <param name="tableName">The table name.</param>
    /// <returns>All surviving row IDs and payloads.</returns>
    public IEnumerable<(long RowId, byte[] RawRow)> ReadAllRows(string databaseName, string tableName)
    {
        string filePath = GetFilePath(databaseName, tableName);
        if (!File.Exists(filePath)) yield break;

        lock (GetFileLock(filePath))
        {
            if (UsesHandlePooling)
            {
                using var lease = Acquire(filePath);
                long offset = RandomAccess.GetLength(lease.Handle) >= FileHeaderSize
                    ? FileHeaderSize
                    : 0;
                long fileLength = RandomAccess.GetLength(lease.Handle);

                while (offset < fileLength)
                {
                    long rowId = offset;
                    int length = ReadInt32(lease.Handle, offset);
                    offset += sizeof(int);
                    if (length < 0)
                    {
                        // Tombstone: length stores -(originalLength), skip past the data
                        offset += -length;
                        continue;
                    }

                    byte[] data = new byte[length];
                    ReadExactly(lease.Handle, data, offset);
                    offset += length;
                    yield return (rowId, data);
                }

                yield break;
            }

            using var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
            using var reader = new BinaryReader(fileStream);

            // Skip file header
            if (fileStream.Length >= FileHeaderSize)
            {
                fileStream.Seek(FileHeaderSize, SeekOrigin.Begin);
            }

            while (fileStream.Position < fileStream.Length)
            {
                long rowId = fileStream.Position;
                int length = reader.ReadInt32();
                if (length < 0)
                {
                    // Tombstone: length stores -(originalLength), skip past the data
                    fileStream.Seek(-length, SeekOrigin.Current);
                    continue;
                }

                byte[] data = reader.ReadBytes(length);
                yield return (rowId, data);
            }
        }
    }

    /// <summary>
    /// Tombstones a row by negating its length prefix.
    /// </summary>
    /// <param name="databaseName">The owning database name.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="rowId">The byte-offset RowId.</param>
    public void DeleteRow(string databaseName, string tableName, long rowId)
    {
        string filePath = GetFilePath(databaseName, tableName);

        lock (GetFileLock(filePath))
        {
            if (UsesHandlePooling)
            {
                using var lease = Acquire(filePath);
                int pooledOriginalLength = ReadInt32(lease.Handle, rowId);
                if (pooledOriginalLength < 0) return; // Already deleted

                Span<byte> tombstone = stackalloc byte[sizeof(int)];
                BinaryPrimitives.WriteInt32LittleEndian(tombstone, -pooledOriginalLength);
                RandomAccess.Write(lease.Handle, tombstone, rowId);
                FlushIfDurable(lease.Handle);
                return;
            }

            using var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            using var reader = new BinaryReader(fileStream);
            using var writer = new BinaryWriter(fileStream);

            // O(1) instant jump to the row's length prefix
            fileStream.Seek(rowId, SeekOrigin.Begin);

            // Read the original length so we know the data size
            int originalLength = reader.ReadInt32();
            if (originalLength < 0) return; // Already deleted

            // Rewrite the length as its negation: -(originalLength)
            // This lets ReadAllRows know how many bytes to skip when scanning
            fileStream.Seek(rowId, SeekOrigin.Begin);
            writer.Write(-originalLength);

            FlushIfDurable(writer, fileStream);
        }
    }

    /// <summary>
    /// Deletes the physical table file.
    /// </summary>
    /// <param name="databaseName">The owning database name.</param>
    /// <param name="tableName">The table name.</param>
    public void DropTable(string databaseName, string tableName)
    {
        string filePath = GetFilePath(databaseName, tableName);
        lock (GetFileLock(filePath))
        {
            if (File.Exists(filePath))
            {
                _handlePool?.Remove(filePath);
                File.Delete(filePath);
            }
        }

        RemoveFileLock(filePath);
    }

    /// <summary>
    /// Deletes a database directory and all table files.
    /// </summary>
    /// <param name="databaseName">The database name.</param>
    public void DropDatabase(string databaseName)
    {
        string dbPath = Path.Combine(_storageDirectory, databaseName);

        string[] tableFiles = Directory.Exists(dbPath)
            ? Directory.GetFiles(dbPath, "*.dat", SearchOption.TopDirectoryOnly)
            : [];

        // Delete the entire database directory
        if (Directory.Exists(dbPath))
        {
            foreach (string tableFile in tableFiles)
            {
                _handlePool?.Remove(tableFile);
            }

            Directory.Delete(dbPath, recursive: true);
        }

        foreach (string tableFile in tableFiles)
        {
            RemoveFileLock(tableFile);
        }
    }

    /// <summary>
    /// Rewrites a table file without tombstoned rows and returns remapped row IDs.
    /// </summary>
    /// <param name="databaseName">The owning database name.</param>
    /// <param name="tableName">The table name.</param>
    /// <returns>The new RowId and payload for each surviving row.</returns>
    public List<(long NewRowId, byte[] RawRow)> CompactTable(string databaseName, string tableName)
    {
        string filePath = GetFilePath(databaseName, tableName);
        var compacted = new List<(long, byte[])>();

        if (!File.Exists(filePath)) return compacted;

        lock (GetFileLock(filePath))
        {
            if (UsesHandlePooling)
            {
                using var lease = Acquire(filePath);
                var pooledSurvivors = new List<byte[]>();
                long offset = RandomAccess.GetLength(lease.Handle) >= FileHeaderSize
                    ? FileHeaderSize
                    : 0;
                long fileLength = RandomAccess.GetLength(lease.Handle);

                while (offset < fileLength)
                {
                    int length = ReadInt32(lease.Handle, offset);
                    offset += sizeof(int);
                    if (length < 0)
                    {
                        offset += -length;
                        continue;
                    }

                    byte[] data = new byte[length];
                    ReadExactly(lease.Handle, data, offset);
                    offset += length;
                    pooledSurvivors.Add(data);
                }

                RandomAccess.SetLength(lease.Handle, 0);
                WriteHeader(lease.Handle);

                long writeOffset = FileHeaderSize;
                foreach (var row in pooledSurvivors)
                {
                    long newRowId = writeOffset;
                    writeOffset = WriteRowRecord(lease.Handle, writeOffset, row);
                    compacted.Add((newRowId, row));
                }

                FlushIfDurable(lease.Handle);
                return compacted;
            }

            // 1. Read all surviving rows directly (no call to ReadAllRows to avoid re-entrant lock)
            var survivors = new List<byte[]>();
            using (var readStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read))
            using (var reader = new BinaryReader(readStream))
            {
                if (readStream.Length >= FileHeaderSize)
                    readStream.Seek(FileHeaderSize, SeekOrigin.Begin);

                while (readStream.Position < readStream.Length)
                {
                    int length = reader.ReadInt32();
                    if (length < 0)
                    {
                        // Tombstone: length stores -(originalLength), skip past the data
                        int dataSize = -length;
                        readStream.Seek(dataSize, SeekOrigin.Current);
                        continue;
                    }
                    byte[] data = reader.ReadBytes(length);
                    survivors.Add(data);
                }
            }

            // 2. Rewrite the file with only surviving rows
            using (var writeStream = new FileStream(filePath, FileMode.Create, FileAccess.Write))
            using (var writer = new BinaryWriter(writeStream))
            {
                // File header
                writer.Write(FileHeaderMagic);
                writer.Write(FileHeaderVersion);

                foreach (var row in survivors)
                {
                    long newRowId = writeStream.Position;
                    writer.Write(row.Length);
                    writer.Write(row);
                    compacted.Add((newRowId, row));
                }

                FlushIfDurable(writer, writeStream);
            }
        }

        return compacted;
    }

    /// <summary>
    /// Releases pooled file handles owned by this engine.
    /// </summary>
    public void Dispose()
    {
        _handlePool?.Dispose();
    }

    private FileHandlePool.FileHandleLease Acquire(string filePath)
    {
        if (_handlePool is null)
        {
            throw new InvalidOperationException("File handle pooling is not enabled for this disk storage engine.");
        }

        return _handlePool.Acquire(filePath);
    }

    private static void WriteHeader(SafeFileHandle handle)
    {
        Span<byte> header = stackalloc byte[FileHeaderSize];
        FileHeaderMagic.CopyTo(header);
        BinaryPrimitives.WriteInt32LittleEndian(header[4..], FileHeaderVersion);
        RandomAccess.Write(handle, header, 0);
    }

    private static long WriteRowRecord(SafeFileHandle handle, long offset, byte[] rowBytes)
    {
        Span<byte> lengthPrefix = stackalloc byte[sizeof(int)];
        BinaryPrimitives.WriteInt32LittleEndian(lengthPrefix, rowBytes.Length);
        RandomAccess.Write(handle, lengthPrefix, offset);
        RandomAccess.Write(handle, rowBytes, offset + sizeof(int));
        return offset + sizeof(int) + rowBytes.Length;
    }

    private static int ReadInt32(SafeFileHandle handle, long offset)
    {
        Span<byte> buffer = stackalloc byte[sizeof(int)];
        ReadExactly(handle, buffer, offset);
        return BinaryPrimitives.ReadInt32LittleEndian(buffer);
    }

    private static void ReadExactly(SafeFileHandle handle, Span<byte> buffer, long offset)
    {
        while (!buffer.IsEmpty)
        {
            int bytesRead = RandomAccess.Read(handle, buffer, offset);
            if (bytesRead == 0)
            {
                throw new EndOfStreamException("Unexpected end of disk table file.");
            }

            buffer = buffer[bytesRead..];
            offset += bytesRead;
        }
    }
}
