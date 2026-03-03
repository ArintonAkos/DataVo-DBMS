using System.Collections.Concurrent;

namespace DataVo.Core.StorageEngine.Disk;

public class DiskStorageEngine : IStorageEngine
{
    private readonly string _storageDirectory;
    private readonly ConcurrentDictionary<string, object> _fileLocks = new();

    // 8-byte file header: [4 bytes magic "DaVo"] + [4 bytes version].
    // This ensures the first row's byte offset is >= 8, never 0.
    // Row ID 0 is reserved as the B+Tree empty-slot sentinel.
    private static readonly byte[] FileHeaderMagic = "DaVo"u8.ToArray();
    private const int FileHeaderVersion = 1;
    private const int FileHeaderSize = 8; // 4 magic + 4 version

    public DiskStorageEngine(string storageDirectory)
    {
        _storageDirectory = storageDirectory;
        if (!Directory.Exists(_storageDirectory))
        {
            Directory.CreateDirectory(_storageDirectory);
        }
    }

    private string GetFilePath(string databaseName, string tableName)
    {
        string dbPath = Path.Combine(_storageDirectory, databaseName);
        if (!Directory.Exists(dbPath)) Directory.CreateDirectory(dbPath);
        return Path.Combine(dbPath, $"{tableName}.dat");
    }

    private object GetFileLock(string filePath)
    {
        return _fileLocks.GetOrAdd(filePath, _ => new object());
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

    public long InsertRow(string databaseName, string tableName, byte[] rowBytes)
    {
        string filePath = GetFilePath(databaseName, tableName);
        
        lock (GetFileLock(filePath))
        {
            EnsureFileHeader(filePath);

            using var fileStream = new FileStream(filePath, FileMode.Append, FileAccess.Write, FileShare.None);
            
            // The RowId is the exact byte offset into the physical file.
            // With header, first row starts at offset 8 (never 0).
            long rawByteOffsetRowId = fileStream.Position;
            
            // Write length prefix (4 bytes) and then the payload
            using var writer = new BinaryWriter(fileStream);
            writer.Write(rowBytes.Length);
            writer.Write(rowBytes);
            
            return rawByteOffsetRowId;
        }
    }

    public List<long> InsertRows(string databaseName, string tableName, List<byte[]> rowsBytes)
    {
        var rowIds = new List<long>();
        string filePath = GetFilePath(databaseName, tableName);
        
        lock (GetFileLock(filePath))
        {
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
        }
        return rowIds;
    }

    public byte[] ReadRow(string databaseName, string tableName, long rowId)
    {
        string filePath = GetFilePath(databaseName, tableName);
        
        if (!File.Exists(filePath)) 
            throw new FileNotFoundException($"DiskStorageEngine: Data file for {tableName} does not exist.");

        lock (GetFileLock(filePath))
        {
            using var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
            using var reader = new BinaryReader(fileStream);
            
            // O(1) instant seek using the RowId coordinate
            fileStream.Seek(rowId, SeekOrigin.Begin);
            
            int length = reader.ReadInt32();
            
            // Tombstone check: A negative length means this row was deleted
            if (length < 0) 
                throw new Exception($"DiskStorageEngine: RowId {rowId} was marked as deleted.");
                
            return reader.ReadBytes(length);
        }
    }

    public IEnumerable<(long RowId, byte[] RawRow)> ReadAllRows(string databaseName, string tableName)
    {
        string filePath = GetFilePath(databaseName, tableName);
        if (!File.Exists(filePath)) yield break;

        lock (GetFileLock(filePath))
        {
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

    public void DeleteRow(string databaseName, string tableName, long rowId)
    {
        string filePath = GetFilePath(databaseName, tableName);
        
        lock (GetFileLock(filePath))
        {
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
        }
    }

    public void DropTable(string databaseName, string tableName)
    {
        string filePath = GetFilePath(databaseName, tableName);
        lock (GetFileLock(filePath))
        {
            if (File.Exists(filePath))
            {
                File.Delete(filePath);
            }
        }
    }

    public void DropDatabase(string databaseName)
    {
        string dbPath = Path.Combine(_storageDirectory, databaseName);

        // Remove file locks for this database
        var locksToRemove = _fileLocks.Keys
            .Where(k => k.StartsWith(dbPath, StringComparison.OrdinalIgnoreCase))
            .ToList();
        foreach (var key in locksToRemove)
        {
            _fileLocks.TryRemove(key, out _);
        }

        // Delete the entire database directory
        if (Directory.Exists(dbPath))
        {
            Directory.Delete(dbPath, recursive: true);
        }
    }

    public List<(long NewRowId, byte[] RawRow)> CompactTable(string databaseName, string tableName)
    {
        string filePath = GetFilePath(databaseName, tableName);
        var compacted = new List<(long, byte[])>();

        if (!File.Exists(filePath)) return compacted;

        lock (GetFileLock(filePath))
        {
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
                writer.Write(System.Text.Encoding.ASCII.GetBytes("DaVo"));
                writer.Write(1); // version

                foreach (var row in survivors)
                {
                    long newRowId = writeStream.Position;
                    writer.Write(row.Length);
                    writer.Write(row);
                    compacted.Add((newRowId, row));
                }
            }
        }

        return compacted;
    }
}

