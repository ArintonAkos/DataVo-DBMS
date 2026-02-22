using System.Collections.Concurrent;

namespace DataVo.Core.StorageEngine.Disk;

public class DiskStorageEngine : IStorageEngine
{
    private readonly string _storageDirectory;
    private readonly ConcurrentDictionary<string, object> _fileLocks = new();

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

    public long InsertRow(string databaseName, string tableName, byte[] rowBytes)
    {
        string filePath = GetFilePath(databaseName, tableName);
        
        lock (GetFileLock(filePath))
        {
            using var fileStream = new FileStream(filePath, FileMode.Append, FileAccess.Write, FileShare.None);
            
            // The RowId is the exact byte offset into the physical file
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
            
            // Tombstone check: A length of -1 means this row was deleted
            if (length == -1) 
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

            while (fileStream.Position < fileStream.Length)
            {
                long rowId = fileStream.Position;
                int length = reader.ReadInt32();
                if (length == -1)
                {
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
            using var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Write, FileShare.None);
            using var writer = new BinaryWriter(fileStream);
            
            // O(1) instant jump
            fileStream.Seek(rowId, SeekOrigin.Begin);
            
            // Write a Tombstone signature (-1 length) to instantly invalidate the row
            // without actually shifting any bytes on the physical disk which would corrupt later RowIds.
            writer.Write(-1); 
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
}
