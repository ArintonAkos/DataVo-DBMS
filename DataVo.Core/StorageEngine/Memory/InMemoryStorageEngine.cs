using System.Collections.Concurrent;

namespace DataVo.Core.StorageEngine.Memory;

public class InMemoryStorageEngine : IStorageEngine
{
    // A thread-safe, purely RAM-based mapping of DatabaseName.TableName -> List<byte[]>
    private readonly ConcurrentDictionary<string, List<byte[]>> _databases = new();

    private string GetKey(string databaseName, string tableName) => $"{databaseName}.{tableName}";

    private List<byte[]> GetOrAddTable(string databaseName, string tableName)
    {
        return _databases.GetOrAdd(GetKey(databaseName, tableName), _ => []);
    }

    public long InsertRow(string databaseName, string tableName, byte[] rowBytes)
    {
        var table = GetOrAddTable(databaseName, tableName);

        // Lock to ensure sequential RowId (Count becomes the RowId)
        lock (table)
        {
            table.Add(rowBytes);
            return table.Count - 1;
        }
    }

    public List<long> InsertRows(string databaseName, string tableName, List<byte[]> rowsBytes)
    {
        var rowIds = new List<long>(rowsBytes.Count);
        var table = GetOrAddTable(databaseName, tableName);

        lock (table)
        {
            foreach (var rowBytes in rowsBytes)
            {
                table.Add(rowBytes);
                rowIds.Add(table.Count - 1);
            }
        }

        return rowIds;
    }

    public byte[] ReadRow(string databaseName, string tableName, long rowId)
    {
        if (_databases.TryGetValue(GetKey(databaseName, tableName), out var table))
        {
            // O(1) Array indexing using the integer RowId
            if (rowId >= 0 && rowId < table.Count)
            {
                var bytes = table[(int)rowId];
                if (bytes != null) return bytes;
            }
        }
        throw new Exception($"InMemoryStorageEngine: RowId {rowId} does not exist in {tableName}.");
    }

    public IEnumerable<(long RowId, byte[] RawRow)> ReadAllRows(string databaseName, string tableName)
    {
        if (_databases.TryGetValue(GetKey(databaseName, tableName), out var table))
        {
            for (int i = 0; i < table.Count; i++)
            {
                if (table[i] != null)
                {
                    yield return (i, table[i]);
                }
            }
        }
    }

    public void DeleteRow(string databaseName, string tableName, long rowId)
    {
        if (_databases.TryGetValue(GetKey(databaseName, tableName), out var table))
        {
            lock (table)
            {
                if (rowId >= 0 && rowId < table.Count)
                {
                    // "Tombstone" physical deletion, we leave a null gap so standard RowIds don't shift.
                    // This perfectly simulates slotted page gap deletion on disk.
                    table[(int)rowId] = null!;
                }
            }
        }
    }

    public void DropTable(string databaseName, string tableName)
    {
        _databases.TryRemove(GetKey(databaseName, tableName), out _);
    }
}
