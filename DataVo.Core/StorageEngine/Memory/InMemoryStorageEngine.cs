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

        // Lock to ensure sequential RowId.
        // Row IDs are 1-based to avoid collision with B+Tree's 0 sentinel value.
        lock (table)
        {
            table.Add(rowBytes);
            return table.Count; // 1-based: first row = 1
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
                rowIds.Add(table.Count); // 1-based
            }
        }

        return rowIds;
    }

    public byte[] ReadRow(string databaseName, string tableName, long rowId)
    {
        if (_databases.TryGetValue(GetKey(databaseName, tableName), out var table))
        {
            // O(1) Array indexing — convert 1-based RowId to 0-based index
            int index = (int)(rowId - 1);
            if (index >= 0 && index < table.Count)
            {
                var bytes = table[index];
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
                    yield return (i + 1, table[i]); // 1-based RowId
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
                int index = (int)(rowId - 1);
                if (index >= 0 && index < table.Count)
                {
                    // "Tombstone" deletion — leave a null gap so RowIds don't shift.
                    table[index] = null!;
                }
            }
        }
    }

    public void DropTable(string databaseName, string tableName)
    {
        _databases.TryRemove(GetKey(databaseName, tableName), out _);
    }

    public void DropDatabase(string databaseName)
    {
        string prefix = $"{databaseName}.";
        var keysToRemove = _databases.Keys.Where(k => k.StartsWith(prefix)).ToList();
        foreach (var key in keysToRemove)
        {
            _databases.TryRemove(key, out _);
        }
    }
}

