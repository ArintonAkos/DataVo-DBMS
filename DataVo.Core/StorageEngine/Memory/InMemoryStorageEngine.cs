using System.Collections.Concurrent;
using DataVo.Core.Exceptions;

namespace DataVo.Core.StorageEngine.Memory;

/// <summary>
/// In-memory storage engine used for fast, non-persistent row operations.
/// </summary>
public class InMemoryStorageEngine : IStorageEngine
{
    // A thread-safe, purely RAM-based mapping of DatabaseName.TableName -> List<byte[]>
    private readonly ConcurrentDictionary<string, List<byte[]>> _databases = new();

    private string GetKey(string databaseName, string tableName) => $"{databaseName}.{tableName}";

    private List<byte[]> GetOrAddTable(string databaseName, string tableName)
    {
        return _databases.GetOrAdd(GetKey(databaseName, tableName), _ => []);
    }

    /// <summary>
    /// Inserts one serialized row and returns its 1-based RowId.
    /// </summary>
    /// <param name="databaseName">The owning database name.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="rowBytes">The serialized row payload.</param>
    /// <returns>The assigned 1-based RowId.</returns>
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

    /// <summary>
    /// Inserts multiple serialized rows and returns assigned 1-based RowIds.
    /// </summary>
    /// <param name="databaseName">The owning database name.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="rowsBytes">The serialized row payloads.</param>
    /// <returns>Assigned row IDs in insertion order.</returns>
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

    /// <summary>
    /// Reads a row payload by 1-based RowId.
    /// </summary>
    /// <param name="databaseName">The owning database name.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="rowId">The 1-based RowId.</param>
    /// <returns>The serialized row payload.</returns>
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

                throw new RowDeletedException(rowId, tableName);
            }

            throw new RowNotFoundException(rowId, tableName);
        }

        throw new RowNotFoundException(rowId, tableName);
    }

    /// <summary>
    /// Enumerates all non-deleted rows for a table.
    /// </summary>
    /// <param name="databaseName">The owning database name.</param>
    /// <param name="tableName">The table name.</param>
    /// <returns>All surviving row IDs and payloads.</returns>
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

    /// <summary>
    /// Tombstones a row by replacing its slot with null.
    /// </summary>
    /// <param name="databaseName">The owning database name.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="rowId">The 1-based RowId.</param>
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

    /// <summary>
    /// Removes a table from the in-memory store.
    /// </summary>
    /// <param name="databaseName">The owning database name.</param>
    /// <param name="tableName">The table name.</param>
    public void DropTable(string databaseName, string tableName)
    {
        _databases.TryRemove(GetKey(databaseName, tableName), out _);
    }

    /// <summary>
    /// Removes all tables belonging to a database from the in-memory store.
    /// </summary>
    /// <param name="databaseName">The database name.</param>
    public void DropDatabase(string databaseName)
    {
        string prefix = $"{databaseName}.";
        var keysToRemove = _databases.Keys.Where(k => k.StartsWith(prefix)).ToList();
        foreach (var key in keysToRemove)
        {
            _databases.TryRemove(key, out _);
        }
    }

    /// <summary>
    /// Compacts a table by removing tombstoned rows and reassigning RowIds.
    /// </summary>
    /// <param name="databaseName">The owning database name.</param>
    /// <param name="tableName">The table name.</param>
    /// <returns>The new RowId and payload for each surviving row.</returns>
    public List<(long NewRowId, byte[] RawRow)> CompactTable(string databaseName, string tableName)
    {
        var table = GetOrAddTable(databaseName, tableName);
        var compacted = new List<(long, byte[])>();
        var newTable = new List<byte[]>();

        lock (table)
        {
            for (int i = 0; i < table.Count; i++)
            {
                if (table[i] != null && table[i].Length > 0)
                {
                    newTable.Add(table[i]);
                    long newRowId = newTable.Count; // 1-based
                    compacted.Add((newRowId, table[i]));
                }
            }
        }

        // Replace the table with the compacted version
        string key = GetKey(databaseName, tableName);
        _databases[key] = newTable;

        return compacted;
    }
}

