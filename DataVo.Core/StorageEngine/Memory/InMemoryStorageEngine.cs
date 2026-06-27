using System.Collections.Concurrent;
using DataVo.Core.Exceptions;
using DataVo.Core.StorageEngine;

namespace DataVo.Core.StorageEngine.Memory;

/// <summary>
/// In-memory storage engine used for fast, non-persistent row operations.
/// </summary>
public class InMemoryStorageEngine : IStorageEngine, ITypedRowStorageEngine, IInMemoryStorageSnapshotProvider
{
    // A thread-safe, purely RAM-based mapping of (DatabaseName, TableName) -> List<byte[]>. A value-tuple key
    // avoids allocating a "db.table" string on every read/write/delete (and removes the latent "a.b"+"c" vs
    // "a"+"b.c" collision); tuple equality uses ordinal string comparison, matching the previous string key.
    private sealed class InMemoryRow
    {
        private InMemoryRow(byte[]? raw, StoredRow? typed)
        {
            Raw = raw;
            Typed = typed;
        }

        public byte[]? Raw { get; }

        public StoredRow? Typed { get; }

        public static InMemoryRow FromRaw(byte[] raw) => new(raw, typed: null);

        public static InMemoryRow FromTyped(StoredRow row) => new(raw: null, row);

        public InMemoryRow Clone() => Raw is not null
            ? FromRaw(Raw.ToArray())
            : FromTyped(Typed!.Clone());
    }

    private readonly ConcurrentDictionary<(string DatabaseName, string TableName), List<InMemoryRow?>> _databases = new();
    private readonly object _syncRoot = new();

    private static (string DatabaseName, string TableName) GetKey(string databaseName, string tableName) => (databaseName, tableName);

    private List<InMemoryRow?> GetOrAddTable(string databaseName, string tableName)
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
        lock (_syncRoot)
        {
            var table = GetOrAddTable(databaseName, tableName);

            // Lock to ensure sequential RowId.
            // Row IDs are 1-based to avoid collision with B+Tree's 0 sentinel value.
            lock (table)
            {
                table.Add(InMemoryRow.FromRaw(rowBytes));
                return table.Count; // 1-based: first row = 1
            }
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

        lock (_syncRoot)
        {
            var table = GetOrAddTable(databaseName, tableName);

            lock (table)
            {
                foreach (var rowBytes in rowsBytes)
                {
                    table.Add(InMemoryRow.FromRaw(rowBytes));
                    rowIds.Add(table.Count); // 1-based
                }
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
        lock (_syncRoot)
        {
            if (_databases.TryGetValue(GetKey(databaseName, tableName), out var table))
            {
                lock (table)
                {
                    // O(1) Array indexing — convert 1-based RowId to 0-based index
                    int index = (int)(rowId - 1);
                    if (index >= 0 && index < table.Count)
                    {
                        var row = table[index];
                        if (row?.Raw != null) return row.Raw;
                        if (row?.Typed != null)
                        {
                            throw new InvalidOperationException(
                                "Typed in-memory rows must be read through StorageContext's typed row path.");
                        }

                        throw new RowDeletedException(rowId, tableName);
                    }

                    throw new RowNotFoundException(rowId, tableName);
                }
            }

            throw new RowNotFoundException(rowId, tableName);
        }
    }

    /// <summary>
    /// Enumerates all non-deleted rows for a table.
    /// </summary>
    /// <param name="databaseName">The owning database name.</param>
    /// <param name="tableName">The table name.</param>
    /// <returns>All surviving row IDs and payloads.</returns>
    public IEnumerable<(long RowId, byte[] RawRow)> ReadAllRows(string databaseName, string tableName)
    {
        var rows = new List<(long RowId, byte[] RawRow)>();

        lock (_syncRoot)
        {
            if (_databases.TryGetValue(GetKey(databaseName, tableName), out var table))
            {
                lock (table)
                {
                    for (int i = 0; i < table.Count; i++)
                    {
                        if (table[i]?.Raw != null)
                        {
                            rows.Add((i + 1, table[i]!.Raw!)); // 1-based RowId
                        }
                    }
                }
            }
        }

        return rows;
    }

    /// <summary>
    /// Tombstones a row by replacing its slot with null.
    /// </summary>
    /// <param name="databaseName">The owning database name.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="rowId">The 1-based RowId.</param>
    public void DeleteRow(string databaseName, string tableName, long rowId)
    {
        lock (_syncRoot)
        {
            if (_databases.TryGetValue(GetKey(databaseName, tableName), out var table))
            {
                lock (table)
                {
                    int index = (int)(rowId - 1);
                    if (index >= 0 && index < table.Count)
                    {
                        // "Tombstone" deletion — leave a null gap so RowIds don't shift.
                        table[index] = null;
                    }
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
        lock (_syncRoot)
        {
            _databases.TryRemove(GetKey(databaseName, tableName), out _);
        }
    }

    /// <summary>
    /// Removes all tables belonging to a database from the in-memory store.
    /// </summary>
    /// <param name="databaseName">The database name.</param>
    public void DropDatabase(string databaseName)
    {
        lock (_syncRoot)
        {
            var keysToRemove = _databases.Keys
                .Where(k => string.Equals(k.DatabaseName, databaseName, StringComparison.Ordinal))
                .ToList();
            foreach (var key in keysToRemove)
            {
                _databases.TryRemove(key, out _);
            }
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
        var compacted = new List<(long, byte[])>();
        var newTable = new List<InMemoryRow?>();

        lock (_syncRoot)
        {
            var table = GetOrAddTable(databaseName, tableName);

            lock (table)
            {
                for (int i = 0; i < table.Count; i++)
                {
                    InMemoryRow? row = table[i];
                    if (row?.Typed != null)
                    {
                        throw new InvalidOperationException(
                            "In-memory compaction is not supported for tables containing typed rows.");
                    }

                    if (row?.Raw != null && row.Raw.Length > 0)
                    {
                        newTable.Add(row);
                        long newRowId = newTable.Count; // 1-based
                        compacted.Add((newRowId, row.Raw));
                    }
                }
            }

            // Replace the table with the compacted version
            var key = GetKey(databaseName, tableName);
            _databases[key] = newTable;
        }

        return compacted;
    }

    long ITypedRowStorageEngine.InsertTypedRow(string databaseName, string tableName, StoredRow row)
    {
        lock (_syncRoot)
        {
            var table = GetOrAddTable(databaseName, tableName);

            lock (table)
            {
                table.Add(InMemoryRow.FromTyped(row));
                return table.Count;
            }
        }
    }

    List<long> ITypedRowStorageEngine.InsertTypedRows(string databaseName, string tableName, IReadOnlyList<StoredRow> rows)
    {
        var rowIds = new List<long>(rows.Count);

        lock (_syncRoot)
        {
            var table = GetOrAddTable(databaseName, tableName);

            lock (table)
            {
                foreach (StoredRow row in rows)
                {
                    table.Add(InMemoryRow.FromTyped(row));
                    rowIds.Add(table.Count);
                }
            }
        }

        return rowIds;
    }

    bool ITypedRowStorageEngine.TryReadTypedRow(string databaseName, string tableName, long rowId, out StoredRow? row)
    {
        row = null;
        lock (_syncRoot)
        {
            if (!_databases.TryGetValue(GetKey(databaseName, tableName), out var table))
            {
                return false;
            }

            lock (table)
            {
                int index = (int)(rowId - 1);
                if (index < 0 || index >= table.Count)
                {
                    return false;
                }

                row = table[index]?.Typed;
                return row is not null;
            }
        }
    }

    IEnumerable<(long RowId, StoredRow Row)> ITypedRowStorageEngine.ReadAllTypedRows(string databaseName, string tableName)
    {
        var rows = new List<(long RowId, StoredRow Row)>();

        lock (_syncRoot)
        {
            if (_databases.TryGetValue(GetKey(databaseName, tableName), out var table))
            {
                lock (table)
                {
                    for (int i = 0; i < table.Count; i++)
                    {
                        if (table[i]?.Typed is StoredRow row)
                        {
                            rows.Add((i + 1, row));
                        }
                    }
                }
            }
        }

        return rows;
    }

    bool ITypedRowStorageEngine.HasAnyRows(string databaseName, string tableName)
    {
        lock (_syncRoot)
        {
            if (!_databases.TryGetValue(GetKey(databaseName, tableName), out var table))
            {
                return false;
            }

            lock (table)
            {
                for (int i = 0; i < table.Count; i++)
                {
                    if (table[i] is not null)
                    {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    InMemoryStorageSnapshot IInMemoryStorageSnapshotProvider.CreateSnapshot()
    {
        var tables = new Dictionary<(string DatabaseName, string TableName), List<object?>>();

        lock (_syncRoot)
        {
            foreach (var entry in _databases)
            {
                lock (entry.Value)
                {
                    tables[entry.Key] = CloneRowsForSnapshot(entry.Value);
                }
            }
        }

        return new InMemoryStorageSnapshot(tables);
    }

    void IInMemoryStorageSnapshotProvider.RestoreSnapshot(InMemoryStorageSnapshot snapshot)
    {
        lock (_syncRoot)
        {
            _databases.Clear();

            foreach (var entry in snapshot.EnumerateTables())
            {
                _databases[entry.Key] = RestoreRowsFromSnapshot(entry.Value);
            }
        }
    }

    private static List<InMemoryRow?> CloneRows(List<InMemoryRow?> rows)
    {
        var clone = new List<InMemoryRow?>(rows.Count);

        foreach (InMemoryRow? row in rows)
        {
            clone.Add(row?.Clone());
        }

        return clone;
    }

    private static List<object?> CloneRowsForSnapshot(List<InMemoryRow?> rows)
    {
        var clone = new List<object?>(rows.Count);

        foreach (InMemoryRow? row in rows)
        {
            if (row?.Raw != null)
            {
                clone.Add(row.Raw.ToArray());
            }
            else if (row?.Typed != null)
            {
                clone.Add(row.Typed.Clone());
            }
            else
            {
                clone.Add(null);
            }
        }

        return clone;
    }

    private static List<InMemoryRow?> RestoreRowsFromSnapshot(List<object?> rows)
    {
        var restored = new List<InMemoryRow?>(rows.Count);

        foreach (object? row in rows)
        {
            restored.Add(row switch
            {
                byte[] bytes => InMemoryRow.FromRaw(bytes.ToArray()),
                StoredRow typed => InMemoryRow.FromTyped(typed.Clone()),
                _ => null
            });
        }

        return restored;
    }
}
