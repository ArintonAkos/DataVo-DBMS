using System.Collections.Concurrent;
using System.Threading;
using DataVo.Core.Exceptions;
using DataVo.Core.StorageEngine;

namespace DataVo.Core.StorageEngine.Memory;

/// <summary>
/// In-memory storage engine used for fast, non-persistent row operations.
/// </summary>
public class InMemoryStorageEngine : IStorageEngine, ITypedRowStorageEngine, IInMemoryStorageSnapshotProvider
{
    // A thread-safe, purely RAM-based mapping of (DatabaseName, TableName) -> rows. A value-tuple key
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

    // Each table owns its own reader/writer lock so concurrent readers proceed in parallel while writers
    // (append, tombstone, compaction) take exclusive access. This replaces the previous global mutex
    // (lock(_syncRoot) + lock(table)) that serialized every read and write across all tables — the cause
    // of reader starvation under concurrent load. The outer ConcurrentDictionary handles table lookup/creation.
    private sealed class TableStore
    {
        public readonly ReaderWriterLockSlim Lock = new(LockRecursionPolicy.NoRecursion);
        public readonly List<InMemoryRow?> Rows = [];
    }

    private readonly ConcurrentDictionary<(string DatabaseName, string TableName), TableStore> _databases = new();

    private static (string DatabaseName, string TableName) GetKey(string databaseName, string tableName) => (databaseName, tableName);

    private TableStore GetOrAddTable(string databaseName, string tableName)
    {
        return _databases.GetOrAdd(GetKey(databaseName, tableName), static _ => new TableStore());
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
        TableStore store = GetOrAddTable(databaseName, tableName);

        // Exclusive write lock guarantees a sequential RowId.
        // Row IDs are 1-based to avoid collision with B+Tree's 0 sentinel value.
        store.Lock.EnterWriteLock();
        try
        {
            store.Rows.Add(InMemoryRow.FromRaw(rowBytes));
            return store.Rows.Count; // 1-based: first row = 1
        }
        finally
        {
            store.Lock.ExitWriteLock();
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
        TableStore store = GetOrAddTable(databaseName, tableName);

        store.Lock.EnterWriteLock();
        try
        {
            foreach (var rowBytes in rowsBytes)
            {
                store.Rows.Add(InMemoryRow.FromRaw(rowBytes));
                rowIds.Add(store.Rows.Count); // 1-based
            }
        }
        finally
        {
            store.Lock.ExitWriteLock();
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
        if (!_databases.TryGetValue(GetKey(databaseName, tableName), out TableStore? store))
        {
            throw new RowNotFoundException(rowId, tableName);
        }

        store.Lock.EnterReadLock();
        try
        {
            // O(1) Array indexing — convert 1-based RowId to 0-based index
            int index = (int)(rowId - 1);
            if (index >= 0 && index < store.Rows.Count)
            {
                var row = store.Rows[index];
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
        finally
        {
            store.Lock.ExitReadLock();
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

        if (_databases.TryGetValue(GetKey(databaseName, tableName), out TableStore? store))
        {
            store.Lock.EnterReadLock();
            try
            {
                for (int i = 0; i < store.Rows.Count; i++)
                {
                    if (store.Rows[i]?.Raw != null)
                    {
                        rows.Add((i + 1, store.Rows[i]!.Raw!)); // 1-based RowId
                    }
                }
            }
            finally
            {
                store.Lock.ExitReadLock();
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
        if (!_databases.TryGetValue(GetKey(databaseName, tableName), out TableStore? store))
        {
            return;
        }

        store.Lock.EnterWriteLock();
        try
        {
            int index = (int)(rowId - 1);
            if (index >= 0 && index < store.Rows.Count)
            {
                // "Tombstone" deletion — leave a null gap so RowIds don't shift.
                store.Rows[index] = null;
            }
        }
        finally
        {
            store.Lock.ExitWriteLock();
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
        foreach (var key in _databases.Keys)
        {
            if (string.Equals(key.DatabaseName, databaseName, StringComparison.Ordinal))
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
        TableStore store = GetOrAddTable(databaseName, tableName);

        store.Lock.EnterWriteLock();
        try
        {
            var survivors = new List<InMemoryRow?>();
            for (int i = 0; i < store.Rows.Count; i++)
            {
                InMemoryRow? row = store.Rows[i];
                if (row?.Typed != null)
                {
                    throw new InvalidOperationException(
                        "In-memory compaction is not supported for tables containing typed rows.");
                }

                if (row?.Raw != null && row.Raw.Length > 0)
                {
                    survivors.Add(row);
                    long newRowId = survivors.Count; // 1-based
                    compacted.Add((newRowId, row.Raw));
                }
            }

            store.Rows.Clear();
            store.Rows.AddRange(survivors);
        }
        finally
        {
            store.Lock.ExitWriteLock();
        }

        return compacted;
    }

    long ITypedRowStorageEngine.InsertTypedRow(string databaseName, string tableName, StoredRow row)
    {
        TableStore store = GetOrAddTable(databaseName, tableName);

        store.Lock.EnterWriteLock();
        try
        {
            store.Rows.Add(InMemoryRow.FromTyped(row));
            return store.Rows.Count;
        }
        finally
        {
            store.Lock.ExitWriteLock();
        }
    }

    List<long> ITypedRowStorageEngine.InsertTypedRows(string databaseName, string tableName, IReadOnlyList<StoredRow> rows)
    {
        var rowIds = new List<long>(rows.Count);
        TableStore store = GetOrAddTable(databaseName, tableName);

        store.Lock.EnterWriteLock();
        try
        {
            foreach (StoredRow row in rows)
            {
                store.Rows.Add(InMemoryRow.FromTyped(row));
                rowIds.Add(store.Rows.Count);
            }
        }
        finally
        {
            store.Lock.ExitWriteLock();
        }

        return rowIds;
    }

    bool ITypedRowStorageEngine.TryReadTypedRow(string databaseName, string tableName, long rowId, out StoredRow? row)
    {
        row = null;
        if (!_databases.TryGetValue(GetKey(databaseName, tableName), out TableStore? store))
        {
            return false;
        }

        store.Lock.EnterReadLock();
        try
        {
            int index = (int)(rowId - 1);
            if (index < 0 || index >= store.Rows.Count)
            {
                return false;
            }

            row = store.Rows[index]?.Typed;
            return row is not null;
        }
        finally
        {
            store.Lock.ExitReadLock();
        }
    }

    IEnumerable<(long RowId, StoredRow Row)> ITypedRowStorageEngine.ReadAllTypedRows(string databaseName, string tableName)
    {
        var rows = new List<(long RowId, StoredRow Row)>();

        if (_databases.TryGetValue(GetKey(databaseName, tableName), out TableStore? store))
        {
            store.Lock.EnterReadLock();
            try
            {
                for (int i = 0; i < store.Rows.Count; i++)
                {
                    if (store.Rows[i]?.Typed is StoredRow row)
                    {
                        rows.Add((i + 1, row));
                    }
                }
            }
            finally
            {
                store.Lock.ExitReadLock();
            }
        }

        return rows;
    }

    bool ITypedRowStorageEngine.HasAnyRows(string databaseName, string tableName)
    {
        if (!_databases.TryGetValue(GetKey(databaseName, tableName), out TableStore? store))
        {
            return false;
        }

        store.Lock.EnterReadLock();
        try
        {
            for (int i = 0; i < store.Rows.Count; i++)
            {
                if (store.Rows[i] is not null)
                {
                    return true;
                }
            }
        }
        finally
        {
            store.Lock.ExitReadLock();
        }

        return false;
    }

    InMemoryStorageSnapshot IInMemoryStorageSnapshotProvider.CreateSnapshot()
    {
        var tables = new Dictionary<(string DatabaseName, string TableName), List<object?>>();

        foreach (var entry in _databases)
        {
            TableStore store = entry.Value;
            store.Lock.EnterReadLock();
            try
            {
                tables[entry.Key] = CloneRowsForSnapshot(store.Rows);
            }
            finally
            {
                store.Lock.ExitReadLock();
            }
        }

        return new InMemoryStorageSnapshot(tables);
    }

    void IInMemoryStorageSnapshotProvider.RestoreSnapshot(InMemoryStorageSnapshot snapshot)
    {
        _databases.Clear();

        foreach (var entry in snapshot.EnumerateTables())
        {
            var store = new TableStore();
            store.Rows.AddRange(RestoreRowsFromSnapshot(entry.Value));
            _databases[entry.Key] = store;
        }
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
