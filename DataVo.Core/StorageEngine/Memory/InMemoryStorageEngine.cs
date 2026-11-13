using System.Collections.Concurrent;
using System.Threading;
using DataVo.Core.Exceptions;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine;

namespace DataVo.Core.StorageEngine.Memory;

/// <summary>
/// In-memory storage engine used for fast, non-persistent row operations.
/// </summary>
public class InMemoryStorageEngine : IRowIdStorageEngine, ITypedRowStorageEngine, IPackedRowSource, IInMemoryStorageSnapshotProvider, IPagedRowScan, DataVo.Core.Indexing.Columnar.IColumnarRowStore
{
    // A thread-safe, purely RAM-based mapping of (DatabaseName, TableName) -> rows. A value-tuple key
    // avoids allocating a "db.table" string on every read/write/delete (and removes the latent "a.b"+"c" vs
    // "a"+"b.c" collision); tuple equality uses ordinal string comparison, matching the previous string key.
    //
    // Each table owns its own reader/writer lock so concurrent readers proceed in parallel while writers
    // (append, tombstone, compaction) take exclusive access. This replaces the previous global mutex
    // (lock(_syncRoot) + lock(table)) that serialized every read and write across all tables — the cause
    // of reader starvation under concurrent load. The outer ConcurrentDictionary handles table lookup/creation.
    private sealed class TableStore : IPackedTable
    {
        private volatile bool _detached;

        /// <inheritdoc />
        public TableRows? CurrentRows => _detached ? null : Rows;

        /// <summary>Marks the table as no longer part of the engine, for readers that kept hold of it.</summary>
        public void Detach() => _detached = true;

        public readonly ReaderWriterLockSlim Lock = new(LockRecursionPolicy.NoRecursion);
        /// <summary>Replaced as a whole when the rows are renumbered, so a reader holds one consistent sequence.</summary>
        public volatile TableRows Rows = new();

        /// <summary>Advances when compaction renumbers rows, which invalidates a saved scan position.</summary>
        public long LayoutGeneration;

        /// <summary>The table's rows as fixed-length records for scans, once a scan has asked for them.</summary>
        public RowImage? Image;

        /// <summary>
        /// Some of the table's columns as dense vectors, when it has a columnar index. Position <c>n</c> is the
        /// row with id <c>n + 1</c>. Written under the table's write lock; read without a lock.
        /// </summary>
        public volatile DataVo.Core.Indexing.Columnar.ColumnarIndex? Columnar;

        /// <summary>Appends a serialized row, and its position in the columnar index. The caller holds the write lock.</summary>
        /// <returns>The row's 1-based id.</returns>
        public long AddRaw(byte[] row)
        {
            int position = Rows.AddRaw(row);
            Columnar?.Append(row, default);
            return position + 1;
        }

        /// <summary>Appends a typed row, and its position in the columnar index. The caller holds the write lock.</summary>
        /// <returns>The row's 1-based id.</returns>
        public long AddTyped(IReadOnlyList<Column> columns, ReactiveRowSchema schema, ReadOnlySpan<CellValue> cells)
        {
            int position = Rows.AddTyped(columns, schema, cells);
            Columnar?.Append(default, cells);
            return position + 1;
        }
    }

    /// <summary>
    /// A table's rows a second time, as fixed-length records laid side by side in large blocks, for scans that
    /// read a few columns of every row. A record holds just those columns, already in the form the scan reads
    /// them in, and the next one follows it, so a scan reads memory in order and decodes nothing.
    /// </summary>
    /// <remarks>
    /// The image is built by the first scan that asks for one and caught up by later scans: rows are only ever
    /// appended or tombstoned, so catching up means encoding the rows added since. A tombstone clears the
    /// record's first byte. Everything is done under the table's lock: writing under the write lock, reading
    /// under the read lock. The scan's visitor encodes the records, since only it knows the column types.
    /// </remarks>
    private sealed class RowImage(int recordBytes, int version)
    {
        // A block holds a power of two of records and is about this large: big enough to be allocated once and
        // never moved, small enough that a table of a few rows does not pay for it (its one block starts small).
        private const int BlockBytes = 128 * 1024;
        private const int FirstBlockRows = 64;

        private readonly List<byte[]> _blocks = [];

        /// <summary>The length of a record: one byte that is 0 for a deleted row, then the visitor's image.</summary>
        public readonly int RecordBytes = recordBytes;

        /// <summary>The column layout the records were encoded for.</summary>
        public readonly int Version = version;

        /// <summary>Row index to block: <c>index &gt;&gt; Shift</c>.</summary>
        public readonly int Shift = ShiftFor(recordBytes);

        /// <summary>How many of the table's rows, from the first, have a record.</summary>
        public int Covered;

        public int BlockRows => 1 << Shift;

        public byte[] Block(int number) => _blocks[number];

        private static int ShiftFor(int recordBytes)
        {
            int shift = 4;
            while (shift < 16 && (recordBytes << shift) < BlockBytes)
            {
                shift++;
            }

            return shift;
        }

        /// <summary>The record of the next row to cover, with room made for it.</summary>
        public Span<byte> Next()
        {
            int number = Covered >> Shift;
            int inBlock = Covered & (BlockRows - 1);
            if (number == _blocks.Count)
            {
                _blocks.Add(new byte[Math.Min(BlockRows, FirstBlockRows) * RecordBytes]);
            }

            byte[] block = _blocks[number];
            if ((inBlock + 1) * RecordBytes > block.Length)
            {
                Array.Resize(ref block, Math.Min(BlockRows * RecordBytes, block.Length * 4));
                _blocks[number] = block;
            }

            return new Span<byte>(block, inBlock * RecordBytes, RecordBytes);
        }

        /// <summary>Marks a covered row deleted.</summary>
        public void Remove(int index)
        {
            if (index < Covered)
            {
                _blocks[index >> Shift][(index & (BlockRows - 1)) * RecordBytes] = 0;
            }
        }
    }

    private readonly ConcurrentDictionary<(string DatabaseName, string TableName), TableStore> _databases = new();

    private static (string DatabaseName, string TableName) GetKey(string databaseName, string tableName) => (databaseName, tableName);

    private TableStore GetOrAddTable(string databaseName, string tableName)
    {
        return _databases.GetOrAdd(GetKey(databaseName, tableName), static _ => new TableStore());
    }

    /// <summary>
    /// Row slots of the table, deleted slots included (an upper bound, used only as a planner estimate).
    /// </summary>
    public long EstimateRowCount(string databaseName, string tableName)
    {
        if (!_databases.TryGetValue(GetKey(databaseName, tableName), out TableStore? store))
        {
            return 0;
        }

        store.Lock.EnterReadLock();
        try
        {
            return store.Rows.Count;
        }
        finally
        {
            store.Lock.ExitReadLock();
        }
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
            return store.AddRaw(rowBytes); // 1-based: first row = 1
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
                rowIds.Add(store.AddRaw(rowBytes));
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
            // O(1): a row's position is its 1-based RowId less one.
            TableRows rows = store.Rows;
            if (rowId < 1 || rowId > rows.Count)
            {
                throw new RowNotFoundException(rowId, tableName);
            }

            int position = (int)(rowId - 1);
            if (rows.RawAt(position) is { } raw)
            {
                return raw;
            }

            if (rows.IsTypedLive(position))
            {
                throw new InvalidOperationException(
                    "Typed in-memory rows must be read through StorageContext's typed row path.");
            }

            throw new RowDeletedException(rowId, tableName);
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
    /// <inheritdoc />
    PageScan IPagedRowScan.VisitPage<TState>(
        string databaseName,
        string tableName,
        long? afterRowId,
        long? expectedGeneration,
        int maxRows,
        ref TState state,
        PagedRowVisitor<TState> visit)
    {
        var visitor = new DelegatePagedRowVisitor<TState>(state, visit);
        try
        {
            return ((IPagedRowScan)this).ScanPage(databaseName, tableName, afterRowId, expectedGeneration, maxRows, ref visitor);
        }
        finally
        {
            state = visitor.State;
        }
    }

    /// <inheritdoc />
    // Compiled fully optimized on first use, with the visitor inlined. Left to tiered compilation, the per-row
    // code of some instantiations (there is one per compiled query) was never promoted past its first,
    // unoptimized version, and those queries ran 3 to 4 times slower than the rest.
    [System.Runtime.CompilerServices.MethodImpl(DataVo.Core.Compat.MethodImplCompat.AggressiveOptimization)]
    PageScan IPagedRowScan.ScanPage<TVisitor>(
        string databaseName,
        string tableName,
        long? afterRowId,
        long? expectedGeneration,
        int maxRows,
        ref TVisitor visitor)
    {
        if (!_databases.TryGetValue(GetKey(databaseName, tableName), out TableStore? store))
        {
            return new PageScan(true, 0);
        }

        // A visitor that reads row images gets them when the image is whole. It is brought up to date first; a
        // writer may slip in between that and the read, so after a few tries the rows are visited as they are.
        int imageBytes = visitor.ImageBytes;
        for (int attempt = 0; imageBytes > 0 && attempt < 3; attempt++)
        {
            store.Lock.EnterReadLock();
            try
            {
                if (store.Image is { } image
                    && image.RecordBytes == imageBytes + 1
                    && image.Version == visitor.ImageVersion
                    && image.Covered == store.Rows.Count)
                {
                    if (expectedGeneration is { } expected && expected != store.LayoutGeneration)
                    {
                        throw PagedRowScanErrors.Rewritten(tableName);
                    }

                    return ScanImage(store, image, (int)(afterRowId ?? 0), maxRows, ref visitor);
                }
            }
            finally
            {
                store.Lock.ExitReadLock();
            }

            store.Lock.EnterWriteLock();
            try
            {
                CoverWithImage(store, imageBytes, ref visitor);
            }
            finally
            {
                store.Lock.ExitWriteLock();
            }
        }

        store.Lock.EnterReadLock();
        try
        {
            if (expectedGeneration is { } generation && generation != store.LayoutGeneration)
            {
                throw PagedRowScanErrors.Rewritten(tableName);
            }

            // Row ids are 1-based positions, so the id of the last row is also the next position.
            int position = (int)(afterRowId ?? 0);
            bool reachedEnd = VisitRows(store.Rows, ref position, maxRows, ref visitor);
            return new PageScan(reachedEnd, store.LayoutGeneration);
        }
        finally
        {
            store.Lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Visits up to <paramref name="maxRows"/> live rows from <paramref name="position"/> on: a serialized row as
    /// its bytes, a typed row as its cells. Called with the table's read lock held.
    /// </summary>
    /// <returns>Whether the last position was reached; <paramref name="position"/> is left after the last row examined.</returns>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private static bool VisitRows<TVisitor>(TableRows rows, ref int position, int maxRows, ref TVisitor visitor)
        where TVisitor : struct, IPagedRowVisitor
    {
        int count = rows.Count;
        int visited = 0;
        var reader = new PackedRowReader();
        try
        {
            for (; position < count && visited < maxRows; position++)
            {
                if (rows.RawAt(position) is { } raw)
                {
                    visitor.Visit(position + 1, raw, default);
                    visited++;
                }
                else if (rows.TryReadTypedCells(position, ref reader, out ReadOnlySpan<CellValue> cells))
                {
                    visitor.Visit(position + 1, default, cells);
                    visited++;
                }
            }
        }
        finally
        {
            reader.Dispose();
        }

        return position >= count;
    }

    /// <summary>Visits up to <paramref name="maxRows"/> live rows from <paramref name="index"/> on, as image records.</summary>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private static PageScan ScanImage<TVisitor>(TableStore store, RowImage image, int index, int maxRows, ref TVisitor visitor)
        where TVisitor : struct, IPagedRowVisitor
    {
        int count = image.Covered;
        int recordBytes = image.RecordBytes;
        int blockRows = image.BlockRows;
        int visited = 0;
        while (index < count && visited < maxRows)
        {
            byte[] block = image.Block(index >> image.Shift);
            int inBlock = index & (blockRows - 1);
            int end = Math.Min(blockRows, inBlock + (count - index));
            for (int at = inBlock * recordBytes; inBlock < end && visited < maxRows; inBlock++, at += recordBytes, index++)
            {
                if (block[at] != 0)
                {
                    visitor.VisitImage(index + 1, new ReadOnlySpan<byte>(block, at + 1, recordBytes - 1));
                    visited++;
                }
            }
        }

        return new PageScan(index >= count, store.LayoutGeneration);
    }

    /// <summary>
    /// Gives every row of the table an image record, starting over when the records on hand were encoded for
    /// another column layout. Called with the table's write lock held.
    /// </summary>
    private static void CoverWithImage<TVisitor>(TableStore store, int imageBytes, ref TVisitor visitor)
        where TVisitor : struct, IPagedRowVisitor
    {
        RowImage? image = store.Image;
        if (image is null || image.RecordBytes != imageBytes + 1 || image.Version != visitor.ImageVersion)
        {
            store.Image = image = new RowImage(imageBytes + 1, visitor.ImageVersion);
        }

        TableRows rows = store.Rows;
        var reader = new PackedRowReader();
        try
        {
            while (image.Covered < rows.Count)
            {
                Span<byte> record = image.Next();
                record[0] = EncodeImage(rows, image.Covered, ref reader, ref visitor, record.Slice(1)) ? (byte)1 : (byte)0;
                image.Covered++;
            }
        }
        finally
        {
            reader.Dispose();
        }
    }

    /// <summary>Writes the image record of the row at a position.</summary>
    /// <returns><see langword="false"/>, having written nothing, when the position holds no row.</returns>
    private static bool EncodeImage<TVisitor>(TableRows rows, int position, ref PackedRowReader reader, ref TVisitor visitor, Span<byte> record)
        where TVisitor : struct, IPagedRowVisitor
    {
        if (rows.RawAt(position) is { } raw)
        {
            visitor.EncodeImage(raw, default, record);
            return true;
        }

        if (rows.TryReadTypedCells(position, ref reader, out ReadOnlySpan<CellValue> cells))
        {
            visitor.EncodeImage(default, cells, record);
            return true;
        }

        return false;
    }

    public IEnumerable<(long RowId, byte[] RawRow)> ReadAllRows(string databaseName, string tableName)
    {
        var rows = new List<(long RowId, byte[] RawRow)>();

        if (_databases.TryGetValue(GetKey(databaseName, tableName), out TableStore? store))
        {
            store.Lock.EnterReadLock();
            try
            {
                TableRows stored = store.Rows;
                for (int i = 0; i < stored.Count; i++)
                {
                    if (stored.RawAt(i) is { } raw)
                    {
                        rows.Add((i + 1, raw)); // 1-based RowId
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
    /// Tombstones a row: its position stays used, so the ids of other rows do not shift.
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
            TableRows rows = store.Rows;
            if (rowId >= 1 && rowId <= rows.Count)
            {
                int position = (int)(rowId - 1);
                rows.Remove(position);
                store.Image?.Remove(position);
                store.Columnar?.Remove(position);
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
        if (_databases.TryRemove(GetKey(databaseName, tableName), out TableStore? store))
        {
            store.Detach();
        }
    }

    /// <summary>
    /// Removes all tables belonging to a database from the in-memory store.
    /// </summary>
    /// <param name="databaseName">The database name.</param>
    public void DropDatabase(string databaseName)
    {
        foreach (var key in _databases.Keys)
        {
            if (string.Equals(key.DatabaseName, databaseName, StringComparison.Ordinal)
                && _databases.TryRemove(key, out TableStore? store))
            {
                store.Detach();
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
            TableRows rows = store.Rows;
            if (rows.HasTypedRows())
            {
                throw new InvalidOperationException(
                    "In-memory compaction is not supported for tables containing typed rows.");
            }

            store.LayoutGeneration++;
            var survivors = new TableRows();
            for (int i = 0; i < rows.Count; i++)
            {
                if (rows.RawAt(i) is { Length: > 0 } raw)
                {
                    long newRowId = survivors.AddRaw(raw) + 1; // 1-based
                    compacted.Add((newRowId, raw));
                }
            }

            store.Rows = survivors;
            store.Image = null; // rows were renumbered; the next scan builds it again
            store.Columnar = null; // and so is this, by the next query that wants it
        }
        finally
        {
            store.Lock.ExitWriteLock();
        }

        return compacted;
    }

    IPackedTable? IPackedRowSource.TableOf(string databaseName, string tableName) =>
        _databases.TryGetValue(GetKey(databaseName, tableName), out TableStore? store) ? store : null;

    bool DataVo.Core.Indexing.Columnar.IColumnarRowStore.KeyedRows => false;

    DataVo.Core.Indexing.Columnar.ColumnarIndex? DataVo.Core.Indexing.Columnar.IColumnarRowStore.Columnar(string databaseName, string tableName) =>
        _databases.TryGetValue(GetKey(databaseName, tableName), out TableStore? store) ? store.Columnar : null;

    DataVo.Core.Indexing.Columnar.ColumnarIndex? DataVo.Core.Indexing.Columnar.IColumnarRowStore.BuildColumnar(
        string databaseName,
        string tableName,
        DataVo.Core.StorageEngine.Serialization.RowSerializer.StorageColumnType[] tableKinds,
        int[] ordinals,
        int schemaVersion,
        int keyOrdinal)
    {
        TableStore store = GetOrAddTable(databaseName, tableName);
        store.Lock.EnterWriteLock();
        try
        {
            // Built whole before it is attached: a reader never sees a half-filled index, and from the moment
            // it is attached every insert and delete, which also hold this lock, keeps it current.
            var index = new DataVo.Core.Indexing.Columnar.ColumnarIndex(tableKinds, ordinals, schemaVersion);
            AppendRows(store.Rows, index);
            store.Columnar = index;
            return index;
        }
        finally
        {
            store.Lock.ExitWriteLock();
        }
    }

    /// <summary>Gives every position of a table its place in a new columnar index, rows and gaps alike.</summary>
    private static void AppendRows(TableRows rows, DataVo.Core.Indexing.Columnar.ColumnarIndex index)
    {
        int count = rows.Count;
        var reader = new PackedRowReader();
        try
        {
            for (int position = 0; position < count; position++)
            {
                if (rows.RawAt(position) is { } raw)
                {
                    index.Append(raw, default);
                }
                else if (rows.TryReadTypedCells(position, ref reader, out ReadOnlySpan<CellValue> cells))
                {
                    index.Append(default, cells);
                }
                else
                {
                    index.AppendEmpty();
                }
            }
        }
        finally
        {
            reader.Dispose();
        }
    }

    void DataVo.Core.Indexing.Columnar.IColumnarRowStore.DropColumnar(string databaseName, string tableName)
    {
        if (_databases.TryGetValue(GetKey(databaseName, tableName), out TableStore? store))
        {
            store.Columnar = null;
        }
    }

    long ITypedRowStorageEngine.InsertTypedRow(string databaseName, string tableName, IReadOnlyList<Column> columns, ReactiveRowSchema schema, ReadOnlySpan<CellValue> cells)
    {
        TableStore store = GetOrAddTable(databaseName, tableName);

        store.Lock.EnterWriteLock();
        try
        {
            return store.AddTyped(columns, schema, cells);
        }
        finally
        {
            store.Lock.ExitWriteLock();
        }
    }

    List<long> ITypedRowStorageEngine.InsertTypedRows(string databaseName, string tableName, IReadOnlyList<Column> columns, ReactiveRowSchema schema, IReadOnlyList<CellValue[]> rows)
    {
        var rowIds = new List<long>(rows.Count);
        TableStore store = GetOrAddTable(databaseName, tableName);

        store.Lock.EnterWriteLock();
        try
        {
            for (int i = 0; i < rows.Count; i++)
            {
                rowIds.Add(store.AddTyped(columns, schema, rows[i]));
            }
        }
        finally
        {
            store.Lock.ExitWriteLock();
        }

        return rowIds;
    }

    // The reads of typed rows below take no lock: see TableRows.
    bool ITypedRowStorageEngine.TryReadTypedRow(string databaseName, string tableName, long rowId, out StoredRow? row)
    {
        row = null;
        return TryFindPosition(databaseName, tableName, rowId, out TableRows? rows, out int position)
            && rows.TryCopyTyped(position, out row);
    }

    bool ITypedRowStorageEngine.HasTypedRow(string databaseName, string tableName, long rowId) =>
        TryFindPosition(databaseName, tableName, rowId, out TableRows? rows, out int position) && rows.IsTypedLive(position);

    bool ITypedRowStorageEngine.TryVisitTypedCells<TState>(string databaseName, string tableName, long rowId, ref TState state, TypedCellsVisitor<TState> visit)
    {
        if (!TryFindPosition(databaseName, tableName, rowId, out TableRows? rows, out int position))
        {
            return false;
        }

        var reader = new PackedRowReader();
        try
        {
            if (!rows.TryReadTypedCells(position, ref reader, out ReadOnlySpan<CellValue> cells))
            {
                return false;
            }

            visit(ref state, rowId, cells);
            return true;
        }
        finally
        {
            reader.Dispose();
        }
    }

    void ITypedRowStorageEngine.VisitTypedRows<TState>(string databaseName, string tableName, ref TState state, TypedCellsVisitor<TState> visit)
    {
        if (!_databases.TryGetValue(GetKey(databaseName, tableName), out TableStore? store))
        {
            return;
        }

        // The rows as they stand now: a row added while the visit runs is not part of it.
        TableRows rows = store.Rows;
        int count = rows.Count;
        var reader = new PackedRowReader();
        try
        {
            for (int position = 0; position < count; position++)
            {
                if (rows.TryReadTypedCells(position, ref reader, out ReadOnlySpan<CellValue> cells)
                    && !visit(ref state, position + 1, cells))
                {
                    return;
                }
            }
        }
        finally
        {
            reader.Dispose();
        }
    }

    IEnumerable<(long RowId, StoredRow Row)> ITypedRowStorageEngine.ReadAllTypedRows(string databaseName, string tableName)
    {
        var found = new List<(long RowId, StoredRow Row)>();

        if (_databases.TryGetValue(GetKey(databaseName, tableName), out TableStore? store))
        {
            store.Lock.EnterReadLock();
            try
            {
                TableRows rows = store.Rows;
                for (int i = 0; i < rows.Count; i++)
                {
                    if (rows.TryCopyTyped(i, out StoredRow? row))
                    {
                        found.Add((i + 1, row!));
                    }
                }
            }
            finally
            {
                store.Lock.ExitReadLock();
            }
        }

        return found;
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
            TableRows rows = store.Rows;
            for (int i = 0; i < rows.Count; i++)
            {
                if (rows.IsLive(i))
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

    /// <summary>Finds a table's rows and the position a 1-based row id names in them.</summary>
    /// <returns><see langword="false"/> when there is no such table or the id is outside it.</returns>
    private bool TryFindPosition(
        string databaseName,
        string tableName,
        long rowId,
        [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out TableRows? rows,
        out int position)
    {
        position = 0;
        rows = _databases.TryGetValue(GetKey(databaseName, tableName), out TableStore? store) ? store.Rows : null;
        if (rows is null || rowId < 1 || rowId > rows.Count)
        {
            return false;
        }

        position = (int)(rowId - 1);
        return true;
    }

    InMemoryStorageSnapshot IInMemoryStorageSnapshotProvider.CreateSnapshot()
    {
        var tables = new Dictionary<(string DatabaseName, string TableName), TableRows>();

        foreach (var entry in _databases)
        {
            TableStore store = entry.Value;
            store.Lock.EnterReadLock();
            try
            {
                tables[entry.Key] = store.Rows.Clone();
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
        // Every table is replaced, so the rows a table held, packed or kept apart, go with it.
        foreach (var key in _databases.Keys)
        {
            if (_databases.TryRemove(key, out TableStore? store))
            {
                store.Detach();
            }
        }

        foreach (var entry in snapshot.EnumerateTables())
        {
            _databases[entry.Key] = new TableStore { Rows = entry.Value };
        }
    }
}
