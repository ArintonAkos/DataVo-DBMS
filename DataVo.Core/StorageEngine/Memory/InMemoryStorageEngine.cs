using System.Collections.Concurrent;
using System.Threading;
using DataVo.Core.Exceptions;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine;

namespace DataVo.Core.StorageEngine.Memory;

/// <summary>
/// In-memory storage engine used for fast, non-persistent row operations.
/// </summary>
public class InMemoryStorageEngine : IRowIdStorageEngine, ITypedRowStorageEngine, IInMemoryStorageSnapshotProvider, IPagedRowScan, DataVo.Core.Indexing.Columnar.IColumnarRowStore
{
    // A thread-safe, purely RAM-based mapping of (DatabaseName, TableName) -> rows. A value-tuple key
    // avoids allocating a "db.table" string on every read/write/delete (and removes the latent "a.b"+"c" vs
    // "a"+"b.c" collision); tuple equality uses ordinal string comparison, matching the previous string key.
    /// <summary>
    /// One row slot: serialized bytes, or a typed row together with its cell array, or neither once deleted.
    /// The cell array is kept beside the row so a point read reaches the cells from the slot in one step,
    /// without first loading the <see cref="StoredRow"/> object that owns them.
    /// </summary>
    private readonly struct InMemoryRow
    {
        private InMemoryRow(byte[]? raw, StoredRow? typed)
        {
            Raw = raw;
            Typed = typed;
            Cells = typed?.OwnedCells;
        }

        public readonly byte[]? Raw;

        public readonly StoredRow? Typed;

        public readonly CellValue[]? Cells;

        public bool IsLive => Raw is not null || Typed is not null;

        public static InMemoryRow FromRaw(byte[] raw) => new(raw, typed: null);

        public static InMemoryRow FromTyped(StoredRow row) => new(raw: null, row);
    }

    /// <summary>
    /// A table's row slots in fixed pages, in row-id order. Slots are only appended or cleared, and a page never
    /// moves once it is full, so one row can be read without the table's lock while a writer appends: the
    /// writer fills the slot before it publishes the new count, and a reader looks only below the count it read.
    /// Every other operation, and all writing, still happens under the table's lock.
    /// </summary>
    private sealed class RowList
    {
        private const int PageShift = 10;
        private const int PageRows = 1 << PageShift;
        private const int PageMask = PageRows - 1;
        private const int FirstPageRows = 16;

        private InMemoryRow[]?[] _pages = new InMemoryRow[]?[4];
        private int _count;

        public int Count => Volatile.Read(ref _count);

        public InMemoryRow this[int index]
        {
            get => _pages[index >> PageShift]![index & PageMask];
            set => _pages[index >> PageShift]![index & PageMask] = value;
        }

        /// <summary>Appends a slot. The caller holds the table's write lock.</summary>
        public void Add(InMemoryRow row)
        {
            int index = _count;
            int number = index >> PageShift;
            int inPage = index & PageMask;
            InMemoryRow[]?[] pages = _pages;
            if (number == pages.Length)
            {
                Array.Resize(ref pages, pages.Length * 2);
                Volatile.Write(ref _pages, pages);
            }

            InMemoryRow[]? page = pages[number];
            if (page is null)
            {
                // The first page starts small, so a table of a few rows does not pay for a whole page.
                page = new InMemoryRow[number == 0 ? FirstPageRows : PageRows];
                Volatile.Write(ref pages[number], page);
            }
            else if (inPage == page.Length)
            {
                Array.Resize(ref page, Math.Min(PageRows, page.Length * 4));
                Volatile.Write(ref pages[number], page);
            }

            page[inPage] = row;
            Volatile.Write(ref _count, index + 1);
        }

        /// <summary>The cells of a live typed row, read without the table's lock; null for any other slot.</summary>
        public CellValue[]? CellsOf(long rowId)
        {
            long index = rowId - 1;
            if ((ulong)index >= (ulong)Volatile.Read(ref _count))
            {
                return null;
            }

            return Volatile.Read(ref _pages)[(int)index >> PageShift]![(int)index & PageMask].Cells;
        }

        /// <summary>A live typed row, read without the table's lock; null for any other slot.</summary>
        public StoredRow? TypedRowOf(long rowId)
        {
            long index = rowId - 1;
            if ((ulong)index >= (ulong)Volatile.Read(ref _count))
            {
                return null;
            }

            return Volatile.Read(ref _pages)[(int)index >> PageShift]![(int)index & PageMask].Typed;
        }
    }

    // Each table owns its own reader/writer lock so concurrent readers proceed in parallel while writers
    // (append, tombstone, compaction) take exclusive access. This replaces the previous global mutex
    // (lock(_syncRoot) + lock(table)) that serialized every read and write across all tables — the cause
    // of reader starvation under concurrent load. The outer ConcurrentDictionary handles table lookup/creation.
    private sealed class TableStore
    {
        public readonly ReaderWriterLockSlim Lock = new(LockRecursionPolicy.NoRecursion);
        /// <summary>Replaced as a whole when the rows are renumbered, so a reader holds one consistent list.</summary>
        public volatile RowList Rows = new();

        /// <summary>Advances when compaction renumbers rows, which invalidates a saved scan position.</summary>
        public long LayoutGeneration;

        /// <summary>The table's rows as fixed-length records for scans, once a scan has asked for them.</summary>
        public RowImage? Image;

        /// <summary>
        /// Some of the table's columns as dense vectors, when it has a columnar index. Position <c>n</c> is the
        /// row with id <c>n + 1</c>. Written under the table's write lock; read without a lock.
        /// </summary>
        public volatile DataVo.Core.Indexing.Columnar.ColumnarIndex? Columnar;

        /// <summary>Appends a row slot, and its position in the columnar index. The caller holds the write lock.</summary>
        public void Add(InMemoryRow row)
        {
            Rows.Add(row);
            Columnar?.Append(row.Raw, row.Typed);
        }
    }

    /// <summary>
    /// A table's rows a second time, as fixed-length records laid side by side in large blocks, for scans that
    /// read a few columns of every row. The rows themselves are separate objects scattered over the heap, each
    /// several cache lines wide; walking them costs a cache miss or more per row, and how many depends on which
    /// columns are read. A record is a few dozen bytes and the next one follows it, so a scan reads memory in
    /// order.
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
            store.Add(InMemoryRow.FromRaw(rowBytes));
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
                store.Add(InMemoryRow.FromRaw(rowBytes));
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
                if (row.Raw != null) return row.Raw;
                if (row.Typed != null)
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

            // Row ids are 1-based list positions, so the id of the last row is also the next index.
            int index = (int)(afterRowId ?? 0);
            int visited = 0;
            RowList rows = store.Rows;
            int count = rows.Count;
            for (; index < count && visited < maxRows; index++)
            {
                InMemoryRow row = rows[index];
                if (row.IsLive)
                {
                    visitor.Visit(index + 1, row.Raw, row.Typed);
                    visited++;
                }
            }

            return new PageScan(index >= count, store.LayoutGeneration);
        }
        finally
        {
            store.Lock.ExitReadLock();
        }
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

        RowList rows = store.Rows;
        while (image.Covered < rows.Count)
        {
            Span<byte> record = image.Next();
            InMemoryRow row = rows[image.Covered];
            if (row.IsLive)
            {
                visitor.EncodeImage(row.Raw, row.Typed, record.Slice(1));
                record[0] = 1;
            }
            else
            {
                record[0] = 0;
            }

            image.Covered++;
        }
    }

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
                    if (store.Rows[i].Raw != null)
                    {
                        rows.Add((i + 1, store.Rows[i].Raw!)); // 1-based RowId
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
                store.Rows[index] = default;
                store.Image?.Remove(index);
                store.Columnar?.Remove(index);
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
            store.LayoutGeneration++;
            var survivors = new RowList();
            for (int i = 0; i < store.Rows.Count; i++)
            {
                InMemoryRow row = store.Rows[i];
                if (row.Typed != null)
                {
                    throw new InvalidOperationException(
                        "In-memory compaction is not supported for tables containing typed rows.");
                }

                if (row.Raw != null && row.Raw.Length > 0)
                {
                    survivors.Add(row);
                    long newRowId = survivors.Count; // 1-based
                    compacted.Add((newRowId, row.Raw));
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

    DataVo.Core.Indexing.Columnar.ColumnarIndex? DataVo.Core.Indexing.Columnar.IColumnarRowStore.Columnar(string databaseName, string tableName) =>
        _databases.TryGetValue(GetKey(databaseName, tableName), out TableStore? store) ? store.Columnar : null;

    DataVo.Core.Indexing.Columnar.ColumnarIndex? DataVo.Core.Indexing.Columnar.IColumnarRowStore.BuildColumnar(
        string databaseName,
        string tableName,
        DataVo.Core.StorageEngine.Serialization.RowSerializer.StorageColumnType[] tableKinds,
        int[] ordinals,
        int schemaVersion)
    {
        TableStore store = GetOrAddTable(databaseName, tableName);
        store.Lock.EnterWriteLock();
        try
        {
            // Built whole before it is attached: a reader never sees a half-filled index, and from the moment
            // it is attached every insert and delete, which also hold this lock, keeps it current.
            var index = new DataVo.Core.Indexing.Columnar.ColumnarIndex(tableKinds, ordinals, schemaVersion);
            RowList rows = store.Rows;
            int count = rows.Count;
            for (int i = 0; i < count; i++)
            {
                InMemoryRow row = rows[i];
                if (row.IsLive)
                {
                    index.Append(row.Raw, row.Typed);
                }
                else
                {
                    index.AppendEmpty();
                }
            }

            store.Columnar = index;
            return index;
        }
        finally
        {
            store.Lock.ExitWriteLock();
        }
    }

    void DataVo.Core.Indexing.Columnar.IColumnarRowStore.DropColumnar(string databaseName, string tableName)
    {
        if (_databases.TryGetValue(GetKey(databaseName, tableName), out TableStore? store))
        {
            store.Columnar = null;
        }
    }

    long ITypedRowStorageEngine.InsertTypedRow(string databaseName, string tableName, StoredRow row)
    {
        TableStore store = GetOrAddTable(databaseName, tableName);

        store.Lock.EnterWriteLock();
        try
        {
            store.Add(InMemoryRow.FromTyped(row));
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
                store.Add(InMemoryRow.FromTyped(row));
                rowIds.Add(store.Rows.Count);
            }
        }
        finally
        {
            store.Lock.ExitWriteLock();
        }

        return rowIds;
    }

    // The two point reads below take no lock: see RowList.
    bool ITypedRowStorageEngine.TryReadTypedRow(string databaseName, string tableName, long rowId, out StoredRow? row)
    {
        row = _databases.TryGetValue(GetKey(databaseName, tableName), out TableStore? store)
            ? store.Rows.TypedRowOf(rowId)
            : null;
        return row is not null;
    }

    bool ITypedRowStorageEngine.TryReadTypedCells(string databaseName, string tableName, long rowId, out ReadOnlySpan<CellValue> cells)
    {
        CellValue[]? found = _databases.TryGetValue(GetKey(databaseName, tableName), out TableStore? store)
            ? store.Rows.CellsOf(rowId)
            : null;
        cells = found;
        return found is not null;
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
                    if (store.Rows[i].Typed is StoredRow row)
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
                if (store.Rows[i].IsLive)
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
            store.Rows = RestoreRowsFromSnapshot(entry.Value);
            _databases[entry.Key] = store;
        }
    }

    private static List<object?> CloneRowsForSnapshot(RowList rows)
    {
        int count = rows.Count;
        var clone = new List<object?>(count);

        for (int i = 0; i < count; i++)
        {
            InMemoryRow row = rows[i];
            if (row.Raw != null)
            {
                clone.Add(row.Raw.ToArray());
            }
            else if (row.Typed != null)
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

    private static RowList RestoreRowsFromSnapshot(List<object?> rows)
    {
        var restored = new RowList();

        foreach (object? row in rows)
        {
            restored.Add(row switch
            {
                byte[] bytes => InMemoryRow.FromRaw(bytes.ToArray()),
                StoredRow typed => InMemoryRow.FromTyped(typed.Clone()),
                _ => default
            });
        }

        return restored;
    }
}
