using System.Buffers;
using System.Runtime.CompilerServices;
using DataVo.Core.BTree;
using DataVo.Core.CompiledQueries;
using DataVo.Core.Exceptions;
using DataVo.Core.Models.Catalog;
using DataVo.Core.MVCC;
using DataVo.Core.Parser.DML;
using DataVo.Core.Runtime.Changes;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine;
using DataVo.Core.ValueConversion;

namespace DataVo.Core.Batches;

/// <summary>
/// Carries out <see cref="DataVoContext.ApplyBatch(DataVoWriteBatch)"/>: folds the batch into one final state per row, finds the
/// stored rows through their primary-key index, validates everything against the batch's final state (see
/// <see cref="BatchIntegrity"/>), and only then writes deletes, updates and inserts through the row writers SQL uses.
/// </summary>
/// <remarks>
/// The caller owns the write unit and the table locks; this class only reads and writes rows. Rows are found
/// by primary-key index lookups, never by scanning, so the cost of a batch grows with its size, not with the
/// size of the tables.
/// </remarks>
internal sealed class BatchApplier(DataVoContext context, string databaseName)
{
    [ThreadStatic]
    private static HashSet<long>? _fastIntKeyScratch;

    [ThreadStatic]
    private static HashSet<Guid>? _fastGuidKeyScratch;

    [ThreadStatic]
    private static HashSet<string>? _fastStringKeyScratch;

    [ThreadStatic]
    private static TypedRowBufferPool? _rowBufferPool;

    private readonly Dictionary<string, TablePlan> _tables = new(StringComparer.OrdinalIgnoreCase);
    private readonly List<TablePlan> _order = [];
    private readonly Dictionary<DataVoBatchLayout, LayoutPlan> _layouts = new(LayoutReferenceComparer.Instance);
    private DataVoWriteBatch? _batch;
    private int _unchanged;
    private int _missing;

    /// <summary>Gets the stored names of the tables the batch writes, in the order they first appear.</summary>
    public IEnumerable<string> Tables => _order.Select(static table => table.Name);

    /// <summary>
    /// Resolves the batch's tables and layouts and folds its operations into one pending change per row. Reads
    /// only the catalog, so it can run before any lock is taken.
    /// </summary>
    /// <param name="batch">The batch to fold.</param>
    /// <exception cref="ArgumentException">A table or column does not exist, a layout misses a key column, or a key is NULL.</exception>
    public void Prepare(DataVoWriteBatch batch)
    {
        _batch = batch;
        for (int i = 0; i < batch.Count; i++)
        {
            DataVoWriteBatch.Entry entry = batch[i];
            LayoutPlan layout = LayoutOf(entry.Layout);
            TablePlan table = layout.Table;

            if (table.FastPathEligible
                && entry.IsTyped
                && entry.Operation == DataVoBatchOperation.Upsert
                && layout.IsFullTypedLayout
                && (table.FastLayout is null || ReferenceEquals(table.FastLayout, layout)))
            {
                ReadOnlySpan<CellValue> cells = batch.TypedValuesOf(entry);
                ValidateTypedPrimaryKeysNonNull(layout, cells);
                table.FastLayout ??= layout;
                (table.FastEntries ??= []).Add(i);
                continue;
            }

            if (table.FastPathEligible)
            {
                DemoteTable(table);
            }

            FoldGeneralEntry(entry, layout);
        }
    }

    /// <summary>
    /// Finds the stored rows, decides what each pending change does, validates it, and writes it. Runs inside
    /// the caller's write unit, with every table of the batch write-locked.
    /// </summary>
    /// <param name="statementTxId">The MVCC transaction id of the write unit.</param>
    /// <param name="recorder">The change recorder, or <see langword="null"/> when capture is off.</param>
    /// <returns>What happened to the stored rows.</returns>
    public DataVoBatchResult Execute(long statementTxId, ChangeRecorder? recorder)
    {
        List<FastInsertPlan>? fastPlans = null;
        try
        {
            fastPlans = PrepareFastInserts();

            bool hasGeneralTables = false;
            foreach (TablePlan table in _order)
            {
                if (!table.FastPathEligible)
                {
                    hasGeneralTables = true;
                    Classify(table);
                }
            }

            // Every constraint is decided against the batch's final state before anything is written, so the
            // writers below run without their own foreign-key checks and cannot stop half-way.
            int deleted = 0;
            if (hasGeneralTables)
            {
                BatchIntegrity integrity = Validate();

                // Deletes (cascaded rows included) go first, so the keys, unique values and parents they give up are
                // free before updates and inserts claim them.
                foreach (string table in integrity.TablesWithDeletes.ToList())
                {
                    List<RowKey> rows = [.. integrity.DeletedRows(table)];
                    deleted += rows.Count;
                    RowDeleter.Delete(context.Engine, rows, table, databaseName, statementTxId, recorder, followForeignKeys: false);
                }

                foreach (TablePlan table in _order)
                {
                    if (!table.FastPathEligible)
                    {
                        WriteUpdates(table, statementTxId, recorder);
                    }
                }

                foreach (string table in integrity.ParentsFirst([.. _order.Where(plan => !plan.FastPathEligible && plan.Inserts.Count > 0).Select(plan => plan.Name)]))
                {
                    WriteInserts(_tables[table], statementTxId, recorder);
                }
            }

            if (fastPlans is not null)
            {
                foreach (FastInsertPlan fastPlan in fastPlans)
                {
                    InsertService.WriteValidatedTypedInsertRows(
                        databaseName,
                        fastPlan.Table.Name,
                        fastPlan.Prepared,
                        statementTxId,
                        recorder,
                        callerOwnsRowBuffers: true);
                    fastPlan.Table.FastInsertedCount = fastPlan.Prepared.Rows.Count;
                }
            }

            return Summarize(deleted);
        }
        finally
        {
            if (fastPlans is not null)
            {
                foreach (FastInsertPlan fastPlan in fastPlans)
                {
                    fastPlan.Dispose();
                }
            }
        }
    }

    private LayoutPlan LayoutOf(DataVoBatchLayout layout)
    {
        if (_layouts.TryGetValue(layout, out LayoutPlan? plan))
        {
            return plan;
        }

        TablePlan table = TableOf(layout.TableName);
        plan = new LayoutPlan(layout, table);
        _layouts[layout] = plan;
        return plan;
    }

    private TablePlan TableOf(string tableName)
    {
        string stored = context.Engine.Catalog.FindTableName(tableName, databaseName)
            ?? throw new ArgumentException($"Table {tableName} doesn't exist in database {databaseName}.");
        if (!_tables.TryGetValue(stored, out TablePlan? table))
        {
            table = new TablePlan(
                stored,
                context.Engine.Catalog.GetTableColumns(stored, databaseName),
                context.Engine.Catalog.GetTablePrimaryKeys(stored, databaseName));
            _tables[stored] = table;
            _order.Add(table);
        }

        return table;
    }

    /// <summary>Builds the row's primary-key index key from the key values, normalized to the column types.</summary>
    private string KeyOf(LayoutPlan layout, ReadOnlySpan<object?> values, out Dictionary<string, object?> keyValues)
    {
        TablePlan table = layout.Table;
        keyValues = new Dictionary<string, object?>(table.PrimaryKeys.Count, StringComparer.OrdinalIgnoreCase);
        for (int k = 0; k < table.PrimaryKeys.Count; k++)
        {
            Column column = layout.KeyColumns[k];
            object? value = Materialize(BindAndNormalize(column, values[layout.KeyPositions[k]]));
            keyValues[column.Name] = value ?? throw new ArgumentException($"Primary key column {column.Name} of {table.Name} cannot be NULL in a batch.");
        }

        return IndexKeyEncoder.BuildKeyString(keyValues, table.PrimaryKeys);
    }

    private object? BindAndNormalize(Column column, object? value) =>
        DataVoCompiledQuery.NormalizeAssignedValue(column, context.BindValueForWrite(value));

    /// <summary>Looks up each pending row by primary key and sorts it into delete, update, insert or unchanged.</summary>
    private void Classify(TablePlan table)
    {
        Dictionary<string, RowKey?> located = Locate(table);
        List<RowKey> storedIds = [.. located.Values.Where(static id => id.HasValue).Select(static id => id!.Value)];
        Dictionary<RowKey, Dictionary<string, object?>> stored = storedIds.Count == 0
            ? []
            : context.Engine.StorageContext.GetTableContents(storedIds, table.Name, databaseName);

        foreach (PendingRow row in table.Rows)
        {
            Dictionary<string, object?>? existing = located[row.Key] is { } id && stored.TryGetValue(id, out var found) ? found : null;
            RowKey rowId = existing is null ? default : located[row.Key]!.Value;
            if (row.IsDelete)
            {
                AddDelete(table, existing, rowId);
            }
            else if (existing is null && row.UpdateOnly)
            {
                _missing++;
            }
            else if (existing is null)
            {
                table.Inserts.Add(row.Values!);
            }
            else
            {
                AddUpdate(table, row, existing, rowId);
            }
        }
    }

    /// <summary>Finds the stored row id of every pending row through the table's primary-key index.</summary>
    private Dictionary<string, RowKey?> Locate(TablePlan table)
    {
        var located = new Dictionary<string, RowKey?>(table.Rows.Count, StringComparer.Ordinal);
        string indexName = $"_PK_{table.Name}";
        foreach (PendingRow row in table.Rows)
        {
            IReadOnlyList<RowKey> ids;
            try
            {
                ids = context.Engine.IndexManager.FilterUsingIndex(row.Key, indexName, table.Name, databaseName);
            }
            catch (IndexException exception)
            {
                throw new InvalidOperationException($"Table {table.Name} has no primary-key index to apply a batch against.", exception);
            }

            located[row.Key] = ids.Count > 0 ? ids[0] : null;
        }

        return located;
    }

    private void AddDelete(TablePlan table, Dictionary<string, object?>? existing, RowKey rowId)
    {
        if (existing is null)
        {
            _unchanged++;
            return;
        }

        table.DeleteIds.Add(rowId);
        table.DeleteRows[rowId] = existing;
    }

    private void AddUpdate(TablePlan table, PendingRow row, Dictionary<string, object?> existing, RowKey rowId)
    {
        Dictionary<string, object?> updated = row.Reset
            ? table.DefaultRow(row.KeyValues, BindAndNormalize)
            : new Dictionary<string, object?>(existing, StringComparer.OrdinalIgnoreCase);
        foreach (KeyValuePair<string, object?> column in row.Values!)
        {
            updated[column.Key] = column.Value;
        }

        if (SameRow(existing, updated))
        {
            _unchanged++;
            return;
        }

        table.UpdateIds.Add(rowId);
        table.UpdateOldRows[rowId] = existing;
        table.UpdateNewRows.Add(updated);
    }

    /// <summary>
    /// Checks MVCC, keys, uniqueness, references and delete restrictions against the batch's final state, and
    /// works out what cascades remove. Updates of rows a cascade removes are dropped: those rows end deleted.
    /// </summary>
    private BatchIntegrity Validate()
    {
        var integrity = new BatchIntegrity(context.Engine, databaseName, InsertService);
        foreach (TablePlan table in _order)
        {
            if (!table.FastPathEligible)
            {
                integrity.AddTable(table.Name, table.DeleteRows, table.UpdateIds, table.UpdateOldRows, table.UpdateNewRows, table.Inserts);
            }
        }

        integrity.Validate();
        foreach (TablePlan table in _order)
        {
            if (!table.FastPathEligible)
            {
                table.DropUpdatesOf(integrity.DeletedRows(table.Name));
            }
        }

        return integrity;
    }

    private void WriteUpdates(TablePlan table, long statementTxId, ChangeRecorder? recorder)
    {
        if (table.UpdateIds.Count == 0)
        {
            return;
        }

        List<Dictionary<string, object?>> oldRows = [.. table.UpdateIds.Select(id => table.UpdateOldRows[id])];
        if (recorder is { WantsAnnouncements: true })
        {
            for (int i = 0; i < oldRows.Count; i++)
            {
                recorder.AnnounceUpdate(table.Name, table.UpdateIds[i], oldRows[i], table.UpdateNewRows[i]);
            }
        }

        RowKey[] newIds = DataVoCompiledQuery.ReplaceRows(context, table.Name, databaseName, table.UpdateIds, oldRows, table.UpdateNewRows, statementTxId);
        for (int i = 0; recorder is not null && i < newIds.Length; i++)
        {
            recorder.RecordUpdate(table.Name, newIds[i], oldRows[i], table.UpdateNewRows[i]);
        }
    }

    private void WriteInserts(TablePlan table, long statementTxId, ChangeRecorder? recorder)
    {
        if (table.Inserts.Count > 0)
        {
            InsertService.InsertRows(databaseName, table.Name, table.Inserts, txContext: null, statementTxId, recorder, checkForeignKeys: false);
        }
    }

    private InsertRowService InsertService =>
        field ??= new InsertRowService(context.Engine, context.Engine.StorageContext, context.Engine.Catalog, context.Engine.IndexManager);

    private void FoldGeneralEntry(DataVoWriteBatch.Entry entry, LayoutPlan layout)
    {
        if (!entry.IsTyped)
        {
            FoldValues(entry.Operation, layout, _batch!.ValuesOf(entry));
            return;
        }

        ReadOnlySpan<CellValue> cells = _batch!.TypedValuesOf(entry);
        object?[] boxed = ArrayPool<object?>.Shared.Rent(cells.Length);
        try
        {
            for (int i = 0; i < cells.Length; i++)
            {
                boxed[i] = cells[i].ToObject();
            }

            FoldValues(entry.Operation, layout, boxed.AsSpan(0, cells.Length));
        }
        finally
        {
            Array.Clear(boxed, 0, cells.Length);
            ArrayPool<object?>.Shared.Return(boxed);
        }
    }

    private void FoldValues(DataVoBatchOperation operation, LayoutPlan layout, ReadOnlySpan<object?> values)
    {
        PendingRow row = layout.Table.Pending(KeyOf(layout, values, out Dictionary<string, object?> keyValues), keyValues);
        if (operation == DataVoBatchOperation.Delete)
        {
            row.MarkDeleted();
        }
        else
        {
            row.Set(layout, values, BindAndNormalize, updateOnly: operation == DataVoBatchOperation.Update);
        }
    }

    private void DemoteTable(TablePlan table)
    {
        table.FastPathEligible = false;
        if (table.FastEntries is null || table.FastEntries.Count == 0)
        {
            return;
        }

        LayoutPlan layout = table.FastLayout!;
        foreach (int entryIndex in table.FastEntries)
        {
            FoldGeneralEntry(_batch![entryIndex], layout);
        }

        table.FastEntries.Clear();
    }

    private static void ValidateTypedPrimaryKeysNonNull(LayoutPlan layout, ReadOnlySpan<CellValue> cells)
    {
        for (int k = 0; k < layout.KeyPositions.Length; k++)
        {
            if (cells[layout.KeyPositions[k]].IsNull)
            {
                throw new ArgumentException($"Primary key column {layout.KeyColumns[k].Name} of {layout.Table.Name} cannot be NULL in a batch.");
            }
        }
    }

    private List<FastInsertPlan>? PrepareFastInserts()
    {
        List<FastInsertPlan>? plans = null;
        foreach (TablePlan table in _order)
        {
            if (!table.FastPathEligible || table.FastEntries is null || table.FastEntries.Count == 0)
            {
                table.FastPathEligible = false;
                continue;
            }

            if (TryPrepareFastInsert(table, out FastInsertPlan? plan))
            {
                (plans ??= []).Add(plan!);
            }
            else
            {
                DemoteTable(table);
            }
        }

        return plans;
    }

    private bool TryPrepareFastInsert(TablePlan table, out FastInsertPlan? plan)
    {
        plan = null;
        if (context.Engine.Catalog.GetTableForeignKeys(table.Name, databaseName).Count > 0
            || IsReferencedByAnotherBatchTable(table.Name))
        {
            return false;
        }

        List<int> entryIndices = table.FastEntries!;
        LayoutPlan layout = table.FastLayout!;
        int rowCount = entryIndices.Count;
        int columnCount = table.AllColumns.Count;
        CellType[] expectedTypes = table.ExpectedCellTypes!;

        TypedRowBufferPool pool = _rowBufferPool ??= new TypedRowBufferPool();
        List<CellValue[]> rowBuffers = pool.Rent(rowCount, columnCount);
        long[]? integerKeys = null;
        bool committedToPlan = false;
        try
        {
            if (!FillTypedRowBuffers(layout, expectedTypes, entryIndices, rowBuffers))
            {
                return false;
            }

            ReactiveRowSchema schema = table.GetReactiveSchema();
            string[]? primaryKeyValues = null;
            if (!TryCheckDisjointPrimaryKeys(table, schema, rowBuffers, out integerKeys, out primaryKeyValues))
            {
                return false;
            }

            InsertRowService.PreparedTypedInsert prepared = InsertService.ValidateTypedInsertRows(
                databaseName,
                table.Name,
                schema,
                rowBuffers,
                checkForeignKeys: false,
                primaryKeysAlreadyValidated: true,
                prevalidatedIntegerPrimaryKeys: integerKeys,
                prevalidatedPrimaryKeyValues: primaryKeyValues);
            plan = new FastInsertPlan(table, pool, rowBuffers, columnCount, integerKeys, prepared);
            committedToPlan = true;
            return true;
        }
        finally
        {
            if (!committedToPlan)
            {
                if (integerKeys is not null)
                {
                    ArrayPool<long>.Shared.Return(integerKeys);
                }

                pool.Return(rowBuffers, columnCount);
            }
        }
    }

    private bool IsReferencedByAnotherBatchTable(string parentTable)
    {
        if (_order.Count <= 1)
        {
            return false;
        }

        foreach (var child in context.Engine.Catalog.GetChildForeignKeys(parentTable, databaseName))
        {
            if (!child.ChildTable.Equals(parentTable, StringComparison.OrdinalIgnoreCase)
                && _tables.ContainsKey(child.ChildTable))
            {
                return true;
            }
        }

        return false;
    }

    private bool FillTypedRowBuffers(
        LayoutPlan layout,
        CellType[] expectedTypes,
        List<int> entryIndices,
        List<CellValue[]> rowBuffers)
    {
        int[] layoutToCatalog = layout.LayoutToCatalog;
        bool isCatalogOrder = layout.IsCatalogOrder;
        int columnCount = expectedTypes.Length;

        for (int r = 0; r < entryIndices.Count; r++)
        {
            ReadOnlySpan<CellValue> source = _batch!.TypedValuesOf(_batch[entryIndices[r]]);
            CellValue[] dest = rowBuffers[r];
            for (int i = 0; i < columnCount; i++)
            {
                int catalogOrdinal = isCatalogOrder ? i : layoutToCatalog[i];
                CellValue cell = source[i];
                if (!cell.IsNull)
                {
                    CellType expected = expectedTypes[catalogOrdinal];
                    if (cell.Type != expected)
                    {
                        if (!TryCoerceTypedCell(cell, expected, out cell))
                        {
                            return false;
                        }
                    }
                }

                dest[catalogOrdinal] = cell;
            }
        }

        return true;
    }

    private static bool TryCoerceTypedCell(CellValue cell, CellType expected, out CellValue coerced)
    {
        if (cell.Type == CellType.Int32 && expected == CellType.Int64)
        {
            coerced = CellValue.From((long)cell.AsInt32());
            return true;
        }

        if (cell.Type == CellType.Int64 && expected == CellType.Int32)
        {
            long value = cell.AsInt64();
            if (value >= int.MinValue && value <= int.MaxValue)
            {
                coerced = CellValue.From((int)value);
                return true;
            }
        }

        coerced = default;
        return false;
    }

    private bool TryCheckDisjointPrimaryKeys(
        TablePlan table,
        ReactiveRowSchema schema,
        List<CellValue[]> rows,
        out long[]? integerKeys,
        out string[]? stringKeys)
    {
        integerKeys = null;
        stringKeys = null;
        bool isEmpty = context.Engine.StorageContext.IsTableEmpty(table.Name, databaseName);
        string pkIndex = $"_PK_{table.Name}";

        if (table.SingleIntegerPrimaryKeyCatalogOrdinal >= 0)
        {
            int pkOrdinal = table.SingleIntegerPrimaryKeyCatalogOrdinal;
            long[] keys = ArrayPool<long>.Shared.Rent(rows.Count);
            HashSet<long> seen = _fastIntKeyScratch ??= [];
            seen.Clear();
            Indexing.ConcurrentInt64Int64Map? lane = isEmpty
                ? null
                : context.Engine.IndexManager.GetIntegerPrimaryKeyLane(pkIndex, table.Name, databaseName);

            for (int i = 0; i < rows.Count; i++)
            {
                CellValue cell = rows[i][pkOrdinal];
                long pk = cell.Type == CellType.Int32 ? cell.AsInt32() : cell.AsInt64();
                if (!seen.Add(pk)
                    || (!isEmpty && IntegerPrimaryKeyExists(table.Name, pkIndex, lane, pk)))
                {
                    ArrayPool<long>.Shared.Return(keys);
                    return false;
                }

                keys[i] = pk;
            }

            integerKeys = keys;
            return true;
        }

        if (table.SingleGuidPrimaryKeyCatalogOrdinal >= 0)
        {
            int pkOrdinal = table.SingleGuidPrimaryKeyCatalogOrdinal;
            HashSet<Guid> seen = _fastGuidKeyScratch ??= [];
            seen.Clear();
            bool hasGuidFastLane = !isEmpty
                && context.Engine.IndexManager.HasGuidPrimaryKeyFastLane(pkIndex, table.Name, databaseName);

            for (int i = 0; i < rows.Count; i++)
            {
                Guid pk = rows[i][pkOrdinal].AsGuid();
                if (!seen.Add(pk)
                    || (!isEmpty && GuidPrimaryKeyExists(table.Name, pkIndex, hasGuidFastLane, pk, schema, rows[i], table.PrimaryKeys)))
                {
                    return false;
                }
            }

            return true;
        }

        HashSet<string> seenStrings = _fastStringKeyScratch ??= new HashSet<string>(StringComparer.Ordinal);
        seenStrings.Clear();
        var keysArr = new string[rows.Count];
        for (int i = 0; i < rows.Count; i++)
        {
            string key = IndexKeyEncoder.BuildKeyString(schema, rows[i], table.PrimaryKeys);
            if (!seenStrings.Add(key) || (!isEmpty && GenericPrimaryKeyExists(table.Name, pkIndex, key)))
            {
                return false;
            }

            keysArr[i] = key;
        }

        stringKeys = keysArr;
        return true;
    }

    private bool IntegerPrimaryKeyExists(
        string tableName,
        string pkIndex,
        Indexing.ConcurrentInt64Int64Map? lane,
        long pk)
    {
        if (lane is not null)
        {
            return lane.TryGetValue(pk, out _);
        }

        if (context.Engine.IndexManager.PersistedIntegerPrimaryKeyExists(pk, pkIndex, tableName, databaseName) is bool persisted)
        {
            return persisted;
        }

        if (context.Engine.IndexManager.TryLookupIntegerPrimaryKey(pk, pkIndex, tableName, databaseName, out _))
        {
            return true;
        }

        if (context.Engine.IndexManager.HasIntegerPrimaryKeyFastLane(pkIndex, tableName, databaseName))
        {
            return false;
        }

        return GenericPrimaryKeyExists(tableName, pkIndex, $"[{pk}]");
    }

    private bool GuidPrimaryKeyExists(
        string tableName,
        string pkIndex,
        bool hasGuidFastLane,
        Guid pk,
        ReactiveRowSchema schema,
        CellValue[] row,
        IReadOnlyList<string> primaryKeys)
    {
        if (context.Engine.IndexManager.TryLookupGuidPrimaryKey(pk, pkIndex, tableName, databaseName, out _))
        {
            return true;
        }

        if (hasGuidFastLane || context.Engine.Config.StorageMode == StorageEngine.Config.StorageMode.Lsm)
        {
            return false;
        }

        string key = IndexKeyEncoder.BuildKeyString(schema, row, primaryKeys);
        return GenericPrimaryKeyExists(tableName, pkIndex, key);
    }

    private bool GenericPrimaryKeyExists(string tableName, string pkIndex, string key)
    {
        try
        {
            return context.Engine.IndexManager.IndexContainsKey(key, pkIndex, tableName, databaseName);
        }
        catch (IndexException exception)
        {
            throw new InvalidOperationException($"Table {tableName} has no primary-key index to apply a batch against.", exception);
        }
    }

    private DataVoBatchResult Summarize(int deleted)
    {
        int inserted = 0, updated = 0;
        foreach (TablePlan table in _order)
        {
            inserted += table.FastPathEligible ? table.FastInsertedCount : table.Inserts.Count;
            updated += table.UpdateIds.Count;
        }

        return new DataVoBatchResult(inserted, updated, deleted, _unchanged, _missing);
    }

    private static bool SameRow(Dictionary<string, object?> stored, Dictionary<string, object?> updated)
    {
        foreach (KeyValuePair<string, object?> column in updated)
        {
            if (!stored.TryGetValue(column.Key, out object? before) || !SameValue(before, Materialize(column.Value)))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Compares a stored value with a new one. FLOAT columns store single precision while values arrive as
    /// doubles, so mixed numbers are compared at the stored precision.
    /// </summary>
    private static bool SameValue(object? stored, object? value) => (stored, value) switch
    {
        (float[] a, float[] b) => a.AsSpan().SequenceEqual(b),
        (float a, double b) => a.Equals((float)b),
        (double a, float b) => ((float)a).Equals(b),
        (int a, long b) => a == b,
        (long a, int b) => a == b,
        _ => Equals(stored, value),
    };

    private static object? Materialize(object? value) =>
        value is DataVoBoundValue bound ? bound.ToMaterializedValue() : value;

    /// <summary>A layout resolved against its table: canonical column metadata and where the key values sit.</summary>
    private sealed class LayoutPlan
    {
        public LayoutPlan(DataVoBatchLayout layout, TablePlan table)
        {
            Table = table;
            Columns = new Column[layout.Columns.Count];
            LayoutToCatalog = new int[layout.Columns.Count];
            bool catalogOrder = layout.Columns.Count == table.AllColumns.Count;
            for (int i = 0; i < Columns.Length; i++)
            {
                Column column = table.ColumnNamed(layout.Columns[i]);
                Columns[i] = column;
                int catalogOrdinal = table.CatalogOrdinalOf(column.Name);
                LayoutToCatalog[i] = catalogOrdinal;
                if (catalogOrdinal != i)
                {
                    catalogOrder = false;
                }
            }

            IsCatalogOrder = catalogOrder;
            IsFullTypedLayout = Columns.Length == table.AllColumns.Count && table.ExpectedCellTypes is not null;

            KeyColumns = new Column[table.PrimaryKeys.Count];
            KeyPositions = new int[table.PrimaryKeys.Count];
            for (int k = 0; k < KeyPositions.Length; k++)
            {
                KeyPositions[k] = Array.FindIndex(Columns, column => column.Name.Equals(table.PrimaryKeys[k], StringComparison.OrdinalIgnoreCase));
                if (KeyPositions[k] < 0)
                {
                    throw new ArgumentException($"A batch layout for {table.Name} must include primary key column {table.PrimaryKeys[k]}.");
                }

                KeyColumns[k] = Columns[KeyPositions[k]];
            }
        }

        public TablePlan Table { get; }

        public Column[] Columns { get; }

        public Column[] KeyColumns { get; }

        public int[] KeyPositions { get; }

        public int[] LayoutToCatalog { get; }

        public bool IsCatalogOrder { get; }

        public bool IsFullTypedLayout { get; }
    }

    /// <summary>One table's pending rows and, once classified, the rows to delete, update and insert.</summary>
    private sealed class TablePlan
    {
        private readonly Dictionary<string, Column> _columns;
        private readonly Dictionary<string, int> _catalogOrdinals;
        private readonly Dictionary<string, PendingRow> _pending = new(StringComparer.Ordinal);
        private ReactiveRowSchema? _reactiveSchema;

        public TablePlan(string name, List<Column> columns, List<string> primaryKeys)
        {
            if (primaryKeys.Count == 0)
            {
                throw new InvalidOperationException($"Table {name} needs a primary key to be written by a batch.");
            }

            Name = name;
            AllColumns = columns;
            _columns = new Dictionary<string, Column>(columns.Count, StringComparer.OrdinalIgnoreCase);
            _catalogOrdinals = new Dictionary<string, int>(columns.Count, StringComparer.OrdinalIgnoreCase);
            CellType[]? expected = new CellType[columns.Count];
            bool allTyped = true;
            for (int i = 0; i < columns.Count; i++)
            {
                _columns[columns[i].Name] = columns[i];
                _catalogOrdinals[columns[i].Name] = i;
                if (!ColumnCellTypes.TryGet(columns[i].Type, out expected[i]))
                {
                    allTyped = false;
                }
            }

            ExpectedCellTypes = allTyped ? expected : null;
            PrimaryKeys = primaryKeys;
            SingleIntegerPrimaryKeyCatalogOrdinal = -1;
            SingleGuidPrimaryKeyCatalogOrdinal = -1;
            if (allTyped && primaryKeys.Count == 1 && _catalogOrdinals.TryGetValue(primaryKeys[0], out int pkOrd))
            {
                if (expected[pkOrd] is CellType.Int32 or CellType.Int64)
                {
                    SingleIntegerPrimaryKeyCatalogOrdinal = pkOrd;
                }
                else if (expected[pkOrd] == CellType.Guid)
                {
                    SingleGuidPrimaryKeyCatalogOrdinal = pkOrd;
                }
            }
        }

        public string Name { get; }

        public List<Column> AllColumns { get; }

        public List<string> PrimaryKeys { get; }

        public CellType[]? ExpectedCellTypes { get; }

        public int SingleIntegerPrimaryKeyCatalogOrdinal { get; }

        public int SingleGuidPrimaryKeyCatalogOrdinal { get; }

        public bool FastPathEligible { get; set; } = true;

        public LayoutPlan? FastLayout { get; set; }

        public List<int>? FastEntries { get; set; }

        public int FastInsertedCount { get; set; }

        public int CatalogOrdinalOf(string name) => _catalogOrdinals[name];

        public ReactiveRowSchema GetReactiveSchema()
        {
            if (_reactiveSchema is null)
            {
                var names = new string[AllColumns.Count];
                for (int i = 0; i < names.Length; i++)
                {
                    names[i] = AllColumns[i].Name;
                }

                _reactiveSchema = new ReactiveRowSchema(names);
            }

            return _reactiveSchema;
        }

        public List<PendingRow> Rows { get; } = [];

        public List<RowKey> DeleteIds { get; } = [];

        /// <summary>Gets the stored values of the rows the batch deletes, by row id.</summary>
        public Dictionary<RowKey, Dictionary<string, object?>> DeleteRows { get; } = [];

        public List<RowKey> UpdateIds { get; } = [];

        public Dictionary<RowKey, Dictionary<string, object?>> UpdateOldRows { get; } = [];

        public List<Dictionary<string, object?>> UpdateNewRows { get; } = [];

        public List<Dictionary<string, object?>> Inserts { get; } = [];

        /// <summary>Drops the updates of rows that end up deleted (a cascade removes them).</summary>
        public void DropUpdatesOf(IReadOnlyCollection<RowKey> deleted)
        {
            if (deleted.Count == 0)
            {
                return;
            }

            for (int i = UpdateIds.Count - 1; i >= 0; i--)
            {
                if (deleted.Contains(UpdateIds[i]))
                {
                    UpdateOldRows.Remove(UpdateIds[i]);
                    UpdateIds.RemoveAt(i);
                    UpdateNewRows.RemoveAt(i);
                }
            }
        }

        public Column ColumnNamed(string name) =>
            _columns.TryGetValue(name, out Column? column)
                ? column
                : throw new ArgumentException($"Column {name} doesn't exist in table {Name}.");

        /// <summary>Gets the pending change for a key, creating it the first time the key appears.</summary>
        public PendingRow Pending(string key, Dictionary<string, object?> keyValues)
        {
            if (!_pending.TryGetValue(key, out PendingRow? row))
            {
                row = new PendingRow(key, keyValues);
                _pending[key] = row;
                Rows.Add(row);
            }

            return row;
        }

        /// <summary>Builds the row an upsert after a delete starts from: column defaults plus the key.</summary>
        public Dictionary<string, object?> DefaultRow(Dictionary<string, object?> keyValues, Func<Column, object?, object?> normalize)
        {
            var row = new Dictionary<string, object?>(AllColumns.Count, StringComparer.OrdinalIgnoreCase);
            foreach (Column column in AllColumns)
            {
                row[column.Name] = keyValues.TryGetValue(column.Name, out object? key)
                    ? key
                    : column.DefaultValue is null ? NullFor(column) : normalize(column, column.DefaultValue);
            }

            return row;
        }

        private static object? NullFor(Column column) =>
            column.IsNotNull ? throw ColumnNullRule.Violation(column.Name) : null;
    }

    /// <summary>The folded effect of every batch operation on one row.</summary>
    private sealed class PendingRow(string key, Dictionary<string, object?> keyValues)
    {
        public string Key { get; } = key;

        public Dictionary<string, object?> KeyValues { get; } = keyValues;

        /// <summary>Gets the columns to set, or <see langword="null"/> when the row is to be deleted.</summary>
        public Dictionary<string, object?>? Values { get; private set; }

        /// <summary>Gets whether a delete came before the upserts, so the row restarts from its defaults.</summary>
        public bool Reset { get; private set; }

        /// <summary>Gets whether only updates set the row, so a missing row must not be created.</summary>
        public bool UpdateOnly { get; private set; }

        public bool IsDelete => Values is null;

        public void MarkDeleted()
        {
            Values = null;
            Reset = false;
            UpdateOnly = false;
            _deleted = true;
        }

        public void Set(LayoutPlan layout, ReadOnlySpan<object?> values, Func<Column, object?, object?> normalize, bool updateOnly)
        {
            if (Values is null)
            {
                if (_deleted && updateOnly)
                {
                    return; // an update after a delete finds no row
                }

                Reset = _deleted;
                UpdateOnly = updateOnly;
                Values = new Dictionary<string, object?>(layout.Columns.Length, StringComparer.OrdinalIgnoreCase);
            }
            else
            {
                UpdateOnly &= updateOnly; // an upsert anywhere in the fold may create the row
            }

            for (int i = 0; i < values.Length; i++)
            {
                Column column = layout.Columns[i];
                Values[column.Name] = normalize(column, values[i]);
            }
        }

        private bool _deleted;
    }

    /// <summary>
    /// Thread-local pool of exact-width <see cref="CellValue"/> row arrays reused across typed batch applies so
    /// multi-page bulk ingests avoid allocating a fresh array per row.
    /// </summary>
    private sealed class TypedRowBufferPool
    {
        private readonly List<CellValue[]> _rows = [];
        private readonly List<CellValue[]> _active = [];
        private int _columnCount;
        private bool _inUse;

        public List<CellValue[]> Rent(int rowCount, int columnCount)
        {
            if (_inUse)
            {
                // Multi-table fast path in one batch: allocate an independent list for the secondary table.
                var extra = new List<CellValue[]>(rowCount);
                for (int i = 0; i < rowCount; i++)
                {
                    extra.Add(new CellValue[columnCount]);
                }

                return extra;
            }

            _inUse = true;
            if (_columnCount != columnCount)
            {
                _rows.Clear();
                _columnCount = columnCount;
            }

            while (_rows.Count < rowCount)
            {
                _rows.Add(new CellValue[columnCount]);
            }

            _active.Clear();
            if (_active.Capacity < rowCount)
            {
                _active.Capacity = rowCount;
            }

            for (int i = 0; i < rowCount; i++)
            {
                _active.Add(_rows[i]);
            }

            return _active;
        }

        public void Return(List<CellValue[]> rows, int columnCount)
        {
            for (int i = 0; i < rows.Count; i++)
            {
                Array.Clear(rows[i], 0, columnCount);
            }

            if (ReferenceEquals(rows, _active))
            {
                _active.Clear();
                _inUse = false;
            }
        }
    }

    private sealed class FastInsertPlan(
        TablePlan table,
        TypedRowBufferPool pool,
        List<CellValue[]> rowBuffers,
        int columnCount,
        long[]? integerKeys,
        InsertRowService.PreparedTypedInsert prepared) : IDisposable
    {
        public TablePlan Table { get; } = table;

        public InsertRowService.PreparedTypedInsert Prepared { get; } = prepared;

        public void Dispose()
        {
            Prepared.Dispose();
            if (integerKeys is not null)
            {
                ArrayPool<long>.Shared.Return(integerKeys);
            }

            pool.Return(rowBuffers, columnCount);
        }
    }

    /// <summary>
    /// Compares <see cref="DataVoBatchLayout"/> instances by reference identity across all target frameworks,
    /// including <c>netstandard2.1</c> where <c>ReferenceEqualityComparer</c> is unavailable.
    /// </summary>
    private sealed class LayoutReferenceComparer : IEqualityComparer<DataVoBatchLayout>
    {
        public static readonly LayoutReferenceComparer Instance = new();

        public bool Equals(DataVoBatchLayout? x, DataVoBatchLayout? y) => ReferenceEquals(x, y);

        public int GetHashCode(DataVoBatchLayout obj) => RuntimeHelpers.GetHashCode(obj);
    }
}
