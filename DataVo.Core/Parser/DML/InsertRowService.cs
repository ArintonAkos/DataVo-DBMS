using System.Globalization;
using DataVo.Core.BTree;
using DataVo.Core.Compat;
using DataVo.Core.Exceptions;
using DataVo.Core.Models.Catalog;
using DataVo.Core.MVCC;
using DataVo.Core.Runtime;
using DataVo.Core.Runtime.Changes;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine;
using DataVo.Core.Transactions;
using DataVo.Core.Utils;
using DataVo.Core.ValueConversion;
using PolyIndexManager = DataVo.Core.Indexing.IndexManager;

namespace DataVo.Core.Parser.DML;

internal sealed partial class InsertRowService(
    DataVoEngine engine,
    StorageContext context,
    EngineCatalog catalog,
    PolyIndexManager indexes)
{
    /// <summary>
    /// Scratch set for batch-local integer primary-key duplicate detection. Thread-static because a
    /// service instance can be shared by sessions on different threads; cleared before every use.
    /// </summary>
    [ThreadStatic]
    private static HashSet<long>? _integerPrimaryKeyScratch;

    /// <summary>
    /// Re-checks rows that a transaction buffered earlier, right before COMMIT applies them. Other
    /// sessions may have written conflicting keys since the rows were validated at INSERT time; without
    /// this check COMMIT could store a duplicate primary key. Throws <see cref="ConstraintViolationException"/>.
    /// </summary>
    /// <param name="checkForeignKeys">
    /// False checks only keys and column rules, for validation before any table of the transaction is
    /// applied: a buffered child row may reference a parent row inserted earlier in the same transaction.
    /// </param>
    private static readonly IReadOnlyDictionary<string, ForeignKey> NoForeignKeys = new Dictionary<string, ForeignKey>();

    /// <summary>
    /// What a transaction's buffered writes do to a table's keys, which storage does not show until COMMIT:
    /// keys it frees (rows it deletes, old keys of rows it updates) and keys it takes (rows it inserts, new
    /// keys of rows it updates). Key checks inside the transaction, and at COMMIT before anything is
    /// applied, read storage through this view.
    /// </summary>
    internal sealed class PendingKeys
    {
        public HashSet<string> FreedPrimaryKeys { get; } = new(StringComparer.Ordinal);
        public HashSet<string> TakenPrimaryKeys { get; } = new(StringComparer.Ordinal);
        public Dictionary<string, HashSet<string>> FreedUniqueValues { get; } = new(StringComparer.OrdinalIgnoreCase);
        public Dictionary<string, HashSet<string>> TakenUniqueValues { get; } = new(StringComparer.OrdinalIgnoreCase);

        public bool IsEmpty => FreedPrimaryKeys.Count == 0 && TakenPrimaryKeys.Count == 0
            && FreedUniqueValues.Count == 0 && TakenUniqueValues.Count == 0;

        public static bool Has(Dictionary<string, HashSet<string>> byColumn, string column, string value) =>
            byColumn.TryGetValue(column, out HashSet<string>? values) && values.Contains(value);

        public static void Add(Dictionary<string, HashSet<string>> byColumn, string column, string value)
        {
            if (!byColumn.TryGetValue(column, out HashSet<string>? values))
            {
                values = new HashSet<string>(StringComparer.Ordinal);
                byColumn[column] = values;
            }

            values.Add(value);
        }
    }

    /// <summary>Builds the <see cref="PendingKeys"/> of <paramref name="transaction"/> for one table.</summary>
    /// <param name="includeInserts">Count the buffered inserts as taken keys (statement time); at COMMIT the
    /// buffered rows are the ones being validated, so they are left out.</param>
    internal PendingKeys? BuildPendingKeys(TransactionContext transaction, string databaseName, string tableName, bool includeInserts)
    {
        TableValidationMetadata metadata = catalog.GetTableValidationMetadata(tableName, databaseName);
        IReadOnlyList<string> primaryKeys = metadata.PrimaryKeys;
        IReadOnlyList<string> uniqueKeys = metadata.UniqueKeys;
        if (primaryKeys.Count == 0 && uniqueKeys.Count == 0)
        {
            return null;
        }

        var pending = new PendingKeys();
        transaction.DeletedRowIds.TryGetValue(tableName, out HashSet<RowKey>? deleted);
        transaction.UpdatedRows.TryGetValue(tableName, out List<(RowKey RowId, Dictionary<string, object?> UpdatedColumns)>? updated);

        var touched = new List<RowKey>();
        if (deleted is not null) touched.AddRange(deleted);
        if (updated is not null) touched.AddRange(updated.Select(entry => entry.RowId));
        Dictionary<RowKey, Dictionary<string, object?>> oldRows = touched.Count > 0
            ? context.GetTableContents(touched.Distinct().ToList(), tableName, databaseName)
            : [];

        void Free(Dictionary<string, object?> row)
        {
            if (primaryKeys.Count > 0 && primaryKeys.All(key => row.TryGetValue(key, out object? value) && value is not null))
            {
                pending.FreedPrimaryKeys.Add(IndexKeyEncoder.BuildKeyString(row, primaryKeys));
            }

            foreach (string column in uniqueKeys)
            {
                if (row.TryGetValue(column, out object? value) && value is not null)
                {
                    PendingKeys.Add(pending.FreedUniqueValues, column, IndexKeyEncoder.BuildKeyString(row, [column]));
                }
            }
        }

        void Take(Dictionary<string, object?> row)
        {
            if (primaryKeys.Count > 0 && primaryKeys.All(key => row.TryGetValue(key, out object? value) && value is not null))
            {
                string key = IndexKeyEncoder.BuildKeyString(row, primaryKeys);
                pending.TakenPrimaryKeys.Add(key);
                pending.FreedPrimaryKeys.Remove(key);
            }

            foreach (string column in uniqueKeys)
            {
                if (row.TryGetValue(column, out object? value) && value is not null)
                {
                    string unique = IndexKeyEncoder.BuildKeyString(row, [column]);
                    PendingKeys.Add(pending.TakenUniqueValues, column, unique);
                    if (pending.FreedUniqueValues.TryGetValue(column, out HashSet<string>? freed))
                    {
                        freed.Remove(unique);
                    }
                }
            }
        }

        // Updates first, then deletes, then inserts: the order COMMIT applies them in.
        var current = new Dictionary<RowKey, Dictionary<string, object?>>();
        if (updated is not null)
        {
            foreach ((RowKey rowId, Dictionary<string, object?> columns) in updated)
            {
                if (deleted?.Contains(rowId) == true)
                {
                    continue; // deleted later in the transaction: its update never applies
                }

                if (!current.TryGetValue(rowId, out Dictionary<string, object?>? row))
                {
                    if (!oldRows.TryGetValue(rowId, out Dictionary<string, object?>? original))
                    {
                        continue;
                    }

                    row = new Dictionary<string, object?>(original, StringComparer.OrdinalIgnoreCase);
                    current[rowId] = row;
                    Free(row);
                }

                foreach ((string column, object? value) in columns)
                {
                    row[column] = value;
                }
            }

            foreach (Dictionary<string, object?> row in current.Values)
            {
                Take(row);
            }
        }

        if (deleted is not null)
        {
            foreach (RowKey rowId in deleted)
            {
                if (oldRows.TryGetValue(rowId, out Dictionary<string, object?>? row))
                {
                    Free(row);
                }
            }
        }

        if (includeInserts && transaction.InsertedRows.TryGetValue(tableName, out List<Dictionary<string, object?>>? inserted))
        {
            foreach (Dictionary<string, object?> row in inserted)
            {
                Take(row);
            }
        }

        return pending.IsEmpty ? null : pending;
    }

    internal void ValidatePendingInserts(
        string databaseName,
        string tableName,
        IReadOnlyList<Dictionary<string, object?>> rows,
        bool checkForeignKeys = true,
        TransactionContext? owner = null)
    {
        if (rows.Count == 0)
        {
            return;
        }

        PendingKeys? pending = owner is null ? null : BuildPendingKeys(owner, databaseName, tableName, includeInserts: false);

        TableValidationMetadata metadata = catalog.GetTableValidationMetadata(tableName, databaseName);
        var messages = new List<string>();
        var acceptedPrimaryKeys = new HashSet<string>(StringComparer.Ordinal);
        var acceptedUniqueValues = metadata.UniqueKeys.ToDictionary(
            key => key,
            _ => new HashSet<string>(StringComparer.Ordinal),
            StringComparer.OrdinalIgnoreCase);

        for (int i = 0; i < rows.Count; i++)
        {
            TryNormalizeRow(
                tableName,
                databaseName,
                i + 1,
                rows[i],
                metadata.Columns,
                metadata.PrimaryKeys,
                metadata.UniqueKeys,
                checkForeignKeys ? metadata.ForeignKeysByAttribute : NoForeignKeys,
                acceptedPrimaryKeys,
                acceptedUniqueValues,
                messages,
                out _,
                pending);
        }

        if (messages.Count > 0)
        {
            throw RejectedRows(tableName, messages);
        }
    }

    public InsertRowsResult InsertRows(
        string databaseName,
        string tableName,
        IReadOnlyList<IReadOnlyDictionary<string, object?>> rows,
        TransactionContext? txContext,
        long statementTxId,
        ChangeRecorder? recorder = null)
    {
        if (string.IsNullOrWhiteSpace(tableName))
        {
            throw new ArgumentException("Table name is required.", nameof(tableName));
        }

        if (rows.Count == 0)
        {
            return new InsertRowsResult([], 0, []);
        }

        // Cached per (table, schema version): computed once instead of per batch (also benefits any
        // repeated single-row inserts hitting the same warm cache).
        TableValidationMetadata metadata = catalog.GetTableValidationMetadata(tableName, databaseName);
        IReadOnlyList<string> primaryKeys = metadata.PrimaryKeys;
        IReadOnlyList<string> uniqueKeys = metadata.UniqueKeys;
        IReadOnlyList<IndexFile> indexFiles = metadata.Indexes;
        IReadOnlyList<Column> tableColumns = metadata.Columns;
        HashSet<string> columnNames = metadata.ColumnNames;
        IReadOnlyDictionary<string, ForeignKey> foreignKeysByAttribute = metadata.ForeignKeysByAttribute;

        var acceptedRows = new List<Dictionary<string, object?>>(rows.Count);
        var messages = new List<string>();
        PendingKeys? pending = txContext is null ? null : BuildPendingKeys(txContext, databaseName, tableName, includeInserts: true);
        var acceptedPrimaryKeys = new HashSet<string>(StringComparer.Ordinal);
        var acceptedUniqueValues = uniqueKeys.ToDictionary(
            key => key,
            _ => new HashSet<string>(StringComparer.Ordinal),
            StringComparer.OrdinalIgnoreCase);

        for (int i = 0; i < rows.Count; i++)
        {
            int rowNumber = i + 1;
            Dictionary<string, object?> inputRow = MaterializeRow(rows[i]);
            EnsureKnownColumns(inputRow, columnNames, tableName);

            if (TryNormalizeRow(
                tableName,
                databaseName,
                rowNumber,
                inputRow,
                tableColumns,
                primaryKeys,
                uniqueKeys,
                foreignKeysByAttribute,
                acceptedPrimaryKeys,
                acceptedUniqueValues,
                messages,
                out Dictionary<string, object?> normalized,
                pending))
            {
                acceptedRows.Add(normalized);
            }
        }

        // A statement is all-or-nothing: one rejected row rejects every row, before anything is written.
        if (messages.Count > 0)
        {
            throw RejectedRows(tableName, messages);
        }

        if (acceptedRows.Count == 0)
        {
            return new InsertRowsResult([], 0, messages);
        }

        if (txContext != null)
        {
            foreach (Dictionary<string, object?> row in acceptedRows)
            {
                txContext.BufferInsert(
                    tableName,
                    row.ToDictionary(pair => pair.Key, pair => pair.Value, row.Comparer));
            }

            return new InsertRowsResult([], acceptedRows.Count, messages);
        }

        if (recorder is not null)
        {
            foreach (Dictionary<string, object?> row in acceptedRows)
            {
                recorder.AnnounceInsert(tableName, row);
            }
        }

        List<RowKey> rowIds = context.InsertIntoTable(acceptedRows, tableName, databaseName);

        for (int i = 0; i < acceptedRows.Count; i++)
        {
            RowKey rowId = rowIds[i];
            MvccCoordinator.RegisterInsertVersion(engine, databaseName, tableName, rowId, statementTxId);
            InsertIndexes(tableName, databaseName, acceptedRows[i], rowId, indexFiles);
            recorder?.RecordInsert(tableName, rowId, acceptedRows[i]);
        }

        if (ImplicitWalCommit.IsEnabled(engine))
        {
            var operations = new List<WalOperation>(acceptedRows.Count);
            foreach (Dictionary<string, object?> row in acceptedRows)
            {
                operations.Add(new WalOperation
                {
                    OperationType = WalOperationType.Insert,
                    TableName = tableName,
                    RowData = new Dictionary<string, object?>(row, row.Comparer),
                });
            }

            ImplicitWalCommit.CommitIfEnabled(engine, databaseName, statementTxId, operations);
        }

        return new InsertRowsResult(rowIds, acceptedRows.Count, messages);
    }

    public RowKey InsertTypedRow(
        string databaseName,
        string tableName,
        ReactiveRowSchema columns,
        ReadOnlySpan<CellValue> row,
        long statementTxId,
        ChangeRecorder? recorder = null)
    {
        if (string.IsNullOrWhiteSpace(tableName))
        {
            throw new ArgumentException("Table name is required.", nameof(tableName));
        }

        if (columns.ColumnCount != row.Length)
        {
            throw new ArgumentException(
                $"Typed row schema has {columns.ColumnCount} columns but row has {row.Length} cells.",
                nameof(row));
        }

        // Cached per (table, schema version): no per-row catalog re-reads or LINQ structure rebuilds.
        TableValidationMetadata metadata = catalog.GetTableValidationMetadata(tableName, databaseName);
        IReadOnlyList<Column> tableColumns = metadata.Columns;
        IReadOnlyList<string> primaryKeys = metadata.PrimaryKeys;
        IReadOnlyList<string> uniqueKeys = metadata.UniqueKeys;
        IReadOnlyList<IndexFile> indexFiles = metadata.Indexes;
        IReadOnlyDictionary<string, ForeignKey> foreignKeysByAttribute = metadata.ForeignKeysByAttribute;

        EnsureTypedSchemaMatchesCatalog(columns, tableColumns, tableName);
        CellValue[] normalizedCells = NormalizeTypedCells(tableColumns, row);

        // Constraint-free tables (no PK/UK/FK) skip the validation scaffolding entirely: no messages
        // List, no acceptedPrimaryKeys HashSet, no acceptedUniqueValues dict (Slice 5 P3, lever 6).
        bool hasConstraints = primaryKeys.Count > 0 || uniqueKeys.Count > 0 || foreignKeysByAttribute.Count > 0;
        if (hasConstraints)
        {
            var messages = new List<string>();
            var acceptedPrimaryKeys = new HashSet<string>(StringComparer.Ordinal);
            var acceptedUniqueValues = uniqueKeys.ToDictionary(
                key => key,
                _ => new HashSet<string>(StringComparer.Ordinal),
                StringComparer.OrdinalIgnoreCase);

            if (!ValidateTypedPrimaryKeys(
                tableName,
                databaseName,
                rowNumber: 1,
                normalizedCells,
                columns,
                primaryKeys,
                acceptedPrimaryKeys,
                messages)
                || !ValidateTypedUniqueKeys(
                    tableName,
                    databaseName,
                    rowNumber: 1,
                    normalizedCells,
                    columns,
                    uniqueKeys,
                    acceptedUniqueValues,
                    messages)
                || !ValidateTypedForeignKeys(
                    databaseName,
                    rowNumber: 1,
                    normalizedCells,
                    columns,
                    foreignKeysByAttribute,
                    messages))
            {
                throw RejectedRows(tableName, messages);
            }
        }

        // normalizedCells is freshly allocated by NormalizeTypedCells and is not mutated after this point;
        // later uses in this method must remain read-only after handing ownership to StoredRow.
        recorder?.AnnounceTypedInsert(tableName, TypedRow.FromOwnedCells(columns, normalizedCells));
        var storedRow = StoredRow.FromOwnedCells(columns, normalizedCells);
        RowKey rowId = context.InsertTypedRow(storedRow, tableColumns, tableName, databaseName);
        MvccCoordinator.RegisterInsertVersion(engine, databaseName, tableName, rowId, statementTxId);
        InsertTypedIndexes(tableName, databaseName, columns, normalizedCells, rowId, indexFiles);

        if (recorder is not null)
        {
            // Share the StoredRow's owned, immutable cells with the captured after-image (no clone, no
            // eager dict); the owned dict is materialized lazily only if an owned subscriber reads it
            // (Slice 5 P3, lever 4). normalizedCells is never mutated after this point.
            recorder.RecordTypedInsert(tableName, rowId, TypedRow.FromOwnedCells(columns, normalizedCells));
        }

        if (ImplicitWalCommit.IsEnabled(engine))
        {
            ImplicitWalCommit.CommitIfEnabled(
                engine,
                databaseName,
                statementTxId,
                [
                    new WalOperation
                    {
                        OperationType = WalOperationType.Insert,
                        TableName = tableName,
                        RowData = ImplicitWalCommit.MaterializeTypedRow(columns, normalizedCells),
                    }
                ]);
        }

        return rowId;
    }

    public IReadOnlyList<RowKey> InsertTypedRows(
        string databaseName,
        string tableName,
        ReactiveRowSchema columns,
        IReadOnlyList<CellValue[]> rows,
        long statementTxId,
        ChangeRecorder? recorder = null,
        bool callerOwnsRowBuffers = false)
    {
        if (string.IsNullOrWhiteSpace(tableName))
        {
            throw new ArgumentException("Table name is required.", nameof(tableName));
        }

        if (rows.Count == 0)
        {
            return [];
        }

        TableValidationMetadata metadata = catalog.GetTableValidationMetadata(tableName, databaseName);
        IReadOnlyList<Column> tableColumns = metadata.Columns;
        IReadOnlyList<string> primaryKeys = metadata.PrimaryKeys;
        IReadOnlyList<string> uniqueKeys = metadata.UniqueKeys;
        IReadOnlyList<IndexFile> indexFiles = metadata.Indexes;
        IReadOnlyDictionary<string, ForeignKey> foreignKeysByAttribute = metadata.ForeignKeysByAttribute;

        EnsureTypedSchemaMatchesCatalog(columns, tableColumns, tableName);

        // A clustered table's storage enforces its primary key (present, unique, also within the batch)
        // atomically when it stores the rows, so the key needs no checks or index entries here.
        bool clustered = context.ResolveClusteredKey(databaseName, tableName) is not null;
        if (clustered)
        {
            primaryKeys = [];
        }

        bool hasIntegerPrimaryKey = TryResolveIntegerPrimaryKeyOrdinal(primaryKeys, columns, tableColumns, out int integerPrimaryKeyOrdinal);
        bool hasConstraints = primaryKeys.Count > 0 || uniqueKeys.Count > 0 || foreignKeysByAttribute.Count > 0;
        List<string>? messages = hasConstraints ? [] : null;
        HashSet<string>? acceptedPrimaryKeys = hasConstraints && !hasIntegerPrimaryKey ? new HashSet<string>(StringComparer.Ordinal) : null;
        HashSet<long>? acceptedIntegerPrimaryKeys = null;
        if (hasIntegerPrimaryKey)
        {
            // Per-thread scratch: batch-local duplicate detection only, so reuse across calls
            // (cleared each time) instead of allocating a fresh set per batch.
            acceptedIntegerPrimaryKeys = _integerPrimaryKeyScratch ??= [];
            acceptedIntegerPrimaryKeys.Clear();
        }
        IReadOnlyDictionary<string, HashSet<string>>? acceptedUniqueValues = hasConstraints
            ? uniqueKeys.ToDictionary(
                key => key,
                _ => new HashSet<string>(StringComparer.Ordinal),
                StringComparer.OrdinalIgnoreCase)
            : null;
        bool skipExistingPrimaryKeyLookup = primaryKeys.Count > 0 && context.IsTableEmpty(tableName, databaseName);

        // Resolve the fast lane once per batch: the per-row existence probe then skips the
        // cache-key hash and dictionary hop that dominated validation CPU on bulk ingest.
        Indexing.ConcurrentInt64Int64Map? integerPrimaryKeyLane =
            hasIntegerPrimaryKey && !skipExistingPrimaryKeyLookup
                ? indexes.GetIntegerPrimaryKeyLane(GetPrimaryKeyIndexName(tableName), tableName, databaseName)
                : null;

        string[]? primaryKeyValues = primaryKeys.Count > 0 && !hasIntegerPrimaryKey ? new string[rows.Count] : null;
        long[]? integerPrimaryKeyValues = hasIntegerPrimaryKey ? new long[rows.Count] : null;
        for (int i = 0; i < rows.Count; i++)
        {
            CellValue[] row = rows[i];
            if (columns.ColumnCount != row.Length)
            {
                throw new ArgumentException(
                    $"Typed row schema has {columns.ColumnCount} columns but row {i + 1} has {row.Length} cells.",
                    nameof(rows));
            }

            ValidateTypedCells(tableColumns, row);

            if (!hasConstraints)
            {
                continue;
            }

            int rowNumber = i + 1;
            string? primaryKeyValue = null;
            bool primaryKeyValid = hasIntegerPrimaryKey
                ? ValidateTypedIntegerPrimaryKey(
                    tableName,
                    databaseName,
                    rowNumber,
                    row,
                    integerPrimaryKeyOrdinal,
                    acceptedIntegerPrimaryKeys!,
                    messages!,
                    skipExistingPrimaryKeyLookup,
                    integerPrimaryKeyLane,
                    out integerPrimaryKeyValues![i])
                : ValidateTypedPrimaryKeys(
                    tableName,
                    databaseName,
                    rowNumber,
                    row,
                    columns,
                    primaryKeys,
                    acceptedPrimaryKeys!,
                    messages!,
                    skipExistingPrimaryKeyLookup,
                    out primaryKeyValue);

            if (!primaryKeyValid
                || !ValidateTypedUniqueKeys(
                    tableName,
                    databaseName,
                    rowNumber,
                    row,
                    columns,
                    uniqueKeys,
                    acceptedUniqueValues!,
                    messages!)
                || !ValidateTypedForeignKeys(
                    databaseName,
                    rowNumber,
                    row,
                    columns,
                    foreignKeysByAttribute,
                    messages!))
            {
                throw RejectedRows(tableName, messages!);
            }

            if (primaryKeyValues is not null)
            {
                primaryKeyValues[i] = primaryKeyValue!;
            }
        }

        // Caller-owned buffers are copied only when something retains cells past this call: in-memory
        // typed storage keeps the StoredRow itself, and a change recorder captures the after-image.
        // Serialize-through backends (LSM/disk) read the cells transiently, so no copy is needed.
        IReadOnlyList<CellValue[]> ownedRows = rows;
        if (callerOwnsRowBuffers && (context.RetainsTypedRowBuffers || recorder is not null))
        {
            var copies = new CellValue[rows.Count][];
            for (int i = 0; i < rows.Count; i++)
            {
                copies[i] = [.. rows[i]];
            }

            ownedRows = copies;
        }

        if (recorder is { WantsAnnouncements: true })
        {
            for (int i = 0; i < ownedRows.Count; i++)
            {
                recorder.AnnounceTypedInsert(tableName, TypedRow.FromOwnedCells(columns, ownedRows[i]));
            }
        }

        IReadOnlyList<RowKey> rowIds;
        if (context.RetainsTypedRowBuffers)
        {
            var storedRows = new List<StoredRow>(rows.Count);
            for (int i = 0; i < rows.Count; i++)
            {
                storedRows.Add(StoredRow.FromOwnedCells(columns, ownedRows[i]));
            }

            rowIds = context.InsertTypedRows(storedRows, tableColumns, tableName, databaseName);
        }
        else
        {
            // Serialize-through backends (LSM/disk) read the cells transiently, so skip the
            // per-row StoredRow wrappers entirely and serialize straight from the cell buffers.
            rowIds = context.InsertTypedCellRows(tableColumns, ownedRows, tableName, databaseName);
        }
        MvccCoordinator.RegisterInsertVersions(engine, databaseName, tableName, rowIds, statementTxId);
        InsertTypedIndexesBatch(
            tableName,
            databaseName,
            columns,
            rows,
            rowIds,
            indexFiles,
            primaryKeys,
            primaryKeyValues,
            integerPrimaryKeyValues);

        if (recorder is not null)
        {
            for (int i = 0; i < rowIds.Count; i++)
            {
                recorder.RecordTypedInsert(tableName, rowIds[i], TypedRow.FromOwnedCells(columns, ownedRows[i]));
            }
        }

        if (ImplicitWalCommit.IsEnabled(engine))
        {
            var operations = new List<WalOperation>(rows.Count);
            for (int i = 0; i < rows.Count; i++)
            {
                operations.Add(new WalOperation
                {
                    OperationType = WalOperationType.Insert,
                    TableName = tableName,
                    RowData = ImplicitWalCommit.MaterializeTypedRow(columns, rows[i]),
                });
            }

            ImplicitWalCommit.CommitIfEnabled(engine, databaseName, statementTxId, operations);
        }

        return rowIds;
    }

    private static Dictionary<string, object?> MaterializeRow(IReadOnlyDictionary<string, object?> row)
    {
        return row is Dictionary<string, object?> dict && dict.Comparer.Equals(StringComparer.OrdinalIgnoreCase)
            ? new Dictionary<string, object?>(dict, StringComparer.OrdinalIgnoreCase)
            : new Dictionary<string, object?>(row, StringComparer.OrdinalIgnoreCase);
    }

    private static void EnsureTypedSchemaMatchesCatalog(
        ReactiveRowSchema columns,
        IReadOnlyList<Column> tableColumns,
        string tableName)
    {
        if (columns.ColumnCount != tableColumns.Count)
        {
            throw new NotSupportedException(
                $"Typed inserts for table {tableName} require the full catalog row in catalog order; use the dictionary BulkInsert path for partial rows.");
        }

        for (int i = 0; i < tableColumns.Count; i++)
        {
            if (!columns.ColumnAt(i).Equals(tableColumns[i].Name, StringComparison.OrdinalIgnoreCase))
            {
                throw new NotSupportedException(
                    $"Typed inserts for table {tableName} require catalog order. Expected column {tableColumns[i].Name} at ordinal {i}; use the dictionary BulkInsert path.");
            }
        }
    }

    private static CellValue[] NormalizeTypedCells(IReadOnlyList<Column> tableColumns, ReadOnlySpan<CellValue> row)
    {
        var normalized = new CellValue[row.Length];
        for (int i = 0; i < row.Length; i++)
        {
            ValidateTypedCell(tableColumns[i], row[i]);
            normalized[i] = row[i];
        }

        return normalized;
    }

    private static void ValidateTypedCells(IReadOnlyList<Column> tableColumns, ReadOnlySpan<CellValue> row)
    {
        for (int i = 0; i < row.Length; i++)
        {
            ValidateTypedCell(tableColumns[i], row[i]);
        }
    }

    /// <summary>
    /// A single INT primary key takes the integer fast path. Any other key type (GUID, VARCHAR) uses the
    /// general validation; treating every single-column key as an integer rejected GUID keys outright.
    /// </summary>
    private static bool TryResolveIntegerPrimaryKeyOrdinal(
        IReadOnlyList<string> primaryKeys,
        ReactiveRowSchema schema,
        IReadOnlyList<Column> tableColumns,
        out int ordinal)
    {
        ordinal = -1;
        if (primaryKeys.Count != 1 || !schema.TryGetOrdinal(primaryKeys[0], out ordinal))
        {
            return false;
        }

        foreach (Column column in tableColumns)
        {
            if (column.Name.Equals(primaryKeys[0], StringComparison.OrdinalIgnoreCase))
            {
                return ExpectedCellType(column) is CellType.Int32 or CellType.Int64;
            }
        }

        return false;
    }

    internal static void ValidateTypedCell(Column column, CellValue cell)
    {
        CellType expected = ExpectedCellType(column);

        if (cell.IsNull)
        {
            if (column.IsNotNull)
            {
                throw ColumnNullRule.Violation(column.Name);
            }

            return;
        }

        if (cell.Type != expected)
        {
            throw new NotSupportedException(
                $"Typed insert cell for column {column.Name} must be {expected} for {column.Type}; use the dictionary BulkInsert path.");
        }

        if (expected == CellType.String && ColumnLengthRule.Exceeds(column, cell.AsString(), out int length))
        {
            throw new ConstraintViolationException(ColumnLengthRule.Message(column.Name, column.Length, length))
            {
                ErrorCode = Contracts.Results.DataVoErrorCodes.ValueTooLong,
            };
        }

        if (expected == CellType.Vector
            && column.Length > 0
            && cell.VectorLength != column.Length)
        {
            throw new NotSupportedException(
                $"Typed insert cell for column {column.Name} must have VECTOR({column.Length}) dimensions; use the dictionary BulkInsert path.");
        }
    }

    private static CellType ExpectedCellType(Column column) =>
        ColumnCellTypes.TryGet(column.Type, out CellType type)
            ? type
            : throw new NotSupportedException(
                $"Typed inserts do not support column {column.Name} of type {column.Type}; use the dictionary BulkInsert path.");

    private bool ValidateTypedPrimaryKeys(
        string tableName,
        string databaseName,
        int rowNumber,
        ReadOnlySpan<CellValue> cells,
        ReactiveRowSchema schema,
        IReadOnlyList<string> primaryKeys,
        HashSet<string> acceptedPrimaryKeys,
        List<string> messages)
    {
        return ValidateTypedPrimaryKeys(
            tableName,
            databaseName,
            rowNumber,
            cells,
            schema,
            primaryKeys,
            acceptedPrimaryKeys,
            messages,
            out _);
    }

    private bool ValidateTypedPrimaryKeys(
        string tableName,
        string databaseName,
        int rowNumber,
        ReadOnlySpan<CellValue> cells,
        ReactiveRowSchema schema,
        IReadOnlyList<string> primaryKeys,
        HashSet<string> acceptedPrimaryKeys,
        List<string> messages,
        out string? primaryKeyValue)
    {
        return ValidateTypedPrimaryKeys(
            tableName,
            databaseName,
            rowNumber,
            cells,
            schema,
            primaryKeys,
            acceptedPrimaryKeys,
            messages,
            skipExistingPrimaryKeyLookup: false,
            out primaryKeyValue);
    }

    private bool ValidateTypedPrimaryKeys(
        string tableName,
        string databaseName,
        int rowNumber,
        ReadOnlySpan<CellValue> cells,
        ReactiveRowSchema schema,
        IReadOnlyList<string> primaryKeys,
        HashSet<string> acceptedPrimaryKeys,
        List<string> messages,
        bool skipExistingPrimaryKeyLookup,
        out string? primaryKeyValue)
    {
        primaryKeyValue = null;
        if (primaryKeys.Count == 0)
        {
            return true;
        }

        foreach (string primaryKey in primaryKeys)
        {
            if (!GetTypedCell(cells, schema, primaryKey).IsNull)
            {
                continue;
            }

            messages.Add($"Primary key cannot be null in row {rowNumber}!");
            return false;
        }

        primaryKeyValue = IndexKeyEncoder.BuildKeyString(schema, cells, primaryKeys);
        if (acceptedPrimaryKeys.Contains(primaryKeyValue)
            || (!skipExistingPrimaryKeyLookup && PrimaryKeyExists(tableName, databaseName, primaryKeyValue, primaryKeys)))
        {
            messages.Add($"Primary key violation in row {rowNumber}!");
            return false;
        }

        acceptedPrimaryKeys.Add(primaryKeyValue);
        return true;
    }

    private bool ValidateTypedIntegerPrimaryKey(
        string tableName,
        string databaseName,
        int rowNumber,
        ReadOnlySpan<CellValue> cells,
        int primaryKeyOrdinal,
        HashSet<long> acceptedPrimaryKeys,
        List<string> messages,
        bool skipExistingPrimaryKeyLookup,
        Indexing.ConcurrentInt64Int64Map? existingKeyLane,
        out long primaryKeyValue)
    {
        CellValue cell = cells[primaryKeyOrdinal];
        primaryKeyValue = 0;

        if (cell.IsNull)
        {
            messages.Add($"Primary key cannot be null in row {rowNumber}!");
            return false;
        }

        primaryKeyValue = cell.Type switch
        {
            CellType.Int32 => cell.AsInt32(),
            CellType.Int64 => cell.AsInt64(),
            _ => throw new NotSupportedException(
                $"Typed integer primary key for row {rowNumber} must be INT or BIGINT; use the dictionary BulkInsert path.")
        };

        // A populated fast lane is authoritative for existence, so the resolved lane replaces the
        // per-row cache-key lookup; the generic probe remains the fallback for lane-less indexes.
        bool existing = !skipExistingPrimaryKeyLookup
            && (existingKeyLane is not null
                ? existingKeyLane.TryGetValue(primaryKeyValue, out _)
                : IntegerPrimaryKeyExists(tableName, databaseName, primaryKeyValue));
        if (acceptedPrimaryKeys.Contains(primaryKeyValue) || existing)
        {
            messages.Add($"Primary key violation in row {rowNumber}!");
            return false;
        }

        acceptedPrimaryKeys.Add(primaryKeyValue);
        return true;
    }

    private bool ValidateTypedUniqueKeys(
        string tableName,
        string databaseName,
        int rowNumber,
        ReadOnlySpan<CellValue> cells,
        ReactiveRowSchema schema,
        IReadOnlyList<string> uniqueKeys,
        IReadOnlyDictionary<string, HashSet<string>> acceptedUniqueValues,
        List<string> messages)
    {
        foreach (string uniqueKey in uniqueKeys)
        {
            if (GetTypedCell(cells, schema, uniqueKey).IsNull)
            {
                continue;
            }

            string uniqueValue = IndexKeyEncoder.BuildKeyString(schema, cells, [uniqueKey]);
            HashSet<string> acceptedValues = acceptedUniqueValues[uniqueKey];

            if (acceptedValues.Contains(uniqueValue)
                || UniqueValueExists(tableName, databaseName, uniqueKey, uniqueValue))
            {
                messages.Add($"Unique key violation in row {rowNumber}!");
                return false;
            }

            acceptedValues.Add(uniqueValue);
        }

        return true;
    }

    private bool ValidateTypedForeignKeys(
        string databaseName,
        int rowNumber,
        ReadOnlySpan<CellValue> cells,
        ReactiveRowSchema schema,
        IReadOnlyDictionary<string, ForeignKey> foreignKeysByAttribute,
        List<string> messages)
    {
        foreach ((string columnName, ForeignKey foreignKey) in foreignKeysByAttribute)
        {
            string candidate = IndexKeyEncoder.BuildKeyString(schema, cells, [columnName]);
            if (!ReferenceExists(foreignKey, candidate, databaseName))
            {
                messages.Add($"Foreign key violation in row {rowNumber}!");
                return false;
            }
        }

        return true;
    }

    private static CellValue GetTypedCell(ReadOnlySpan<CellValue> cells, ReactiveRowSchema schema, string columnName)
    {
        if (!schema.TryGetOrdinal(columnName, out int ordinal))
        {
            throw new BindingException($"Column {columnName} doesn't exist in typed row schema!");
        }

        return cells[ordinal];
    }

    private static void EnsureKnownColumns(
        IReadOnlyDictionary<string, object?> inputRow,
        HashSet<string> columnNames,
        string tableName)
    {
        foreach (string columnName in inputRow.Keys)
        {
            if (!columnNames.Contains(columnName))
            {
                throw new BindingException($"Column {columnName} doesn't exist in table {tableName}!");
            }
        }
    }

    private bool TryNormalizeRow(
        string tableName,
        string databaseName,
        int rowNumber,
        IReadOnlyDictionary<string, object?> inputRow,
        IReadOnlyList<Column> tableColumns,
        IReadOnlyList<string> primaryKeys,
        IReadOnlyList<string> uniqueKeys,
        IReadOnlyDictionary<string, ForeignKey> foreignKeysByAttribute,
        HashSet<string> acceptedPrimaryKeys,
        IReadOnlyDictionary<string, HashSet<string>> acceptedUniqueValues,
        List<string> messages,
        out Dictionary<string, object?> normalized,
        PendingKeys? pending = null)
    {
        normalized = new Dictionary<string, object?>(tableColumns.Count, StringComparer.OrdinalIgnoreCase);

        foreach (Column tableColumn in tableColumns)
        {
            object? rawValue = inputRow.TryGetValue(tableColumn.Name, out object? suppliedValue)
                ? suppliedValue
                : tableColumn.DefaultValue;

            if (!TryNormalizeValue(tableColumn, rawValue, out object? normalizedValue))
            {
                messages.Add($"Type of argument doesn't match with column type in row {rowNumber}!");
                return false;
            }

            if (ColumnLengthRule.Exceeds(tableColumn, normalizedValue, out int length))
            {
                messages.Add(ColumnLengthRule.Message(tableColumn.Name, tableColumn.Length, length, rowNumber));
                return false;
            }

            if (tableColumn.IsNotNull && IsNullValue(normalizedValue))
            {
                messages.Add(ColumnNullRule.Message(tableColumn.Name, rowNumber));
                return false;
            }

            normalized[tableColumn.Name] = normalizedValue;
        }

        if (!ValidatePrimaryKeys(tableName, databaseName, rowNumber, normalized, primaryKeys, acceptedPrimaryKeys, messages, pending))
        {
            return false;
        }

        if (!ValidateUniqueKeys(tableName, databaseName, rowNumber, normalized, uniqueKeys, acceptedUniqueValues, messages, pending))
        {
            return false;
        }

        if (!ValidateForeignKeys(databaseName, rowNumber, normalized, foreignKeysByAttribute, messages))
        {
            return false;
        }

        return true;
    }

    /// <summary>The SQL path's value conversion, shared with the typed fast path of SQL INSERT.</summary>
    internal static bool TryNormalizeSqlValue(Column column, object? rawValue, out object? normalizedValue) =>
        TryNormalizeValue(column, rawValue, out normalizedValue);

    private static bool TryNormalizeValue(Column column, object? rawValue, out object? normalizedValue)
    {
        normalizedValue = null;

        if (rawValue == null || rawValue is DBNull)
        {
            return true;
        }

        if (rawValue is DataVoBoundValue boundValue)
        {
            ValidateTypedCell(column, boundValue.Cell);
            normalizedValue = boundValue.Cell.IsNull ? null : boundValue;
            return true;
        }

        string formattedValue = FormatColumnValue(rawValue, column.Type);
        column.Value = formattedValue;

        object? parsedValue = column.ParsedValue;
        if (!IsParsedValueCompatible(column, parsedValue))
        {
            return false;
        }

        normalizedValue = NormalizeParsedValue(column, parsedValue);
        return true;
    }

    private static string FormatColumnValue(object rawValue, string columnType)
    {
        if (rawValue is string text)
        {
            return text;
        }

        if (columnType.Equals("VECTOR", StringComparison.OrdinalIgnoreCase)
            && VectorParser.TryCoerceToVector(rawValue, out float[] vector))
        {
            return VectorParser.SerializeVector(vector);
        }

        if (rawValue is DataVoDate dataVoDate)
        {
            return dataVoDate.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        }

        if (DateObjectAdapter.TryConvert(rawValue, out DataVoDate adaptedDate))
        {
            return adaptedDate.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        }

        if (rawValue is DateTime dateTime && columnType.Equals("DATE", StringComparison.OrdinalIgnoreCase))
        {
            return DataVoDate.FromDateTime(dateTime).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        }

        if (rawValue is IFormattable formattable)
        {
            return formattable.ToString(null, CultureInfo.InvariantCulture);
        }

        return Convert.ToString(rawValue, CultureInfo.InvariantCulture) ?? "null";
    }

    private static bool IsParsedValueCompatible(Column column, object? parsedValue)
    {
        if (parsedValue == null)
        {
            return true;
        }

        return column.Type.ToUpperInvariant() switch
        {
            "INT" => parsedValue is int,
            "BIGINT" => parsedValue is long,
            "FLOAT" => parsedValue is double,
            "DOUBLE" => parsedValue is double,
            "BIT" => parsedValue is bool,
            "DATE" => parsedValue is DataVoDate,
            "GUID" => parsedValue is Guid,
            "VECTOR" => parsedValue is float[] vector && (column.Length <= 0 || vector.Length == column.Length),
            _ => true
        };
    }

    private static object? NormalizeParsedValue(Column column, object? parsedValue)
    {
        if (parsedValue is float[] vector)
        {
            return vector.ToArray();
        }

        if (column.Type.Equals("DATE", StringComparison.OrdinalIgnoreCase) && parsedValue is DateTime dateTime)
        {
            return DataVoDate.FromDateTime(dateTime);
        }

        if (column.Type.Equals("GUID", StringComparison.OrdinalIgnoreCase)
            && parsedValue is string text
            && Guid.TryParse(text, out Guid parsedGuid))
        {
            return parsedGuid;
        }

        return parsedValue;
    }

    private bool ValidatePrimaryKeys(
        string tableName,
        string databaseName,
        int rowNumber,
        Dictionary<string, object?> row,
        IReadOnlyList<string> primaryKeys,
        HashSet<string> acceptedPrimaryKeys,
        List<string> messages,
        PendingKeys? pending = null)
    {
        if (primaryKeys.Count == 0)
        {
            return true;
        }

        if (primaryKeys.Any(primaryKey => row[primaryKey] == null))
        {
            messages.Add($"Primary key cannot be null in row {rowNumber}!");
            return false;
        }

        string primaryKeyValue = IndexKeyEncoder.BuildKeyString(row, primaryKeys);
        bool taken = pending?.TakenPrimaryKeys.Contains(primaryKeyValue) == true
            || (pending?.FreedPrimaryKeys.Contains(primaryKeyValue) != true && PrimaryKeyExists(tableName, databaseName, row, primaryKeys));
        if (acceptedPrimaryKeys.Contains(primaryKeyValue) || taken)
        {
            messages.Add($"Primary key violation in row {rowNumber}!");
            return false;
        }

        acceptedPrimaryKeys.Add(primaryKeyValue);
        return true;
    }

    private bool ValidateUniqueKeys(
        string tableName,
        string databaseName,
        int rowNumber,
        Dictionary<string, object?> row,
        IReadOnlyList<string> uniqueKeys,
        IReadOnlyDictionary<string, HashSet<string>> acceptedUniqueValues,
        List<string> messages,
        PendingKeys? pending = null)
    {
        foreach (string uniqueKey in uniqueKeys)
        {
            if (row[uniqueKey] == null)
            {
                continue;
            }

            string uniqueValue = IndexKeyEncoder.BuildKeyString(row, [uniqueKey]);
            HashSet<string> acceptedValues = acceptedUniqueValues[uniqueKey];
            bool taken = (pending is not null && PendingKeys.Has(pending.TakenUniqueValues, uniqueKey, uniqueValue))
                || ((pending is null || !PendingKeys.Has(pending.FreedUniqueValues, uniqueKey, uniqueValue))
                    && UniqueValueExists(tableName, databaseName, uniqueKey, row));

            if (acceptedValues.Contains(uniqueValue) || taken)
            {
                messages.Add($"Unique key violation in row {rowNumber}!");
                return false;
            }

            acceptedValues.Add(uniqueValue);
        }

        return true;
    }

    private bool ValidateForeignKeys(
        string databaseName,
        int rowNumber,
        Dictionary<string, object?> row,
        IReadOnlyDictionary<string, ForeignKey> foreignKeysByAttribute,
        List<string> messages)
    {
        foreach ((string columnName, ForeignKey foreignKey) in foreignKeysByAttribute)
        {
            string candidate = IndexKeyEncoder.BuildKeyString(row, [columnName]);
            if (!ReferenceExists(foreignKey, candidate, databaseName))
            {
                messages.Add($"Foreign key violation in row {rowNumber}!");
                return false;
            }
        }

        return true;
    }

    private bool PrimaryKeyExists(
        string tableName,
        string databaseName,
        Dictionary<string, object?> row,
        IReadOnlyList<string> primaryKeys)
    {
        string primaryKeyValue = IndexKeyEncoder.BuildKeyString(row, primaryKeys);
        return PrimaryKeyExists(tableName, databaseName, primaryKeyValue, primaryKeys);
    }

    private bool PrimaryKeyExists(
        string tableName,
        string databaseName,
        string primaryKeyValue,
        IReadOnlyList<string> primaryKeys)
    {
        // The _PK_ index is created with the table and maintained on every insert (it also backs keyed
        // SELECTs), so it is authoritative: trust both a hit and a miss. Only when the index is genuinely
        // unavailable (IndexException) do we fall back to a full-table scan. Trusting the miss keeps keyed
        // bulk inserts at O(log n) per row instead of materializing+scanning the whole table each time.
        try
        {
            return indexes.IndexContainsKey(primaryKeyValue, $"_PK_{tableName}", tableName, databaseName);
        }
        catch (IndexException)
        {
        }

        Dictionary<RowKey, Dictionary<string, object?>> existingRows = context.GetTableContents(tableName, databaseName);
        return existingRows.Values.Any(existingRow =>
            string.Equals(IndexKeyEncoder.BuildKeyString(existingRow, primaryKeys), primaryKeyValue, StringComparison.Ordinal));
    }

    private bool IntegerPrimaryKeyExists(string tableName, string databaseName, long primaryKeyValue)
    {
        string indexName = GetPrimaryKeyIndexName(tableName);

        // A persisted index answers authoritatively in one probe, hit or miss (for a clustered table it is
        // the row lookup itself): no fast lane to consult, and no key string to build for a second probe.
        if (indexes.PersistedIntegerPrimaryKeyExists(primaryKeyValue, indexName, tableName, databaseName) is bool persisted)
        {
            return persisted;
        }

        if (indexes.TryLookupIntegerPrimaryKey(primaryKeyValue, indexName, tableName, databaseName, out _))
        {
            return true;
        }

        // A populated integer fast lane is authoritative (IndexContainsKey treats a miss the same way),
        // so a miss means the key is absent: skip the bracketed key string and generic BTree probe.
        if (indexes.HasIntegerPrimaryKeyFastLane(indexName, tableName, databaseName))
        {
            return false;
        }

        try
        {
            return indexes.IndexContainsKey($"[{primaryKeyValue}]", indexName, tableName, databaseName);
        }
        catch (IndexException)
        {
            return false;
        }
    }

    // Memoizes the "_PK_{table}" index-name string: existence checks run once per row during batch
    // validation and must not allocate a fresh name each time. Single reference write keeps the
    // (table, name) pair consistent under concurrent readers.
    private PkIndexName? _pkIndexNameCache;

    private sealed record PkIndexName(string TableName, string IndexName);

    private string GetPrimaryKeyIndexName(string tableName)
    {
        PkIndexName? cached = _pkIndexNameCache;
        if (cached is null || !string.Equals(cached.TableName, tableName, StringComparison.Ordinal))
        {
            cached = new PkIndexName(tableName, $"_PK_{tableName}");
            _pkIndexNameCache = cached;
        }

        return cached.IndexName;
    }

    private bool UniqueValueExists(
        string tableName,
        string databaseName,
        string columnName,
        Dictionary<string, object?> row)
    {
        string uniqueValue = IndexKeyEncoder.BuildKeyString(row, [columnName]);
        return UniqueValueExists(tableName, databaseName, columnName, uniqueValue);
    }

    private bool UniqueValueExists(
        string tableName,
        string databaseName,
        string columnName,
        string uniqueValue)
    {
        // The _UK_ index is created with the table and maintained on every insert, so it is authoritative:
        // trust hit and miss alike; only scan when the index is genuinely unavailable (IndexException).
        try
        {
            return indexes.IndexContainsKey(uniqueValue, $"_UK_{columnName}", tableName, databaseName);
        }
        catch (IndexException)
        {
        }

        Dictionary<RowKey, Dictionary<string, object?>> existingRows = context.GetTableContents(tableName, databaseName);
        return existingRows.Values.Any(existingRow =>
            string.Equals(IndexKeyEncoder.BuildKeyString(existingRow, [columnName]), uniqueValue, StringComparison.Ordinal));
    }

    private bool ReferenceExists(ForeignKey foreignKey, string expectedValue, string databaseName)
    {
        foreach (Reference reference in foreignKey.References)
        {
            try
            {
                if (indexes.IndexContainsKey(expectedValue, $"_PK_{reference.ReferenceTableName}", reference.ReferenceTableName, databaseName))
                {
                    continue;
                }
            }
            catch (IndexException)
            {
            }

            Dictionary<RowKey, Dictionary<string, object?>> existingRows = context.GetTableContents(reference.ReferenceTableName, databaseName);
            if (existingRows.Values.Any(existingRow =>
                existingRow.TryGetValue(reference.ReferenceAttributeName, out object? value)
                && value != null
                && string.Equals(value.ToString(), expectedValue, StringComparison.Ordinal)))
            {
                continue;
            }

            return false;
        }

        return true;
    }

    private void InsertIndexes(
        string tableName,
        string databaseName,
        Dictionary<string, object?> row,
        RowKey rowId,
        IReadOnlyList<IndexFile> indexFiles)
    {
        foreach (IndexFile index in indexFiles)
        {
            if (index.AttributeNames.Any(attribute => row[attribute] == null))
            {
                continue;
            }

            string indexName = index.IndexFileName ?? string.Empty;
            if (string.IsNullOrWhiteSpace(indexName))
            {
                continue;
            }

            string indexKind = index.IndexKind ?? string.Empty;
            if (indexes.SupportsVectorIndexType(indexKind))
            {
                if (index.AttributeNames.Count != 1)
                {
                    throw new BindingException($"Vector index '{indexName}' (type '{indexKind}') must reference exactly one VECTOR column.");
                }

                string vectorColumn = index.AttributeNames[0];
                if (row[vectorColumn] is DataVoBoundValue boundValue)
                {
                    if (boundValue.Cell.Type != CellType.Vector)
                    {
                        throw new EvaluationException($"Cannot coerce value of '{vectorColumn}' into VECTOR for index '{indexName}'.");
                    }

                    indexes.InsertIntoVectorIndex(boundValue.Cell.AsVectorReadOnlySpan(), rowId, indexName, tableName, databaseName, indexKind);
                    continue;
                }

                if (!VectorParser.TryCoerceToVector(row[vectorColumn], out float[] vector))
                {
                    throw new EvaluationException($"Cannot coerce value of '{vectorColumn}' into VECTOR for index '{indexName}'.");
                }

                indexes.InsertIntoVectorIndex(vector, rowId, indexName, tableName, databaseName, indexKind);
                continue;
            }

            if (IsPrimaryKeyIndex(tableName, indexName)
                && TryGetSingleIntegerKey(row, index.AttributeNames, out long integerKey))
            {
                indexes.InsertIntegerPrimaryKeys([(integerKey, rowId)], indexName, tableName, databaseName, keysAreNew: true);
                continue;
            }

            string indexValue = IndexKeyEncoder.BuildKeyString(row, index.AttributeNames);
            indexes.InsertIntoIndex(indexValue, rowId, indexName, tableName, databaseName);
        }
    }

    private void InsertTypedIndexes(
        string tableName,
        string databaseName,
        ReactiveRowSchema schema,
        ReadOnlySpan<CellValue> cells,
        RowKey rowId,
        IReadOnlyList<IndexFile> indexFiles)
    {
        foreach (IndexFile index in indexFiles)
        {
            bool hasNullKeyPart = false;
            foreach (string attribute in index.AttributeNames)
            {
                if (GetTypedCell(cells, schema, attribute).IsNull)
                {
                    hasNullKeyPart = true;
                    break;
                }
            }

            if (hasNullKeyPart)
            {
                continue;
            }

            string indexName = index.IndexFileName ?? string.Empty;
            if (string.IsNullOrWhiteSpace(indexName))
            {
                continue;
            }

            string indexKind = index.IndexKind ?? string.Empty;
            if (indexes.SupportsVectorIndexType(indexKind))
            {
                if (index.AttributeNames.Count != 1)
                {
                    throw new BindingException($"Vector index '{indexName}' (type '{indexKind}') must reference exactly one VECTOR column.");
                }

                string vectorColumn = index.AttributeNames[0];
                CellValue vectorCell = GetTypedCell(cells, schema, vectorColumn);
                if (vectorCell.Type != CellType.Vector)
                {
                    throw new EvaluationException($"Cannot coerce value of '{vectorColumn}' into VECTOR for index '{indexName}'.");
                }

                indexes.InsertIntoVectorIndex(vectorCell.AsVectorReadOnlySpan(), rowId, indexName, tableName, databaseName, indexKind);
                continue;
            }

            if (IsPrimaryKeyIndex(tableName, indexName)
                && TryGetSingleIntegerKey(schema, cells, index.AttributeNames, out long integerKey))
            {
                indexes.InsertIntegerPrimaryKeys([(integerKey, rowId)], indexName, tableName, databaseName, keysAreNew: true);
                continue;
            }

            if (IsPrimaryKeyIndex(tableName, indexName)
                && TryGetSingleGuidKey(schema, cells, index.AttributeNames, out Guid guidKey))
            {
                indexes.InsertGuidPrimaryKeys([(guidKey, rowId)], indexName, tableName, databaseName, keysAreNew: true);
                continue;
            }

            if (TryGetSingleIntegerKey(schema, cells, index.AttributeNames, out long integerIndexKey))
            {
                indexes.InsertIntegerIndexEntries([(integerIndexKey, rowId)], indexName, tableName, databaseName);
                continue;
            }

            if (TryGetSingleGuidKey(schema, cells, index.AttributeNames, out Guid guidIndexKey))
            {
                indexes.InsertGuidIndexEntries([(guidIndexKey, rowId)], indexName, tableName, databaseName);
                continue;
            }

            string indexValue = IndexKeyEncoder.BuildKeyString(schema, cells, index.AttributeNames);
            indexes.InsertIntoIndex(indexValue, rowId, indexName, tableName, databaseName);
        }
    }

    private void InsertTypedIndexesBatch(
        string tableName,
        string databaseName,
        ReactiveRowSchema schema,
        IReadOnlyList<CellValue[]> rows,
        IReadOnlyList<RowKey> rowIds,
        IReadOnlyList<IndexFile> indexFiles,
        IReadOnlyList<string> primaryKeys,
        string[]? primaryKeyValues,
        long[]? integerPrimaryKeyValues)
    {
        foreach (IndexFile index in indexFiles)
        {
            string indexName = index.IndexFileName ?? string.Empty;
            if (string.IsNullOrWhiteSpace(indexName))
            {
                continue;
            }

            if (IsPrimaryKeyIndex(tableName, indexName) && context.ResolveClusteredKey(databaseName, tableName) is not null)
            {
                continue; // a clustered table's rows are its primary-key index
            }

            string indexKind = index.IndexKind ?? string.Empty;
            if (indexes.SupportsVectorIndexType(indexKind))
            {
                InsertTypedVectorIndexBatch(tableName, databaseName, schema, rows, rowIds, index, indexName, indexKind);
                continue;
            }

            bool canReusePrimaryKey = primaryKeyValues is not null
                && IsPrimaryKeyIndex(tableName, indexName)
                && AttributeNamesEqual(index.AttributeNames, primaryKeys);
            bool canUseIntegerPrimaryKey = integerPrimaryKeyValues is not null
                && IsPrimaryKeyIndex(tableName, indexName)
                && AttributeNamesEqual(index.AttributeNames, primaryKeys);

            if (canUseIntegerPrimaryKey)
            {
                indexes.InsertIntegerPrimaryKeys(
                    integerPrimaryKeyValues.AsSpan(0, rowIds.Count),
                    AsRowIdSpan(rowIds),
                    indexName,
                    tableName,
                    databaseName,
                    keysAreNew: true);
                continue;
            }

            if (IsPrimaryKeyIndex(tableName, indexName)
                && AttributeNamesEqual(index.AttributeNames, primaryKeys)
                && TryBuildGuidIndexEntries(schema, rows, rowIds, index.AttributeNames, out List<(Guid Key, RowKey RowId)>? guidPrimaryEntries)
                && guidPrimaryEntries is not null)
            {
                indexes.InsertGuidPrimaryKeys(guidPrimaryEntries, indexName, tableName, databaseName, keysAreNew: true);
                continue;
            }

            if (TryBuildIntegerIndexEntries(schema, rows, rowIds, index.AttributeNames, out List<(long Key, RowKey RowId)>? integerIndexEntries)
                && integerIndexEntries is not null)
            {
                indexes.InsertIntegerIndexEntries(integerIndexEntries, indexName, tableName, databaseName);
                continue;
            }

            if (TryBuildGuidIndexEntries(schema, rows, rowIds, index.AttributeNames, out List<(Guid Key, RowKey RowId)>? guidIndexEntries)
                && guidIndexEntries is not null)
            {
                indexes.InsertGuidIndexEntries(guidIndexEntries, indexName, tableName, databaseName);
                continue;
            }

            var entries = new List<(string Value, RowKey RowId)>(rowIds.Count);
            for (int i = 0; i < rows.Count; i++)
            {
                ReadOnlySpan<CellValue> cells = rows[i];
                if (!canReusePrimaryKey && HasNullKeyPart(cells, schema, index.AttributeNames))
                {
                    continue;
                }

                string indexValue = canReusePrimaryKey
                    ? primaryKeyValues![i]
                    : IndexKeyEncoder.BuildKeyString(schema, cells, index.AttributeNames);
                entries.Add((indexValue, rowIds[i]));
            }

            indexes.InsertManyIntoIndex(entries, indexName, tableName, databaseName);
        }
    }

    private static bool TryBuildIntegerIndexEntries(
        ReactiveRowSchema schema,
        IReadOnlyList<CellValue[]> rows,
        IReadOnlyList<RowKey> rowIds,
        IReadOnlyList<string> attributes,
        out List<(long Key, RowKey RowId)>? entries)
    {
        entries = null;
        if (attributes.Count != 1 || !schema.TryGetOrdinal(attributes[0], out int ordinal))
        {
            return false;
        }

        var result = new List<(long Key, RowKey RowId)>(rowIds.Count);
        for (int i = 0; i < rows.Count; i++)
        {
            CellValue cell = rows[i][ordinal];
            if (cell.IsNull)
            {
                continue;
            }

            if (cell.Type == CellType.Int32)
            {
                result.Add((cell.AsInt32(), rowIds[i]));
                continue;
            }

            if (cell.Type == CellType.Int64)
            {
                result.Add((cell.AsInt64(), rowIds[i]));
                continue;
            }

            return false;
        }

        entries = result;
        return true;
    }

    private static bool TryBuildGuidIndexEntries(
        ReactiveRowSchema schema,
        IReadOnlyList<CellValue[]> rows,
        IReadOnlyList<RowKey> rowIds,
        IReadOnlyList<string> attributes,
        out List<(Guid Key, RowKey RowId)>? entries)
    {
        entries = null;
        if (attributes.Count != 1 || !schema.TryGetOrdinal(attributes[0], out int ordinal))
        {
            return false;
        }

        var result = new List<(Guid Key, RowKey RowId)>(rowIds.Count);
        for (int i = 0; i < rows.Count; i++)
        {
            CellValue cell = rows[i][ordinal];
            if (cell.IsNull)
            {
                continue;
            }

            if (cell.Type != CellType.Guid)
            {
                return false;
            }

            result.Add((cell.AsGuid(), rowIds[i]));
        }

        entries = result;
        return true;
    }

    private void InsertTypedVectorIndexBatch(
        string tableName,
        string databaseName,
        ReactiveRowSchema schema,
        IReadOnlyList<CellValue[]> rows,
        IReadOnlyList<RowKey> rowIds,
        IndexFile index,
        string indexName,
        string indexKind)
    {
        if (index.AttributeNames.Count != 1)
        {
            throw new BindingException($"Vector index '{indexName}' (type '{indexKind}') must reference exactly one VECTOR column.");
        }

        string vectorColumn = index.AttributeNames[0];
        int vectorDimension = -1;
        int vectorCount = 0;
        for (int i = 0; i < rows.Count; i++)
        {
            CellValue vectorCell = GetTypedCell(rows[i], schema, vectorColumn);
            if (vectorCell.IsNull)
            {
                continue;
            }

            if (vectorCell.Type != CellType.Vector)
            {
                throw new EvaluationException($"Cannot coerce value of '{vectorColumn}' into VECTOR for index '{indexName}'.");
            }

            int currentDimension = vectorCell.VectorLength;
            if (vectorDimension < 0)
            {
                vectorDimension = currentDimension;
            }
            else if (currentDimension != vectorDimension)
            {
                throw new EvaluationException($"Vector dimension mismatch for index '{indexName}'. Expected {vectorDimension}, got {currentDimension}.");
            }

            vectorCount++;
        }

        if (vectorCount == 0)
        {
            return;
        }

        var vectorRowIds = new RowKey[vectorCount];
        var vectorBuffer = new float[checked(vectorCount * vectorDimension)];
        int writeIndex = 0;
        for (int i = 0; i < rows.Count; i++)
        {
            CellValue vectorCell = GetTypedCell(rows[i], schema, vectorColumn);
            if (vectorCell.IsNull)
            {
                continue;
            }

            if (vectorCell.Type != CellType.Vector)
            {
                throw new EvaluationException($"Cannot coerce value of '{vectorColumn}' into VECTOR for index '{indexName}'.");
            }

            vectorRowIds[writeIndex] = rowIds[i];
            vectorCell.AsVectorReadOnlySpan().CopyTo(vectorBuffer.AsSpan(writeIndex * vectorDimension, vectorDimension));
            writeIndex++;
        }

        indexes.InsertManyIntoVectorIndex(vectorRowIds, vectorBuffer, vectorDimension, indexName, tableName, databaseName, indexKind);
    }

    private static bool HasNullKeyPart(
        ReadOnlySpan<CellValue> cells,
        ReactiveRowSchema schema,
        IReadOnlyList<string> attributes)
    {
        foreach (string attribute in attributes)
        {
            if (GetTypedCell(cells, schema, attribute).IsNull)
            {
                return true;
            }
        }

        return false;
    }

    private static bool TryGetSingleIntegerKey(
        IReadOnlyDictionary<string, object?> row,
        IReadOnlyList<string> attributes,
        out long key)
    {
        key = 0;
        if (attributes.Count != 1
            || !row.TryGetValue(attributes[0], out object? value)
            || value is null)
        {
            return false;
        }

        switch (value)
        {
            case int intValue:
                key = intValue;
                return true;
            case long longValue:
                key = longValue;
                return true;
            default:
                return false;
        }
    }

    private static bool TryGetSingleIntegerKey(
        ReactiveRowSchema schema,
        ReadOnlySpan<CellValue> cells,
        IReadOnlyList<string> attributes,
        out long key)
    {
        key = 0;
        if (attributes.Count != 1 || !schema.TryGetOrdinal(attributes[0], out int ordinal))
        {
            return false;
        }

        CellValue cell = cells[ordinal];
        if (cell.IsNull)
        {
            return false;
        }

        if (cell.Type == CellType.Int32)
        {
            key = cell.AsInt32();
            return true;
        }

        if (cell.Type == CellType.Int64)
        {
            key = cell.AsInt64();
            return true;
        }

        return false;
    }

    private static bool TryGetSingleGuidKey(
        ReactiveRowSchema schema,
        ReadOnlySpan<CellValue> cells,
        IReadOnlyList<string> attributes,
        out Guid key)
    {
        key = default;
        if (attributes.Count != 1 || !schema.TryGetOrdinal(attributes[0], out int ordinal))
        {
            return false;
        }

        CellValue cell = cells[ordinal];
        if (cell.Type != CellType.Guid)
        {
            return false;
        }

        key = cell.AsGuid();
        return true;
    }

    private static bool IsNullValue(object? value) =>
        value is null || value is DBNull || (value is DataVoBoundValue bound && bound.Cell.IsNull);

    /// <summary>
    /// Builds the error for a rejected insert. The first violation decides the error code; the message
    /// says how many further rows were also rejected.
    /// </summary>
    private static ConstraintViolationException RejectedRows(string tableName, List<string> messages)
    {
        string first = messages.Count == 0 ? $"Insert into {tableName} was rejected." : messages[0];
        string text = messages.Count <= 1 ? first : $"{first} ({messages.Count - 1} more row(s) also rejected.)";
        return new ConstraintViolationException(text)
        {
            ErrorCode = Contracts.Results.DataVoErrorClassifier.ClassifyMessage(first).Code,
        };
    }

    private static bool IsPrimaryKeyIndex(string tableName, string indexName) =>
        indexName.Equals($"_PK_{tableName}", StringComparison.OrdinalIgnoreCase);

    private static bool AttributeNamesEqual(IReadOnlyList<string> left, IReadOnlyList<string> right)
    {
        if (left.Count != right.Count)
        {
            return false;
        }

        for (int i = 0; i < left.Count; i++)
        {
            if (!left[i].Equals(right[i], StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }
        }

        return true;
    }
}

internal sealed record InsertRowsResult(
    IReadOnlyList<RowKey> RowIds,
    int AcceptedRowCount,
    IReadOnlyList<string> Messages);
