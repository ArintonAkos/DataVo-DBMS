using DataVo.Core.BTree;
using DataVo.Core.CompiledQueries;
using DataVo.Core.Exceptions;
using DataVo.Core.Models.Catalog;
using DataVo.Core.MVCC;
using DataVo.Core.Parser.DML;
using DataVo.Core.Runtime.Changes;
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
    private readonly Dictionary<string, TablePlan> _tables = new(StringComparer.OrdinalIgnoreCase);
    private readonly List<TablePlan> _order = [];
    private readonly Dictionary<DataVoBatchLayout, LayoutPlan> _layouts = new(ReferenceEqualityComparer.Instance);
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
        for (int i = 0; i < batch.Count; i++)
        {
            DataVoWriteBatch.Entry entry = batch[i];
            LayoutPlan layout = LayoutOf(entry.Layout);
            ReadOnlySpan<object?> values = batch.ValuesOf(entry);
            PendingRow row = layout.Table.Pending(KeyOf(layout, values, out Dictionary<string, object?> keyValues), keyValues);
            if (entry.Operation == DataVoBatchOperation.Delete)
            {
                row.MarkDeleted();
            }
            else
            {
                row.Set(layout, values, BindAndNormalize, updateOnly: entry.Operation == DataVoBatchOperation.Update);
            }
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
        foreach (TablePlan table in _order)
        {
            Classify(table);
        }

        // Every constraint is decided against the batch's final state before anything is written, so the
        // writers below run without their own foreign-key checks and cannot stop half-way.
        BatchIntegrity integrity = Validate();

        // Deletes (cascaded rows included) go first, so the keys, unique values and parents they give up are
        // free before updates and inserts claim them.
        int deleted = 0;
        foreach (string table in integrity.TablesWithDeletes.ToList())
        {
            List<RowKey> rows = [.. integrity.DeletedRows(table)];
            deleted += rows.Count;
            RowDeleter.Delete(context.Engine, rows, table, databaseName, statementTxId, recorder, followForeignKeys: false);
        }

        foreach (TablePlan table in _order)
        {
            WriteUpdates(table, statementTxId, recorder);
        }

        foreach (string table in integrity.ParentsFirst([.. _order.Where(plan => plan.Inserts.Count > 0).Select(plan => plan.Name)]))
        {
            WriteInserts(_tables[table], statementTxId, recorder);
        }

        return Summarize(deleted);
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
            integrity.AddTable(table.Name, table.DeleteRows, table.UpdateIds, table.UpdateOldRows, table.UpdateNewRows, table.Inserts);
        }

        integrity.Validate();
        foreach (TablePlan table in _order)
        {
            table.DropUpdatesOf(integrity.DeletedRows(table.Name));
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

    private DataVoBatchResult Summarize(int deleted)
    {
        int inserted = 0, updated = 0;
        foreach (TablePlan table in _order)
        {
            inserted += table.Inserts.Count;
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
            for (int i = 0; i < Columns.Length; i++)
            {
                Columns[i] = table.ColumnNamed(layout.Columns[i]);
            }

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
    }

    /// <summary>One table's pending rows and, once classified, the rows to delete, update and insert.</summary>
    private sealed class TablePlan
    {
        private readonly Dictionary<string, Column> _columns;
        private readonly Dictionary<string, PendingRow> _pending = new(StringComparer.Ordinal);

        public TablePlan(string name, List<Column> columns, List<string> primaryKeys)
        {
            if (primaryKeys.Count == 0)
            {
                throw new InvalidOperationException($"Table {name} needs a primary key to be written by a batch.");
            }

            Name = name;
            AllColumns = columns;
            _columns = columns.ToDictionary(static column => column.Name, StringComparer.OrdinalIgnoreCase);
            PrimaryKeys = primaryKeys;
        }

        public string Name { get; }

        public List<Column> AllColumns { get; }

        public List<string> PrimaryKeys { get; }

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
}
