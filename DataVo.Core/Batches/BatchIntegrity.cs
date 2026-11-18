using System.Globalization;
using DataVo.Core.BTree;
using DataVo.Core.Exceptions;
using DataVo.Core.Models.Catalog;
using DataVo.Core.MVCC;
using DataVo.Core.Parser.DML;
using DataVo.Core.Runtime;

namespace DataVo.Core.Batches;

/// <summary>
/// Checks a write batch against the state the database will have once the whole batch is written, before
/// anything is written: which rows <c>ON DELETE CASCADE</c> removes with the batch's deletes, whether an
/// <c>ON DELETE RESTRICT</c> child is left behind, whether every new or changed reference finds its parent,
/// and whether primary and unique keys stay unique once the batch's own deletes and updates free or take them.
/// </summary>
/// <remarks>
/// Because every constraint is decided here, the writers that follow run with their own foreign-key checks
/// and cascades turned off, so a batch cannot fail half-way through and leave part of itself behind, on the
/// in-memory engine (which has no write rollback) as on LSM.
/// </remarks>
internal sealed class BatchIntegrity(DataVoEngine engine, string databaseName, InsertRowService inserts)
{
    private readonly Dictionary<string, TableState> _tables = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<(string Table, string Column), Dictionary<string, List<RowKey>>> _scannedChildren = [];

    /// <summary>Gets the tables to write-lock for a batch over <paramref name="tables"/>: the tables and every table their deletes can cascade into.</summary>
    /// <param name="engine">The owning engine.</param>
    /// <param name="databaseName">The database.</param>
    /// <param name="tables">The tables the batch writes.</param>
    /// <returns>The tables, with their <c>ON DELETE CASCADE</c> descendants.</returns>
    public static IReadOnlyCollection<string> TablesToLock(DataVoEngine engine, string databaseName, IEnumerable<string> tables)
    {
        var locked = new HashSet<string>(tables, StringComparer.OrdinalIgnoreCase);
        var queue = new Queue<string>(locked);
        while (queue.TryDequeue(out string? parent))
        {
            foreach (var child in engine.Catalog.GetChildForeignKeys(parent, databaseName))
            {
                if (IsCascade(child.OnDeleteAction) && locked.Add(child.ChildTable))
                {
                    queue.Enqueue(child.ChildTable);
                }
            }
        }

        return locked;
    }

    /// <summary>Records what the batch does to one table.</summary>
    public void AddTable(
        string table,
        IReadOnlyDictionary<RowKey, Dictionary<string, object?>> deletes,
        IReadOnlyList<RowKey> updateIds,
        IReadOnlyDictionary<RowKey, Dictionary<string, object?>> updateOldRows,
        IReadOnlyList<Dictionary<string, object?>> updateNewRows,
        IReadOnlyList<Dictionary<string, object?>> insertRows)
    {
        TableState state = StateOf(table);
        foreach ((RowKey rowId, Dictionary<string, object?> row) in deletes)
        {
            state.Delete(rowId, row);
        }

        for (int i = 0; i < updateIds.Count; i++)
        {
            state.Updates[updateIds[i]] = (updateOldRows[updateIds[i]], updateNewRows[i]);
        }

        foreach (Dictionary<string, object?> row in insertRows)
        {
            state.Inserts.Add(row);
            state.InsertedKeys.Add(IndexKeyEncoder.BuildKeyString(row, state.PrimaryKeys));
        }
    }

    /// <summary>
    /// Follows cascades and checks every constraint against the batch's final state; throws before anything
    /// is written when one fails.
    /// </summary>
    /// <exception cref="EvaluationException">A RESTRICT child would be left without its parent.</exception>
    /// <exception cref="ConstraintViolationException">A key, unique value or reference would be violated.</exception>
    public void Validate()
    {
        List<(string Parent, string Value, string Child, string Column, List<RowKey> Rows)> restricted = FollowCascades();
        CheckRestrictions(restricted);
        foreach (TableState state in _tables.Values.ToList())
        {
            CheckModifiable(state);
            CheckKeys(state);
            CheckReferences(state);
        }
    }

    /// <summary>Gets every row the batch deletes from a table, cascaded rows included.</summary>
    public IReadOnlyCollection<RowKey> DeletedRows(string table) =>
        _tables.TryGetValue(table, out TableState? state) ? state.Deleted.Keys : [];

    /// <summary>Gets the tables that lose rows, explicit deletes or cascades.</summary>
    public IEnumerable<string> TablesWithDeletes => _tables.Values.Where(state => state.Deleted.Count > 0).Select(state => state.Name);

    /// <summary>Orders tables so that every parent comes before the tables that reference it (cycles keep their order).</summary>
    public List<string> ParentsFirst(IReadOnlyList<string> tables)
    {
        var ordered = new List<string>(tables.Count);
        var placed = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var pending = new List<string>(tables);
        while (pending.Count > 0)
        {
            int before = ordered.Count;
            foreach (string table in pending.ToList())
            {
                bool parentsPlaced = engine.Catalog.GetTableForeignKeys(table, databaseName)
                    .SelectMany(foreignKey => foreignKey.References)
                    .Select(reference => reference.ReferenceTableName)
                    .All(parent => placed.Contains(parent) || parent.Equals(table, StringComparison.OrdinalIgnoreCase)
                        || !pending.Contains(parent, StringComparer.OrdinalIgnoreCase));
                if (parentsPlaced)
                {
                    ordered.Add(table);
                    placed.Add(table);
                    pending.Remove(table);
                }
            }

            if (ordered.Count == before)
            {
                ordered.AddRange(pending); // a reference cycle: keep the batch's order
                break;
            }
        }

        return ordered;
    }

    /// <summary>
    /// Adds to the delete set every row an <c>ON DELETE CASCADE</c> removes with it, and returns the
    /// <c>RESTRICT</c> children found on the way, to be checked once the set is complete.
    /// </summary>
    private List<(string Parent, string Value, string Child, string Column, List<RowKey> Rows)> FollowCascades()
    {
        var restricted = new List<(string, string, string, string, List<RowKey>)>();
        var queue = new Queue<(TableState Table, Dictionary<string, object?> Row)>(
            _tables.Values.SelectMany(state => state.Deleted.Values.Select(row => (state, row))));
        while (queue.TryDequeue(out var deleted))
        {
            foreach (var edge in engine.Catalog.GetChildForeignKeys(deleted.Table.Name, databaseName))
            {
                if (!deleted.Row.TryGetValue(edge.ParentColumn, out object? value) || value is null)
                {
                    continue;
                }

                string key = KeyOf(edge.ParentColumn, value);
                TableState child = StateOf(edge.ChildTable);
                List<RowKey> referencing = [.. ChildrenReferencing(child, edge.ChildColumn, key).Where(rowId => !child.Deleted.ContainsKey(rowId))];
                if (referencing.Count == 0)
                {
                    continue;
                }

                if (!IsCascade(edge.OnDeleteAction))
                {
                    restricted.Add((deleted.Table.Name, Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty, edge.ChildTable, edge.ChildColumn, referencing));
                    continue;
                }

                foreach ((RowKey rowId, Dictionary<string, object?> row) in engine.StorageContext.GetTableContents(referencing, child.Name, databaseName))
                {
                    if (child.Delete(rowId, row))
                    {
                        queue.Enqueue((child, row));
                    }
                }
            }
        }

        return restricted;
    }

    /// <summary>Fails when a RESTRICT child of a deleted row is neither deleted nor moved to another parent by the batch.</summary>
    private void CheckRestrictions(List<(string Parent, string Value, string Child, string Column, List<RowKey> Rows)> restricted)
    {
        foreach ((string parent, string value, string child, string column, List<RowKey> rows) in restricted)
        {
            int left = rows.Count(rowId => !_tables[child].Deleted.ContainsKey(rowId));
            if (left > 0)
            {
                throw new EvaluationException(
                    $"Foreign key violation: Cannot delete from {parent} — {left} row(s) in {child}.{column} reference it ({value}).");
            }
        }
    }

    /// <summary>Checks MVCC for every row the batch removes or changes, cascaded rows included.</summary>
    private void CheckModifiable(TableState state)
    {
        foreach (RowKey rowId in state.Deleted.Keys)
        {
            MvccCoordinator.ValidateCanModifyRow(engine, databaseName, state.Name, rowId, null, "DELETE");
        }

        foreach (RowKey rowId in state.Updates.Keys)
        {
            MvccCoordinator.ValidateCanModifyRow(engine, databaseName, state.Name, rowId, null, "UPDATE");
        }
    }

    /// <summary>
    /// Checks primary and unique keys with the batch's own changes taken into account: values its deletes and
    /// updates give up are free, values its updates take are used, so swaps and reuse inside one batch pass.
    /// </summary>
    private void CheckKeys(TableState state)
    {
        if (state.Updates.Count == 0 && state.Inserts.Count == 0)
        {
            return;
        }

        IReadOnlyList<string> uniqueKeys = engine.Catalog.GetTableUniqueKeys(state.Name, databaseName);
        var freed = new InsertRowService.PendingKeys();
        foreach (Dictionary<string, object?> row in state.Deleted.Values)
        {
            freed.FreedPrimaryKeys.Add(IndexKeyEncoder.BuildKeyString(row, state.PrimaryKeys));
            AddUniqueValues(freed.FreedUniqueValues, row, uniqueKeys);
        }

        foreach ((Dictionary<string, object?> oldRow, Dictionary<string, object?> newRow) in state.Updates.Values)
        {
            AddUniqueValues(freed.FreedUniqueValues, oldRow, ChangedColumns(oldRow, newRow, uniqueKeys));
        }

        var validator = new UpdateKeyValidator(engine.IndexManager, engine.StorageContext, state.Name, databaseName, freed);
        int number = 0;
        foreach ((Dictionary<string, object?> oldRow, Dictionary<string, object?> newRow) in state.Updates.Values)
        {
            validator.Validate(newRow, oldRow, state.PrimaryKeys, uniqueKeys, ++number);
        }

        // New rows may not take what an update now holds.
        foreach ((Dictionary<string, object?> oldRow, Dictionary<string, object?> newRow) in state.Updates.Values)
        {
            AddUniqueValues(freed.TakenUniqueValues, newRow, ChangedColumns(oldRow, newRow, uniqueKeys));
        }

        inserts.ValidatePendingInserts(databaseName, state.Name, state.Inserts, freed, checkForeignKeys: false);
    }

    /// <summary>Checks that every reference a new row or a changed column makes finds a parent the batch keeps.</summary>
    private void CheckReferences(TableState state)
    {
        List<ForeignKey> foreignKeys = engine.Catalog.GetTableForeignKeys(state.Name, databaseName);
        if (foreignKeys.Count == 0)
        {
            return;
        }

        IEnumerable<(Dictionary<string, object?>? Old, Dictionary<string, object?> New)> written =
            state.Inserts.Select(row => ((Dictionary<string, object?>?)null, row))
                .Concat(state.Updates.Values.Select(update => ((Dictionary<string, object?>?)update.Old, update.New)));
        foreach ((Dictionary<string, object?>? oldRow, Dictionary<string, object?> newRow) in written)
        {
            foreach (ForeignKey foreignKey in foreignKeys)
            {
                if (!newRow.TryGetValue(foreignKey.AttributeName, out object? value) || value is null
                    || (oldRow is not null && SameKey(oldRow, newRow, foreignKey.AttributeName)))
                {
                    continue; // NULL references nothing; an unchanged reference is checked by RESTRICT instead
                }

                string key = KeyOf(foreignKey.AttributeName, value);
                foreach (Reference reference in foreignKey.References)
                {
                    if (!ParentKept(reference.ReferenceTableName, key))
                    {
                        throw new ConstraintViolationException(
                            $"Foreign key violation: {state.Name}.{foreignKey.AttributeName} = {value} has no row in {reference.ReferenceTableName}.")
                        {
                            ErrorCode = Contracts.Results.DataVoErrorCodes.ForeignKeyViolation,
                        };
                    }
                }
            }
        }
    }

    /// <summary>Gets whether the parent key exists once the batch is written: inserted by it, or stored and not deleted by it.</summary>
    private bool ParentKept(string parentTable, string key)
    {
        TableState parent = StateOf(parentTable);
        if (parent.InsertedKeys.Contains(key))
        {
            return true;
        }

        if (parent.DeletedKeys.Contains(key))
        {
            return false;
        }

        try
        {
            return engine.IndexManager.IndexContainsKey(key, $"_PK_{parent.Name}", parent.Name, databaseName);
        }
        catch (IndexException)
        {
            return engine.StorageContext.GetTableContents(parent.Name, databaseName).Values
                .Any(row => string.Equals(IndexKeyEncoder.BuildKeyString(row, parent.PrimaryKeys), key, StringComparison.Ordinal));
        }
    }

    /// <summary>
    /// Finds the rows of a child table that reference <paramref name="key"/> once the batch's updates are
    /// applied: stored rows holding it, except those an update moves elsewhere.
    /// </summary>
    private IEnumerable<RowKey> ChildrenReferencing(TableState child, string column, string key)
    {
        foreach (RowKey rowId in StoredChildren(child.Name, column, key))
        {
            if (!child.Updates.TryGetValue(rowId, out var update) || string.Equals(KeyOf(column, update.New.GetValueOrDefault(column)), key, StringComparison.Ordinal))
            {
                yield return rowId;
            }
        }
    }

    /// <summary>Looks up stored rows by a column value through a single-column index, or by scanning the table once per column.</summary>
    private IReadOnlyList<RowKey> StoredChildren(string table, string column, string key)
    {
        IndexFile? index = engine.Catalog.GetTableIndexes(table, databaseName).FirstOrDefault(candidate =>
            candidate.AttributeNames.Count == 1
            && candidate.AttributeNames[0].Equals(column, StringComparison.OrdinalIgnoreCase)
            && !engine.IndexManager.SupportsVectorIndexType(candidate.IndexKind ?? string.Empty));
        if (index?.IndexFileName is { Length: > 0 } indexName)
        {
            try
            {
                return [.. engine.IndexManager.FilterUsingIndex(key, indexName, table, databaseName)];
            }
            catch (IndexException)
            {
            }
        }

        if (!_scannedChildren.TryGetValue((table, column), out Dictionary<string, List<RowKey>>? byValue))
        {
            byValue = new Dictionary<string, List<RowKey>>(StringComparer.Ordinal);
            foreach ((RowKey rowId, Dictionary<string, object?> row) in engine.StorageContext.GetTableContents(table, databaseName))
            {
                if (row.TryGetValue(column, out object? value) && value is not null)
                {
                    string valueKey = KeyOf(column, value);
                    if (!byValue.TryGetValue(valueKey, out List<RowKey>? rows))
                    {
                        byValue[valueKey] = rows = [];
                    }

                    rows.Add(rowId);
                }
            }

            _scannedChildren[(table, column)] = byValue;
        }

        return byValue.TryGetValue(key, out List<RowKey>? found) ? found : [];
    }

    private TableState StateOf(string table)
    {
        string stored = engine.Catalog.FindTableName(table, databaseName) ?? table;
        if (!_tables.TryGetValue(stored, out TableState? state))
        {
            state = new TableState(stored, engine.Catalog.GetTablePrimaryKeys(stored, databaseName));
            _tables[stored] = state;
        }

        return state;
    }

    private static string KeyOf(string column, object? value) =>
        IndexKeyEncoder.BuildKeyString(new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase) { [column] = Materialize(value) }, [column]);

    private static object? Materialize(object? value) =>
        value is ValueConversion.DataVoBoundValue bound ? bound.ToMaterializedValue() : value;

    private static bool SameKey(Dictionary<string, object?> oldRow, Dictionary<string, object?> newRow, string column) =>
        string.Equals(KeyOf(column, oldRow.GetValueOrDefault(column)), KeyOf(column, newRow.GetValueOrDefault(column)), StringComparison.Ordinal)
        && (oldRow.GetValueOrDefault(column) is null) == (newRow.GetValueOrDefault(column) is null);

    private static IEnumerable<string> ChangedColumns(Dictionary<string, object?> oldRow, Dictionary<string, object?> newRow, IReadOnlyList<string> columns) =>
        columns.Where(column => !SameKey(oldRow, newRow, column));

    private static void AddUniqueValues(Dictionary<string, HashSet<string>> byColumn, Dictionary<string, object?> row, IEnumerable<string> columns)
    {
        foreach (string column in columns)
        {
            if (row.TryGetValue(column, out object? value) && value is not null)
            {
                InsertRowService.PendingKeys.Add(byColumn, column, KeyOf(column, value));
            }
        }
    }

    private static bool IsCascade(string? action) => string.Equals(action, "CASCADE", StringComparison.OrdinalIgnoreCase);

    /// <summary>What the batch does to one table, as far as constraints are concerned.</summary>
    private sealed class TableState(string name, List<string> primaryKeys)
    {
        public string Name { get; } = name;

        public List<string> PrimaryKeys { get; } = primaryKeys;

        /// <summary>Gets the rows removed, by explicit deletes or cascades, with their stored values.</summary>
        public Dictionary<RowKey, Dictionary<string, object?>> Deleted { get; } = [];

        public HashSet<string> DeletedKeys { get; } = new(StringComparer.Ordinal);

        public Dictionary<RowKey, (Dictionary<string, object?> Old, Dictionary<string, object?> New)> Updates { get; } = [];

        public List<Dictionary<string, object?>> Inserts { get; } = [];

        public HashSet<string> InsertedKeys { get; } = new(StringComparer.Ordinal);

        /// <summary>Adds a deleted row; returns <see langword="false"/> when it was already deleted.</summary>
        public bool Delete(RowKey rowId, Dictionary<string, object?> row)
        {
            if (!Deleted.TryAdd(rowId, row))
            {
                return false;
            }

            Updates.Remove(rowId); // a row the batch changes but a cascade then removes ends up deleted

            if (PrimaryKeys.Count > 0)
            {
                DeletedKeys.Add(IndexKeyEncoder.BuildKeyString(row, PrimaryKeys));
            }

            return true;
        }
    }
}
