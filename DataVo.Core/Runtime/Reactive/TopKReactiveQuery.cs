using DataVo.Core.Models.Catalog;
using DataVo.Core.Parser.AST;
using DataVo.Core.Runtime.Changes;

namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// A single-table maintained top-K (L2): <c>ORDER BY … LIMIT</c> over the matching rows.
/// </summary>
/// <remarks>
/// Maintains a sorted index of every matching row, keyed by the <c>ORDER BY</c> keys and broken on
/// ties by the row's <b>primary key</b>. The primary key (not the storage row id) provides stable
/// per-row identity across the engine's out-of-place <c>UPDATE</c>, which reassigns the physical row
/// id. The current window is the first <c>k</c> entries (after any <c>OFFSET</c>); each
/// <see cref="Apply"/> updates the index, recomputes the window, and emits the diff against the
/// previous window: rows entering are Added, rows leaving are Removed, and rows that stay but whose
/// projected values changed are Updated.
/// </remarks>
internal sealed class TopKReactiveQuery : IBorrowedReactiveQuery
{
    private sealed record SortKey(string Column, bool Ascending);

    private sealed class Entry
    {
        public required string Key { get; init; }
        public required IReadOnlyDictionary<string, object?> Row { get; init; }
    }

    private readonly ReactivePredicate _predicate;
    private readonly List<SortKey> _sortKeys = [];
    private readonly List<string> _primaryKeys;
    private readonly int _skip;
    private readonly int _take;
    private readonly bool _selectStar;
    private readonly List<string> _projection = [];

    private readonly SortedSet<Entry> _index;
    private readonly Dictionary<string, Entry> _byKey = new(StringComparer.Ordinal);
    private Dictionary<string, IReadOnlyDictionary<string, object?>> _window = new(StringComparer.Ordinal);

    private readonly ReactiveRowSchema _outputSchema;
    private readonly string[] _outputColumns;
    private readonly QueryChangeBuilder _legacyBuilder;
    private readonly CellValue[] _rowScratch;

    /// <summary>
    /// Compiles the supplied parsed SELECT into a maintained top-K operator.
    /// </summary>
    /// <param name="select">The parsed top-K SELECT.</param>
    /// <param name="engine">The owning engine (catalog access for the primary key).</param>
    /// <param name="databaseName">The database that owns the source table.</param>
    public TopKReactiveQuery(SelectStatement select, DataVoEngine engine, string databaseName)
    {
        Validate(select);

        Table = select.FromTable!.Name;
        _predicate = ReactivePredicate.Compile(select.WhereExpression);

        foreach (OrderByColumnNode column in select.OrderByExpression!.Columns)
        {
            _sortKeys.Add(new SortKey(StripQualifier(column.Column.Name), column.IsAscending));
        }

        _skip = Math.Max(0, select.LimitExpression!.SkipTarget);
        _take = Math.Max(0, select.LimitExpression!.TakeTarget);

        foreach (SelectColumnNode column in select.Columns)
        {
            if (IsStarColumn(column))
            {
                _selectStar = true;
                continue;
            }

            _projection.Add(ResolveProjectedColumnName(column));
        }

        if (_projection.Count == 0)
        {
            _selectStar = true;
        }

        _primaryKeys = engine.Catalog.GetTablePrimaryKeys(Table, databaseName);
        if (_primaryKeys.Count == 0)
        {
            throw new NotSupportedException(
                $"Reactive top-K requires a primary key on table '{Table}' to identify rows across updates.");
        }

        _index = new SortedSet<Entry>(new EntryComparer(_sortKeys));

        // Output schema: the explicit projection columns, or the table's catalog columns for SELECT *.
        // A SELECT * top-K carries no schema in the SQL, so the borrowed fast lane takes it from the
        // catalog here (the same catalog access already used for the primary key above).
        string[] outputColumns = _selectStar
            ? engine.Catalog.GetTableColumns(Table, databaseName).Select(c => c.Name).ToArray()
            : _projection.ToArray();
        _outputColumns = outputColumns;
        _outputSchema = new ReactiveRowSchema(outputColumns);
        _rowScratch = new CellValue[outputColumns.Length];
        _legacyBuilder = new QueryChangeBuilder(_outputSchema);
    }

    /// <summary>Gets the source table this top-K observes.</summary>
    public string Table { get; }

    /// <inheritdoc />
    public IReadOnlyCollection<string> Tables => [Table];

    /// <inheritdoc />
    public void Seed(string table, IEnumerable<(long RowId, IReadOnlyDictionary<string, object?> Row)> rows)
    {
        foreach ((long _, IReadOnlyDictionary<string, object?> row) in rows)
        {
            if (_predicate.Matches(row))
            {
                AddEntry(row);
            }
        }

        _window = ComputeWindow();
    }

    /// <inheritdoc />
    public ReactiveRowSchema OutputSchema => _outputSchema;

    /// <summary>Owned path: build the borrowed delta into our own arena, then materialize. Behavior-
    /// identical to the pre-migration <c>Apply</c> (same <see cref="QueryChange"/> shape and values).</summary>
    public QueryChange Apply(IReadOnlyList<RowChange> tableChanges)
    {
        _legacyBuilder.Reset();
        ApplyInto(tableChanges, _legacyBuilder);
        return _legacyBuilder.Build().Materialize();
    }

    /// <inheritdoc />
    public void ApplyInto(IReadOnlyList<RowChange> tableChanges, QueryChangeBuilder builder)
    {
        // Index loop (not foreach) so iterating the IReadOnlyList does not allocate a boxed enumerator.
        for (int i = 0; i < tableChanges.Count; i++)
        {
            RowChange change = tableChanges[i];
            bool beforeMatches = change.Before is not null && _predicate.Matches(change.Before);
            bool afterMatches = change.After is not null && _predicate.Matches(change.After);

            switch (change.Kind)
            {
                case ChangeKind.Insert:
                    if (afterMatches)
                    {
                        AddEntry(change.After!);
                    }

                    break;

                case ChangeKind.Delete:
                    if (beforeMatches)
                    {
                        RemoveEntry(change.Before!);
                    }

                    break;

                case ChangeKind.Update:
                    if (beforeMatches)
                    {
                        RemoveEntry(change.Before!);
                    }

                    if (afterMatches)
                    {
                        AddEntry(change.After!);
                    }

                    break;
            }
        }

        Dictionary<string, IReadOnlyDictionary<string, object?>> next = ComputeWindow();

        // Rows entering the window are Added; rows that stayed but changed value are Updated (with their
        // before-image); rows that left are Removed. Each output row is written into the reused scratch.
        foreach ((string key, IReadOnlyDictionary<string, object?> row) in next)
        {
            if (!_window.TryGetValue(key, out IReadOnlyDictionary<string, object?>? previous))
            {
                WriteRow(row);
                builder.AddAddedRow(_rowScratch);
            }
            else if (!RowsEqual(previous, row))
            {
                WriteRow(previous);
                builder.AddUpdatedBeforeRow(_rowScratch);
                WriteRow(row);
                builder.AddUpdatedRow(_rowScratch);
            }
        }

        foreach ((string key, IReadOnlyDictionary<string, object?> row) in _window)
        {
            if (!next.ContainsKey(key))
            {
                WriteRow(row);
                builder.AddRemovedRow(_rowScratch);
            }
        }

        _window = next;
    }

    /// <summary>
    /// Writes a (already-projected) window row into the reused <see cref="_rowScratch"/>, in
    /// <see cref="_outputSchema"/> column order. Step 1 (emit-side) wraps the boxed cell values via
    /// <c>CellValue.From(object?)</c> — the documented residual boxing, removed in Step 2.
    /// </summary>
    private void WriteRow(IReadOnlyDictionary<string, object?> row)
    {
        for (int i = 0; i < _outputColumns.Length; i++)
        {
            _rowScratch[i] = CellValue.From(row.TryGetValue(_outputColumns[i], out object? v) ? v : null);
        }
    }

    private void AddEntry(IReadOnlyDictionary<string, object?> row)
    {
        string key = ComputeIdentity(row);

        // Out-of-place updates can produce a fresh row image for an existing identity; replace.
        if (_byKey.TryGetValue(key, out Entry? existing))
        {
            _index.Remove(existing);
        }

        var entry = new Entry { Key = key, Row = Copy(row) };
        _index.Add(entry);
        _byKey[key] = entry;
    }

    private void RemoveEntry(IReadOnlyDictionary<string, object?> row)
    {
        string key = ComputeIdentity(row);
        if (_byKey.TryGetValue(key, out Entry? existing))
        {
            _index.Remove(existing);
            _byKey.Remove(key);
        }
    }

    private Dictionary<string, IReadOnlyDictionary<string, object?>> ComputeWindow()
    {
        var window = new Dictionary<string, IReadOnlyDictionary<string, object?>>(StringComparer.Ordinal);
        int skipped = 0;
        int taken = 0;

        foreach (Entry entry in _index)
        {
            if (skipped < _skip)
            {
                skipped++;
                continue;
            }

            if (taken >= _take)
            {
                break;
            }

            window[entry.Key] = Project(entry.Row);
            taken++;
        }

        return window;
    }

    private string ComputeIdentity(IReadOnlyDictionary<string, object?> row)
    {
        return string.Join(
            "",
            _primaryKeys.Select(column =>
            {
                object? value = row.TryGetValue(column, out object? v) ? v : null;
                return value is null ? " NULL" : "v:" + Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture);
            }));
    }

    private IReadOnlyDictionary<string, object?> Project(IReadOnlyDictionary<string, object?> row)
    {
        if (_selectStar)
        {
            return Copy(row);
        }

        var projected = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (string column in _projection)
        {
            projected[column] = row.TryGetValue(column, out object? value) ? value : null;
        }

        return projected;
    }

    private static IReadOnlyDictionary<string, object?> Copy(IReadOnlyDictionary<string, object?> row) =>
        new Dictionary<string, object?>(
            row.ToDictionary(pair => pair.Key, pair => pair.Value),
            StringComparer.OrdinalIgnoreCase);

    private static bool RowsEqual(IReadOnlyDictionary<string, object?> a, IReadOnlyDictionary<string, object?> b)
    {
        if (a.Count != b.Count)
        {
            return false;
        }

        foreach ((string key, object? value) in a)
        {
            if (!b.TryGetValue(key, out object? other))
            {
                return false;
            }

            if (ReactiveValueComparer.Instance.Compare(value, other) != 0)
            {
                return false;
            }
        }

        return true;
    }

    private sealed class EntryComparer(List<SortKey> sortKeys) : IComparer<Entry>
    {
        public int Compare(Entry? x, Entry? y)
        {
            if (ReferenceEquals(x, y))
            {
                return 0;
            }

            ArgumentNullException.ThrowIfNull(x);
            ArgumentNullException.ThrowIfNull(y);

            foreach (SortKey sortKey in sortKeys)
            {
                object? left = x.Row.TryGetValue(sortKey.Column, out object? lv) ? lv : null;
                object? right = y.Row.TryGetValue(sortKey.Column, out object? rv) ? rv : null;

                int comparison = ReactiveValueComparer.Instance.Compare(left, right);
                if (comparison != 0)
                {
                    return sortKey.Ascending ? comparison : -comparison;
                }
            }

            // Break ties on the stable primary-key identity so distinct rows never collapse and the
            // ordering is deterministic.
            return string.CompareOrdinal(x.Key, y.Key);
        }
    }

    private static void Validate(SelectStatement select)
    {
        if (select.Joins.Count > 0)
        {
            throw new NotSupportedException("Reactive top-K does not support JOINs.");
        }

        if (select.Ctes.Count > 0)
        {
            throw new NotSupportedException("Reactive top-K does not support common table expressions.");
        }

        if (select.IsDistinct)
        {
            throw new NotSupportedException("Reactive top-K does not support DISTINCT.");
        }

        if (select.GroupByExpression is not null || select.HavingExpression is not null)
        {
            throw new NotSupportedException("Reactive top-K does not support GROUP BY / HAVING.");
        }

        if (select.OrderByExpression is null || select.OrderByExpression.Columns.Count == 0)
        {
            throw new NotSupportedException("Reactive top-K requires an ORDER BY clause.");
        }

        if (select.LimitExpression is null)
        {
            throw new NotSupportedException("Reactive top-K requires a LIMIT clause.");
        }

        if (select.FromTable is null)
        {
            throw new NotSupportedException("Reactive top-K requires a single FROM table.");
        }

        foreach (SelectColumnNode column in select.Columns)
        {
            if (column.Expression is AggregateExpressionNode || column.Expression is WindowFunctionExpressionNode)
            {
                throw new NotSupportedException("Reactive top-K does not support aggregate or window functions.");
            }
        }
    }

    private static bool IsStarColumn(SelectColumnNode column)
    {
        return column.RawExpression.Trim() == "*"
            || (column.Expression is null && column.RawExpression.Trim().EndsWith("*", StringComparison.Ordinal));
    }

    private static string ResolveProjectedColumnName(SelectColumnNode column)
    {
        if (column.Expression is ColumnRefNode columnRef)
        {
            return columnRef.Column;
        }

        if (column.Expression is ResolvedColumnRefNode resolved)
        {
            return resolved.Column;
        }

        return StripQualifier(column.RawExpression.Trim());
    }

    private static string StripQualifier(string name)
    {
        int dot = name.LastIndexOf('.');
        return dot >= 0 ? name[(dot + 1)..] : name;
    }
}
