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
/// <para>
/// The arrangement is typed: each entry keeps only the columns the view reads (sort keys, primary key and
/// output columns) as unboxed <see cref="CellValue"/> cells, identified by a <see cref="PackedKey"/>. Entries
/// are pooled and reused once they leave both the index and the delivered window, so a steady stream of
/// updates allocates no dictionaries, identity strings or boxed values during dispatch.
/// </para>
/// </remarks>
internal sealed class TopKReactiveQuery : IBorrowedReactiveQuery
{
    private sealed record SortKey(int Ordinal, bool Ascending);

    /// <summary>One indexed row: its identity and its stored cells, immutable while it is indexed.</summary>
    private sealed class Entry
    {
        public Entry(int width) => Cells = new CellValue[width];

        public PackedKey Key;

        public readonly CellValue[] Cells;

        // Whether the sorted index holds the entry, and whether the delivered window still references it.
        // An entry returns to the pool only when both are false.
        public bool InIndex;

        public bool InWindow;
    }

    private readonly ReactivePredicate _predicate;
    private readonly List<SortKey> _sortKeys = [];
    private readonly int _skip;
    private readonly int _take;

    private readonly ReactiveRowLayout _layout;
    private readonly int[] _primaryKeyOrdinals;
    private readonly int[] _outputOrdinals;

    private readonly SortedSet<Entry> _index;
    private readonly Dictionary<PackedKey, Entry> _byKey = [];
    private readonly Stack<Entry> _pool = new();
    private readonly CellValue[] _imageScratch;

    // Double-buffered window: the current one and a spare that is cleared, refilled and swapped in on each
    // dispatch, so recomputing the window allocates nothing at steady state.
    private List<Entry> _window = [];
    private List<Entry> _spareWindow = [];
    private Dictionary<PackedKey, Entry> _windowByKey = [];
    private Dictionary<PackedKey, Entry> _spareWindowByKey = [];

    private readonly ReactiveRowSchema _outputSchema;
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

        select.LimitExpression!.EnsureBound();
        _skip = Math.Max(0, select.LimitExpression!.SkipTarget);
        _take = Math.Max(0, select.LimitExpression!.TakeTarget);

        List<string> projection = ResolveProjection(select, out bool selectStar);
        List<string> primaryKeys = engine.Catalog.GetTablePrimaryKeys(Table, databaseName);
        if (primaryKeys.Count == 0)
        {
            throw new NotSupportedException(
                $"Reactive top-K requires a primary key on table '{Table}' to identify rows across updates.");
        }

        // Output schema: the explicit projection columns, or the table's catalog columns for SELECT *.
        // A SELECT * top-K carries no schema in the SQL, so the borrowed fast lane takes it from the
        // catalog here (the same catalog access already used for the primary key above).
        string[] outputColumns = selectStar
            ? engine.Catalog.GetTableColumns(Table, databaseName).Select(c => c.Name).ToArray()
            : projection.ToArray();

        List<string> sortColumns = select.OrderByExpression!.Columns
            .Select(column => StripQualifier(column.Column.Name))
            .ToList();
        _layout = new ReactiveRowLayout(DistinctColumns(outputColumns, sortColumns, primaryKeys));
        _outputOrdinals = outputColumns.Select(_layout.OrdinalOf).ToArray();
        _primaryKeyOrdinals = primaryKeys.Select(_layout.OrdinalOf).ToArray();
        for (int i = 0; i < sortColumns.Count; i++)
        {
            _sortKeys.Add(new SortKey(_layout.OrdinalOf(sortColumns[i]), select.OrderByExpression.Columns[i].IsAscending));
        }

        _imageScratch = new CellValue[_layout.Count];
        _index = new SortedSet<Entry>(new EntryComparer(_sortKeys));
        _outputSchema = new ReactiveRowSchema(outputColumns);
        _rowScratch = new CellValue[outputColumns.Length];
        _legacyBuilder = new QueryChangeBuilder(_outputSchema);
    }

    /// <summary>Gets the source table this top-K observes.</summary>
    public string Table { get; }

    /// <inheritdoc />
    public IReadOnlyCollection<string> Tables => [Table];

    /// <inheritdoc />
    public void Seed(string table, IEnumerable<(RowKey RowId, IReadOnlyDictionary<string, object?> Row)> rows)
    {
        foreach ((RowKey _, IReadOnlyDictionary<string, object?> row) in rows)
        {
            if (_predicate.Matches(row))
            {
                Entry entry = RentEntry();
                _layout.Load(row, entry.Cells);
                AddEntry(entry);
            }
        }

        FillWindowInto(_window, _windowByKey);
        foreach (Entry entry in _window)
        {
            entry.InWindow = true;
        }
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
            if (change.Kind is ChangeKind.Delete or ChangeKind.Update
                && change.Before is not null
                && _predicate.Matches(change.Before))
            {
                RemoveEntry(change.Before);
            }

            if (change.Kind is ChangeKind.Insert or ChangeKind.Update)
            {
                ApplyAfterImage(change);
            }
        }

        FillWindowInto(_spareWindow, _spareWindowByKey);
        EmitWindowDiff(builder);
        SwapWindows();
    }

    /// <summary>Indexes a change's after-image when it matches, reading the typed image when one was captured.</summary>
    private void ApplyAfterImage(RowChange change)
    {
        if (change.TypedAfter is { } typed)
        {
            RowRef row = typed.AsRowRef();
            if (_predicate.Matches(row))
            {
                Entry entry = RentEntry();
                _layout.Load(row, typed.Schema, entry.Cells);
                AddEntry(entry);
            }
        }
        else if (change.After is { } after && _predicate.Matches(after))
        {
            Entry entry = RentEntry();
            _layout.Load(after, entry.Cells);
            AddEntry(entry);
        }
    }

    /// <summary>
    /// Emits the window diff: rows entering the window are Added, rows that stayed but changed value are
    /// Updated (with their before-image), and rows that left are Removed.
    /// </summary>
    private void EmitWindowDiff(QueryChangeBuilder builder)
    {
        foreach (Entry entry in _spareWindow)
        {
            if (!_windowByKey.TryGetValue(entry.Key, out Entry? previous))
            {
                WriteRow(entry);
                builder.AddAddedRow(_rowScratch);
            }
            else if (!ReferenceEquals(previous, entry) && !OutputsEqual(previous, entry))
            {
                // An untouched window row keeps the same entry across dispatches, so the reference check
                // skips the value comparison for every row the batch did not change.
                WriteRow(previous);
                builder.AddUpdatedBeforeRow(_rowScratch);
                WriteRow(entry);
                builder.AddUpdatedRow(_rowScratch);
            }
        }

        foreach (Entry previous in _window)
        {
            if (!_spareWindowByKey.ContainsKey(previous.Key))
            {
                WriteRow(previous);
                builder.AddRemovedRow(_rowScratch);
            }
        }
    }

    /// <summary>
    /// Makes the just-filled window current and returns entries that neither the index nor the new window
    /// references to the pool.
    /// </summary>
    private void SwapWindows()
    {
        foreach (Entry previous in _window)
        {
            previous.InWindow = false;
        }

        foreach (Entry entry in _spareWindow)
        {
            entry.InWindow = true;
        }

        foreach (Entry previous in _window)
        {
            if (!previous.InWindow && !previous.InIndex)
            {
                _pool.Push(previous);
            }
        }

        (_window, _spareWindow) = (_spareWindow, _window);
        (_windowByKey, _spareWindowByKey) = (_spareWindowByKey, _windowByKey);
    }

    /// <summary>Writes an entry's output columns into the reused <see cref="_rowScratch"/>, in output order.</summary>
    private void WriteRow(Entry entry)
    {
        for (int i = 0; i < _outputOrdinals.Length; i++)
        {
            int ordinal = _outputOrdinals[i];
            _rowScratch[i] = ordinal >= 0 ? entry.Cells[ordinal] : CellValue.Null;
        }
    }

    private bool OutputsEqual(Entry left, Entry right)
    {
        foreach (int ordinal in _outputOrdinals)
        {
            if (ordinal >= 0 && ReactiveCellOrdering.Compare(left.Cells[ordinal], right.Cells[ordinal]) != 0)
            {
                return false;
            }
        }

        return true;
    }

    private Entry RentEntry() => _pool.Count > 0 ? _pool.Pop() : new Entry(_layout.Count);

    /// <summary>Indexes a loaded entry, replacing any entry with the same primary key.</summary>
    private void AddEntry(Entry entry)
    {
        entry.Key = PackedKey.From(entry.Cells, _primaryKeyOrdinals);

        // Out-of-place updates can produce a fresh row image for an existing identity; replace.
        if (_byKey.TryGetValue(entry.Key, out Entry? existing))
        {
            Unindex(existing);
        }

        entry.InIndex = true;
        _index.Add(entry);
        _byKey[entry.Key] = entry;
    }

    private void RemoveEntry(IReadOnlyDictionary<string, object?> row)
    {
        _layout.Load(row, _imageScratch);
        PackedKey key = PackedKey.From(_imageScratch, _primaryKeyOrdinals);
        if (_byKey.TryGetValue(key, out Entry? existing))
        {
            _byKey.Remove(key);
            Unindex(existing);
        }
    }

    private void Unindex(Entry entry)
    {
        _index.Remove(entry);
        entry.InIndex = false;
        if (!entry.InWindow)
        {
            _pool.Push(entry);
        }
    }

    // Fills the target window buffers (cleared first) with the first `take` entries after `skip`.
    private void FillWindowInto(List<Entry> target, Dictionary<PackedKey, Entry> targetByKey)
    {
        target.Clear();
        targetByKey.Clear();
        int skipped = 0;

        foreach (Entry entry in _index)
        {
            if (skipped < _skip)
            {
                skipped++;
                continue;
            }

            if (target.Count >= _take)
            {
                break;
            }

            target.Add(entry);
            targetByKey[entry.Key] = entry;
        }
    }

    private static List<string> DistinctColumns(params IEnumerable<string>[] groups)
    {
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var columns = new List<string>();
        foreach (IEnumerable<string> group in groups)
        {
            foreach (string column in group)
            {
                if (seen.Add(column))
                {
                    columns.Add(column);
                }
            }
        }

        return columns;
    }

    private static List<string> ResolveProjection(SelectStatement select, out bool selectStar)
    {
        selectStar = false;
        var projection = new List<string>();
        foreach (SelectColumnNode column in select.Columns)
        {
            if (IsStarColumn(column))
            {
                selectStar = true;
                continue;
            }

            projection.Add(ResolveProjectedColumnName(column));
        }

        selectStar |= projection.Count == 0;
        return projection;
    }

    private sealed class EntryComparer(List<SortKey> sortKeys) : IComparer<Entry>
    {
        public int Compare(Entry? x, Entry? y)
        {
            if (ReferenceEquals(x, y))
            {
                return 0;
            }

            if (x is null || y is null)
            {
                throw new ArgumentNullException(x is null ? nameof(x) : nameof(y));
            }

            foreach (SortKey sortKey in sortKeys)
            {
                CellValue left = sortKey.Ordinal >= 0 ? x.Cells[sortKey.Ordinal] : CellValue.Null;
                CellValue right = sortKey.Ordinal >= 0 ? y.Cells[sortKey.Ordinal] : CellValue.Null;

                int comparison = ReactiveCellOrdering.Compare(left, right);
                if (comparison != 0)
                {
                    return sortKey.Ascending ? comparison : -comparison;
                }
            }

            // Break ties on the stable primary-key identity so distinct rows never collapse and the
            // ordering is deterministic.
            return x.Key.CompareTo(y.Key);
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
