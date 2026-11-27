using DataVo.Core.Enums;
using DataVo.Core.Parser.AST;
using DataVo.Core.Runtime.Changes;

namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// An incrementally maintained aggregate over an N-way inner equi-join (L3): <c>SELECT … FROM T0 JOIN T1 ON … JOIN
/// T2 ON … [WHERE …] GROUP BY …</c> with <c>COUNT</c>/<c>SUM</c>/<c>AVG</c>/<c>MIN</c>/<c>MAX</c>.
/// </summary>
/// <remarks>
/// <para>
/// Each table's rows are kept typed in its catalog layout, bucketed by every column a join condition reads, so a
/// probe costs one hash lookup per joined table. No primary key is required: rows are a bag, and a retracted
/// image removes one stored row with the same values.
/// </para>
/// <para>
/// Every committed row change is applied on its own, in batch order: a before-image is joined against the current
/// rows of the other tables (along a spanning-tree probe plan precomputed for its table) and each joined row that
/// passes the <c>WHERE</c> is subtracted from its group; then the row leaves its table. An after-image enters its
/// table and its joined rows are added. Because each step is an exact single-row update of the join, the maintained
/// groups always equal a re-execution of the query. Group state lives in the shared
/// <see cref="ReactiveGroupAggregator"/>.
/// </para>
/// <para>Self-joins (one table twice) and non-inner joins are rejected.</para>
/// </remarks>
internal sealed class JoinAggregateReactiveQuery : IBorrowedReactiveQuery
{
    /// <summary>One probe of a join plan: bind <see cref="Target"/> rows whose <see cref="TargetOrdinal"/> equals the bound <see cref="From"/> row's <see cref="FromOrdinal"/>.</summary>
    private readonly record struct ProbeStep(int Target, int TargetOrdinal, int From, int FromOrdinal);

    /// <summary>One equality edge of the join graph.</summary>
    private readonly record struct JoinEdge(int LeftTable, int LeftOrdinal, int RightTable, int RightOrdinal);

    private readonly Source[] _sources;
    private readonly ProbeStep[][] _plans;
    private readonly ReactivePredicate _where;
    private readonly ReactiveRowSchema _joinedSchema;
    private readonly ReactiveGroupAggregator _aggregator;

    // Reused per-change scratch: the row bound for each table while joining, and the flattened joined row.
    private readonly CellValue[]?[] _bound;
    private readonly CellValue[] _joined;

    /// <summary>
    /// Compiles a join aggregate SELECT.
    /// </summary>
    /// <param name="select">The parsed SELECT with at least one JOIN and an aggregate shape.</param>
    /// <param name="engine">The owning engine (catalog access for every joined table).</param>
    /// <param name="databaseName">The database that owns the tables.</param>
    /// <exception cref="NotSupportedException">The query is outside the supported subset.</exception>
    public JoinAggregateReactiveQuery(SelectStatement select, DataVoEngine engine, string databaseName)
    {
        Validate(select);

        _sources = BuildSources(select, engine, databaseName);
        List<JoinEdge> edges = BuildEdges(select);
        foreach (JoinEdge edge in edges)
        {
            _sources[edge.LeftTable].EnsureIndex(edge.LeftOrdinal);
            _sources[edge.RightTable].EnsureIndex(edge.RightOrdinal);
        }

        _plans = Enumerable.Range(0, _sources.Length).Select(start => BuildPlan(start, edges)).ToArray();

        var joinedColumns = new List<string>();
        foreach (Source source in _sources)
        {
            joinedColumns.AddRange(source.Layout.Columns.Select(column => JoinedName(source.Index, column)));
        }

        _joinedSchema = new ReactiveRowSchema(joinedColumns);
        _joined = new CellValue[joinedColumns.Count];
        _bound = new CellValue[]?[_sources.Length];
        _where = ReactivePredicate.Compile(select.WhereExpression is null ? null : BindColumns(select.WhereExpression));
        _aggregator = new ReactiveGroupAggregator(select, ResolveJoinedOrdinal);
        Tables = _sources.Select(source => source.Table).ToArray();
    }

    /// <inheritdoc />
    public IReadOnlyCollection<string> Tables { get; }

    /// <inheritdoc />
    public ReactiveRowSchema OutputSchema => _aggregator.OutputSchema;

    /// <inheritdoc />
    /// <remarks>
    /// Seeded rows are inserted one by one exactly like committed inserts, so after the last table is seeded the
    /// groups hold the full join result. The touched groups are then discarded: the baseline is never delivered.
    /// </remarks>
    public void Seed(string table, IEnumerable<TypedRow> rows)
    {
        if (TryGetSource(table, out Source? source))
        {
            foreach (TypedRow row in rows)
            {
                Insert(source!, row);
            }
        }

        _aggregator.DiscardTouched();
    }

    /// <inheritdoc />
    public void ApplyInto(IReadOnlyList<RowChange> changes, QueryChangeBuilder builder)
    {
        for (int i = 0; i < changes.Count; i++)
        {
            RowChange change = changes[i];
            if (!TryGetSource(change.Table, out Source? source))
            {
                continue;
            }

            if (source!.Layout.TryLoadBefore(change, source.Image) && source.TryRemove(source.Image, out CellValue[]? removed))
            {
                Join(source, removed!, weight: -1);
            }

            if (change.Kind != ChangeKind.Delete && change.TypedAfter is { } after)
            {
                Insert(source, after);
            }
        }

        _aggregator.FlushInto(builder);
    }

    private void Insert(Source source, TypedRow row)
    {
        CellValue[] cells = source.Layout.AdoptOrCopy(row);
        source.Add(cells);
        Join(source, cells, weight: +1);
    }

    /// <summary>Feeds every joined row containing <paramref name="row"/> of <paramref name="source"/> to the aggregator.</summary>
    private void Join(Source source, CellValue[] row, int weight)
    {
        _bound[source.Index] = row;
        Probe(_plans[source.Index], 0, weight);
        _bound[source.Index] = null;
    }

    private void Probe(ProbeStep[] plan, int step, int weight)
    {
        if (step == plan.Length)
        {
            EmitJoinedRow(weight);
            return;
        }

        ProbeStep probe = plan[step];
        CellValue value = _bound[probe.From]![probe.FromOrdinal];
        if (value.IsNull || !_sources[probe.Target].TryGetBucket(probe.TargetOrdinal, value, out List<CellValue[]>? bucket))
        {
            return;
        }

        for (int i = 0; i < bucket!.Count; i++)
        {
            _bound[probe.Target] = bucket[i];
            Probe(plan, step + 1, weight);
        }

        _bound[probe.Target] = null;
    }

    private void EmitJoinedRow(int weight)
    {
        for (int i = 0; i < _sources.Length; i++)
        {
            _bound[i].AsSpan().CopyTo(_joined.AsSpan(_sources[i].Offset));
        }

        if (!_where.Matches(new RowRef(_joinedSchema, _joined)))
        {
            return;
        }

        if (weight > 0)
        {
            _aggregator.Add(_joined);
        }
        else
        {
            _aggregator.Remove(_joined);
        }
    }

    private bool TryGetSource(string table, out Source? source)
    {
        foreach (Source candidate in _sources)
        {
            if (candidate.Table.Equals(table, StringComparison.OrdinalIgnoreCase))
            {
                source = candidate;
                return true;
            }
        }

        source = null;
        return false;
    }

    /// <summary>
    /// Orders the joined tables for a change arriving at <paramref name="start"/>: a breadth-first walk of the join
    /// tree, each step probing an unbound table through an edge to an already bound one.
    /// </summary>
    private ProbeStep[] BuildPlan(int start, List<JoinEdge> edges)
    {
        var bound = new HashSet<int> { start };
        var plan = new List<ProbeStep>();
        while (bound.Count < _sources.Length)
        {
            bool progressed = false;
            foreach (JoinEdge edge in edges)
            {
                if (bound.Contains(edge.LeftTable) && !bound.Contains(edge.RightTable))
                {
                    plan.Add(new ProbeStep(edge.RightTable, edge.RightOrdinal, edge.LeftTable, edge.LeftOrdinal));
                    bound.Add(edge.RightTable);
                    progressed = true;
                }
                else if (bound.Contains(edge.RightTable) && !bound.Contains(edge.LeftTable))
                {
                    plan.Add(new ProbeStep(edge.LeftTable, edge.LeftOrdinal, edge.RightTable, edge.RightOrdinal));
                    bound.Add(edge.LeftTable);
                    progressed = true;
                }
            }

            if (!progressed)
            {
                throw new NotSupportedException("Reactive join aggregates require every joined table to be connected by an ON equality.");
            }
        }

        return [.. plan];
    }

    private static Source[] BuildSources(SelectStatement select, DataVoEngine engine, string databaseName)
    {
        var sources = new List<Source>();
        int offset = 0;
        void AddSource(string table, string? alias)
        {
            if (sources.Any(existing => existing.Table.Equals(table, StringComparison.OrdinalIgnoreCase)))
            {
                throw new NotSupportedException($"Reactive join aggregates do not support joining table '{table}' more than once.");
            }

            var source = new Source(sources.Count, table, alias ?? table, ReactiveTableLayouts.ForTable(engine, table, databaseName), offset);
            offset += source.Layout.Count;
            sources.Add(source);
        }

        AddSource(select.FromTable!.Name, select.FromAlias?.Name);
        foreach (JoinDetailNode join in select.Joins)
        {
            AddSource(join.TableName.Name, join.Alias?.Name);
        }

        return [.. sources];
    }

    /// <summary>Resolves each join's ON equality to an edge between the joined table and an earlier one.</summary>
    private List<JoinEdge> BuildEdges(SelectStatement select)
    {
        var edges = new List<JoinEdge>(select.Joins.Count);
        for (int j = 0; j < select.Joins.Count; j++)
        {
            JoinConditionNode condition = select.Joins[j].Condition
                ?? throw new NotSupportedException("Reactive join aggregates require an ON equality for every JOIN.");
            (int leftTable, int leftOrdinal) = ResolveTableColumn(condition.Left.TableOrAlias, condition.Left.Column);
            (int rightTable, int rightOrdinal) = ResolveTableColumn(condition.Right.TableOrAlias, condition.Right.Column);
            int joined = j + 1;
            if (leftTable == rightTable || (leftTable != joined && rightTable != joined) || Math.Max(leftTable, rightTable) != joined)
            {
                throw new NotSupportedException(
                    $"Reactive join aggregates require each ON to equate a column of '{_sources[joined].Table}' with a column of an earlier table.");
            }

            edges.Add(new JoinEdge(leftTable, leftOrdinal, rightTable, rightOrdinal));
        }

        return edges;
    }

    private int ResolveJoinedOrdinal(string? qualifier, string column)
    {
        (int table, int ordinal) = ResolveTableColumn(qualifier, column);
        return _sources[table].Offset + ordinal;
    }

    /// <summary>
    /// Resolves a column reference to (table index, ordinal): by table name or alias when qualified, otherwise to the
    /// one table that has the column.
    /// </summary>
    private (int Table, int Ordinal) ResolveTableColumn(string? qualifier, string column)
    {
        if (!string.IsNullOrWhiteSpace(qualifier))
        {
            foreach (Source source in _sources)
            {
                if (source.Alias.Equals(qualifier, StringComparison.OrdinalIgnoreCase)
                    || source.Table.Equals(qualifier, StringComparison.OrdinalIgnoreCase))
                {
                    return (source.Index, ReactiveTableLayouts.RequireOrdinal(source.Layout, source.Table, source.Alias, null, column));
                }
            }

            throw new NotSupportedException($"Reactive join aggregates cannot resolve '{qualifier}.{column}': no joined table is named '{qualifier}'.");
        }

        int found = -1;
        int foundOrdinal = -1;
        foreach (Source source in _sources)
        {
            int ordinal = source.Layout.OrdinalOf(column);
            if (ordinal < 0)
            {
                continue;
            }

            if (found >= 0)
            {
                throw new NotSupportedException($"Reactive join aggregates cannot resolve column '{column}': it is ambiguous; qualify it with a table name or alias.");
            }

            found = source.Index;
            foundOrdinal = ordinal;
        }

        return found >= 0
            ? (found, foundOrdinal)
            : throw new NotSupportedException($"Reactive join aggregates cannot resolve column '{column}'.");
    }

    /// <summary>
    /// Rewrites the WHERE so every column reference names its joined-row column, letting the shared
    /// <see cref="ReactivePredicate"/> evaluate it over the flattened joined row. Unqualified <c>true</c>/<c>false</c>
    /// tokens that name no column stay literals.
    /// </summary>
    private ExpressionNode BindColumns(ExpressionNode expression) => expression switch
    {
        BinaryExpressionNode binary => new BinaryExpressionNode
        {
            Operator = binary.Operator,
            Left = BindColumns(binary.Left),
            Right = binary.Right is null ? null! : BindColumns(binary.Right),
        },
        ColumnRefNode column when string.IsNullOrWhiteSpace(column.TableOrAlias) && IsBooleanToken(column.Column) && !NamesAnyColumn(column.Column) => column,
        ColumnRefNode column => BoundColumn(column.TableOrAlias, column.Column),
        ResolvedColumnRefNode resolved => BoundColumn(resolved.TableName, resolved.Column),
        _ => expression,
    };

    private ColumnRefNode BoundColumn(string? qualifier, string column)
    {
        (int table, int ordinal) = ResolveTableColumn(qualifier, column);
        return new ColumnRefNode { Column = JoinedName(table, _sources[table].Layout.Columns[ordinal]) };
    }

    private bool NamesAnyColumn(string column) => _sources.Any(source => source.Layout.OrdinalOf(column) >= 0);

    private static bool IsBooleanToken(string token) =>
        token.Equals("true", StringComparison.OrdinalIgnoreCase) || token.Equals("false", StringComparison.OrdinalIgnoreCase);

    private static string JoinedName(int table, string column) => "#" + table + "." + column;

    private static void Validate(SelectStatement select)
    {
        if (select.FromTable is null)
        {
            throw new NotSupportedException("Reactive join aggregates require a FROM table.");
        }

        foreach (JoinDetailNode join in select.Joins)
        {
            if (!JoinTypes.INNER.Equals(join.JoinType, StringComparison.OrdinalIgnoreCase))
            {
                throw new NotSupportedException($"Reactive join aggregates support only INNER joins, not {join.JoinType} JOIN.");
            }
        }

        if (select.Ctes.Count > 0)
        {
            throw new NotSupportedException("Reactive join aggregates do not support common table expressions.");
        }

        if (select.IsDistinct)
        {
            throw new NotSupportedException("Reactive join aggregates do not support DISTINCT.");
        }

        if (select.HavingExpression is not null)
        {
            throw new NotSupportedException("Reactive join aggregates do not support HAVING.");
        }

        if (select.OrderByExpression is not null || select.LimitExpression is not null)
        {
            throw new NotSupportedException("Reactive join aggregates do not support ORDER BY / LIMIT.");
        }

        foreach (SelectColumnNode column in select.Columns)
        {
            if (column.Expression is WindowFunctionExpressionNode)
            {
                throw new NotSupportedException("Reactive join aggregates do not support window functions.");
            }
        }
    }

    /// <summary>One joined table: its layout, its place in the joined row, and its rows bucketed by join column.</summary>
    private sealed class Source(int index, string table, string alias, ReactiveRowLayout layout, int offset)
    {
        // ordinal -> join value -> rows with that value (a bag: equal rows appear once per copy).
        private readonly Dictionary<int, Dictionary<CellValue, List<CellValue[]>>> _indexes = [];
        private int _firstIndexedOrdinal = -1;

        public int Index { get; } = index;

        public string Table { get; } = table;

        public string Alias { get; } = alias;

        public ReactiveRowLayout Layout { get; } = layout;

        public int Offset { get; } = offset;

        /// <summary>Scratch for loading a before-image.</summary>
        public CellValue[] Image { get; } = new CellValue[layout.Count];

        public void EnsureIndex(int ordinal)
        {
            if (!_indexes.ContainsKey(ordinal))
            {
                _indexes[ordinal] = new Dictionary<CellValue, List<CellValue[]>>(CellValueEqualityComparer.Instance);
                _firstIndexedOrdinal = _firstIndexedOrdinal < 0 ? ordinal : _firstIndexedOrdinal;
            }
        }

        public bool TryGetBucket(int ordinal, CellValue value, out List<CellValue[]>? bucket) =>
            _indexes[ordinal].TryGetValue(value, out bucket);

        public void Add(CellValue[] row)
        {
            foreach (KeyValuePair<int, Dictionary<CellValue, List<CellValue[]>>> index in _indexes)
            {
                CellValue key = row[index.Key];
                if (!index.Value.TryGetValue(key, out List<CellValue[]>? bucket))
                {
                    bucket = [];
                    index.Value[key] = bucket;
                }

                bucket.Add(row);
            }
        }

        /// <summary>Removes one stored row equal to <paramref name="image"/> and returns it.</summary>
        public bool TryRemove(CellValue[] image, out CellValue[]? removed)
        {
            removed = null;
            if (!_indexes[_firstIndexedOrdinal].TryGetValue(image[_firstIndexedOrdinal], out List<CellValue[]>? candidates))
            {
                return false;
            }

            foreach (CellValue[] candidate in candidates)
            {
                if (ReactiveCellOrdering.RowsEqual(candidate, image))
                {
                    removed = candidate;
                    break;
                }
            }

            if (removed is null)
            {
                return false;
            }

            foreach (KeyValuePair<int, Dictionary<CellValue, List<CellValue[]>>> index in _indexes)
            {
                CellValue key = removed[index.Key];
                List<CellValue[]> bucket = index.Value[key];
                int at = IndexOfReference(bucket, removed);
                bucket[at] = bucket[^1];
                bucket.RemoveAt(bucket.Count - 1);
                if (bucket.Count == 0)
                {
                    index.Value.Remove(key);
                }
            }

            return true;
        }

        private static int IndexOfReference(List<CellValue[]> bucket, CellValue[] row)
        {
            for (int i = 0; i < bucket.Count; i++)
            {
                if (ReferenceEquals(bucket[i], row))
                {
                    return i;
                }
            }

            return -1;
        }
    }
}
