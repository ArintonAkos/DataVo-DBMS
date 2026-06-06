using DataVo.Core.Parser.AST;
using DataVo.Core.Runtime.Changes;

namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// Incrementally maintained linear <c>WITH RECURSIVE</c> CTE (graph reachability / transitive closure).
/// </summary>
/// <remarks>
/// <para>
/// The maintained CTE relation is the <em>set</em> of distinct tuples reachable by the linear recursion
/// (base arm ∪ repeated application of the recursive arm). Set (distinct) semantics — not bag/UNION ALL
/// multiplicity — is the only terminating interpretation for cyclic graphs and is exactly what the
/// reachability oracle computes, so the fixpoint always terminates.
/// </para>
/// <para>
/// <b>Insertions</b> grow the closure incrementally with a semi-naïve forward pass over the inserted base
/// tuples (no full rescan): newly-derivable tuples are added until no change, emitting <c>Added</c> on
/// <c>absent → present</c>. <b>Deletions and updates</b> are the hardest correctness point for recursive
/// IVM — naïve derivation counting cannot retract tuples whose support runs through a cycle. As the plan
/// authorizes, any batch containing a deletion (an update expands to delete + insert) recomputes the CTE
/// from a fixpoint over the <em>current</em> base contents (read from the operator's own maintained base
/// relation — no storage rescan) and diffs it against the previous output, emitting the exact
/// <c>Added</c>/<c>Removed</c>. Correctness first; this from-scratch-fixpoint deletion fallback is the
/// documented behavior.
/// </para>
/// </remarks>
internal sealed class RecursiveCteReactiveQuery : IReactiveQuery
{
    private readonly RecursiveCteShape _shape;
    private readonly ReactivePredicate _finalPredicate;
    private readonly bool _finalSelectStar;
    private readonly List<string> _finalProjection = [];

    // Primary-key columns per observed base table, used to identify base rows stably across operations.
    // Physical row ids are NOT stable across updates (update = physical delete + reinsert), so the base
    // relation is keyed by primary-key value, not by RowId.
    private readonly Dictionary<string, IReadOnlyList<string>> _basePrimaryKeys =
        new(StringComparer.OrdinalIgnoreCase);

    // Maintained base relation: identity (table + primary-key values) -> base row image.
    private readonly Dictionary<string, (string Table, IReadOnlyDictionary<string, object?> Row)> _baseRows =
        new(StringComparer.Ordinal);

    // Present CTE tuples: tupleKey -> tuple image (set semantics; presence-based).
    private readonly Dictionary<string, IReadOnlyDictionary<string, object?>> _present = new(StringComparer.Ordinal);

    // Emitted (post-final-projection) output rows, keyed by CTE tupleKey, for exact output diffing.
    private readonly Dictionary<string, IReadOnlyDictionary<string, object?>> _emitted = new(StringComparer.Ordinal);

    private bool _seeded;

    public RecursiveCteReactiveQuery(RecursiveCteShape shape, DataVoEngine engine, string databaseName)
    {
        _shape = shape;
        _finalPredicate = ReactivePredicate.Compile(shape.FinalWhere);

        foreach (SelectColumnNode column in shape.FinalColumns)
        {
            if (IsStarColumn(column))
            {
                _finalSelectStar = true;
                continue;
            }

            _finalProjection.Add(ResolveProjectedColumnName(column));
        }

        if (_finalProjection.Count == 0)
        {
            _finalSelectStar = true;
        }

        Tables = _shape.BaseTable.Equals(_shape.RecursiveBaseTable, StringComparison.OrdinalIgnoreCase)
            ? [_shape.BaseTable]
            : [_shape.BaseTable, _shape.RecursiveBaseTable];

        foreach (string table in Tables)
        {
            IReadOnlyList<string> keys = engine.Catalog.GetTablePrimaryKeys(table, databaseName);
            if (keys.Count == 0)
            {
                throw new NotSupportedException(
                    $"Reactive recursive CTE requires a primary key on base table '{table}' to identify rows across updates.");
            }

            _basePrimaryKeys[table] = keys;
        }
    }

    public IReadOnlyCollection<string> Tables { get; }

    public void Seed(string table, IEnumerable<(long RowId, IReadOnlyDictionary<string, object?> Row)> rows)
    {
        foreach ((long _, IReadOnlyDictionary<string, object?> row) in rows)
        {
            StoreBase(table, row);
        }
    }

    public QueryChange Apply(IReadOnlyList<RowChange> changes)
    {
        EnsureSeededClosure();

        bool hasRetraction = false;

        // Inserted rows, partitioned by table, so the forward growth pass can use base-arm and recursive
        // join roles correctly even when the base arm and recursive arm read different tables.
        var insertedByTable = new Dictionary<string, List<IReadOnlyDictionary<string, object?>>>(StringComparer.OrdinalIgnoreCase);

        foreach (RowChange change in changes)
        {
            if (!IsObserved(change.Table))
            {
                continue;
            }

            if (change.Before is not null && change.Kind is ChangeKind.Delete or ChangeKind.Update)
            {
                RemoveBase(change.Table, change.Before);
                hasRetraction = true;
            }

            if (change.After is not null && change.Kind is ChangeKind.Insert or ChangeKind.Update)
            {
                IReadOnlyDictionary<string, object?> image = StoreBase(change.Table, change.After);
                if (!insertedByTable.TryGetValue(change.Table, out List<IReadOnlyDictionary<string, object?>>? list))
                {
                    list = [];
                    insertedByTable[change.Table] = list;
                }

                list.Add(image);
            }
        }

        if (hasRetraction)
        {
            RecomputeClosureFromScratch();
        }
        else if (insertedByTable.Count > 0)
        {
            GrowClosure(insertedByTable);
        }

        return EmitDiff();
    }

    private string BaseIdentity(string table, IReadOnlyDictionary<string, object?> row)
    {
        IReadOnlyList<string> keys = _basePrimaryKeys[table];
        string pk = string.Join("", keys.Select(column => DistinctReactiveQuery.ValueKey(ValueAt(row, column))));
        return table.Length.ToString(System.Globalization.CultureInfo.InvariantCulture) + ":" + table + pk;
    }

    private IReadOnlyDictionary<string, object?> StoreBase(string table, IReadOnlyDictionary<string, object?> row)
    {
        IReadOnlyDictionary<string, object?> image = Copy(row);
        _baseRows[BaseIdentity(table, row)] = (table, image);
        return image;
    }

    private void RemoveBase(string table, IReadOnlyDictionary<string, object?> row)
    {
        _baseRows.Remove(BaseIdentity(table, row));
    }

    private IEnumerable<IReadOnlyDictionary<string, object?>> BaseArmRows() =>
        _baseRows.Values
            .Where(entry => entry.Table.Equals(_shape.BaseTable, StringComparison.OrdinalIgnoreCase))
            .Select(entry => entry.Row);

    private IEnumerable<IReadOnlyDictionary<string, object?>> RecursiveBaseRows() =>
        _baseRows.Values
            .Where(entry => entry.Table.Equals(_shape.RecursiveBaseTable, StringComparison.OrdinalIgnoreCase))
            .Select(entry => entry.Row);

    private void EnsureSeededClosure()
    {
        if (_seeded)
        {
            return;
        }

        _seeded = true;
        RecomputeClosureFromScratch();
        // Establish the emitted baseline without producing output (Subscribe seeds, never re-delivers).
        foreach ((string key, IReadOnlyDictionary<string, object?> tuple) in _present)
        {
            if (_finalPredicate.Matches(tuple))
            {
                _emitted[key] = ProjectFinal(tuple);
            }
        }
    }

    /// <summary>
    /// Recomputes the full closure from a fixpoint over the current base relation, replacing <c>_present</c>.
    /// </summary>
    private void RecomputeClosureFromScratch()
    {
        _present.Clear();

        // Base arm: project each base-arm row to a CTE tuple.
        var frontier = new List<IReadOnlyDictionary<string, object?>>();
        foreach (IReadOnlyDictionary<string, object?> baseRow in BaseArmRows())
        {
            IReadOnlyDictionary<string, object?> tuple = ProjectBaseArm(baseRow);
            if (AddPresent(tuple))
            {
                frontier.Add(tuple);
            }
        }

        Saturate(frontier);
    }

    /// <summary>
    /// Grows the closure after a batch of base-row insertions: newly-inserted base-arm rows seed new CTE
    /// tuples, and newly-inserted recursive-base rows join against existing CTE tuples (the Δbase ⋈ cte
    /// half of the recursive delta). Saturation then closes the result to a fixpoint.
    /// </summary>
    private void GrowClosure(Dictionary<string, List<IReadOnlyDictionary<string, object?>>> insertedByTable)
    {
        var frontier = new List<IReadOnlyDictionary<string, object?>>();

        // New base-arm tuples from inserted rows of the base-arm table.
        if (insertedByTable.TryGetValue(_shape.BaseTable, out List<IReadOnlyDictionary<string, object?>>? baseArmInserts))
        {
            foreach (IReadOnlyDictionary<string, object?> baseRow in baseArmInserts)
            {
                IReadOnlyDictionary<string, object?> tuple = ProjectBaseArm(baseRow);
                if (AddPresent(tuple))
                {
                    frontier.Add(tuple);
                }
            }
        }

        // Existing CTE tuples joined with newly-inserted recursive-base rows.
        if (insertedByTable.TryGetValue(_shape.RecursiveBaseTable, out List<IReadOnlyDictionary<string, object?>>? recursiveInserts))
        {
            foreach (IReadOnlyDictionary<string, object?> baseRow in recursiveInserts)
            {
                object? baseKey = ValueAt(baseRow, _shape.BaseJoinColumn);
                foreach (IReadOnlyDictionary<string, object?> cteTuple in _present.Values.ToArray())
                {
                    if (JoinMatches(cteTuple, baseKey))
                    {
                        IReadOnlyDictionary<string, object?> derived = ProjectRecursiveArm(cteTuple, baseRow);
                        if (AddPresent(derived))
                        {
                            frontier.Add(derived);
                        }
                    }
                }
            }
        }

        Saturate(frontier);
    }

    /// <summary>
    /// Semi-naïve saturation: repeatedly join the growing frontier of CTE tuples against the full base
    /// relation, adding newly-derived tuples, until the frontier is empty (fixpoint).
    /// </summary>
    private void Saturate(List<IReadOnlyDictionary<string, object?>> frontier)
    {
        // Index the recursive-base relation by the recursive join key for efficient probing.
        var baseByJoinKey = new Dictionary<string, List<IReadOnlyDictionary<string, object?>>>(StringComparer.Ordinal);
        foreach (IReadOnlyDictionary<string, object?> baseRow in RecursiveBaseRows())
        {
            string key = DistinctReactiveQuery.ValueKey(ValueAt(baseRow, _shape.BaseJoinColumn));
            if (!baseByJoinKey.TryGetValue(key, out List<IReadOnlyDictionary<string, object?>>? bucket))
            {
                bucket = [];
                baseByJoinKey[key] = bucket;
            }

            bucket.Add(baseRow);
        }

        while (frontier.Count > 0)
        {
            var next = new List<IReadOnlyDictionary<string, object?>>();
            foreach (IReadOnlyDictionary<string, object?> cteTuple in frontier)
            {
                string probe = DistinctReactiveQuery.ValueKey(ValueAt(cteTuple, _shape.CteJoinColumn));
                if (!baseByJoinKey.TryGetValue(probe, out List<IReadOnlyDictionary<string, object?>>? bucket))
                {
                    continue;
                }

                foreach (IReadOnlyDictionary<string, object?> baseRow in bucket)
                {
                    IReadOnlyDictionary<string, object?> derived = ProjectRecursiveArm(cteTuple, baseRow);
                    if (AddPresent(derived))
                    {
                        next.Add(derived);
                    }
                }
            }

            frontier = next;
        }
    }

    private bool JoinMatches(IReadOnlyDictionary<string, object?> cteTuple, object? baseKey)
    {
        object? cteKey = ValueAt(cteTuple, _shape.CteJoinColumn);
        return DistinctReactiveQuery.ValueKey(cteKey) == DistinctReactiveQuery.ValueKey(baseKey);
    }

    private bool AddPresent(IReadOnlyDictionary<string, object?> tuple)
    {
        string key = DistinctReactiveQuery.TupleKey(tuple);
        if (_present.ContainsKey(key))
        {
            return false;
        }

        _present[key] = tuple;
        return true;
    }

    /// <summary>
    /// Diffs the (final-projected, filtered) current CTE relation against the last emitted output.
    /// </summary>
    private QueryChange EmitDiff()
    {
        List<IReadOnlyDictionary<string, object?>> added = [];
        List<IReadOnlyDictionary<string, object?>> removed = [];

        // Current output keyed by CTE tupleKey.
        var current = new Dictionary<string, IReadOnlyDictionary<string, object?>>(StringComparer.Ordinal);
        foreach ((string key, IReadOnlyDictionary<string, object?> tuple) in _present)
        {
            if (_finalPredicate.Matches(tuple))
            {
                current[key] = ProjectFinal(tuple);
            }
        }

        foreach ((string key, IReadOnlyDictionary<string, object?> row) in current)
        {
            if (!_emitted.ContainsKey(key))
            {
                added.Add(row);
            }
        }

        foreach ((string key, IReadOnlyDictionary<string, object?> row) in _emitted)
        {
            if (!current.ContainsKey(key))
            {
                removed.Add(row);
            }
        }

        _emitted.Clear();
        foreach ((string key, IReadOnlyDictionary<string, object?> row) in current)
        {
            _emitted[key] = row;
        }

        return new QueryChange(added, removed, []);
    }

    private IReadOnlyDictionary<string, object?> ProjectBaseArm(IReadOnlyDictionary<string, object?> baseRow)
    {
        var tuple = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < _shape.OutputColumns.Count; i++)
        {
            tuple[_shape.OutputColumns[i]] = ValueAt(baseRow, _shape.BaseSourceColumns[i]);
        }

        return tuple;
    }

    private IReadOnlyDictionary<string, object?> ProjectRecursiveArm(
        IReadOnlyDictionary<string, object?> cteTuple,
        IReadOnlyDictionary<string, object?> baseRow)
    {
        var tuple = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < _shape.OutputColumns.Count; i++)
        {
            RecursiveColumnSource source = _shape.RecursiveColumns[i];
            tuple[_shape.OutputColumns[i]] = source.FromCte
                ? ValueAt(cteTuple, source.SourceColumn)
                : ValueAt(baseRow, source.SourceColumn);
        }

        return tuple;
    }

    private IReadOnlyDictionary<string, object?> ProjectFinal(IReadOnlyDictionary<string, object?> tuple)
    {
        if (_finalSelectStar)
        {
            return Copy(tuple);
        }

        var projected = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (string column in _finalProjection)
        {
            projected[column] = tuple.TryGetValue(column, out object? value) ? value : null;
        }

        return projected;
    }

    private bool IsObserved(string table) =>
        Tables.Any(t => t.Equals(table, StringComparison.OrdinalIgnoreCase));

    private static object? ValueAt(IReadOnlyDictionary<string, object?> row, string column) =>
        row.TryGetValue(column, out object? value) ? value : null;

    private static IReadOnlyDictionary<string, object?> Copy(IReadOnlyDictionary<string, object?> row) =>
        new Dictionary<string, object?>(
            row.ToDictionary(pair => pair.Key, pair => pair.Value),
            StringComparer.OrdinalIgnoreCase);

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

        string raw = column.RawExpression.Trim();
        int dot = raw.LastIndexOf('.');
        return dot >= 0 ? raw[(dot + 1)..] : raw;
    }
}
