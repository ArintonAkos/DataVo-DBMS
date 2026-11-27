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
/// <para>
/// <b>Cost trade-off.</b> Insertions are sub-linear (semi-naïve forward growth over only the new base
/// rows). Any batch carrying a retraction is <c>O(closure size)</c> for that batch — a full fixpoint
/// recompute of the CTE over the current base relation — which on cyclic / densely-connected graphs is
/// the dominant cost and is <em>not</em> sub-linear. This is a deliberate correctness-over-speed choice
/// for the hardest case in recursive IVM (retracting support that flows through a cycle).
/// </para>
/// <para>
/// <b>Identity invariants.</b> The CTE is maintained as a <em>set</em> (no bag multiplicity). Base rows
/// are identified by <b>primary key</b> value, never by <see cref="RowChange.RowId"/> — the physical row
/// id is not stable across the engine's out-of-place updates — so every observed base table must declare
/// a primary key; a PK-less base table is rejected at construction with <see cref="NotSupportedException"/>.
/// Base rows and CTE tuples are kept typed (base rows in their table's catalog layout, tuples in the CTE's
/// column order) and keyed by <see cref="PackedKey"/>.
/// </para>
/// </remarks>
internal sealed class RecursiveCteReactiveQuery : IBorrowedReactiveQuery
{
    private readonly RecursiveCteShape _shape;
    private readonly ReactivePredicate _finalPredicate;
    private readonly ReactiveRowSchema _cteSchema;
    private readonly ReactiveProjection _finalProjection;

    private readonly ReactiveRowLayout _baseLayout;
    private readonly ReactiveRowLayout _recursiveLayout;
    private readonly int[] _basePrimaryKey;
    private readonly int[] _recursivePrimaryKey;

    // Base arm: CTE column i comes from base-table ordinal _baseArmSources[i]. Recursive arm: from the CTE
    // tuple (ordinal >= 0 in _recursiveFromCte) or the recursive base row (_recursiveFromBase).
    private readonly int[] _baseArmSources;
    private readonly int[] _recursiveFromCte;
    private readonly int[] _recursiveFromBase;
    private readonly int[] _cteJoin;
    private readonly int[] _baseJoin;
    private readonly int[] _tupleKey;

    // Maintained base relations, keyed by primary key; the same instance when both arms read one table.
    private readonly Dictionary<PackedKey, CellValue[]> _baseRows = [];
    private readonly Dictionary<PackedKey, CellValue[]> _recursiveRows;

    // Present CTE tuples (set semantics) and the delivered final rows, both keyed by the CTE tuple.
    private readonly Dictionary<PackedKey, CellValue[]> _present = [];
    private Dictionary<PackedKey, CellValue[]> _emitted = [];
    private Dictionary<PackedKey, CellValue[]> _spareEmitted = [];

    private readonly CellValue[] _baseImage;
    private readonly CellValue[] _recursiveImage;
    private bool _seeded;

    /// <summary>
    /// Compiles a validated recursive CTE shape.
    /// </summary>
    /// <param name="shape">The parsed recursive CTE shape.</param>
    /// <param name="engine">The owning engine (catalog access for the base tables).</param>
    /// <param name="databaseName">The database that owns the base tables.</param>
    /// <exception cref="NotSupportedException">A base table has no primary key, or a column does not exist.</exception>
    public RecursiveCteReactiveQuery(RecursiveCteShape shape, DataVoEngine engine, string databaseName)
    {
        _shape = shape;
        _finalPredicate = ReactivePredicate.Compile(shape.FinalWhere);
        var cteLayout = new ReactiveRowLayout(shape.OutputColumns);
        _cteSchema = cteLayout.Schema;
        _finalProjection = FinalProjection(shape.FinalColumns, cteLayout);

        bool oneTable = shape.BaseTable.Equals(shape.RecursiveBaseTable, StringComparison.OrdinalIgnoreCase);
        Tables = oneTable ? [shape.BaseTable] : [shape.BaseTable, shape.RecursiveBaseTable];
        _baseLayout = ReactiveTableLayouts.ForTable(engine, shape.BaseTable, databaseName);
        _recursiveLayout = oneTable ? _baseLayout : ReactiveTableLayouts.ForTable(engine, shape.RecursiveBaseTable, databaseName);
        _recursiveRows = oneTable ? _baseRows : [];
        _basePrimaryKey = PrimaryKey(engine, shape.BaseTable, databaseName, _baseLayout);
        _recursivePrimaryKey = oneTable ? _basePrimaryKey : PrimaryKey(engine, shape.RecursiveBaseTable, databaseName, _recursiveLayout);

        _baseArmSources = shape.BaseSourceColumns.Select(column => Require(_baseLayout, shape.BaseTable, column)).ToArray();
        _recursiveFromCte = shape.RecursiveColumns.Select(source => source.FromCte ? Require(cteLayout, shape.CteName, source.SourceColumn) : -1).ToArray();
        _recursiveFromBase = shape.RecursiveColumns.Select(source => source.FromCte ? -1 : Require(_recursiveLayout, shape.RecursiveBaseTable, source.SourceColumn)).ToArray();
        _cteJoin = [Require(cteLayout, shape.CteName, shape.CteJoinColumn)];
        _baseJoin = [Require(_recursiveLayout, shape.RecursiveBaseTable, shape.BaseJoinColumn)];
        _tupleKey = Enumerable.Range(0, shape.OutputColumns.Count).ToArray();

        _baseImage = new CellValue[_baseLayout.Count];
        _recursiveImage = new CellValue[_recursiveLayout.Count];
    }

    /// <inheritdoc />
    public IReadOnlyCollection<string> Tables { get; }

    /// <inheritdoc />
    public ReactiveRowSchema OutputSchema => _finalProjection.Schema;

    /// <inheritdoc />
    /// <remarks>
    /// Seeding only populates the maintained base relations; the closure itself is computed on the first
    /// Apply so the baseline is established but never delivered.
    /// </remarks>
    public void Seed(string table, IEnumerable<TypedRow> rows)
    {
        foreach (TypedRow row in rows)
        {
            StoreBase(table, row);
        }
    }

    /// <inheritdoc />
    public void ApplyInto(IReadOnlyList<RowChange> changes, QueryChangeBuilder builder)
    {
        EnsureSeededClosure();

        bool hasRetraction = false;
        var baseArmInserts = new List<CellValue[]>();
        var recursiveInserts = new List<CellValue[]>();

        for (int i = 0; i < changes.Count; i++)
        {
            RowChange change = changes[i];
            if (change.Kind != ChangeKind.Insert && change.TypedBefore is { } before)
            {
                hasRetraction |= RemoveBase(change.Table, before);
            }

            if (change.Kind != ChangeKind.Delete && change.TypedAfter is { } after)
            {
                (CellValue[]? baseRow, CellValue[]? recursiveRow) = StoreBase(change.Table, after);
                if (baseRow is not null)
                {
                    baseArmInserts.Add(baseRow);
                }

                if (recursiveRow is not null)
                {
                    recursiveInserts.Add(recursiveRow);
                }
            }
        }

        if (hasRetraction)
        {
            RecomputeClosureFromScratch();
        }
        else if (baseArmInserts.Count > 0 || recursiveInserts.Count > 0)
        {
            GrowClosure(baseArmInserts, recursiveInserts);
        }

        EmitDiffInto(builder);
    }

    /// <summary>Stores a base row in the relation(s) its table feeds and returns the stored image per role.</summary>
    private (CellValue[]? BaseRow, CellValue[]? RecursiveRow) StoreBase(string table, TypedRow row)
    {
        CellValue[]? baseRow = null;
        CellValue[]? recursiveRow = null;
        if (table.Equals(_shape.BaseTable, StringComparison.OrdinalIgnoreCase))
        {
            baseRow = _baseLayout.AdoptOrCopy(row);
            _baseRows[PackedKey.From(baseRow, _basePrimaryKey)] = baseRow;
        }

        if (table.Equals(_shape.RecursiveBaseTable, StringComparison.OrdinalIgnoreCase))
        {
            recursiveRow = baseRow ?? _recursiveLayout.AdoptOrCopy(row);
            _recursiveRows[PackedKey.From(recursiveRow, _recursivePrimaryKey)] = recursiveRow;
        }

        return (baseRow, recursiveRow);
    }

    /// <summary>Removes a retracted base row from the relation(s) its table feeds.</summary>
    /// <returns><see langword="true"/> when the table is observed.</returns>
    private bool RemoveBase(string table, TypedRow row)
    {
        bool observed = false;
        if (table.Equals(_shape.BaseTable, StringComparison.OrdinalIgnoreCase) && _baseLayout.TryLoad(row, _baseImage))
        {
            _baseRows.Remove(PackedKey.From(_baseImage, _basePrimaryKey));
            observed = true;
        }

        if (table.Equals(_shape.RecursiveBaseTable, StringComparison.OrdinalIgnoreCase) && _recursiveLayout.TryLoad(row, _recursiveImage))
        {
            _recursiveRows.Remove(PackedKey.From(_recursiveImage, _recursivePrimaryKey));
            observed = true;
        }

        return observed;
    }

    /// <summary>Computes the closure once and records its qualifying rows as the baseline (never delivered).</summary>
    private void EnsureSeededClosure()
    {
        if (_seeded)
        {
            return;
        }

        _seeded = true;
        RecomputeClosureFromScratch();
        CollectCurrentOutput(_emitted);
    }

    /// <summary>Rebuilds the CTE relation as the fixpoint over the current base relations.</summary>
    private void RecomputeClosureFromScratch()
    {
        _present.Clear();
        var frontier = new List<CellValue[]>();
        foreach (CellValue[] baseRow in _baseRows.Values)
        {
            AddDerived(ProjectBaseArm(baseRow), frontier);
        }

        Saturate(frontier);
    }

    /// <summary>Semi-naïve forward growth from newly inserted base rows only.</summary>
    private void GrowClosure(List<CellValue[]> baseArmInserts, List<CellValue[]> recursiveInserts)
    {
        var frontier = new List<CellValue[]>();
        foreach (CellValue[] baseRow in baseArmInserts)
        {
            AddDerived(ProjectBaseArm(baseRow), frontier);
        }

        // Existing CTE tuples joined with newly inserted recursive-base rows.
        if (recursiveInserts.Count > 0)
        {
            CellValue[][] existing = [.. _present.Values];
            foreach (CellValue[] baseRow in recursiveInserts)
            {
                PackedKey baseKey = PackedKey.From(baseRow, _baseJoin);
                foreach (CellValue[] cteTuple in existing)
                {
                    if (PackedKey.From(cteTuple, _cteJoin).Equals(baseKey))
                    {
                        AddDerived(ProjectRecursiveArm(cteTuple, baseRow), frontier);
                    }
                }
            }
        }

        Saturate(frontier);
    }

    /// <summary>Applies the recursive arm to the frontier until no new tuple appears.</summary>
    private void Saturate(List<CellValue[]> frontier)
    {
        var baseByJoinKey = new Dictionary<PackedKey, List<CellValue[]>>();
        foreach (CellValue[] baseRow in _recursiveRows.Values)
        {
            PackedKey key = PackedKey.From(baseRow, _baseJoin);
            if (!baseByJoinKey.TryGetValue(key, out List<CellValue[]>? bucket))
            {
                bucket = [];
                baseByJoinKey[key] = bucket;
            }

            bucket.Add(baseRow);
        }

        while (frontier.Count > 0)
        {
            var next = new List<CellValue[]>();
            foreach (CellValue[] cteTuple in frontier)
            {
                if (!baseByJoinKey.TryGetValue(PackedKey.From(cteTuple, _cteJoin), out List<CellValue[]>? bucket))
                {
                    continue;
                }

                foreach (CellValue[] baseRow in bucket)
                {
                    AddDerived(ProjectRecursiveArm(cteTuple, baseRow), next);
                }
            }

            frontier = next;
        }
    }

    private void AddDerived(CellValue[] tuple, List<CellValue[]> frontier)
    {
        PackedKey key = PackedKey.From(tuple, _tupleKey);
        if (_present.ContainsKey(key))
        {
            return;
        }

        _present[key] = tuple;
        frontier.Add(tuple);
    }

    /// <summary>Diffs the current final output against the delivered one and makes it the delivered one.</summary>
    private void EmitDiffInto(QueryChangeBuilder builder)
    {
        Dictionary<PackedKey, CellValue[]> current = _spareEmitted;
        CollectCurrentOutput(current);

        foreach (KeyValuePair<PackedKey, CellValue[]> row in current)
        {
            if (!_emitted.ContainsKey(row.Key))
            {
                builder.AddAddedRow(row.Value);
            }
        }

        foreach (KeyValuePair<PackedKey, CellValue[]> row in _emitted)
        {
            if (!current.ContainsKey(row.Key))
            {
                builder.AddRemovedRow(row.Value);
            }
        }

        (_emitted, _spareEmitted) = (current, _emitted);
    }

    private void CollectCurrentOutput(Dictionary<PackedKey, CellValue[]> target)
    {
        target.Clear();
        foreach (KeyValuePair<PackedKey, CellValue[]> tuple in _present)
        {
            if (_finalPredicate.Matches(new RowRef(_cteSchema, tuple.Value)))
            {
                var row = new CellValue[_finalProjection.Ordinals.Length];
                _finalProjection.Project(tuple.Value, row);
                target[tuple.Key] = row;
            }
        }
    }

    private CellValue[] ProjectBaseArm(CellValue[] baseRow)
    {
        var tuple = new CellValue[_baseArmSources.Length];
        for (int i = 0; i < tuple.Length; i++)
        {
            tuple[i] = baseRow[_baseArmSources[i]];
        }

        return tuple;
    }

    private CellValue[] ProjectRecursiveArm(CellValue[] cteTuple, CellValue[] baseRow)
    {
        var tuple = new CellValue[_recursiveFromCte.Length];
        for (int i = 0; i < tuple.Length; i++)
        {
            tuple[i] = _recursiveFromCte[i] >= 0 ? cteTuple[_recursiveFromCte[i]] : baseRow[_recursiveFromBase[i]];
        }

        return tuple;
    }

    /// <summary>
    /// Compiles the final SELECT list over the CTE columns; qualifiers name the CTE (or its alias) and are
    /// ignored, and <c>*</c> selects every CTE column.
    /// </summary>
    private static ReactiveProjection FinalProjection(IReadOnlyList<SelectColumnNode> columns, ReactiveRowLayout cteLayout)
    {
        var names = new List<string>();
        var ordinals = new List<int>();
        foreach (SelectColumnNode column in columns)
        {
            if (ReactiveTableLayouts.IsStarColumn(column))
            {
                names.AddRange(cteLayout.Columns);
                ordinals.AddRange(Enumerable.Range(0, cteLayout.Count));
                continue;
            }

            (_, string name) = ReactiveTableLayouts.ColumnReference(column);
            names.Add(column.Alias ?? name);
            ordinals.Add(Require(cteLayout, "the recursive CTE", name));
        }

        if (names.Count == 0)
        {
            names.AddRange(cteLayout.Columns);
            ordinals.AddRange(Enumerable.Range(0, cteLayout.Count));
        }

        return new ReactiveProjection(new ReactiveRowSchema(names), [.. ordinals]);
    }

    private static int[] PrimaryKey(DataVoEngine engine, string table, string databaseName, ReactiveRowLayout layout)
    {
        List<string> keys = engine.Catalog.GetTablePrimaryKeys(table, databaseName);
        if (keys.Count == 0)
        {
            throw new NotSupportedException(
                $"Reactive recursive CTE requires a primary key on base table '{table}' to identify rows across updates.");
        }

        return keys.Select(key => Require(layout, table, key)).ToArray();
    }

    private static int Require(ReactiveRowLayout layout, string owner, string column)
    {
        int ordinal = layout.OrdinalOf(column);
        return ordinal >= 0
            ? ordinal
            : throw new NotSupportedException($"Reactive recursive CTE cannot resolve column '{column}' of {owner}.");
    }
}
