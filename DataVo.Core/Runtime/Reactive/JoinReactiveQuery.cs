using DataVo.Core.Parser.AST;
using DataVo.Core.Runtime.Changes;

namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// A two-table reactive equi-join (L3): <c>INNER</c>/<c>LEFT</c>/<c>RIGHT</c>/<c>FULL</c> over one or
/// more AND-ed equality conjuncts, with an optional post-join <c>WHERE</c>.
/// </summary>
/// <remarks>
/// Both sides are maintained as indexed <b>arrangements</b> keyed by the join key and then by the
/// side's primary key, so per-drain work is proportional to the change batch rather than a storage
/// rescan. Each committed <see cref="RowChange"/> is expanded into signed image deltas
/// (Insert→<c>(After,+1)</c>; Delete→<c>(Before,−1)</c>; Update→<c>(Before,−1)</c>,<c>(After,+1)</c>),
/// and the output delta is computed with the DBSP delta-join linearization (probe left deltas against
/// the old right arrangement, apply left deltas, probe right deltas against the new left arrangement,
/// apply right deltas) so the <c>ΔR⋈ΔS</c> cross term is never double-counted. Outer joins additionally
/// track per-key match counts to emit and retract null-padded rows.
/// </remarks>
internal sealed partial class JoinReactiveQuery : IBorrowedReactiveQuery
{
    private readonly JoinShape _shape;
    private readonly ReactivePredicate _where;
    private readonly bool _hasWhere;
    private readonly JoinSide _left;
    private readonly JoinSide _right;
    private readonly JoinWhereContext _whereContext;

    private readonly ReactiveRowSchema _outputSchema;
    private readonly OutputSource[] _outputSources;
    private readonly QueryChangeBuilder _legacyBuilder;
    private readonly CellValue[] _rowScratch;
    private readonly CellValue[] _previousScratch;

    /// <summary>
    /// Compiles the supplied join shape into an incremental equi-join operator.
    /// </summary>
    /// <param name="shape">The validated two-table equi-join shape.</param>
    /// <param name="engine">The owning engine (catalog access for the per-side primary keys).</param>
    /// <param name="databaseName">The database that owns both source tables.</param>
    /// <exception cref="NotSupportedException">
    /// Thrown when either table lacks a primary key, the WHERE is outside the supported subset, or the
    /// projection references unknown qualifiers.
    /// </exception>
    public JoinReactiveQuery(JoinShape shape, DataVoEngine engine, string databaseName)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(shape);
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(engine);

        _shape = shape;
        _where = ReactivePredicate.Compile(shape.Where);
        _hasWhere = shape.Where is not null;

        List<string> leftPrimaryKeys = engine.Catalog.GetTablePrimaryKeys(shape.LeftTable, databaseName);
        List<string> rightPrimaryKeys = engine.Catalog.GetTablePrimaryKeys(shape.RightTable, databaseName);

        if (leftPrimaryKeys.Count == 0)
        {
            throw new NotSupportedException(
                $"Reactive joins require a primary key on table '{shape.LeftTable}' to identify rows across updates.");
        }

        if (rightPrimaryKeys.Count == 0)
        {
            throw new NotSupportedException(
                $"Reactive joins require a primary key on table '{shape.RightTable}' to identify rows across updates.");
        }

        // Each side stores its catalog columns, so SELECT *, unqualified columns and the WHERE context
        // all resolve against the same layout the batch join reads.
        _left = new JoinSide(
            CatalogLayout(engine, shape.LeftTable, databaseName),
            leftPrimaryKeys,
            shape.Keys.Select(key => key.LeftColumn).ToList());
        _right = new JoinSide(
            CatalogLayout(engine, shape.RightTable, databaseName),
            rightPrimaryKeys,
            shape.Keys.Select(key => key.RightColumn).ToList());
        _whereContext = new JoinWhereContext(shape, _left.Layout, _right.Layout);

        BuildProjection();

        // Built once so the borrowed delta has a fixed schema.
        (string[] outputColumns, _outputSources) = BuildOutputs();
        _outputSchema = new ReactiveRowSchema(outputColumns);
        _rowScratch = new CellValue[outputColumns.Length];
        _previousScratch = new CellValue[outputColumns.Length];
        _legacyBuilder = new QueryChangeBuilder(_outputSchema);
    }

    private static ReactiveRowLayout CatalogLayout(DataVoEngine engine, string table, string databaseName) =>
        new(engine.Catalog.GetTableColumns(table, databaseName).Select(column => column.Name).ToList());

    /// <inheritdoc />
    public IReadOnlyCollection<string> Tables => [_shape.LeftTable, _shape.RightTable];

    /// <inheritdoc />
    public void Seed(string table, IEnumerable<(RowKey RowId, IReadOnlyDictionary<string, object?> Row)> rows)
    {
        SeedSide(table, rows);
    }

    /// <inheritdoc />
    public ReactiveRowSchema OutputSchema => _outputSchema;

    /// <summary>Owned path: build the borrowed delta into our own arena, then materialize. Behavior-
    /// identical to the pre-migration <c>Apply</c> (same <see cref="QueryChange"/> shape and values).</summary>
    public QueryChange Apply(IReadOnlyList<RowChange> changes)
    {
        _legacyBuilder.Reset();
        ApplyInto(changes, _legacyBuilder);
        return _legacyBuilder.Build().Materialize();
    }
}
