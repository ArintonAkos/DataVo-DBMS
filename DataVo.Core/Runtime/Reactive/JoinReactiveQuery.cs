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
    private readonly IReadOnlyList<string> _leftPrimaryKeys;
    private readonly IReadOnlyList<string> _rightPrimaryKeys;

    private readonly ReactiveRowSchema _outputSchema;
    private readonly string[] _outputColumns;
    private readonly QueryChangeBuilder _legacyBuilder;
    private readonly CellValue[] _rowScratch;

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
        ArgumentNullException.ThrowIfNull(shape);
        ArgumentNullException.ThrowIfNull(engine);

        _shape = shape;
        _where = ReactivePredicate.Compile(shape.Where);
        _hasWhere = shape.Where is not null;

        _leftPrimaryKeys = engine.Catalog.GetTablePrimaryKeys(shape.LeftTable, databaseName);
        _rightPrimaryKeys = engine.Catalog.GetTablePrimaryKeys(shape.RightTable, databaseName);

        if (_leftPrimaryKeys.Count == 0)
        {
            throw new NotSupportedException(
                $"Reactive joins require a primary key on table '{shape.LeftTable}' to identify rows across updates.");
        }

        if (_rightPrimaryKeys.Count == 0)
        {
            throw new NotSupportedException(
                $"Reactive joins require a primary key on table '{shape.RightTable}' to identify rows across updates.");
        }

        BuildProjection();

        // Output schema: each explicit projection column's output name, or — for SELECT * — both tables'
        // catalog columns qualified as "Table.Column" (matching ProjectStar), followed by the hidden
        // identity columns every joined row carries. Built once so the borrowed delta has a fixed schema.
        var outputColumns = new List<string>();
        foreach (ProjectedColumn column in _projection)
        {
            if (column.IsStar)
            {
                foreach (var c in engine.Catalog.GetTableColumns(_shape.LeftTable, databaseName))
                {
                    outputColumns.Add(_shape.LeftTable + "." + c.Name);
                }

                foreach (var c in engine.Catalog.GetTableColumns(_shape.RightTable, databaseName))
                {
                    outputColumns.Add(_shape.RightTable + "." + c.Name);
                }
            }
            else
            {
                outputColumns.Add(column.OutputName);
            }
        }

        outputColumns.Add(LeftIdentityColumn);
        outputColumns.Add(RightIdentityColumn);
        _outputColumns = outputColumns.ToArray();
        _outputSchema = new ReactiveRowSchema(_outputColumns);
        _rowScratch = new CellValue[_outputColumns.Length];
        _legacyBuilder = new QueryChangeBuilder(_outputSchema);
    }

    /// <inheritdoc />
    public IReadOnlyCollection<string> Tables => [_shape.LeftTable, _shape.RightTable];

    /// <inheritdoc />
    public void Seed(string table, IEnumerable<(long RowId, IReadOnlyDictionary<string, object?> Row)> rows)
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
