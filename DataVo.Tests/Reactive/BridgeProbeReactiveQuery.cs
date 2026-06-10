using DataVo.Core.Runtime.Changes;
using DataVo.Core.Runtime.Reactive;

namespace DataVo.Tests.Reactive;

/// <summary>
/// Test-only reactive operator that drives the borrowed -> materialized bridge through the registry.
/// On each applied batch it builds a <see cref="QueryChangeRef"/> from a <see cref="QueryChangeBuilder"/>
/// and materializes it at the boundary, mirroring how P2 operators will emit deltas.
/// </summary>
internal sealed class BridgeProbeReactiveQuery : IReactiveQuery
{
    private readonly ReactiveRowSchema _schema = new("Id", "Stake");
    private readonly QueryChangeBuilder _builder;
    private readonly CellValue[] _scratch;

    public BridgeProbeReactiveQuery()
    {
        _builder = new QueryChangeBuilder(_schema);
        _scratch = new CellValue[_schema.ColumnCount];
    }

    public IReadOnlyCollection<string> Tables { get; } = new[] { "Probe" };

    public void Seed(string table, IEnumerable<(long RowId, IReadOnlyDictionary<string, object?> Row)> rows)
    {
        // No baseline state needed for the bridge probe.
    }

    public QueryChange Apply(IReadOnlyList<RowChange> changes)
    {
        _builder.Reset();
        foreach (RowChange change in changes)
        {
            if (change.Kind != ChangeKind.Insert || change.After is null)
            {
                continue;
            }

            // From(object?) is the compatibility boundary; acceptable in this test probe.
            _scratch[0] = CellValue.From(change.After["Id"]);
            _scratch[1] = CellValue.From(change.After["Stake"]);
            _builder.AddAddedRow(_scratch);
        }

        // Borrowed delta materialized at the registry boundary into the owned QueryChange.
        return _builder.Build().Materialize();
    }
}
