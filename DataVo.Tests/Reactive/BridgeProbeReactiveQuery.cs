using DataVo.Core.Runtime.Changes;
using DataVo.Core.Runtime.Reactive;

namespace DataVo.Tests.Reactive;

/// <summary>
/// Test-only reactive operator that emits each inserted probe row as a borrowed added row, so the registry's
/// central borrowed -> owned materialization can be exercised end to end.
/// </summary>
internal sealed class BridgeProbeReactiveQuery : IBorrowedReactiveQuery
{
    private readonly CellValue[] _scratch = new CellValue[2];

    public IReadOnlyCollection<string> Tables { get; } = new[] { "Probe" };

    public ReactiveRowSchema OutputSchema { get; } = new("Id", "Stake");

    public void Seed(string table, IEnumerable<TypedRow> rows)
    {
        // No baseline state needed for the bridge probe.
    }

    public void ApplyInto(IReadOnlyList<RowChange> changes, QueryChangeBuilder builder)
    {
        foreach (RowChange change in changes)
        {
            if (change.Kind != ChangeKind.Insert || change.TypedAfter is not { } after)
            {
                continue;
            }

            RowRef row = after.AsRowRef();
            _scratch[0] = row["Id"];
            _scratch[1] = row["Stake"];
            builder.AddAddedRow(_scratch);
        }
    }
}
