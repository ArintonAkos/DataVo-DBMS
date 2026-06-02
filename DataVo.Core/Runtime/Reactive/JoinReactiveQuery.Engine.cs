using DataVo.Core.Runtime.Changes;

namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// Task 2 stub for the join engine. The arrangements, delta-join, and output diff are implemented in
/// Task 3; until then construction (and projection validation) succeed but seeding/applying throw.
/// </summary>
internal sealed partial class JoinReactiveQuery
{
    private void BuildProjection()
    {
        // Projection compilation is implemented in Task 3.
    }

    private void SeedSide(string table, IEnumerable<(long RowId, IReadOnlyDictionary<string, object?> Row)> rows)
    {
        // No-op until Task 3 wires up the arrangements; routing-only tests seed but never dispatch.
    }

    private QueryChange ApplyDelta(IReadOnlyList<RowChange> changes)
    {
        throw new NotImplementedException("Reactive join maintenance is implemented in Task 3.");
    }
}
