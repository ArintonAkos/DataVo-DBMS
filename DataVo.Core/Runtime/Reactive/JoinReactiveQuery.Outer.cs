namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// Outer-join (LEFT/RIGHT/FULL) null-padding extension. INNER joins never produce null-padded output
/// identities, so for them these hooks are inert. The full match-count tracking is implemented in
/// Task 5; until then a dispatched outer join fails loudly rather than silently behaving like INNER.
/// </summary>
internal sealed partial class JoinReactiveQuery
{
    private bool IsInner => _shape.Kind == JoinKind.Inner;

    private IReadOnlyDictionary<string, object?>? ComputeOuterOutputRow(string leftPk, string rightPk)
    {
        // INNER produces no null-padded identities; the outer implementation (Task 5) overrides this.
        return null;
    }

    private void MaterializeOuterBaseline()
    {
        if (!IsInner)
        {
            throw new NotSupportedException(
                "Outer reactive joins (LEFT/RIGHT/FULL) are not yet available.");
        }
    }

    private void CollectOuterCandidates(
        List<SideDelta> leftDeltas,
        List<SideDelta> rightDeltas,
        HashSet<string> candidates)
    {
        if (!IsInner)
        {
            throw new NotSupportedException(
                "Outer reactive joins (LEFT/RIGHT/FULL) are not yet available.");
        }
    }
}
