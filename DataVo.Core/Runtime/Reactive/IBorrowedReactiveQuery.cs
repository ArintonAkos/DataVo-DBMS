using DataVo.Core.Runtime.Changes;

namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// A reactive operator that can emit its delta as a borrowed <see cref="QueryChangeRef"/> built into a
/// caller-owned <see cref="QueryChangeBuilder"/>, enabling materialization-free delivery on the
/// zero-allocation fast lane. Operators opt in one at a time; the fast lane serves only opted-in shapes.
/// </summary>
internal interface IBorrowedReactiveQuery : IReactiveQuery
{
    /// <summary>The shared schema of the rows this operator emits (built once).</summary>
    ReactiveRowSchema OutputSchema { get; }

    /// <summary>
    /// Builds this batch's delta into the supplied (already <c>Reset</c>) builder. The supported hot
    /// path performs no owned allocation.
    /// </summary>
    void ApplyInto(IReadOnlyList<RowChange> changes, QueryChangeBuilder builder);
}
