using DataVo.Core.Runtime.Changes;

namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// A reactive operator that emits each batch's delta as a borrowed <see cref="QueryChangeRef"/> built into a
/// caller-owned <see cref="QueryChangeBuilder"/>. Every SQL-compiled operator implements it: the registry serves
/// zero-allocation subscribers the borrowed delta directly and materializes it into an owned
/// <see cref="QueryChange"/> for everyone else, so operators carry no owned-output path of their own.
/// </summary>
internal interface IBorrowedReactiveQuery : IReactiveQuery
{
    /// <summary>The shared schema of the rows this operator emits (built once).</summary>
    ReactiveRowSchema OutputSchema { get; }

    /// <summary>
    /// Builds this batch's delta into the supplied (already <c>Reset</c>) builder.
    /// </summary>
    /// <param name="changes">The committed changes for this operator's tables.</param>
    /// <param name="builder">The builder receiving the delta.</param>
    void ApplyInto(IReadOnlyList<RowChange> changes, QueryChangeBuilder builder);
}
