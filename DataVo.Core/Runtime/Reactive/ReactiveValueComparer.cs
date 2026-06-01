using DataVo.Core.Parser.Utils;

namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// Orders raw column values using the engine's <c>ORDER BY</c> / comparison semantics.
/// </summary>
/// <remarks>
/// Delegates to <see cref="ExpressionValueComparer.Compare"/> so reactive operators sort and rank
/// values exactly as the batch engine does (numeric-aware, then <see cref="IComparable"/>, then
/// ordinal string fallback). <c>null</c> sorts lowest, consistent with the comparer. This single
/// comparer backs both the per-group MIN/MAX multiset and the top-K sorted index.
/// </remarks>
internal sealed class ReactiveValueComparer : IComparer<object?>
{
    /// <summary>Gets the shared comparer instance.</summary>
    public static ReactiveValueComparer Instance { get; } = new();

    /// <inheritdoc />
    public int Compare(object? x, object? y) =>
        ExpressionValueComparer.Compare(x, y, trimQuotedStrings: true);
}
