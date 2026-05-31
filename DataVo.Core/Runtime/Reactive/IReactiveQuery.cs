namespace DataVo.Core.Runtime.Reactive;

using DataVo.Core.Runtime.Changes;

/// <summary>
/// A compiled, incrementally maintained reactive query operator over a single source table.
/// </summary>
/// <remarks>
/// Each subscription is backed by one operator implementation: linear filters (L1), per-group
/// aggregates, or maintained top-K (L2). The operator is seeded once from the current table contents
/// and thereafter consumes committed <see cref="RowChange"/> batches, emitting the resulting
/// added/removed/updated rows. All implementations share the stable <c>Subscribe</c> /
/// <c>DispatchPendingNotifications</c> public surface.
/// </remarks>
internal interface IReactiveQuery
{
    /// <summary>Gets the source table this query observes.</summary>
    string Table { get; }

    /// <summary>
    /// Seeds operator state from the current table contents without emitting any output.
    /// </summary>
    /// <param name="rows">The current <c>(rowId, row)</c> pairs in the table.</param>
    void Seed(IEnumerable<(long RowId, IReadOnlyDictionary<string, object?> Row)> rows);

    /// <summary>
    /// Applies a batch of committed row changes (already filtered to <see cref="Table"/>) and returns
    /// the resulting added/removed/updated rows.
    /// </summary>
    /// <param name="tableChanges">The committed changes for this query's table.</param>
    QueryChange Apply(IReadOnlyList<RowChange> tableChanges);
}
