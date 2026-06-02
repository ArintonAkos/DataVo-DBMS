namespace DataVo.Core.Runtime.Reactive;

using DataVo.Core.Runtime.Changes;

/// <summary>
/// A compiled, incrementally maintained reactive query operator over one or more source tables.
/// </summary>
/// <remarks>
/// Each subscription is backed by one operator implementation: linear filters (L1), per-group
/// aggregates, maintained top-K (L2), or a two-table equi-join (L3). The operator is seeded once from
/// the current contents of each of its <see cref="Tables"/> and thereafter consumes committed
/// <see cref="RowChange"/> batches, emitting the resulting added/removed/updated rows. All
/// implementations share the stable <c>Subscribe</c> / <c>DispatchPendingNotifications</c> public
/// surface.
/// </remarks>
internal interface IReactiveQuery
{
    /// <summary>Gets the source tables this query observes (one for L1/L2, two for joins).</summary>
    IReadOnlyCollection<string> Tables { get; }

    /// <summary>
    /// Seeds operator state for one of <see cref="Tables"/> from its current contents without emitting
    /// any output.
    /// </summary>
    /// <param name="table">The table whose contents are supplied.</param>
    /// <param name="rows">The current <c>(rowId, row)</c> pairs in <paramref name="table"/>.</param>
    void Seed(string table, IEnumerable<(long RowId, IReadOnlyDictionary<string, object?> Row)> rows);

    /// <summary>
    /// Applies a batch of committed row changes for this query's tables and returns the resulting
    /// added/removed/updated rows.
    /// </summary>
    /// <param name="changes">The committed changes for this query's tables.</param>
    QueryChange Apply(IReadOnlyList<RowChange> changes);
}
