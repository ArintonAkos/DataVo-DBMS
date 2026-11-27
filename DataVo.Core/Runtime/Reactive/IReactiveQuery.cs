namespace DataVo.Core.Runtime.Reactive;

using DataVo.Core.Runtime.Changes;

/// <summary>
/// A compiled, incrementally maintained reactive query operator over one or more source tables.
/// </summary>
/// <remarks>
/// Each subscription is backed by one operator implementation, selected by parsed query shape: linear
/// filters (L1), per-group aggregates and maintained top-K (L2), equi-joins and join aggregates (L3), and the
/// long tail — DISTINCT, UNION/UNION ALL, IN/EXISTS subqueries, and recursive CTEs (L4). The operator is
/// seeded once from the current typed contents of each of its <see cref="Tables"/> (the seed establishes the
/// baseline and is never delivered) and thereafter consumes committed <see cref="RowChange"/> batches.
/// </remarks>
internal interface IReactiveQuery
{
    /// <summary>Gets the source tables this query observes.</summary>
    IReadOnlyCollection<string> Tables { get; }

    /// <summary>
    /// Seeds operator state for one of <see cref="Tables"/> from its current contents without emitting
    /// any output.
    /// </summary>
    /// <param name="table">The table whose contents are supplied.</param>
    /// <param name="rows">The current rows of <paramref name="table"/>. The cells are shared with storage and must never be written.</param>
    void Seed(string table, IEnumerable<TypedRow> rows);
}
