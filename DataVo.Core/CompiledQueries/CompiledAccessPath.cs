namespace DataVo.Core.CompiledQueries;

/// <summary>
/// The access path a compiled query plan should use to locate matching rows. Plans authored by hand or
/// emitted without schema knowledge use <see cref="RuntimeResolve"/>; the source generator may pre-resolve a
/// plan to a faster path at compile time. A pre-resolved path is a bet about runtime state and must fail safe
/// (see <c>DataVoCompiledQuery.TryReadMatchingRowEntries</c>).
/// </summary>
public enum CompiledAccessPath
{
    /// <summary>Resolve the access path at runtime (primary-key / secondary-index / scan). The default.</summary>
    RuntimeResolve = 0,

    /// <summary>Reserved for a future compile-time primary-key fast path. Not emitted by the current generator.</summary>
    PrimaryKey = 1,

    /// <summary>Use the single-column secondary index named by <c>ResolvedIndexName</c>, resolved at compile time.</summary>
    SingleColumnIndex = 2,
}
