namespace DataVo.Core.MVCC;

/// <summary>
/// Ambient execution scope for snapshot-aware MVCC reads.
/// This scope is request/thread-local and is used by storage read APIs.
/// </summary>
public static class MvccExecutionScope
{
    private static readonly AsyncLocal<TransactionSnapshot?> CurrentSnapshotHolder = new();

    /// <summary>
    /// Gets the current snapshot for the active execution scope, if any.
    /// </summary>
    public static TransactionSnapshot? CurrentSnapshot => CurrentSnapshotHolder.Value;

    /// <summary>
    /// Pushes a snapshot into the current async context and returns a scope guard.
    /// </summary>
    public static IDisposable PushSnapshot(TransactionSnapshot? snapshot)
    {
        var previous = CurrentSnapshotHolder.Value;
        CurrentSnapshotHolder.Value = snapshot;
        return new ScopeGuard(previous);
    }

    private sealed class ScopeGuard(TransactionSnapshot? previous) : IDisposable
    {
        public void Dispose()
        {
            CurrentSnapshotHolder.Value = previous;
        }
    }
}
