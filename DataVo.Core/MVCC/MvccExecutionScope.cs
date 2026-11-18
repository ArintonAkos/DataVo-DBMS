using DataVo.Core.Transactions;

namespace DataVo.Core.MVCC;

/// <summary>
/// Ambient execution scope for snapshot-aware MVCC reads.
/// This scope is request/thread-local and is used by storage read APIs.
/// </summary>
public static class MvccExecutionScope
{
    private static readonly AsyncLocal<TransactionSnapshot?> CurrentSnapshotHolder = new();
    private static readonly AsyncLocal<TransactionContext?> CurrentTransactionHolder = new();

    /// <summary>
    /// Gets the current snapshot for the active execution scope, if any.
    /// </summary>
    public static TransactionSnapshot? CurrentSnapshot => CurrentSnapshotHolder.Value;

    /// <summary>Gets the transaction whose uncommitted writes the current read must include.</summary>
    internal static TransactionContext? CurrentTransaction => CurrentTransactionHolder.Value;

    /// <summary>
    /// Pushes a snapshot into the current async context and returns a scope guard.
    /// </summary>
    public static IDisposable PushSnapshot(TransactionSnapshot? snapshot) => Push(snapshot, null, null);

    /// <summary>
    /// Makes reads on this flow use a transaction's snapshot and include its uncommitted writes. While the scope is
    /// open the statement holds the engine's <see cref="SnapshotPublicationGate"/> as a reader, so no write that
    /// publishes versions is half applied while it reads.
    /// </summary>
    /// <param name="transaction">The transaction, or <see langword="null"/> for a plain read.</param>
    /// <returns>A scope that restores the previous state when disposed.</returns>
    internal static IDisposable PushTransaction(TransactionContext? transaction)
    {
        SnapshotPublicationGate? gate = transaction?.Snapshot is null ? null : Runtime.DataVoEngine.Current().CommitClock.Gate;
        gate?.EnterRead();
        return Push(transaction?.Snapshot, transaction, gate);
    }

    private static ScopeGuard Push(TransactionSnapshot? snapshot, TransactionContext? transaction, SnapshotPublicationGate? gate)
    {
        var guard = new ScopeGuard(CurrentSnapshotHolder.Value, CurrentTransactionHolder.Value, gate);
        CurrentSnapshotHolder.Value = snapshot;
        CurrentTransactionHolder.Value = transaction;
        return guard;
    }

    private sealed class ScopeGuard(TransactionSnapshot? snapshot, TransactionContext? transaction, SnapshotPublicationGate? gate) : IDisposable
    {
        private bool _disposed;

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            CurrentSnapshotHolder.Value = snapshot;
            CurrentTransactionHolder.Value = transaction;
            gate?.Exit();
        }
    }
}
