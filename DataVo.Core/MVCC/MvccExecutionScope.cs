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
    public static IDisposable PushSnapshot(TransactionSnapshot? snapshot) => Push(snapshot, null);

    /// <summary>Makes reads on this flow use a transaction's snapshot and include its uncommitted writes.</summary>
    /// <param name="transaction">The transaction, or <see langword="null"/> for a plain read.</param>
    /// <returns>A scope that restores the previous state when disposed.</returns>
    internal static IDisposable PushTransaction(TransactionContext? transaction) => Push(transaction?.Snapshot, transaction);

    private static ScopeGuard Push(TransactionSnapshot? snapshot, TransactionContext? transaction)
    {
        var guard = new ScopeGuard(CurrentSnapshotHolder.Value, CurrentTransactionHolder.Value);
        CurrentSnapshotHolder.Value = snapshot;
        CurrentTransactionHolder.Value = transaction;
        return guard;
    }

    private sealed class ScopeGuard(TransactionSnapshot? snapshot, TransactionContext? transaction) : IDisposable
    {
        public void Dispose()
        {
            CurrentSnapshotHolder.Value = snapshot;
            CurrentTransactionHolder.Value = transaction;
        }
    }
}
