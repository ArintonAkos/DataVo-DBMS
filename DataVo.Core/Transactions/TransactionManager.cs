using System.Collections.Concurrent;
using DataVo.Core.Logging;
using DataVo.Core.MVCC;

namespace DataVo.Core.Transactions;

/// <summary>
/// Thread-safe singleton managing explicit transaction lifecycles across sessions.
/// <para>
/// Each session (<see cref="Guid"/>) may have at most one active <see cref="TransactionContext"/>.
/// When no explicit transaction is active, the session operates in auto-commit mode where
/// DML operations are applied directly to the storage engine.
/// </para>
/// </summary>
/// <example>
/// <code>
/// transactions.Begin(session);
/// // ... buffered DML operations ...
/// transactions.Commit(session);
/// </code>
/// </example>
public sealed class TransactionManager
{
    private readonly ConcurrentDictionary<Guid, TransactionContext> _activeTransactions = new();

    // Upper bound on _activeTransactions.Count, read without a lock by every statement. It is raised before an
    // add and lowered after a remove, so zero always means no transaction is open. (ConcurrentDictionary.IsEmpty
    // and .Values take every bucket lock when the dictionary is empty, which made each point read contend with
    // every commit.)
    private int _activeCount;

    /// <summary>
    /// Creates a transaction manager instance.
    /// </summary>
    /// <remarks>Each engine owns one instance.</remarks>
    public TransactionManager() { }

    /// <summary>
    /// Opens a new explicit transaction for the given session with MVCC support.
    /// Allocates a transaction ID and creates a snapshot that defines visibility.
    /// Throws if the session already has an active transaction.
    /// </summary>
    /// <param name="session">The unique session identifier.</param>
    /// <param name="idAllocator">The transaction ID allocator for assigning globally unique IDs.</param>
    /// <exception cref="InvalidOperationException">Thrown when a transaction is already active for this session.</exception>
    public void Begin(Guid session, TransactionIdAllocator idAllocator)
    {
        long txId = idAllocator.AllocateTransactionId();
        Register(session, new TransactionSnapshot(snapshotTimestamp: txId, transactionId: txId));
    }

    /// <summary>
    /// Opens a new explicit transaction whose snapshot is ordered by commit: it sees exactly the commits that
    /// finished before it began (see <see cref="CommitClock.BeginSnapshot"/>), never a transaction that was still
    /// open or committing at that point.
    /// </summary>
    /// <param name="session">The unique session identifier.</param>
    /// <param name="clock">The engine's commit clock.</param>
    /// <exception cref="InvalidOperationException">Thrown when a transaction is already active for this session.</exception>
    public void Begin(Guid session, CommitClock clock)
    {
        if (_activeTransactions.ContainsKey(session))
        {
            throw new InvalidOperationException("A transaction is already active for this session.");
        }

        clock.BeginSnapshot(snapshot => Register(session, snapshot), () => TryRemoveActive(session, out _));
    }

    private void Register(Guid session, TransactionSnapshot snapshot)
    {
        var context = new TransactionContext
        {
            TransactionId = snapshot.TransactionId,
            Snapshot = snapshot
        };

        if (!TryAddActive(session, context))
        {
            throw new InvalidOperationException("A transaction is already active for this session.");
        }

        Logger.Info($"Transaction started with ID {snapshot.TransactionId}.");
    }

    /// <summary>
    /// Finalizes the active transaction, returning the buffered context for the caller
    /// to flush changes to the storage engine. Removes the transaction from the active set.
    /// </summary>
    /// <param name="session">The unique session identifier.</param>
    /// <returns>The <see cref="TransactionContext"/> containing all buffered operations.</returns>
    /// <exception cref="InvalidOperationException">Thrown when no active transaction exists for this session.</exception>
    public TransactionContext Commit(Guid session)
    {
        if (!TryRemoveActive(session, out TransactionContext? context))
        {
            throw new InvalidOperationException("No active transaction to commit.");
        }

        Logger.Info("Transaction committed.");
        return context;
    }

    /// <summary>
    /// Starts committing the session's transaction: returns its buffered context but keeps it registered (and its
    /// snapshot counted as open) until <see cref="EndCommit"/>, so the version metadata its conflict checks read is
    /// not vacuumed while it validates and applies.
    /// </summary>
    /// <param name="session">The unique session identifier.</param>
    /// <returns>The <see cref="TransactionContext"/> containing all buffered operations.</returns>
    /// <exception cref="InvalidOperationException">Thrown when no active transaction exists for this session.</exception>
    public TransactionContext BeginCommit(Guid session)
    {
        if (!_activeTransactions.TryGetValue(session, out var context))
        {
            throw new InvalidOperationException("No active transaction to commit.");
        }

        return context;
    }

    /// <summary>Ends the commit started by <see cref="BeginCommit"/>, whether it succeeded or failed.</summary>
    /// <param name="session">The unique session identifier.</param>
    public void EndCommit(Guid session)
    {
        if (TryRemoveActive(session, out _))
        {
            Logger.Info("Transaction committed.");
        }
    }

    /// <summary>
    /// Discards all buffered changes for the active transaction and removes it.
    /// </summary>
    /// <param name="session">The unique session identifier.</param>
    /// <exception cref="InvalidOperationException">Thrown when no active transaction exists for this session.</exception>
    public void Rollback(Guid session)
    {
        if (!TryRemoveActive(session, out _))
        {
            throw new InvalidOperationException("No active transaction to roll back.");
        }

        Logger.Info("Transaction rolled back.");
    }

    /// <summary>
    /// Creates or replaces a named savepoint inside the active transaction for the session.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when no active transaction exists for this session.</exception>
    public void Savepoint(Guid session, string savepointName)
    {
        TransactionContext context = GetRequiredContext(session);
        context.CreateSavepoint(savepointName);
        Logger.Info($"Savepoint '{savepointName}' created.");
    }

    /// <summary>
    /// Restores buffered state to a previously created savepoint while keeping the transaction active.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when no active transaction exists for this session or savepoint is missing.</exception>
    public void RollbackToSavepoint(Guid session, string savepointName)
    {
        TransactionContext context = GetRequiredContext(session);
        context.RollbackToSavepoint(savepointName);
        Logger.Info($"Rolled back to savepoint '{savepointName}'.");
    }

    /// <summary>
    /// Releases a named savepoint from the active transaction.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when no active transaction exists for this session or savepoint is missing.</exception>
    public void ReleaseSavepoint(Guid session, string savepointName)
    {
        TransactionContext context = GetRequiredContext(session);
        context.ReleaseSavepoint(savepointName);
        Logger.Info($"Savepoint '{savepointName}' released.");
    }

    /// <summary>
    /// Determines whether the given session has an active explicit transaction.
    /// </summary>
    /// <param name="session">The unique session identifier.</param>
    /// <returns><c>true</c> if the session is inside a <c>BEGIN</c> block; otherwise, <c>false</c>.</returns>
    public bool HasActiveTransaction(Guid session)
    {
        return _activeTransactions.ContainsKey(session);
    }

    /// <summary>
    /// Determines whether any session currently has an active explicit transaction.
    /// </summary>
    /// <remarks>Lock-free: it runs on every statement.</remarks>
    public bool HasAnyActiveTransaction()
    {
        return Volatile.Read(ref _activeCount) != 0;
    }

    /// <summary>
    /// Determines whether any transaction other than <paramref name="excludedTransactionId"/> is active: whether a
    /// write by that transaction's commit (or an auto-commit statement, with 0) can matter to another snapshot.
    /// </summary>
    /// <param name="excludedTransactionId">The committing transaction's id, or 0.</param>
    public bool HasAnyOtherActiveTransaction(long excludedTransactionId)
    {
        if (Volatile.Read(ref _activeCount) == 0)
        {
            return false;
        }

        foreach (KeyValuePair<Guid, TransactionContext> active in _activeTransactions)
        {
            if (active.Value.TransactionId != excludedTransactionId)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>Gets the oldest open snapshot, or <see langword="null"/> when no transaction is open.</summary>
    /// <returns>The smallest open snapshot timestamp.</returns>
    public long? OldestActiveSnapshot()
    {
        long? oldest = null;
        if (Volatile.Read(ref _activeCount) == 0)
        {
            return null;
        }

        foreach (KeyValuePair<Guid, TransactionContext> active in _activeTransactions)
        {
            TransactionContext context = active.Value;
            long timestamp = context.Snapshot?.SnapshotTimestamp ?? context.TransactionId;
            oldest = oldest is null ? timestamp : Math.Min(oldest.Value, timestamp);
        }

        return oldest;
    }

    /// <summary>
    /// Retrieves the active <see cref="TransactionContext"/> for the session without removing it.
    /// Returns <c>null</c> if no explicit transaction is active (auto-commit mode).
    /// </summary>
    /// <param name="session">The unique session identifier.</param>
    /// <returns>The active context, or <c>null</c> if the session is in auto-commit mode.</returns>
    public TransactionContext? GetContext(Guid session)
    {
        return _activeTransactions.GetValueOrDefault(session);
    }

    /// <summary>Registers an open transaction, raising the open count first so it never reads low.</summary>
    private bool TryAddActive(Guid session, TransactionContext context)
    {
        Interlocked.Increment(ref _activeCount);
        if (_activeTransactions.TryAdd(session, context))
        {
            return true;
        }

        Interlocked.Decrement(ref _activeCount);
        return false;
    }

    /// <summary>Removes an open transaction, lowering the open count only after it is gone.</summary>
    private bool TryRemoveActive(Guid session, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out TransactionContext? context)
    {
        if (!_activeTransactions.TryRemove(session, out context))
        {
            return false;
        }

        Interlocked.Decrement(ref _activeCount);
        return true;
    }

    private TransactionContext GetRequiredContext(Guid session)
    {
        if (!_activeTransactions.TryGetValue(session, out TransactionContext? context))
        {
            throw new InvalidOperationException("No active transaction.");
        }

        return context;
    }
}
