using System.Collections.Concurrent;
using DataVo.Core.Logging;

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
/// TransactionManager.Instance.Begin(session);
/// // ... buffered DML operations ...
/// TransactionManager.Instance.Commit(session);
/// </code>
/// </example>
public sealed class TransactionManager
{
    private static readonly Lazy<TransactionManager> _instance = new(() => new TransactionManager());

    /// <summary>
    /// Gets the global singleton instance of the <see cref="TransactionManager"/>.
    /// </summary>
    public static TransactionManager Instance => _instance.Value;

    private readonly ConcurrentDictionary<Guid, TransactionContext> _activeTransactions = new();

    /// <summary>
    /// Creates a transaction manager instance.
    /// </summary>
    /// <remarks>
    /// The legacy process-wide singleton remains available through <see cref="Instance"/>,
    /// while engine-scoped runtimes can create dedicated instances directly.
    /// </remarks>
    public TransactionManager() { }

    /// <summary>
    /// Opens a new explicit transaction for the given session.
    /// Throws if the session already has an active transaction.
    /// </summary>
    /// <param name="session">The unique session identifier.</param>
    /// <exception cref="InvalidOperationException">Thrown when a transaction is already active for this session.</exception>
    public void Begin(Guid session)
    {
        if (!_activeTransactions.TryAdd(session, new TransactionContext()))
        {
            throw new InvalidOperationException("A transaction is already active for this session.");
        }

        Logger.Info("Transaction started.");
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
        if (!_activeTransactions.TryRemove(session, out var context))
        {
            throw new InvalidOperationException("No active transaction to commit.");
        }

        Logger.Info("Transaction committed.");
        return context;
    }

    /// <summary>
    /// Discards all buffered changes for the active transaction and removes it.
    /// </summary>
    /// <param name="session">The unique session identifier.</param>
    /// <exception cref="InvalidOperationException">Thrown when no active transaction exists for this session.</exception>
    public void Rollback(Guid session)
    {
        if (!_activeTransactions.TryRemove(session, out _))
        {
            throw new InvalidOperationException("No active transaction to roll back.");
        }

        Logger.Info("Transaction rolled back.");
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
    /// Retrieves the active <see cref="TransactionContext"/> for the session without removing it.
    /// Returns <c>null</c> if no explicit transaction is active (auto-commit mode).
    /// </summary>
    /// <param name="session">The unique session identifier.</param>
    /// <returns>The active context, or <c>null</c> if the session is in auto-commit mode.</returns>
    public TransactionContext? GetContext(Guid session)
    {
        return _activeTransactions.GetValueOrDefault(session);
    }
}
