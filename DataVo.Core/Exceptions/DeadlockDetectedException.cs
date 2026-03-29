namespace DataVo.Core.Exceptions;

/// <summary>
/// Raised when the lock manager detects a wait-for cycle that would deadlock.
/// </summary>
public sealed class DeadlockDetectedException : DataVoException
{
    /// <summary>
    /// Initializes a deadlock-detected exception.
    /// </summary>
    /// <param name="lockScope">The lock scope (table/row).</param>
    /// <param name="lockKey">The lock key involved in the deadlock.</param>
    /// <param name="waitingThreadId">The waiting thread identifier.</param>
    /// <param name="blockingThreadIds">Blocking thread identifiers.</param>
    /// <param name="cycleThreadIds">Thread identifiers participating in the wait-for cycle.</param>
    /// <param name="message">The exception message.</param>
    public DeadlockDetectedException(
        string lockScope,
        string lockKey,
        int waitingThreadId,
        IReadOnlyList<int> blockingThreadIds,
        IReadOnlyList<int> cycleThreadIds,
        string message)
        : base(message)
    {
        LockScope = lockScope;
        LockKey = lockKey;
        WaitingThreadId = waitingThreadId;
        BlockingThreadIds = blockingThreadIds;
        CycleThreadIds = cycleThreadIds;
    }

    /// <summary>
    /// Gets the lock scope where deadlock was detected.
    /// </summary>
    public string LockScope { get; }

    /// <summary>
    /// Gets the lock key where deadlock was detected.
    /// </summary>
    public string LockKey { get; }

    /// <summary>
    /// Gets the waiting thread identifier.
    /// </summary>
    public int WaitingThreadId { get; }

    /// <summary>
    /// Gets blocking thread identifiers.
    /// </summary>
    public IReadOnlyList<int> BlockingThreadIds { get; }

    /// <summary>
    /// Gets thread identifiers in the detected cycle.
    /// </summary>
    public IReadOnlyList<int> CycleThreadIds { get; }
}
