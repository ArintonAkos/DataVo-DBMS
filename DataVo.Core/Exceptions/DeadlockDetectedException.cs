namespace DataVo.Core.Exceptions;

/// <summary>
/// Raised when the lock manager detects a wait-for cycle that would deadlock.
/// </summary>
public sealed class DeadlockDetectedException : DataVoException
{
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

    public string LockScope { get; }

    public string LockKey { get; }

    public int WaitingThreadId { get; }

    public IReadOnlyList<int> BlockingThreadIds { get; }

    public IReadOnlyList<int> CycleThreadIds { get; }
}
