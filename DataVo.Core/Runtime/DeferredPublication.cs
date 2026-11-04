namespace DataVo.Core.Runtime;

/// <summary>
/// Holds back in-memory effects of a statement until its commit-log record is durable. While a durable
/// (Lsm) write batch is open on the current flow, index updates, MVCC registrations, and change
/// notifications are queued here instead of being applied, so other sessions never observe a change
/// that a crash could still undo. Write-lock releases are deferred as well, so a later statement on the
/// same table waits until this one is published.
/// </summary>
/// <remarks>
/// Publish actions run once the batch is durable, in commit order. Finally actions (lock releases) always
/// run, also when the statement fails and its pending work is discarded. Both run on the flow that queued
/// them: the lock manager's locks are thread-affine. Without an open batch (in-memory and Disk modes)
/// nothing is deferred.
/// </remarks>
internal sealed class DeferredPublication
{
    private static readonly AsyncLocal<DeferredPublication?> CurrentPublication = new();

    private readonly List<Action> _publish = [];
    private readonly List<Action> _finally = [];

    // Locks this flow still holds because their release is deferred, with the number of nested
    // "virtual" re-acquisitions that must be matched by no-op releases.
    private readonly Dictionary<string, int> _heldWriteLocks = new(StringComparer.Ordinal);

    /// <summary>The publication of the current flow's open durable write batch, if any.</summary>
    internal static DeferredPublication? Current => CurrentPublication.Value;

    internal static void SetCurrent(DeferredPublication? publication) => CurrentPublication.Value = publication;

    /// <summary>Queues <paramref name="action"/> until publication; returns false when nothing is deferred.</summary>
    internal static bool TryDefer(Action action)
    {
        DeferredPublication? current = CurrentPublication.Value;
        if (current is null)
        {
            return false;
        }

        current._publish.Add(action);
        return true;
    }

    internal bool HasPublishActions => _publish.Count > 0;

    /// <summary>
    /// Called before acquiring a write or read lock on <paramref name="lockKey"/>. Returns true when this
    /// flow already holds the write lock (its release is only deferred), so the acquisition is virtual.
    /// </summary>
    internal bool TryReacquireHeldLock(string lockKey)
    {
        if (!_heldWriteLocks.TryGetValue(lockKey, out int nested))
        {
            return false;
        }

        _heldWriteLocks[lockKey] = nested + 1;
        return true;
    }

    /// <summary>
    /// Called when releasing a lock. Returns true when the release was handled here: either it matches a
    /// virtual acquisition (no-op), or it is a write-lock release that is deferred via <paramref name="release"/>.
    /// </summary>
    internal bool TryHandleRelease(string lockKey, bool write, Action release)
    {
        if (_heldWriteLocks.TryGetValue(lockKey, out int nested))
        {
            if (nested > 0)
            {
                _heldWriteLocks[lockKey] = nested - 1;
                return true;
            }

            // Releasing the real acquisition while it is already deferred cannot happen; keep it held.
            return true;
        }

        if (!write)
        {
            return false;
        }

        _heldWriteLocks[lockKey] = 0;
        _finally.Add(() =>
        {
            _heldWriteLocks.Remove(lockKey);
            release();
        });
        return true;
    }

    /// <summary>Runs every publish action in queue order. Called once the batch is durable.</summary>
    internal void RunPublish()
    {
        foreach (Action action in _publish)
        {
            action();
        }

        _publish.Clear();
    }

    /// <summary>Discards publish actions (the statement failed and is rolled back).</summary>
    internal void DiscardPublish() => _publish.Clear();

    /// <summary>Runs every finally action (lock releases) in reverse acquisition order.</summary>
    internal void RunFinally()
    {
        List<Exception>? errors = null;
        for (int i = _finally.Count - 1; i >= 0; i--)
        {
            try
            {
                _finally[i]();
            }
            catch (Exception ex)
            {
                (errors ??= []).Add(ex);
            }
        }

        _finally.Clear();
        _heldWriteLocks.Clear();
        if (errors is not null)
        {
            throw new AggregateException(errors);
        }
    }
}
