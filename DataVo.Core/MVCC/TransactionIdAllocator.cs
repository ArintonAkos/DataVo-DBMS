using System.Threading;

namespace DataVo.Core.MVCC;

/// <summary>
/// Allocates globally unique transaction IDs in strictly increasing order.
/// Uses lock-free <see cref="Interlocked"/> operations for single-ID allocation
/// and a lightweight <see cref="SpinLock"/> for batch range allocation.
/// </summary>
public class TransactionIdAllocator
{
    /// <summary>
    /// The next transaction ID to allocate. Initialized to 1 (0 is reserved for system use).
    /// </summary>
    private long _nextTransactionId = 1;

    /// <summary>
    /// Lightweight spinlock used only by <see cref="AllocateRange"/> which must atomically
    /// advance the counter by an arbitrary stride.
    /// </summary>
    private SpinLock _rangeLock = new(enableThreadOwnerTracking: false);
    private Action<long>? _highWaterMarkObserver;

    /// <summary>
    /// Registers an observer invoked when the high-water mark advances.
    /// </summary>
    public void SetHighWaterMarkObserver(Action<long>? observer)
    {
        _highWaterMarkObserver = observer;
    }

    /// <summary>
    /// Allocates and returns the next transaction ID in sequence.
    /// Lock-free via <see cref="Interlocked.Increment(ref long)"/>.
    /// </summary>
    public long AllocateTransactionId()
    {
        long id = Interlocked.Increment(ref _nextTransactionId) - 1;
        _highWaterMarkObserver?.Invoke(id);
        return id;
    }

    /// <summary>
    /// Allocates and returns a batch of N consecutive transaction IDs.
    /// Uses a <see cref="SpinLock"/> to atomically advance the counter by the requested count.
    /// </summary>
    /// <param name="count">The number of consecutive IDs to allocate. Must be positive.</param>
    /// <returns>The inclusive start and end of the allocated range.</returns>
    public (long Start, long End) AllocateRange(int count)
    {
        if (count <= 0)
        {
            throw new ArgumentException("Count must be positive.", nameof(count));
        }

        bool lockTaken = false;
        try
        {
            _rangeLock.Enter(ref lockTaken);
            long start = _nextTransactionId;
            _nextTransactionId += count;
            long end = _nextTransactionId - 1;
            _highWaterMarkObserver?.Invoke(end);
            return (start, end);
        }
        finally
        {
            if (lockTaken) _rangeLock.Exit();
        }
    }

    /// <summary>
    /// Gets the current highest allocated transaction ID (without allocating a new one).
    /// </summary>
    public long GetCurrentHighWaterMark()
    {
        return Interlocked.Read(ref _nextTransactionId) - 1;
    }

    /// <summary>
    /// Advances the allocator so the next issued ID is at least <paramref name="minimumNextId"/>.
    /// Used during recovery to restore the high-water mark from a persisted value.
    /// </summary>
    /// <param name="minimumNextId">The floor for the next transaction ID.</param>
    public void RestoreHighWaterMark(long minimumNextId)
    {
        SpinWait spinner = default;
        while (true)
        {
            long current = Interlocked.Read(ref _nextTransactionId);
            if (current >= minimumNextId) return;
            if (Interlocked.CompareExchange(ref _nextTransactionId, minimumNextId, current) == current) return;
            spinner.SpinOnce();
        }
    }

    /// <summary>
    /// Resets the allocator to the initial state (for testing only).
    /// </summary>
    public void Reset()
    {
        Interlocked.Exchange(ref _nextTransactionId, 1);
        _highWaterMarkObserver?.Invoke(0);
    }
}

