using System.Threading;

namespace DataVo.Core.MVCC;

/// <summary>
/// Allocates globally unique transaction IDs in strictly increasing order.
/// Used by MVCC to assign timestamps to transactions and row versions.
/// </summary>
public class TransactionIdAllocator
{
    /// <summary>
    /// The next transaction ID to allocate. Initialized to 1 (0 is reserved for system use).
    /// </summary>
    private long _nextTransactionId = 1;

    /// <summary>
    /// Lock for ensuring thread-safe allocation.
    /// </summary>
    private readonly object _lock = new object();

    /// <summary>
    /// Allocates and returns the next transaction ID in sequence.
    /// IDs are strictly increasing and unique across all allocations.
    /// </summary>
    public long AllocateTransactionId()
    {
        lock (_lock)
        {
            return _nextTransactionId++;
        }
    }

    /// <summary>
    /// Allocates and returns a batch of N consecutive transaction IDs.
    /// Useful for pre-allocating multiple IDs at once.
    /// </summary>
    public (long Start, long End) AllocateRange(int count)
    {
        if (count <= 0)
        {
            throw new ArgumentException("Count must be positive.", nameof(count));
        }

        lock (_lock)
        {
            long start = _nextTransactionId;
            _nextTransactionId += count;
            return (start, _nextTransactionId - 1);
        }
    }

    /// <summary>
    /// Gets the current highest allocated transaction ID (without allocating a new one).
    /// </summary>
    public long GetCurrentHighWaterMark()
    {
        lock (_lock)
        {
            return _nextTransactionId - 1;
        }
    }

    /// <summary>
    /// Resets the allocator to the initial state (for testing only).
    /// </summary>
    public void Reset()
    {
        lock (_lock)
        {
            _nextTransactionId = 1;
        }
    }
}
