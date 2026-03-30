namespace DataVo.Core.MVCC;

/// <summary>
/// Represents a consistent snapshot of the database at a specific point in time.
/// Used to provide snapshot isolation and repeatable reads in MVCC mode.
/// </summary>
public class TransactionSnapshot
{
    /// <summary>
    /// The transaction timestamp at which this snapshot was taken.
    /// Used to determine which row versions are visible to this transaction.
    /// </summary>
    public long SnapshotTimestamp { get; }

    /// <summary>
    /// The transaction ID that requested this snapshot.
    /// </summary>
    public long TransactionId { get; }

    /// <summary>
    /// When the snapshot was created (for debugging/diagnostics).
    /// </summary>
    public DateTime CreatedAt { get; }

    /// <summary>
    /// Creates a new transaction snapshot.
    /// </summary>
    public TransactionSnapshot(long snapshotTimestamp, long transactionId)
    {
        SnapshotTimestamp = snapshotTimestamp;
        TransactionId = transactionId;
        CreatedAt = DateTime.UtcNow;
    }

    /// <summary>
    /// Determines if a row version should be visible to this snapshot.
    /// Delegates to the RowVersion visibility check.
    /// </summary>
    public bool CanSee(RowVersion version) => version.IsVisibleTo(this);

    /// <summary>
    /// Returns a string representation of the snapshot.
    /// </summary>
    public override string ToString() =>
        $"TransactionSnapshot(txId={TransactionId}, timestamp={SnapshotTimestamp}, createdAt={CreatedAt:O})";
}
