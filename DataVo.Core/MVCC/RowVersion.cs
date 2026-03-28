namespace DataVo.Core.MVCC;

/// <summary>
/// Represents version metadata for a row in the database. MVCC uses versions to provide
/// snapshot isolation and non-blocking readers.
/// 
/// xmin: The transaction ID that created (inserted/first-updated) this version.
/// xmax: The transaction ID that deleted or superseded this version (0 if current/valid).
/// version_chain: Index into the version storage for the next version in the chain (0 if terminal).
/// </summary>
public struct RowVersion
{
    /// <summary>
    /// Transaction ID that created this row version.
    /// </summary>
    public long Xmin { get; set; }

    /// <summary>
    /// Transaction ID that ended this row version (deleted or superseded). 0 if still valid.
    /// </summary>
    public long Xmax { get; set; }

    /// <summary>
    /// Pointer to the next version in the chain (row identifier). 0 if no next version.
    /// </summary>
    public long VersionChain { get; set; }

    /// <summary>
    /// Creates a default RowVersion with all fields zero.
    /// </summary>
    public RowVersion() 
    {
        Xmin = 0;
        Xmax = 0;
        VersionChain = 0;
    }

    /// <summary>
    /// Creates a RowVersion with the specified transaction and chain information.
    /// </summary>
    public RowVersion(long xmin, long xmax = 0, long versionChain = 0)
    {
        Xmin = xmin;
        Xmax = xmax;
        VersionChain = versionChain;
    }

    /// <summary>
    /// Checks if this version is visible to a given transaction snapshot.
    /// A version is visible if it was created by a transaction that committed
    /// before the snapshot was taken, and it hasn't been superseded.
    /// </summary>
    public bool IsVisibleTo(TransactionSnapshot snapshot)
    {
        // Versions from future transactions are not visible.
        if (Xmin > snapshot.SnapshotTimestamp)
        {
            return false;
        }

        // Read-your-own-writes: versions created by the current transaction are visible.
        if (Xmin == snapshot.TransactionId)
        {
            return Xmax != snapshot.TransactionId;
        }

        // If xmax is 0, the version is still valid (not deleted/superseded)
        if (Xmax == 0)
        {
            return true;
        }

        // If current transaction deleted/superseded it, it is not visible to itself.
        if (Xmax == snapshot.TransactionId)
        {
            return false;
        }

        // If xmax >= snapshot's read timestamp, it wasn't deleted when snapshot was taken
        if (Xmax > snapshot.SnapshotTimestamp)
        {
            return true;
        }

        // Version was deleted before snapshot was taken
        return false;
    }

    /// <summary>
    /// Returns a string representation of the row version.
    /// </summary>
    public override string ToString() => $"RowVersion(xmin={Xmin}, xmax={Xmax}, chain={VersionChain})";
}
