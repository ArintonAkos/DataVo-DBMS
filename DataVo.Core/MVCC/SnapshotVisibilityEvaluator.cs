namespace DataVo.Core.MVCC;

/// <summary>
/// Evaluates row version visibility against a transaction snapshot.
/// Provides centralized logic for determining which row versions a transaction can see,
/// enabling snapshot isolation and non-blocking reads in MVCC mode.
/// </summary>
public class SnapshotVisibilityEvaluator
{
    /// <summary>
    /// Determines if a row version is visible to the given snapshot.
    /// A version is visible if it was created before the snapshot's timestamp
    /// and either has no end (xmax=0) or was not deleted by the snapshot's timestamp.
    /// </summary>
    public static bool IsVersionVisible(RowVersion version, TransactionSnapshot snapshot)
    {
        if (snapshot == null)
        {
            throw new ArgumentNullException(nameof(snapshot));
        }

        return version.IsVisibleTo(snapshot);
    }

    /// <summary>
    /// Checks if a row can be updated by a transaction.
    /// A row is updatable if it's visible and not deleted by the current snapshot.
    /// </summary>
    public static bool CanUpdateRow(RowVersion version, TransactionSnapshot snapshot)
    {
        // Must be visible to the transaction
        if (!IsVersionVisible(version, snapshot))
        {
            return false;
        }

        // Must not be marked for deletion (xmax should be 0)
        if (version.Xmax != 0)
        {
            return false;
        }

        return true;
    }
}
