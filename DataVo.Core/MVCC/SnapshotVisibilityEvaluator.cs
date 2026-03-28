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
    /// Determines if a row is visible to a transaction by checking its current version.
    /// Returns false if no version exists or the version is not visible.
    /// </summary>
    public static bool IsRowVisible(
        RowVersion? version,
        TransactionSnapshot snapshot)
    {
        if (version == null)
        {
            return false;
        }

        return IsVersionVisible(version.Value, snapshot);
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

    /// <summary>
    /// Checks if a row can be deleted by a transaction.
    /// Same conditions as update: must be visible and not already deleted.
    /// </summary>
    public static bool CanDeleteRow(RowVersion version, TransactionSnapshot snapshot)
    {
        return CanUpdateRow(version, snapshot);
    }

    /// <summary>
    /// Filters a set of row IDs, keeping only those visible to the snapshot.
    /// Used to exclude deleted/future rows from query results.
    /// </summary>
    public static List<long> FilterVisibleRows(
        IEnumerable<long> rowIds,
        VersionStorageManager versionStorage,
        string databaseName,
        string tableName,
        TransactionSnapshot snapshot)
    {
        if (versionStorage == null)
        {
            throw new ArgumentNullException(nameof(versionStorage));
        }

        if (snapshot == null)
        {
            throw new ArgumentNullException(nameof(snapshot));
        }

        var visibleRows = new List<long>();

        foreach (long rowId in rowIds)
        {
            var version = versionStorage.GetVersion(databaseName, tableName, rowId);
            if (IsRowVisible(version, snapshot))
            {
                visibleRows.Add(rowId);
            }
        }

        return visibleRows;
    }
}
