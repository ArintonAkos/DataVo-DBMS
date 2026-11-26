using DataVo.Core.StorageEngine;
using DataVo.Core.Runtime;

namespace DataVo.Core.MVCC;

/// <summary>
/// Centralizes MVCC read/write lifecycle operations for version metadata.
/// </summary>
public static class MvccCoordinator
{
    private const long LegacyBootstrapXmin = 0;

    /// <summary>
    /// Resolves the MVCC identifier of the current statement: the transaction's id inside an explicit transaction
    /// (its writes are buffered until COMMIT), otherwise the commit stamp of the statement's write unit
    /// (see <see cref="CommitClock"/>). Taking the stamp fixes whether the statement records versions, so callers
    /// resolve it before they take row or table locks and before they write.
    /// </summary>
    public static long ResolveStatementTransactionId(DataVoEngine engine, long? explicitTransactionId)
    {
        if (explicitTransactionId.HasValue && explicitTransactionId.Value > 0)
        {
            return explicitTransactionId.Value;
        }

        return engine.CommitClock.AcquireStamp();
    }

    /// <summary>
    /// Gets whether the current write unit records MVCC versions: true when a transaction was open when the unit
    /// took its stamp, so some snapshot may need the versions it replaces.
    /// </summary>
    /// <param name="engine">The owning engine.</param>
    /// <returns><see langword="true"/> when versions and retained images must be recorded.</returns>
    public static bool RecordsVersions(DataVoEngine engine) => engine.CommitClock.CurrentUnitRecordsVersions();

    /// <summary>
    /// Gets a row's version metadata, or the always-visible base version when none is recorded, without
    /// recording anything (absent metadata means the row predates every open snapshot).
    /// </summary>
    public static RowVersion GetVersionOrBase(DataVoEngine engine, string databaseName, string tableName, RowKey rowId) =>
        engine.VersionStorageManager.GetVersion(databaseName, tableName, rowId) ?? new RowVersion(LegacyBootstrapXmin);

    /// <summary>
    /// Throws when the target row cannot be modified under MVCC rules.
    /// </summary>
    public static void ValidateCanModifyRow(
        DataVoEngine engine,
        string databaseName,
        string tableName,
        RowKey rowId,
        TransactionSnapshot? snapshot,
        string operationName)
    {
        RowVersion? existing = engine.VersionStorageManager.GetVersion(databaseName, tableName, rowId);
        if (existing is { Xmax: not 0 })
        {
            throw new InvalidOperationException($"MVCC conflict: row {rowId} in {tableName} was already modified/deleted.");
        }

        if (snapshot != null && !SnapshotVisibilityEvaluator.CanUpdateRow(existing ?? new RowVersion(LegacyBootstrapXmin), snapshot))
        {
            throw new InvalidOperationException($"MVCC conflict: {operationName} cannot modify non-visible row {rowId} in {tableName}.");
        }
    }

    /// <summary>
    /// Registers version metadata for a committed insert. Skipped in auto-commit mode with no open
    /// transactions, for the same reason as <see cref="RegisterInsertVersions"/>.
    /// </summary>
    public static void RegisterInsertVersion(DataVoEngine engine, string databaseName, string tableName, RowKey rowId, long transactionId)
    {
        // Version metadata becomes visible with the statement, after its commit record is durable.
        if (DeferredPublication.TryDefer(() => RegisterInsertVersion(engine, databaseName, tableName, rowId, transactionId)))
        {
            return;
        }

        if (!RecordsVersions(engine))
        {
            // No snapshot can need this version. A clustered key may be re-inserted after a delete, so
            // drop whatever an earlier version left behind (its xmax would read as a conflict).
            engine.VersionStorageManager.RemoveVersion(databaseName, tableName, rowId);
            return;
        }

        engine.VersionStorageManager.AllocateVersion(databaseName, tableName, rowId, transactionId);
    }

    /// <summary>
    /// Registers version metadata for a committed insert batch. When no session holds an open
    /// transaction there is no snapshot the metadata could gate — absent metadata is bootstrapped as an
    /// always-visible base version (<see cref="GetVersionOrBase"/>) — so auto-commit bulk ingest
    /// skips the per-row entries entirely.
    /// </summary>
    public static void RegisterInsertVersions(
        DataVoEngine engine,
        string databaseName,
        string tableName,
        IReadOnlyList<RowKey> rowIds,
        long transactionId)
    {
        // Version metadata becomes visible with the statement, after its commit record is durable.
        if (DeferredPublication.Current is not null)
        {
            RowKey[] copy = [.. rowIds];
            DeferredPublication.TryDefer(() => RegisterInsertVersions(engine, databaseName, tableName, copy, transactionId));
            return;
        }

        if (!RecordsVersions(engine))
        {
            engine.VersionStorageManager.RemoveVersions(databaseName, tableName, rowIds); // see RegisterInsertVersion
            return;
        }

        engine.VersionStorageManager.AllocateInsertVersions(databaseName, tableName, rowIds, transactionId);
    }

    /// <summary>
    /// Registers version metadata transitions for an update (old version obsoleted, new version linked).
    /// </summary>
    public static void RegisterUpdateVersion(
        DataVoEngine engine,
        string databaseName,
        string tableName,
        RowKey oldRowId,
        RowKey newRowId,
        long transactionId,
        IReadOnlyDictionary<string, object?>? oldImage = null)
    {
        // Version metadata becomes visible with the statement, after its commit record is durable.
        if (DeferredPublication.TryDefer(() => RegisterUpdateVersion(engine, databaseName, tableName, oldRowId, newRowId, transactionId, oldImage)))
        {
            return;
        }

        if (!RecordsVersions(engine))
        {
            // No snapshot can see either version: keep no metadata (absent reads as always visible).
            engine.VersionStorageManager.RemoveVersion(databaseName, tableName, oldRowId);
            engine.VersionStorageManager.RemoveVersion(databaseName, tableName, newRowId);
            return;
        }

        RetainImage(engine, databaseName, tableName, oldRowId, oldImage, transactionId);

        if (oldRowId == newRowId)
        {
            // A clustered row keeps its key: the new version replaces the old one under the same key.
            engine.VersionStorageManager.AllocateVersion(databaseName, tableName, newRowId, transactionId);
            return;
        }

        engine.VersionStorageManager.MarkVersionObsolete(databaseName, tableName, oldRowId, transactionId);
        engine.VersionStorageManager.AllocateVersion(databaseName, tableName, newRowId, transactionId);
        engine.VersionStorageManager.LinkVersionChain(databaseName, tableName, oldRowId, newRowId);
    }

    /// <summary>
    /// Registers version metadata for a delete operation.
    /// </summary>
    public static void RegisterDeleteVersion(DataVoEngine engine, string databaseName, string tableName, RowKey rowId, long transactionId, IReadOnlyDictionary<string, object?>? oldImage = null)
    {
        // Version metadata becomes visible with the statement, after its commit record is durable.
        if (DeferredPublication.TryDefer(() => RegisterDeleteVersion(engine, databaseName, tableName, rowId, transactionId, oldImage)))
        {
            return;
        }

        if (!RecordsVersions(engine))
        {
            // A deleted row no snapshot can see needs no metadata; keeping it would also make a re-insert
            // of the same clustered key look already deleted.
            engine.VersionStorageManager.RemoveVersion(databaseName, tableName, rowId);
            return;
        }

        RetainImage(engine, databaseName, tableName, rowId, oldImage, transactionId);
        engine.VersionStorageManager.MarkVersionObsolete(databaseName, tableName, rowId, transactionId);
    }

    /// <summary>
    /// Gets whether a non-authoritative index probe must fall back to a full table scan. Authoritative single-column
    /// index lookups use <see cref="FilterUsingIndex"/> instead, which merges retained images and buffered transaction
    /// writes directly without scanning the table.
    /// </summary>
    /// <param name="engine">The owning engine.</param>
    /// <param name="databaseName">The owning database.</param>
    /// <param name="tableName">The table being read.</param>
    /// <returns><see langword="true"/> when a non-authoritative index lookup must scan the table.</returns>
    /// <example>
    /// <code>
    /// if (MvccCoordinator.SnapshotRequiresScan(engine, database, table)) { /* scan */ }
    /// </code>
    /// </example>
    public static bool SnapshotRequiresScan(DataVoEngine engine, string databaseName, string tableName) =>
        MvccExecutionScope.CurrentSnapshot is not null
        && (engine.VersionStorageManager.HasRetainedImages(databaseName, tableName)
            || MvccExecutionScope.CurrentTransaction?.HasBufferedWrites(tableName) == true);

    /// <summary>
    /// Resolves the row identifiers matching an authoritative single-column index lookup under the active
    /// <see cref="MvccExecutionScope"/>, combining committed index entries with visible retained images and the
    /// current transaction's buffered inserts, updates, and deletes.
    /// </summary>
    /// <param name="engine">The owning engine.</param>
    /// <param name="databaseName">The owning database.</param>
    /// <param name="tableName">The table being read.</param>
    /// <param name="indexName">The index covering <paramref name="columnName"/>.</param>
    /// <param name="columnName">The indexed column name.</param>
    /// <param name="key">The canonical encoded index lookup key.</param>
    /// <returns>The matching row identifiers visible to the current execution scope.</returns>
    /// <example>
    /// <code>
    /// List&lt;RowKey&gt; rowIds = MvccCoordinator.FilterUsingIndex(engine, "AppDb", "Users", "_PK_Users", "Id", "[42]");
    /// </code>
    /// </example>
    public static List<RowKey> FilterUsingIndex(
        DataVoEngine engine,
        string databaseName,
        string tableName,
        string indexName,
        string columnName,
        string key)
    {
        IReadOnlyList<RowKey> committed = engine.IndexManager.FilterUsingIndex(key, indexName, tableName, databaseName);
        TransactionSnapshot? snapshot = MvccExecutionScope.CurrentSnapshot;
        if (snapshot is null)
        {
            return [.. committed];
        }

        Transactions.TransactionContext? transaction = MvccExecutionScope.CurrentTransaction;
        bool hasRetainedImages = engine.VersionStorageManager.HasRetainedImages(databaseName, tableName);
        bool hasBufferedWrites = transaction?.HasBufferedWrites(tableName) == true;
        if (!hasRetainedImages && !hasBufferedWrites)
        {
            return FilterVisibleCommittedRows(engine, databaseName, tableName, committed, snapshot);
        }

        Dictionary<RowKey, string?>? retainedKeysByRowId = hasRetainedImages
            ? BuildRetainedImageKeyMap(engine, databaseName, tableName, columnName, snapshot)
            : null;

        var matched = new List<RowKey>(committed.Count + 4);
        var seen = new HashSet<RowKey>();

        AppendCommittedMatches(engine, databaseName, tableName, committed, snapshot, transaction, retainedKeysByRowId, seen, matched);
        AppendRetainedImageMatches(tableName, key, transaction, retainedKeysByRowId, seen, matched);
        AppendBufferedMatches(engine, databaseName, tableName, columnName, key, snapshot, transaction, retainedKeysByRowId, seen, matched);

        return matched;
    }

    private static List<RowKey> FilterVisibleCommittedRows(
        DataVoEngine engine,
        string databaseName,
        string tableName,
        IReadOnlyList<RowKey> committed,
        TransactionSnapshot snapshot)
    {
        var visible = new List<RowKey>(committed.Count);
        for (int i = 0; i < committed.Count; i++)
        {
            RowKey rowId = committed[i];
            if (SnapshotVisibilityEvaluator.IsVersionVisible(GetVersionOrBase(engine, databaseName, tableName, rowId), snapshot))
            {
                visible.Add(rowId);
            }
        }

        return visible;
    }

    private static Dictionary<RowKey, string?>? BuildRetainedImageKeyMap(
        DataVoEngine engine,
        string databaseName,
        string tableName,
        string columnName,
        TransactionSnapshot snapshot)
    {
        List<RetainedRowImage> images = engine.VersionStorageManager.GetVisibleImages(databaseName, tableName, snapshot);
        if (images.Count == 0)
        {
            return null;
        }

        var map = new Dictionary<RowKey, string?>(images.Count);
        for (int i = 0; i < images.Count; i++)
        {
            RetainedRowImage image = images[i];
            map[image.RowId] = BTree.IndexKeyEncoder.TryBuildSingleColumnKey(image.Row, columnName, out string imageKey)
                ? imageKey
                : null;
        }

        return map;
    }

    private static void AppendCommittedMatches(
        DataVoEngine engine,
        string databaseName,
        string tableName,
        IReadOnlyList<RowKey> committed,
        TransactionSnapshot snapshot,
        Transactions.TransactionContext? transaction,
        Dictionary<RowKey, string?>? retainedKeysByRowId,
        HashSet<RowKey> seen,
        List<RowKey> matched)
    {
        for (int i = 0; i < committed.Count; i++)
        {
            RowKey rowId = committed[i];
            if (transaction?.IsDeleted(tableName, rowId) == true)
            {
                continue;
            }

            if (transaction is not null && transaction.TryGetUpdatedRow(tableName, rowId, out _))
            {
                continue;
            }

            if (retainedKeysByRowId is not null && retainedKeysByRowId.ContainsKey(rowId))
            {
                continue;
            }

            if (SnapshotVisibilityEvaluator.IsVersionVisible(GetVersionOrBase(engine, databaseName, tableName, rowId), snapshot)
                && seen.Add(rowId))
            {
                matched.Add(rowId);
            }
        }
    }

    private static void AppendRetainedImageMatches(
        string tableName,
        string key,
        Transactions.TransactionContext? transaction,
        Dictionary<RowKey, string?>? retainedKeysByRowId,
        HashSet<RowKey> seen,
        List<RowKey> matched)
    {
        if (retainedKeysByRowId is null)
        {
            return;
        }

        foreach ((RowKey rowId, string? imageKey) in retainedKeysByRowId)
        {
            if (transaction?.IsDeleted(tableName, rowId) == true)
            {
                continue;
            }

            if (transaction is not null && transaction.TryGetUpdatedRow(tableName, rowId, out _))
            {
                continue;
            }

            if (string.Equals(imageKey, key, StringComparison.Ordinal) && seen.Add(rowId))
            {
                matched.Add(rowId);
            }
        }
    }

    private static void AppendBufferedMatches(
        DataVoEngine engine,
        string databaseName,
        string tableName,
        string columnName,
        string key,
        TransactionSnapshot snapshot,
        Transactions.TransactionContext? transaction,
        Dictionary<RowKey, string?>? retainedKeysByRowId,
        HashSet<RowKey> seen,
        List<RowKey> matched)
    {
        if (transaction is null || !transaction.HasBufferedWrites(tableName))
        {
            return;
        }

        foreach (RowKey rowId in transaction.GetMatchingBufferedRowIds(tableName, columnName, key))
        {
            if (Transactions.TransactionContext.IsBufferedRowKey(rowId))
            {
                if (seen.Add(rowId))
                {
                    matched.Add(rowId);
                }

                continue;
            }

            bool baseVisible = (retainedKeysByRowId is not null && retainedKeysByRowId.ContainsKey(rowId))
                || SnapshotVisibilityEvaluator.IsVersionVisible(GetVersionOrBase(engine, databaseName, tableName, rowId), snapshot);
            if (baseVisible && seen.Add(rowId))
            {
                matched.Add(rowId);
            }
        }
    }

    /// <summary>
    /// Drops retained images and version metadata no open snapshot can need. Call when a transaction ends; the
    /// cost is bounded by the versions recorded since, not by the size of the version map.
    /// </summary>
    /// <param name="engine">The owning engine.</param>
    public static void PruneRetainedImages(DataVoEngine engine)
    {
        // Compute a bounded cutoff under CommitClock so a stalled prune pass can never wipe versions or
        // images belonging to transactions that opened after the snapshot check.
        long cutoff = engine.CommitClock.GetVacuumCutoff(engine.TransactionManager.OldestActiveSnapshot);
        engine.VersionStorageManager.PruneImages(cutoff);
        engine.VersionStorageManager.VacuumVersions(cutoff);
    }

    private static void RetainImage(
        DataVoEngine engine,
        string databaseName,
        string tableName,
        RowKey rowId,
        IReadOnlyDictionary<string, object?>? image,
        long transactionId)
    {
        if (image is null)
        {
            return;
        }

        long xmin = engine.VersionStorageManager.GetVersion(databaseName, tableName, rowId)?.Xmin ?? LegacyBootstrapXmin;
        var copy = new Dictionary<string, object?>(image.Count, StringComparer.OrdinalIgnoreCase);
        foreach (KeyValuePair<string, object?> column in image)
        {
            copy[column.Key] = column.Value;
        }

        engine.VersionStorageManager.RetainImage(databaseName, tableName, rowId, copy, xmin, transactionId);
    }
}
