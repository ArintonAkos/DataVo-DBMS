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
    /// Ensures a row has version metadata. Legacy rows are bootstrapped as always-visible base versions.
    /// Writers use this before marking a version obsolete; readers use <see cref="GetVersionOrBase"/>, which
    /// records nothing.
    /// </summary>
    public static RowVersion EnsureRowVersionExists(DataVoEngine engine, string databaseName, string tableName, RowKey rowId)
    {
        RowVersion? version = engine.VersionStorageManager.GetVersion(databaseName, tableName, rowId);
        if (version.HasValue)
        {
            return version.Value;
        }

        return engine.VersionStorageManager.AllocateVersion(databaseName, tableName, rowId, LegacyBootstrapXmin);
    }

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
    /// always-visible base version (<see cref="EnsureRowVersionExists"/>) — so auto-commit bulk ingest
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

        EnsureRowVersionExists(engine, databaseName, tableName, oldRowId);
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
        EnsureRowVersionExists(engine, databaseName, tableName, rowId);
        engine.VersionStorageManager.MarkVersionObsolete(databaseName, tableName, rowId, transactionId);
    }

    /// <summary>
    /// Gets whether a read must scan the table instead of using an index. Indexes describe only current rows,
    /// so once a table holds retained images, an index cannot answer for an older snapshot.
    /// </summary>
    /// <param name="engine">The owning engine.</param>
    /// <param name="databaseName">The owning database.</param>
    /// <param name="tableName">The table being read.</param>
    /// <returns><see langword="true"/> when the current snapshot needs a table scan.</returns>
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
    /// Drops retained images and version metadata no open snapshot can need. Call when a transaction ends; the
    /// cost is bounded by the versions recorded since, not by the size of the version map.
    /// </summary>
    /// <param name="engine">The owning engine.</param>
    public static void PruneRetainedImages(DataVoEngine engine)
    {
        long? oldest = engine.TransactionManager.OldestActiveSnapshot();
        engine.VersionStorageManager.PruneImages(oldest);
        engine.VersionStorageManager.VacuumVersions(oldest);
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
