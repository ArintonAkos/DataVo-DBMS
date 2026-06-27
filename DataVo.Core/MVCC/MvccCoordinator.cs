using DataVo.Core.Runtime;

namespace DataVo.Core.MVCC;

/// <summary>
/// Centralizes MVCC read/write lifecycle operations for version metadata.
/// </summary>
public static class MvccCoordinator
{
    private const long LegacyBootstrapXmin = 0;

    /// <summary>
    /// Resolves the transaction identifier for the current statement.
    /// Uses the explicit transaction ID when available; otherwise allocates an auto-commit ID.
    /// </summary>
    public static long ResolveStatementTransactionId(DataVoEngine engine, long? explicitTransactionId)
    {
        if (explicitTransactionId.HasValue && explicitTransactionId.Value > 0)
        {
            return explicitTransactionId.Value;
        }

        return engine.TransactionIdAllocator.AllocateTransactionId();
    }

    /// <summary>
    /// Ensures a row has version metadata. Legacy rows are bootstrapped as always-visible base versions.
    /// </summary>
    public static RowVersion EnsureRowVersionExists(DataVoEngine engine, string databaseName, string tableName, long rowId)
    {
        RowVersion? version = engine.VersionStorageManager.GetVersion(databaseName, tableName, rowId);
        if (version.HasValue)
        {
            return version.Value;
        }

        return engine.VersionStorageManager.AllocateVersion(databaseName, tableName, rowId, LegacyBootstrapXmin);
    }

    /// <summary>
    /// Throws when the target row cannot be modified under MVCC rules.
    /// </summary>
    public static void ValidateCanModifyRow(
        DataVoEngine engine,
        string databaseName,
        string tableName,
        long rowId,
        TransactionSnapshot? snapshot,
        string operationName)
    {
        RowVersion version = EnsureRowVersionExists(engine, databaseName, tableName, rowId);

        if (version.Xmax != 0)
        {
            throw new InvalidOperationException($"MVCC conflict: row {rowId} in {tableName} was already modified/deleted.");
        }

        if (snapshot != null && !SnapshotVisibilityEvaluator.CanUpdateRow(version, snapshot))
        {
            throw new InvalidOperationException($"MVCC conflict: {operationName} cannot modify non-visible row {rowId} in {tableName}.");
        }
    }

    /// <summary>
    /// Registers version metadata for a committed insert.
    /// </summary>
    public static void RegisterInsertVersion(DataVoEngine engine, string databaseName, string tableName, long rowId, long transactionId)
    {
        engine.VersionStorageManager.AllocateVersion(databaseName, tableName, rowId, transactionId);
    }

    /// <summary>
    /// Registers version metadata for a committed insert batch.
    /// </summary>
    public static void RegisterInsertVersions(
        DataVoEngine engine,
        string databaseName,
        string tableName,
        IReadOnlyList<long> rowIds,
        long transactionId)
    {
        engine.VersionStorageManager.AllocateInsertVersions(databaseName, tableName, rowIds, transactionId);
    }

    /// <summary>
    /// Registers version metadata transitions for an update (old version obsoleted, new version linked).
    /// </summary>
    public static void RegisterUpdateVersion(
        DataVoEngine engine,
        string databaseName,
        string tableName,
        long oldRowId,
        long newRowId,
        long transactionId)
    {
        EnsureRowVersionExists(engine, databaseName, tableName, oldRowId);
        engine.VersionStorageManager.MarkVersionObsolete(databaseName, tableName, oldRowId, transactionId);
        engine.VersionStorageManager.AllocateVersion(databaseName, tableName, newRowId, transactionId);
        engine.VersionStorageManager.LinkVersionChain(databaseName, tableName, oldRowId, newRowId);
    }

    /// <summary>
    /// Registers version metadata for a delete operation.
    /// </summary>
    public static void RegisterDeleteVersion(DataVoEngine engine, string databaseName, string tableName, long rowId, long transactionId)
    {
        EnsureRowVersionExists(engine, databaseName, tableName, rowId);
        engine.VersionStorageManager.MarkVersionObsolete(databaseName, tableName, rowId, transactionId);
    }
}
