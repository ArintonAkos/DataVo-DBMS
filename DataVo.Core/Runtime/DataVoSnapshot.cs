using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.StorageEngine.Memory;

namespace DataVo.Core.Runtime;

/// <summary>
/// Represents an immutable engine snapshot that can be restored into a compatible DataVo engine.
/// </summary>
/// <remarks>
/// An in-memory snapshot captures the whole storage image and catalog, so restoring it also undoes schema changes.
/// An LSM snapshot captures the catalog and every table's rows; restoring it rewrites all tables in one durable write
/// and requires the schema to be unchanged since the snapshot was taken.
/// </remarks>
public sealed class DataVoSnapshot
{
    internal DataVoSnapshot(
        StorageMode storageMode,
        string catalogState,
        string? selectedDatabase,
        InMemoryStorageSnapshot storageSnapshot)
    {
        StorageMode = storageMode;
        CatalogState = catalogState;
        SelectedDatabase = selectedDatabase;
        StorageSnapshot = storageSnapshot;
    }

    internal DataVoSnapshot(
        StorageMode storageMode,
        string catalogState,
        string? selectedDatabase,
        IReadOnlyList<SnapshotTableRows> tableRows)
    {
        StorageMode = storageMode;
        CatalogState = catalogState;
        SelectedDatabase = selectedDatabase;
        TableRows = tableRows;
    }

    /// <summary>
    /// Gets the storage mode used when the snapshot was captured.
    /// </summary>
    public StorageMode StorageMode { get; }

    /// <summary>
    /// Gets the serialized catalog state captured with the snapshot.
    /// </summary>
    public string CatalogState { get; }

    /// <summary>
    /// Gets the selected database for the capturing session, if one was selected.
    /// </summary>
    public string? SelectedDatabase { get; }

    /// <summary>Gets the in-memory storage image, or <c>null</c> for a snapshot of durable storage.</summary>
    internal InMemoryStorageSnapshot? StorageSnapshot { get; }

    /// <summary>Gets every table's rows for a snapshot of durable storage, or <c>null</c> for an in-memory snapshot.</summary>
    internal IReadOnlyList<SnapshotTableRows>? TableRows { get; }
}

/// <summary>The rows of one table captured by a durable-storage snapshot, in the table's catalog column order.</summary>
/// <param name="Database">The owning database.</param>
/// <param name="Table">The stored table name.</param>
/// <param name="Schema">The column names of <paramref name="Rows"/>' cells.</param>
/// <param name="Rows">The rows; never written after capture.</param>
internal sealed record SnapshotTableRows(string Database, string Table, ReactiveRowSchema Schema, IReadOnlyList<CellValue[]> Rows);
