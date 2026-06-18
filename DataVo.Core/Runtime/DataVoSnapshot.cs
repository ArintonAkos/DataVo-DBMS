using DataVo.Core.StorageEngine.Config;
using DataVo.Core.StorageEngine.Memory;

namespace DataVo.Core.Runtime;

/// <summary>
/// Represents an immutable engine snapshot that can be restored into a compatible DataVo engine.
/// </summary>
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

    internal InMemoryStorageSnapshot StorageSnapshot { get; }
}
