namespace DataVo.Core.StorageEngine.Backends.Abstractions;

/// <summary>
/// Logical backend abstraction for engine storage implementations.
/// </summary>
/// <remarks>
/// This extends the byte-level <see cref="IStorageEngine"/> contract with backend metadata,
/// allowing runtime selection between Disk, InMemory, WASM (OPFS), or custom providers.
/// </remarks>
public interface IStorageBackend : IStorageEngine
{
    /// <summary>
    /// Backend identifier (e.g., "InMemory", "Disk", "Wasm", "Custom").
    /// </summary>
    string BackendKind { get; }
}
