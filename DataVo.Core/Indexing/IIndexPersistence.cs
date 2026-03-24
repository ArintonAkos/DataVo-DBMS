namespace DataVo.Core.Indexing;

/// <summary>
/// Abstraction for persisting and loading index data to/from disk.
/// </summary>
/// <remarks>
/// <para>
/// Each index implementation (BTree, HNSW, etc.) provides its own serialization format.
/// This interface allows <see cref="IndexManager"/> to save and load indices without
/// knowing the underlying format details.
/// </para>
/// <para>
/// Implementations must handle:
/// - Serializing index state to a file
/// - Deserializing index state from a file
/// - Metadata management (index type, version, etc.)
/// </para>
/// </remarks>
public interface IIndexPersistence
{
    /// <summary>
    /// Gets the file extension for this persistence format (e.g., ".json" or ".bin").
    /// </summary>
    string FileExtension { get; }

    /// <summary>
    /// Persists an index to disk.
    /// </summary>
    /// <param name="index">The index to persist. May be any index type.</param>
    /// <param name="filePath">Path where the index should be written.</param>
    void SaveIndex(object index, string filePath);

    /// <summary>
    /// Loads an index from disk.
    /// </summary>
    /// <param name="filePath">Path to the serialized index file.</param>
    /// <returns>The deserialized index instance.</returns>
    object LoadIndex(string filePath);

    /// <summary>
    /// Clears all buffered data for an index (if applicable).
    /// Used when flushing an index to disk.
    /// </summary>
    /// <param name="index">The index to flush.</param>
    void Flush(object index);

    /// <summary>
    /// Checks if an index file exists on disk.
    /// </summary>
    /// <param name="filePath">The path to check.</param>
    /// <returns>True if the file exists; false otherwise.</returns>
    bool FileExists(string filePath);

    /// <summary>
    /// Ensures the target directory exists.
    /// </summary>
    /// <param name="directoryPath">Directory path to create if missing.</param>
    void EnsureDirectory(string directoryPath);

    /// <summary>
    /// Attempts to delete the index file at the provided path.
    /// </summary>
    /// <param name="filePath">The file path to delete.</param>
    /// <returns>True when deletion succeeds or the file does not exist; otherwise false.</returns>
    bool TryDeleteFile(string filePath);

    /// <summary>
    /// Attempts to delete a directory recursively.
    /// </summary>
    /// <param name="directoryPath">The directory path to delete.</param>
    /// <returns>True when deletion succeeds or the directory does not exist; otherwise false.</returns>
    bool TryDeleteDirectory(string directoryPath);
}
