using System.Text.Json;
using DataVo.Core.Serialization;

namespace DataVo.Core.Indexing.Flat;

/// <summary>
/// Persistence handler for exact flat vector indices.
/// </summary>
public sealed class FlatVectorIndexPersistence : IIndexPersistence
{
    internal sealed class FlatSnapshot
    {
        public string IndexType { get; set; } = "FLAT";
        public required FlatVectorIndex.FlatVectorState State { get; set; }
    }

    /// <summary>
    /// Gets the serialized file extension for flat vector index files.
    /// </summary>
    public string FileExtension => ".flat.vector.json";

    /// <summary>
    /// Persists a flat vector index instance.
    /// </summary>
    public void SaveIndex(object index, string filePath)
    {
        if (index is not FlatVectorIndex flat)
        {
            throw new ArgumentException($"Expected FlatVectorIndex but got {index?.GetType().Name}", nameof(index));
        }

        var directory = Path.GetDirectoryName(filePath);
        if (!string.IsNullOrWhiteSpace(directory))
        {
            Directory.CreateDirectory(directory);
        }

        var payload = new FlatSnapshot
        {
            IndexType = "FLAT",
            State = flat.ExportFlatState()
        };

        string json = JsonSerializer.Serialize(payload, DataVoJsonContext.Default.FlatSnapshot);
        File.WriteAllText(filePath, json);
    }

    /// <summary>
    /// Loads a flat vector index instance from storage.
    /// </summary>
    public object LoadIndex(string filePath)
    {
        if (!File.Exists(filePath))
        {
            throw new FileNotFoundException($"Flat vector index file not found: {filePath}");
        }

        string json = File.ReadAllText(filePath);
        FlatSnapshot? snapshot = JsonSerializer.Deserialize(json, DataVoJsonContext.Default.FlatSnapshot);
        if (snapshot == null)
        {
            throw new InvalidOperationException($"Failed to deserialize flat vector index payload: {filePath}");
        }

        var index = new FlatVectorIndex();
        index.ImportFlatState(snapshot.State);
        return index;
    }

    /// <summary>
    /// Flushes any pending state for flat vector index persistence.
    /// </summary>
    public void Flush(object index)
    {
        // JSON persistence writes the full snapshot in SaveIndex.
    }

    /// <summary>
    /// Returns whether the backing file exists.
    /// </summary>
    public bool FileExists(string filePath)
    {
        return File.Exists(filePath);
    }

    /// <summary>
    /// Ensures the specified directory exists.
    /// </summary>
    public void EnsureDirectory(string directoryPath)
    {
        if (!string.IsNullOrWhiteSpace(directoryPath))
        {
            Directory.CreateDirectory(directoryPath);
        }
    }

    /// <summary>
    /// Attempts to delete the backing index file.
    /// </summary>
    public bool TryDeleteFile(string filePath)
    {
        try
        {
            if (!File.Exists(filePath))
            {
                return true;
            }

            File.Delete(filePath);
            return true;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// Attempts to delete an index directory recursively.
    /// </summary>
    public bool TryDeleteDirectory(string directoryPath)
    {
        try
        {
            if (!Directory.Exists(directoryPath))
            {
                return true;
            }

            Directory.Delete(directoryPath, recursive: true);
            return true;
        }
        catch
        {
            return false;
        }
    }
}
