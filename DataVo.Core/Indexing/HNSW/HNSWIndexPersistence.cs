using System.Text.Json;

namespace DataVo.Core.Indexing.HNSW;

/// <summary>
/// Persistence handler for HNSW (vector) indices (JSON serialization format).
/// </summary>
public class HNSWIndexPersistence : IIndexPersistence
{
    /// <summary>
    /// Gets the serialized file extension for vector index files.
    /// </summary>
    public string FileExtension => ".vector.json";

    /// <summary>
    /// Persists a vector index instance.
    /// </summary>
    public void SaveIndex(object index, string filePath)
    {
        if (index is not HNSWIndex hnsw)
            throw new ArgumentException($"Expected HNSWIndex but got {index?.GetType().Name}", nameof(index));

        var directory = Path.GetDirectoryName(filePath);
        if (!string.IsNullOrWhiteSpace(directory))
            Directory.CreateDirectory(directory);

        var payload = new
        {
            IndexType = "HNSW",
            Metric = hnsw.Metric,
            Entries = hnsw.Entries
        };

        string json = JsonSerializer.Serialize(payload, new JsonSerializerOptions 
        { 
            WriteIndented = true 
        });
        
        File.WriteAllText(filePath, json);
    }

    /// <summary>
    /// Loads a vector index instance from storage.
    /// </summary>
    public object LoadIndex(string filePath)
    {
        if (!File.Exists(filePath))
            throw new FileNotFoundException($"Vector index file not found: {filePath}");

        string json = File.ReadAllText(filePath);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        var index = new HNSWIndex();

        // Extract metric
        if (root.TryGetProperty("Metric", out var metricElem) && metricElem.ValueKind == JsonValueKind.String)
            index.Metric = metricElem.GetString() ?? "cosine";

        // Extract entries
        if (root.TryGetProperty("Entries", out var entriesElem) && entriesElem.ValueKind == JsonValueKind.Object)
        {
            foreach (var prop in entriesElem.EnumerateObject())
            {
                if (long.TryParse(prop.Name, out var rowId))
                {
                    if (prop.Value.ValueKind == JsonValueKind.Array)
                    {
                        var vector = new List<float>();
                        foreach (var elem in prop.Value.EnumerateArray())
                        {
                            if (elem.ValueKind == JsonValueKind.Number)
                                vector.Add(elem.GetSingle());
                        }
                        index.Insert(rowId, vector.ToArray());
                    }
                }
            }
        }

        return index;
    }

    /// <summary>
    /// Flushes any pending state for vector index persistence.
    /// </summary>
    public void Flush(object index)
    {
        // JSON-based HNSW has no additional flushing needed beyond Save
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
