using System.Text.Json;
using DataVo.Core.Serialization;

namespace DataVo.Core.Indexing.HNSW;

/// <summary>
/// Persistence handler for HNSW (vector) indices (JSON serialization format).
/// </summary>
public class HNSWIndexPersistence : IIndexPersistence
{
    internal sealed class HnswSnapshot
    {
        public string IndexType { get; set; } = "HNSW";
        public required HNSWIndex.FlatState State { get; set; }
    }

    internal sealed class FallbackSnapshot
    {
        public string IndexType { get; set; } = "HNSW";
        public string Metric { get; set; } = "cosine";
        public List<FallbackEntry> Entries { get; set; } = [];
    }

    internal sealed class FallbackEntry
    {
        public long RowId { get; set; }
        public float[] Vector { get; set; } = [];
    }

    /// <summary>
    /// Gets the serialized file extension for vector index files.
    /// </summary>
    public string FileExtension => ".vector.json";

    /// <summary>
    /// Persists a vector index instance.
    /// </summary>
    public void SaveIndex(object index, string filePath)
    {
        var directory = Path.GetDirectoryName(filePath);
        if (!string.IsNullOrWhiteSpace(directory))
            Directory.CreateDirectory(directory);

        if (index is BrowserFallbackVectorIndex fallback)
        {
            var payload = new FallbackSnapshot
            {
                IndexType = "HNSW",
                Metric = fallback.Metric,
                Entries = fallback
                    .ExportEntries()
                    .Select(entry => new FallbackEntry { RowId = entry.RowId, Vector = entry.Vector })
                    .ToList()
            };

            string fallbackJson = JsonSerializer.Serialize(payload, DataVoJsonContext.Default.FallbackSnapshot);
            File.WriteAllText(filePath, fallbackJson);
            return;
        }

        if (index is not HNSWIndex hnsw)
            throw new ArgumentException($"Expected HNSWIndex or BrowserFallbackVectorIndex but got {index?.GetType().Name}", nameof(index));

        var hnswPayload = new HnswSnapshot
        {
            IndexType = "HNSW",
            State = hnsw.ExportFlatState()
        };

        string json = JsonSerializer.Serialize(hnswPayload, DataVoJsonContext.Default.HnswSnapshot);

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

        if (IsBrowserRuntime())
        {
            FallbackSnapshot? fallbackSnapshot = JsonSerializer.Deserialize(json, DataVoJsonContext.Default.FallbackSnapshot);
            if (fallbackSnapshot != null)
            {
                var fallback = new BrowserFallbackVectorIndex
                {
                    Metric = string.IsNullOrWhiteSpace(fallbackSnapshot.Metric)
                        ? "cosine"
                        : fallbackSnapshot.Metric
                };

                fallback.ImportEntries(fallbackSnapshot.Entries.Select(entry => (entry.RowId, entry.Vector)));
                return fallback;
            }
        }

        HnswSnapshot? snapshot = JsonSerializer.Deserialize(json, DataVoJsonContext.Default.HnswSnapshot);
        if (snapshot == null)
            throw new InvalidOperationException($"Failed to deserialize HNSW index payload: {filePath}");

        if (snapshot.State == null)
        {
            throw new InvalidOperationException($"HNSW snapshot does not contain flat state payload: {filePath}");
        }

        var index = new HNSWIndex();
        index.ImportFlatState(snapshot.State);

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

    private static bool IsBrowserRuntime()
    {
        return BrowserRuntimeFlags.ForceVectorFallback;
    }
}
