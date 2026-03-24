using System.Text.Json;

namespace DataVo.Core.Indexing.HNSW;

/// <summary>
/// Persistence handler for HNSW (vector) indices (JSON serialization format).
/// </summary>
public class HNSWIndexPersistence : IIndexPersistence
{
    private sealed class HnswSnapshot
    {
        public string IndexType { get; set; } = "HNSW";
        public string Metric { get; set; } = "cosine";
        public int M { get; set; } = 16;
        public int EfConstruction { get; set; } = 64;
        public int EfSearch { get; set; } = 64;
        public bool EnableDiversityHeuristic { get; set; } = true;
        public bool EnableDeleteGraphRepair { get; set; } = true;
        public long? EntryPointId { get; set; }
        public int MaxLevel { get; set; } = -1;
        public Dictionary<long, int> NodeLevels { get; set; } = [];
        public Dictionary<int, Dictionary<long, List<long>>> Layers { get; set; } = [];
        public Dictionary<long, float[]> Entries { get; set; } = [];
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
        if (index is not HNSWIndex hnsw)
            throw new ArgumentException($"Expected HNSWIndex but got {index?.GetType().Name}", nameof(index));

        var directory = Path.GetDirectoryName(filePath);
        if (!string.IsNullOrWhiteSpace(directory))
            Directory.CreateDirectory(directory);

        var payload = new HnswSnapshot
        {
            IndexType = "HNSW",
            Metric = hnsw.Metric,
            M = hnsw.M,
            EfConstruction = hnsw.EfConstruction,
            EfSearch = hnsw.EfSearch,
            EnableDiversityHeuristic = hnsw.EnableDiversityHeuristic,
            EnableDeleteGraphRepair = hnsw.EnableDeleteGraphRepair,
            EntryPointId = hnsw.EntryPointId,
            MaxLevel = hnsw.MaxLevel,
            NodeLevels = hnsw.NodeLevels,
            Layers = hnsw.Layers,
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
        var options = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true
        };

        HnswSnapshot? snapshot = JsonSerializer.Deserialize<HnswSnapshot>(json, options);
        if (snapshot == null)
            throw new InvalidOperationException($"Failed to deserialize HNSW index payload: {filePath}");

        var index = new HNSWIndex
        {
            Metric = string.IsNullOrWhiteSpace(snapshot.Metric) ? "cosine" : snapshot.Metric,
            M = snapshot.M > 0 ? snapshot.M : 16,
            EfConstruction = snapshot.EfConstruction > 0 ? snapshot.EfConstruction : 64,
            EfSearch = snapshot.EfSearch > 0 ? snapshot.EfSearch : 64,
            EnableDiversityHeuristic = snapshot.EnableDiversityHeuristic,
            EnableDeleteGraphRepair = snapshot.EnableDeleteGraphRepair,
            EntryPointId = snapshot.EntryPointId,
            MaxLevel = snapshot.MaxLevel,
            Entries = snapshot.Entries ?? [],
            NodeLevels = snapshot.NodeLevels ?? [],
            Layers = snapshot.Layers ?? []
        };

        // Backward compatibility: legacy snapshots only had Entries + Metric.
        if (index.Entries.Count > 0 && (index.NodeLevels.Count == 0 || index.Layers.Count == 0))
        {
            index.RebuildGraphFromEntries();
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
