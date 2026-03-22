using System.Text.Json;

namespace DataVo.Core.Indexing.HNSW;

/// <summary>
/// Persistence handler for HNSW (vector) indices (JSON serialization format).
/// </summary>
public class HNSWIndexPersistence : IIndexPersistence
{
    public string FileExtension => ".vector.json";

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

    public void Flush(object index)
    {
        // JSON-based HNSW has no additional flushing needed beyond Save
    }

    public bool FileExists(string filePath)
    {
        return File.Exists(filePath);
    }
}
