using System.Text;
using Microsoft.Extensions.VectorData;

namespace MicrosoftAiLocalRag;

/// <summary>One section of a manual page: its text, where it came from and its embedding.</summary>
public sealed class ManualChunk
{
    /// <summary>The page path and section number, such as <c>fundamentals/storage-modes.md#2</c>.</summary>
    public string Id { get; set; } = "";

    /// <summary>The manual section (the page's folder), such as <c>fundamentals</c>.</summary>
    public string Category { get; set; } = "";

    /// <summary>The page, relative to <c>docs/manual</c>.</summary>
    public string FilePath { get; set; } = "";

    /// <summary>The section heading.</summary>
    public string Heading { get; set; } = "";

    /// <summary>The section text (trimmed to a few hundred words).</summary>
    public string Text { get; set; } = "";

    /// <summary>When the page last changed (stored as a DataVo <c>DATETIME</c>).</summary>
    public DateTime UpdatedAt { get; set; }

    /// <summary>The embedding of <see cref="Heading"/> and <see cref="Text"/>; <see cref="ReadOnlyMemory{T}"/> as produced by Microsoft.Extensions.AI.</summary>
    public ReadOnlyMemory<float> Embedding { get; set; }

    /// <summary>
    /// The collection schema: indexed metadata columns for filtering and an HNSW cosine index on the embedding.
    /// Built at run time because the embedding size depends on the generator.
    /// </summary>
    public static VectorStoreCollectionDefinition Definition(int dimensions) => new()
    {
        Properties =
        [
            new VectorStoreKeyProperty(nameof(Id), typeof(string)),
            new VectorStoreDataProperty(nameof(Category), typeof(string)) { IsIndexed = true },
            new VectorStoreDataProperty(nameof(FilePath), typeof(string)) { IsIndexed = true },
            new VectorStoreDataProperty(nameof(Heading), typeof(string)),
            new VectorStoreDataProperty(nameof(Text), typeof(string)),
            new VectorStoreDataProperty(nameof(UpdatedAt), typeof(DateTime)) { IsIndexed = true },
            new VectorStoreVectorProperty(nameof(Embedding), typeof(ReadOnlyMemory<float>), dimensions)
            {
                DistanceFunction = DistanceFunction.CosineSimilarity,
                IndexKind = IndexKind.Hnsw,
            },
        ],
    };
}

/// <summary>Splits DataVo's markdown manual into <see cref="ManualChunk"/>s at its <c>##</c> headings.</summary>
public static class ManualCorpus
{
    private const int MaxChunkCharacters = 1_600;

    /// <summary>Finds <c>docs/manual</c> above the working directory (the demo runs from anywhere in the repository).</summary>
    public static string? FindManual(string start)
    {
        for (DirectoryInfo? directory = new(start); directory is not null; directory = directory.Parent)
        {
            string candidate = Path.Combine(directory.FullName, "docs", "manual");
            if (Directory.Exists(candidate))
            {
                return candidate;
            }
        }

        return null;
    }

    /// <summary>Every section of every page under <paramref name="manual"/>, in path order.</summary>
    public static List<ManualChunk> Load(string manual)
    {
        var chunks = new List<ManualChunk>();
        foreach (string file in Directory.EnumerateFiles(manual, "*.md", SearchOption.AllDirectories).Order(StringComparer.Ordinal))
        {
            string relative = Path.GetRelativePath(manual, file).Replace('\\', '/');
            string category = relative.Contains('/') ? relative[..relative.IndexOf('/')] : "manual";
            DateTime updatedAt = File.GetLastWriteTimeUtc(file);
            int section = 0;
            foreach ((string heading, string text) in Sections(File.ReadAllText(file)))
            {
                chunks.Add(new ManualChunk
                {
                    Id = $"{relative}#{section++}",
                    Category = category,
                    FilePath = relative,
                    Heading = heading,
                    Text = text.Length > MaxChunkCharacters ? text[..MaxChunkCharacters] : text,
                    UpdatedAt = updatedAt,
                });
            }
        }

        return chunks;
    }

    private static IEnumerable<(string Heading, string Text)> Sections(string markdown)
    {
        string[] lines = markdown.Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n');
        string heading = "";
        int start = 0;

        // YAML front matter: keep its title as the first section's heading, drop the rest.
        if (lines.Length > 0 && lines[0] == "---")
        {
            int end = Array.IndexOf(lines, "---", 1);
            if (end > 0)
            {
                string? title = lines[1..end].FirstOrDefault(line => line.StartsWith("title:", StringComparison.Ordinal));
                heading = title is null ? "" : title["title:".Length..].Trim().Trim('"');
                start = end + 1;
            }
        }

        var text = new StringBuilder();
        for (int i = start; i < lines.Length; i++)
        {
            string line = lines[i];
            if (line.StartsWith("# ", StringComparison.Ordinal) || line.StartsWith("## ", StringComparison.Ordinal))
            {
                if (text.ToString().Trim().Length > 0)
                {
                    yield return (heading, Collapse(text.ToString()));
                }

                heading = line.TrimStart('#').Trim();
                text.Clear();
                continue;
            }

            text.Append(line).Append(' ');
        }

        if (text.ToString().Trim().Length > 0)
        {
            yield return (heading, Collapse(text.ToString()));
        }
    }

    private static string Collapse(string text) => string.Join(' ', text.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));
}
