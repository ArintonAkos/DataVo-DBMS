using System.Text;

namespace HybridVectorRagAot;

/// <summary>
/// A dependency-free stand-in for a sentence-embedding model: feature hashing of word unigrams, word bigrams and
/// character trigrams into a fixed number of dimensions, with signed buckets, then L2 normalization.
/// </summary>
/// <remarks>
/// Texts that share words and word pieces land close in cosine distance, which is enough to demonstrate hybrid
/// retrieval end to end without downloading a model or loading native ONNX runtimes. In a real application, replace
/// <see cref="Embed"/> with the output of your embedding model (384 dimensions matches all-MiniLM-L6-v2 and
/// bge-small); nothing else in the program changes.
/// </remarks>
public sealed class HashingEmbedder(int dimensions = 384)
{
    private static readonly HashSet<string> StopWords =
    [
        "a", "an", "and", "are", "as", "at", "be", "by", "can", "do", "for", "from", "how", "i", "in", "is", "it",
        "its", "of", "on", "or", "our", "the", "this", "to", "we", "what", "when", "which", "with", "you", "your",
    ];

    /// <summary>The number of dimensions every vector has.</summary>
    public int Dimensions { get; } = dimensions;

    /// <summary>Embeds <paramref name="text"/> as a unit-length vector.</summary>
    public float[] Embed(string text)
    {
        var vector = new float[Dimensions];
        string[] words = Tokenize(text);
        for (int i = 0; i < words.Length; i++)
        {
            Add(vector, words[i], 1.0f);
            if (i + 1 < words.Length)
            {
                Add(vector, words[i] + " " + words[i + 1], 0.7f);
            }

            string padded = "#" + words[i] + "#";
            for (int c = 0; c + 3 <= padded.Length; c++)
            {
                Add(vector, padded.Substring(c, 3), 0.35f);
            }
        }

        Normalize(vector);
        return vector;
    }

    private static string[] Tokenize(string text)
    {
        var words = new List<string>();
        var current = new StringBuilder();
        foreach (char c in text)
        {
            if (char.IsLetterOrDigit(c))
            {
                current.Append(char.ToLowerInvariant(c));
            }
            else
            {
                Flush();
            }
        }

        Flush();
        return [.. words];

        void Flush()
        {
            if (current.Length > 0)
            {
                string word = current.ToString();
                if (!StopWords.Contains(word))
                {
                    // A crude stem so "keys"/"key" and "rotating"/"rotate" share a feature.
                    words.Add(word.Length > 4 && word.EndsWith('s') ? word[..^1] : word.Length > 6 && word.EndsWith("ing", StringComparison.Ordinal) ? word[..^3] : word);
                }

                current.Clear();
            }
        }
    }

    private void Add(float[] vector, string feature, float weight)
    {
        uint hash = Fnv1a(feature);
        int bucket = (int)(hash % (uint)Dimensions);
        vector[bucket] += (hash & 0x8000_0000) == 0 ? weight : -weight;
    }

    private static void Normalize(float[] vector)
    {
        double norm = 0;
        foreach (float v in vector)
        {
            norm += v * v;
        }

        if (norm == 0)
        {
            vector[0] = 1;
            return;
        }

        float scale = (float)(1 / Math.Sqrt(norm));
        for (int i = 0; i < vector.Length; i++)
        {
            vector[i] *= scale;
        }
    }

    private static uint Fnv1a(string text)
    {
        uint hash = 2166136261;
        foreach (char c in text)
        {
            hash ^= c;
            hash *= 16777619;
        }

        return hash;
    }
}
