using System.Numerics;
using Research.Benchmark.Runners.SqlMatrix;

namespace Research.Benchmark.Runners.HybridVector;

/// <summary>The query shapes of the hybrid scenario: nearest neighbours with and without relational filters and a join.</summary>
public enum HybridShape
{
    /// <summary>Top-10, no filter.</summary>
    V0Unfiltered,

    /// <summary>Top-10 among rows with <c>Category = c AND Price &lt; 50</c> (about 5%).</summary>
    V1Selective,

    /// <summary>Top-10 among rows with <c>Price &lt; 50</c> (about 50%).</summary>
    V2Broad,

    /// <summary>V1, joined to <c>Authors</c> for the author's name.</summary>
    V3JoinDocFilter,

    /// <summary>Top-10 among documents whose author has <c>Country = k</c> (about 10%), joined to <c>Authors</c>.</summary>
    V4JoinAuthorFilter,
}

/// <summary>
/// Clustered embeddings with relational attributes, the held-out query vectors, and the exact filtered
/// top-k oracle. Attributes are independent of the clusters, so a filter is uncorrelated with distance.
/// </summary>
public sealed class HybridVectorData
{
    public const int TopK = 10;
    public const int CategoryCount = 10;
    public const int CountryCount = 10;
    public const double PriceThreshold = 50d;

    public HybridVectorData(int documents, int dimensions, int queries, int authors = 2_000, int clusters = 128, int seed = 20261114)
    {
        Documents = documents;
        Dimensions = dimensions;
        Authors = authors;
        var random = new Random(seed);
        float[][] centers = [.. Enumerable.Range(0, clusters).Select(_ => Unit(Enumerable.Range(0, dimensions).Select(_ => (float)(random.NextDouble() * 2 - 1)).ToArray()))];
        float noise = 0.8f / MathF.Sqrt(dimensions);
        float[] Make()
        {
            float[] center = centers[random.Next(clusters)];
            var v = new float[dimensions];
            for (int i = 0; i < dimensions; i++)
            {
                v[i] = center[i] + (float)(random.NextDouble() * 2 - 1) * noise;
            }

            return Unit(v);
        }

        Embeddings = new float[documents + 1][];
        Category = new int[documents + 1];
        Price = new double[documents + 1];
        AuthorId = new int[documents + 1];
        Embeddings[0] = [];
        for (int id = 1; id <= documents; id++)
        {
            Embeddings[id] = Make();
            uint h = SqlMatrixData.Mix((uint)id, 0x51ED270Bu);
            Category[id] = (int)(h % CategoryCount);
            Price[id] = ((h >> 4) % 400) * 0.25d;
            AuthorId[id] = 1 + (int)((h >> 13) % (uint)authors);
        }

        QueryVectors = [.. Enumerable.Range(0, queries).Select(_ => Make())];
    }

    public int Documents { get; }

    public int Dimensions { get; }

    public int Authors { get; }

    /// <summary>Unit-length embeddings indexed by document id (index 0 unused).</summary>
    public float[][] Embeddings { get; }

    public int[] Category { get; }

    public double[] Price { get; }

    public int[] AuthorId { get; }

    /// <summary>Held-out queries: drawn like the documents but not in the table.</summary>
    public float[][] QueryVectors { get; }

    public static string AuthorName(int id) => "A" + id.ToString(System.Globalization.CultureInfo.InvariantCulture);

    public static string AuthorCountry(int id) => "K" + (id % CountryCount).ToString(System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>The bound parameters of query <paramref name="queryIndex"/>: category and country rotate with the query.</summary>
    public static (int Category, double Price, string Country) ParametersFor(int queryIndex) =>
        (queryIndex % CategoryCount, PriceThreshold, AuthorCountry(queryIndex % CountryCount));

    public bool Matches(HybridShape shape, int id, int queryIndex)
    {
        (int category, double price, string country) = ParametersFor(queryIndex);
        return shape switch
        {
            HybridShape.V0Unfiltered => true,
            HybridShape.V1Selective or HybridShape.V3JoinDocFilter => Category[id] == category && Price[id] < price,
            HybridShape.V2Broad => Price[id] < price,
            HybridShape.V4JoinAuthorFilter => AuthorCountry(AuthorId[id]) == country,
            _ => throw new ArgumentOutOfRangeException(nameof(shape)),
        };
    }

    /// <summary>Cosine distance of the query to every document (index 0 unused); vectors are unit length.</summary>
    public float[] Distances(float[] query)
    {
        var distances = new float[Documents + 1];
        for (int id = 1; id <= Documents; id++)
        {
            distances[id] = 1f - Dot(query, Embeddings[id]);
        }

        return distances;
    }

    /// <summary>The exact top-k among the rows that satisfy the shape's filter, ordered by (distance, id).</summary>
    public (int[] Ids, float KthDistance, int Matching) Truth(HybridShape shape, int queryIndex, float[] distances)
    {
        var candidates = new List<int>();
        for (int id = 1; id <= Documents; id++)
        {
            if (Matches(shape, id, queryIndex))
            {
                candidates.Add(id);
            }
        }

        int[] top = [.. candidates.OrderBy(id => distances[id]).ThenBy(id => id).Take(TopK)];
        return (top, top.Length > 0 ? distances[top[^1]] : float.NaN, candidates.Count);
    }

    private static float Dot(float[] a, float[] b)
    {
        int width = Vector<float>.Count;
        var sum = Vector<float>.Zero;
        int i = 0;
        for (; i <= a.Length - width; i += width)
        {
            sum += new Vector<float>(a, i) * new Vector<float>(b, i);
        }

        float total = Vector.Sum(sum);
        for (; i < a.Length; i++)
        {
            total += a[i] * b[i];
        }

        return total;
    }

    private static float[] Unit(float[] v)
    {
        double squares = 0;
        foreach (float x in v)
        {
            squares += x * (double)x;
        }

        float norm = (float)Math.Sqrt(squares);
        for (int i = 0; i < v.Length; i++)
        {
            v[i] /= norm;
        }

        return v;
    }
}
