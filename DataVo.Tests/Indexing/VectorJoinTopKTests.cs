using System.Globalization;
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Indexing;

/// <summary>
/// Vector top-k queries over a JOIN (<c>ORDER BY dist LIMIT k</c>) must return the exact top-k of the rows that
/// satisfy the whole WHERE clause, whether the filter is on the vector table (written with an alias or with
/// the table name) or on the joined table. Expected rows are computed by brute force. Some documents point at
/// an author that does not exist, so the inner join drops them.
/// </summary>
public sealed class VectorJoinTopKTests
{
    public static TheoryData<StorageMode, string> Cases => new()
    {
        { StorageMode.InMemory, "FLAT" },
        { StorageMode.Lsm, "FLAT" },
        { StorageMode.InMemory, "HNSW" },
        { StorageMode.Lsm, "HNSW" },
    };

    private const int DocumentCount = 600;
    private const int AuthorCount = 40;
    private const int K = 5;
    private static readonly float[] Query = [0.3f, 0.9f, 0.1f, 0.4f];

    private static int Category(int id) => id % 10;
    private static int AuthorOf(int id) => (id * 7 % 45) + 1;                // 41..45 do not exist
    private static string Country(int authorId) => "K" + (authorId % 10);
    private static bool HasAuthor(int id) => AuthorOf(id) <= AuthorCount;

    private static float[] Embedding(int id)
    {
        double a = id * 0.37, b = id * 0.11;
        return [(float)Math.Cos(a), (float)Math.Sin(a), (float)Math.Cos(b), (float)(1 + Math.Sin(b))];
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public void FilterOnVectorTable_WithAliasOrTableName_ReturnsFilteredTopK(StorageMode mode, string indexKind)
    {
        WithSchema(mode, indexKind, context =>
        {
            int[] expected = ExpectedTopK(id => HasAuthor(id) && Category(id) == 3);

            int[] aliased = Ids(context,
                $"SELECT d.Id, a.Name, d.Emb <=> '{Vector(Query)}' AS dist FROM Docs d JOIN Authors a ON d.AuthorId = a.Id " +
                $"WHERE d.Category = 3 ORDER BY dist LIMIT {K}");
            int[] named = Ids(context,
                $"SELECT Docs.Id, Authors.Name, Docs.Emb <=> '{Vector(Query)}' AS dist FROM Docs JOIN Authors ON Docs.AuthorId = Authors.Id " +
                $"WHERE Docs.Category = 3 ORDER BY dist LIMIT {K}");

            AssertTopK(indexKind, expected, aliased, id => HasAuthor(id) && Category(id) == 3);
            AssertTopK(indexKind, expected, named, id => HasAuthor(id) && Category(id) == 3);
        });
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public void FilterOnJoinedTable_IsApplied(StorageMode mode, string indexKind)
    {
        WithSchema(mode, indexKind, context =>
        {
            Func<int, bool> predicate = id => HasAuthor(id) && Country(AuthorOf(id)) == "K4";
            int[] expected = ExpectedTopK(predicate);

            int[] actual = Ids(context,
                $"SELECT d.Id, a.Name, d.Emb <=> '{Vector(Query)}' AS dist FROM Docs d JOIN Authors a ON d.AuthorId = a.Id " +
                $"WHERE a.Country = 'K4' ORDER BY dist LIMIT {K}");
            AssertTopK(indexKind, expected, actual, predicate);

            // Filters on both tables.
            Func<int, bool> both = id => HasAuthor(id) && Country(AuthorOf(id)) == "K4" && Category(id) < 5;
            AssertTopK(indexKind, ExpectedTopK(both), Ids(context,
                $"SELECT d.Id, d.Emb <=> '{Vector(Query)}' AS dist FROM Docs d JOIN Authors a ON d.AuthorId = a.Id " +
                $"WHERE a.Country = 'K4' AND d.Category < 5 ORDER BY dist LIMIT {K}"), both);
        });
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public void InnerJoinWithoutWhere_StillReturnsKRows(StorageMode mode, string indexKind)
    {
        WithSchema(mode, indexKind, context =>
        {
            AssertTopK(indexKind, ExpectedTopK(HasAuthor), Ids(context,
                $"SELECT d.Id, d.Emb <=> '{Vector(Query)}' AS dist FROM Docs d JOIN Authors a ON d.AuthorId = a.Id " +
                $"ORDER BY dist LIMIT {K}"), HasAuthor);
        });
    }

    /// <summary>
    /// FLAT is exact, so the ids must match in order. HNSW is approximate: every row must satisfy the filter
    /// and there must be exactly k of them.
    /// </summary>
    private static void AssertTopK(string indexKind, int[] expected, int[] actual, Func<int, bool> predicate)
    {
        Assert.Equal(K, expected.Length);
        if (indexKind == "FLAT")
        {
            Assert.Equal(expected, actual);
            return;
        }

        Assert.Equal(K, actual.Length);
        Assert.All(actual, id => Assert.True(predicate(id), $"row {id} does not satisfy the WHERE clause"));
    }

    private static int[] ExpectedTopK(Func<int, bool> predicate) =>
    [
        .. Enumerable.Range(1, DocumentCount)
            .Where(predicate)
            .OrderBy(id => CosineDistance(Query, Embedding(id)))
            .ThenBy(id => id)
            .Take(K)
    ];

    private static double CosineDistance(float[] left, float[] right)
    {
        double dot = 0, l = 0, r = 0;
        for (int i = 0; i < left.Length; i++)
        {
            dot += left[i] * right[i];
            l += left[i] * left[i];
            r += right[i] * right[i];
        }

        return 1 - dot / (Math.Sqrt(l) * Math.Sqrt(r));
    }

    private static string Vector(float[] values) =>
        "[" + string.Join(",", values.Select(v => v.ToString("R", CultureInfo.InvariantCulture))) + "]";

    private static void WithSchema(StorageMode mode, string indexKind, Action<DataVoContext> test)
    {
        string? directory = mode == StorageMode.Lsm ? TestStorageDirectories.Create("vector_join_topk") : null;
        using var context = new DataVoContext(new DataVoConfig { StorageMode = mode, DataDirectory = directory });
        Execute(context, "CREATE DATABASE VectorJoin");
        Execute(context, "USE VectorJoin");
        Execute(context, "CREATE TABLE Authors (Id INT PRIMARY KEY, Name VARCHAR(16), Country VARCHAR(4))");
        Execute(context, "CREATE TABLE Docs (Id INT PRIMARY KEY, Category INT, AuthorId INT, Emb VECTOR(4))");
        Execute(context, $"CREATE INDEX vx_docs ON Docs (Emb) USING {indexKind}");
        Execute(context, "INSERT INTO Authors (Id, Name, Country) VALUES " +
            string.Join(", ", Enumerable.Range(1, AuthorCount).Select(i => $"({i}, 'A{i}', '{Country(i)}')")));
        Execute(context, "INSERT INTO Docs (Id, Category, AuthorId, Emb) VALUES " +
            string.Join(", ", Enumerable.Range(1, DocumentCount).Select(i => $"({i}, {Category(i)}, {AuthorOf(i)}, '{Vector(Embedding(i))}')")));
        test(context);
    }

    private static void Execute(DataVoContext context, string sql)
    {
        var result = context.Execute(sql)[^1];
        Assert.False(result.IsError, string.Join("; ", result.Messages));
    }

    private static int[] Ids(DataVoContext context, string sql)
    {
        var result = context.Execute(sql)[^1];
        Assert.False(result.IsError, string.Join("; ", result.Messages));
        return [.. result.Data.Select(row => Convert.ToInt32(row.Values.First(), CultureInfo.InvariantCulture))];
    }
}
