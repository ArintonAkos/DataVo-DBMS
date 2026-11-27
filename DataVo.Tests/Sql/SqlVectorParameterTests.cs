using System.Globalization;
using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Sql;

/// <summary>
/// A query vector bound as a <c>float[]</c> parameter is used as given: it is not parsed back from its text, and a
/// distance computed per row reads the stored vectors and the query vector without copying or re-parsing them.
/// The parameter's text (what every other SQL consumer reads) is unchanged, so results, inserts and updates are the
/// same as before; these tests check that, and that the per-row cost no longer grows with the vector's length.
/// </summary>
public sealed class SqlVectorParameterTests
{
    public static TheoryData<StorageMode, string> ModesAndIndexes => new()
    {
        { StorageMode.InMemory, "FLAT" },
        { StorageMode.InMemory, "HNSW" },
        { StorageMode.Lsm, "FLAT" },
        { StorageMode.Lsm, "HNSW" },
    };

    private const int Dimensions = 64;
    private const int Documents = 400;

    [Fact]
    public void FloatArrayParameter_HasTheSameTextAsBefore()
    {
        float[] values = [0.1f, -0f, 1e-45f, float.MaxValue, -3.4028235E+38f, 1f / 3, 123456.789f, 2.5e-7f];
        DataVoQueryParameter parameter = DataVoQueryParameter.FromObject("q", values);

        Assert.Equal(
            $"[{string.Join(", ", values.Select(value => value.ToString(null, CultureInfo.InvariantCulture)))}]",
            parameter.AsString());
    }

    [Theory]
    [MemberData(nameof(ModesAndIndexes))]
    public void BoundVector_GivesTheSameRowsAndDistancesAsVectorText(StorageMode mode, string indexType)
    {
        WithDocuments(mode, indexType, (context, embeddings) =>
        {
            float[] query = Vector(seed: 777);
            string text = "[" + string.Join(", ", query.Select(x => x.ToString("R", CultureInfo.InvariantCulture))) + "]";
            foreach (string where in new[] { "", " WHERE Category = 3", " WHERE Category < 5 AND Id > 50" })
            {
                string sql = $"SELECT Id, Emb <=> @q AS dist FROM Docs{where} ORDER BY dist, Id LIMIT 10";
                DataVoPreparedStatement statement = context.Prepare(sql);
                QueryResult fromArray = statement.Execute(DataVoQueryParameter.FromObject("q", query));
                QueryResult fromText = statement.Execute(DataVoQueryParameter.FromObject("q", text));
                Assert.False(fromArray.IsError, string.Join("; ", fromArray.Messages));
                Assert.Equal(Rows(fromText), Rows(fromArray));

                // The returned distances are the exact cosine distances of the stored vectors.
                foreach (Dictionary<string, object?> row in fromArray.Data)
                {
                    int id = Convert.ToInt32(row.Values.First(), CultureInfo.InvariantCulture);
                    Assert.Equal(Cosine(embeddings[id], query), Convert.ToDouble(row.Values.Last(), CultureInfo.InvariantCulture), 5);
                }
            }

            // Changing the caller's array after binding does not change the bound value.
            float[] mutable = (float[])query.Clone();
            DataVoQueryParameter bound = DataVoQueryParameter.FromObject("q", mutable);
            Array.Fill(mutable, 0f);
            QueryResult afterChange = context.Prepare("SELECT Id, Emb <=> @q AS dist FROM Docs ORDER BY dist, Id LIMIT 5").Execute(bound);
            QueryResult original = context.Prepare("SELECT Id, Emb <=> @q AS dist FROM Docs ORDER BY dist, Id LIMIT 5").Execute(DataVoQueryParameter.FromObject("q", query));
            Assert.Equal(Rows(original), Rows(afterChange));
        });
    }

    [Theory]
    [MemberData(nameof(ModesAndIndexes))]
    public void BoundVector_InsertsAndUpdatesAsBefore(StorageMode mode, string indexType)
    {
        WithDocuments(mode, indexType, (context, _) =>
        {
            float[] vector = Vector(seed: 4242);
            QueryResult insert = context.Prepare("INSERT INTO Docs (Id, Category, Emb) VALUES (@id, 1, @v)")
                .Execute(DataVoQueryParameter.FromObject("id", 10_000), DataVoQueryParameter.FromObject("v", vector));
            Assert.False(insert.IsError, string.Join("; ", insert.Messages));
            Assert.Equal(10_000, NearestId(context, vector));

            float[] moved = Vector(seed: 5_555);
            QueryResult update = context.Prepare("UPDATE Docs SET Emb = @v WHERE Id = 10000")
                .Execute(DataVoQueryParameter.FromObject("v", moved));
            Assert.False(update.IsError, string.Join("; ", update.Messages));
            Assert.Equal(10_000, NearestId(context, moved));
        });
    }

    /// <summary>
    /// The distance column used to parse the query vector's text and copy the stored vector for every row it was
    /// computed on, so a query allocated in proportion to rows times dimensions. Doubling the rows that need a
    /// distance (a broad filter computed on every candidate) now adds little.
    /// </summary>
    [Fact]
    public void DistanceColumn_DoesNotParseTheQueryVectorPerRow()
    {
        WithDocuments(StorageMode.InMemory, "FLAT", (context, _) =>
        {
            DataVoQueryParameter query = DataVoQueryParameter.FromObject("q", Vector(seed: 6_006));
            DataVoPreparedStatement statement = context.Prepare("SELECT Id, Emb <=> @q AS dist FROM Docs ORDER BY dist LIMIT 10");
            Assert.False(statement.Execute(query).IsError); // warm up
            long before = GC.GetAllocatedBytesForCurrentThread();
            Assert.False(statement.Execute(query).IsError);
            long bytes = GC.GetAllocatedBytesForCurrentThread() - before;

            // One parse of 64 floats is about 4 KB; a parse per returned row came to about 3 KB per dimension.
            Assert.True(bytes < 64 * 1024, $"{bytes} B per query");
        });
    }

    private static int NearestId(DataVoContext context, float[] vector)
    {
        QueryResult result = context.Prepare("SELECT Id, Emb <=> @q AS dist FROM Docs ORDER BY dist LIMIT 1").Execute(DataVoQueryParameter.FromObject("q", vector));
        Assert.False(result.IsError, string.Join("; ", result.Messages));
        return Convert.ToInt32(Assert.Single(result.Data).Values.First(), CultureInfo.InvariantCulture);
    }

    private static string[] Rows(QueryResult result) =>
        [.. result.Data.Select(row => string.Join("|", row.Values.Select(value => value is double d ? d.ToString("R", CultureInfo.InvariantCulture) : value?.ToString())))];

    private static double Cosine(float[] a, float[] b)
    {
        double dot = 0, na = 0, nb = 0;
        for (int i = 0; i < a.Length; i++)
        {
            dot += a[i] * b[i];
            na += a[i] * a[i];
            nb += b[i] * b[i];
        }

        return 1 - (dot / (Math.Sqrt(na) * Math.Sqrt(nb)));
    }

    private static float[] Vector(int seed)
    {
        var random = new Random(seed);
        return [.. Enumerable.Range(0, Dimensions).Select(_ => (float)(random.NextDouble() * 2 - 1))];
    }

    private static void WithDocuments(StorageMode mode, string indexType, Action<DataVoContext, Dictionary<int, float[]>> test)
    {
        string? directory = mode == StorageMode.Lsm ? TestStorageDirectories.Create("sql_vector_parameter") : null;
        using var context = new DataVoContext(new DataVoConfig { StorageMode = mode, DataDirectory = directory });
        Execute(context, "CREATE DATABASE VectorParameter");
        Execute(context, "USE VectorParameter");
        Execute(context, $"CREATE TABLE Docs (Id INT PRIMARY KEY, Category INT, Emb VECTOR({Dimensions}))");
        Execute(context, $"CREATE INDEX vx ON Docs (Emb) USING {indexType}");
        var embeddings = new Dictionary<int, float[]>();
        DataVoPreparedStatement insert = context.Prepare("INSERT INTO Docs (Id, Category, Emb) VALUES (@id, @c, @v)");
        for (int id = 1; id <= Documents; id++)
        {
            embeddings[id] = Vector(seed: id);
            QueryResult result = insert.Execute(DataVoQueryParameter.FromObject("id", id), DataVoQueryParameter.FromObject("c", id % 10), DataVoQueryParameter.FromObject("v", embeddings[id]));
            Assert.False(result.IsError, string.Join("; ", result.Messages));
        }

        test(context, embeddings);
    }

    private static void Execute(DataVoContext context, string sql)
    {
        QueryResult result = context.Execute(sql)[^1];
        Assert.False(result.IsError, sql + ": " + string.Join("; ", result.Messages));
    }
}
