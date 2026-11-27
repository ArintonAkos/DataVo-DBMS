using System.Globalization;
using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Sql;

/// <summary>
/// Filtered nearest-neighbour queries (<c>... WHERE ... ORDER BY Emb &lt;=&gt; @q LIMIT k</c>, with and without
/// a join) test the index's candidates on stored rows and build only the rows that pass. These tests compare
/// the returned ids with the exact answer computed by LINQ over the seed data (the nearest rows among those the
/// WHERE and the join keep), on InMemory and Lsm with a FLAT and an HNSW index, and bound what a query allocates.
/// </summary>
public sealed class SqlFilteredVectorSearchTests
{
    public static TheoryData<StorageMode, string> ModesAndIndexes => new()
    {
        { StorageMode.InMemory, "FLAT" },
        { StorageMode.InMemory, "HNSW" },
        { StorageMode.Lsm, "FLAT" },
        { StorageMode.Lsm, "HNSW" },
    };

    private const int Dimensions = 24;
    private const int Documents = 1_500;
    private const int Authors = 60;

    private sealed record Doc(int Id, int Category, double Price, int? AuthorId, string? Tag, float[] Emb);

    private sealed record Author(int Id, string Name, string Country);

    private static readonly string[] Countries = ["DE", "FR", "HU", "US", "JP", "BR", "IN", "SE", "IT", "ES"];

    private static readonly Author[] AuthorRows = [.. Enumerable.Range(1, Authors).Select(i => new Author(i, $"author{i}", Countries[i % Countries.Length]))];

    // Every 17th document has no author and every 23rd names an author that does not exist.
    private static readonly Doc[] Docs = [.. Enumerable.Range(1, Documents).Select(i => new Doc(
        i,
        i % 20,
        i * 37 % 100 + 0.5,
        i % 17 == 0 ? null : i % 23 == 0 ? Authors + 1 + i % 5 : i * 7 % Authors + 1,
        i % 9 == 0 ? null : $"T{i % 6}-{(char)('a' + i % 4)}",
        Vector(i)))];

    private static readonly Dictionary<int, Author> AuthorById = AuthorRows.ToDictionary(a => a.Id);

    [Theory]
    [MemberData(nameof(ModesAndIndexes))]
    public void FilterOnTheTable_ReturnsTheNearestRowsThatPassIt(StorageMode mode, string indexType)
    {
        WithDocuments(mode, indexType, context =>
        {
            foreach (int seed in new[] { 5_001, 5_002, 5_003 })
            {
                float[] query = Vector(seed);

                // About 2.5% of the rows, then about half of them.
                AssertNearest(context, query, "FROM Docs WHERE Category = 3 AND Price < 50", d => d.Category == 3 && d.Price < 50);
                AssertNearest(context, query, "FROM Docs WHERE Price < 50", d => d.Price < 50);

                // OR, LIKE, IS NULL and a key range are tested on the stored rows too.
                AssertNearest(context, query, "FROM Docs WHERE (Category = 1 OR Category = 2) AND Tag LIKE 'T3%'",
                    d => d.Category is 1 or 2 && d.Tag is not null && d.Tag.StartsWith("T3", StringComparison.Ordinal));
                AssertNearest(context, query, "FROM Docs WHERE Tag IS NULL AND Price >= 20", d => d.Tag is null && d.Price >= 20);
                AssertNearest(context, query, "FROM Docs WHERE Id > 1400 AND Category < 4", d => d.Id > 1400 && d.Category < 4);

                // Fewer matching rows than the LIMIT: all of them, nearest first. None: no row.
                AssertNearest(context, query, "FROM Docs WHERE Category = 7 AND Price < 4", d => d.Category == 7 && d.Price < 4);
                AssertNearest(context, query, "FROM Docs WHERE Category = 99", d => false);
            }
        });
    }

    [Theory]
    [MemberData(nameof(ModesAndIndexes))]
    public void FilterWithAJoin_ReturnsTheNearestJoinedRowsThatPassIt(StorageMode mode, string indexType)
    {
        WithDocuments(mode, indexType, context =>
        {
            foreach (int seed in new[] { 6_001, 6_002 })
            {
                float[] query = Vector(seed);
                const string join = "FROM Docs d JOIN Authors a ON d.AuthorId = a.Id ";

                // An INNER JOIN drops documents without an author or with an unknown one.
                AssertNearest(context, query, join, d => HasAuthor(d), alias: "d");
                AssertNearest(context, query, join + "WHERE d.Category = 3 AND d.Price < 50", d => HasAuthor(d) && d.Category == 3 && d.Price < 50, alias: "d");

                // A filter on the joined table only, and on both tables.
                AssertNearest(context, query, join + "WHERE a.Country = 'HU'", d => HasAuthor(d) && AuthorOf(d).Country == "HU", alias: "d");
                AssertNearest(context, query, join + "WHERE a.Country = 'JP' AND d.Price < 30", d => HasAuthor(d) && AuthorOf(d).Country == "JP" && d.Price < 30, alias: "d");
                AssertNearest(context, query, join + "WHERE a.Country = 'XX'", d => false, alias: "d");
                AssertNearest(context, query, join + "WHERE a.Name LIKE 'author1%' OR a.Country = 'SE'",
                    d => HasAuthor(d) && (AuthorOf(d).Name.StartsWith("author1", StringComparison.Ordinal) || AuthorOf(d).Country == "SE"), alias: "d");
            }
        });
    }

    [Theory]
    [MemberData(nameof(ModesAndIndexes))]
    public void FilterWithALeftJoin_KeepsUnmatchedRowsAndTestsTheNullSide(StorageMode mode, string indexType)
    {
        WithDocuments(mode, indexType, context =>
        {
            float[] query = Vector(7_001);
            const string join = "FROM Docs d LEFT JOIN Authors a ON d.AuthorId = a.Id ";

            // Every document is kept; the joined name is NULL where there is no author.
            QueryResult all = Run(context, $"SELECT d.Id, a.Name, d.Emb <=> @q AS dist {join}ORDER BY dist LIMIT 25", query);
            Doc[] nearest = [.. Docs.OrderBy(d => Cosine(d.Emb, query)).Take(25)];
            Assert.Equal(nearest.Select(d => d.Id), all.Data.Select(IdOf));
            Assert.Equal(nearest.Select(d => HasAuthor(d) ? AuthorOf(d).Name : null), all.Data.Select(row => (string?)row.Values.ElementAt(1)));

            // The anti-join: only documents whose author is missing.
            AssertNearest(context, query, join + "WHERE a.Id IS NULL", d => !HasAuthor(d), alias: "d");
            AssertNearest(context, query, join + "WHERE a.Id IS NULL AND d.Category < 10", d => !HasAuthor(d) && d.Category < 10, alias: "d");

            // A condition on the joined table drops the unmatched rows, as SQL defines it.
            AssertNearest(context, query, join + "WHERE a.Country = 'FR' AND d.Price >= 10", d => HasAuthor(d) && AuthorOf(d).Country == "FR" && d.Price >= 10, alias: "d");
        });
    }

    [Theory]
    [MemberData(nameof(ModesAndIndexes))]
    public void BoundFilterValues_GiveTheSameRowsAsLiterals(StorageMode mode, string indexType)
    {
        WithDocuments(mode, indexType, context =>
        {
            float[] query = Vector(8_001);
            DataVoPreparedStatement single = context.Prepare("SELECT Id, Emb <=> @q AS dist FROM Docs WHERE Category = @c AND Price < @p ORDER BY dist LIMIT 10");
            DataVoPreparedStatement joined = context.Prepare(
                "SELECT d.Id, a.Name, d.Emb <=> @q AS dist FROM Docs d JOIN Authors a ON d.AuthorId = a.Id WHERE a.Country = @k AND d.Price < @p ORDER BY dist LIMIT 10");
            foreach ((int category, double price, string country) in new[] { (3, 50d, "DE"), (11, 80.5d, "US"), (19, 5d, "IT") })
            {
                QueryResult one = single.Execute(DataVoQueryParameter.FromObject("q", query), DataVoQueryParameter.FromObject("c", category), DataVoQueryParameter.FromObject("p", price));
                Assert.False(one.IsError, string.Join("; ", one.Messages));
                Assert.Equal(Expected(query, d => d.Category == category && d.Price < price, 10), one.Data.Select(IdOf));

                QueryResult two = joined.Execute(DataVoQueryParameter.FromObject("q", query), DataVoQueryParameter.FromObject("k", country), DataVoQueryParameter.FromObject("p", price));
                Assert.False(two.IsError, string.Join("; ", two.Messages));
                Assert.Equal(Expected(query, d => HasAuthor(d) && AuthorOf(d).Country == country && d.Price < price, 10), two.Data.Select(IdOf));
            }
        });
    }

    /// <summary>
    /// A selective filter asks the index for a few hundred candidates. Each used to be loaded as a dictionary
    /// with its stored vector, twice (and every row of a joined table once): about 3 MB a query at this size.
    /// Now only the rows that pass are built, about 90 to 170 KB.
    /// </summary>
    [Theory]
    [MemberData(nameof(ModesAndIndexes))]
    public void SelectiveFilters_DoNotMaterializeTheCandidatesThatFail(StorageMode mode, string indexType)
    {
        WithDocuments(mode, indexType, context =>
        {
            float[] query = Vector(9_001);
            foreach (string sql in new[]
            {
                "SELECT Id, Emb <=> @q AS dist FROM Docs WHERE Category = 3 AND Price < 50 ORDER BY dist LIMIT 10",
                "SELECT d.Id, a.Name, d.Emb <=> @q AS dist FROM Docs d JOIN Authors a ON d.AuthorId = a.Id WHERE d.Category = 3 AND d.Price < 50 ORDER BY dist LIMIT 10",
                "SELECT d.Id, a.Name, d.Emb <=> @q AS dist FROM Docs d JOIN Authors a ON d.AuthorId = a.Id WHERE a.Country = 'HU' ORDER BY dist LIMIT 10",
            })
            {
                DataVoPreparedStatement statement = context.Prepare(sql);
                DataVoQueryParameter parameter = DataVoQueryParameter.FromObject("q", query);
                Assert.False(statement.Execute(parameter).IsError);
                long before = GC.GetAllocatedBytesForCurrentThread();
                QueryResult result = statement.Execute(parameter);
                long bytes = GC.GetAllocatedBytesForCurrentThread() - before;

                Assert.False(result.IsError, string.Join("; ", result.Messages));
                Assert.Equal(10, result.Data.Count);
                Assert.True(bytes < 250_000, $"{mode} {indexType}: {bytes:N0} B for {sql}");
            }
        });
    }

    private static bool HasAuthor(Doc doc) => doc.AuthorId is { } id && AuthorById.ContainsKey(id);

    private static Author AuthorOf(Doc doc) => AuthorById[doc.AuthorId!.Value];

    /// <summary>The ids of the <paramref name="take"/> documents nearest to <paramref name="query"/> among those <paramref name="filter"/> keeps.</summary>
    private static IEnumerable<int> Expected(float[] query, Func<Doc, bool> filter, int take) =>
        Docs.Where(filter).OrderBy(d => Cosine(d.Emb, query)).Take(take).Select(d => d.Id);

    /// <summary>Runs a top-10 and a top-3 query over <paramref name="fromAndWhere"/> and compares the ids, in order, with the exact answer.</summary>
    private static void AssertNearest(DataVoContext context, float[] query, string fromAndWhere, Func<Doc, bool> filter, string? alias = null)
    {
        string prefix = alias is null ? string.Empty : alias + ".";
        foreach (int limit in new[] { 10, 3 })
        {
            string sql = $"SELECT {prefix}Id, {prefix}Emb <=> @q AS dist {fromAndWhere} ORDER BY dist LIMIT {limit}";
            QueryResult result = Run(context, sql, query);
            Assert.True(Expected(query, filter, limit).SequenceEqual(result.Data.Select(IdOf)),
                $"{sql}: expected [{string.Join(",", Expected(query, filter, limit))}], got [{string.Join(",", result.Data.Select(IdOf))}]");
        }
    }

    private static QueryResult Run(DataVoContext context, string sql, float[] query)
    {
        QueryResult result = context.Prepare(sql).Execute(DataVoQueryParameter.FromObject("q", query));
        Assert.False(result.IsError, sql + ": " + string.Join("; ", result.Messages));
        return result;
    }

    private static int IdOf(Dictionary<string, object?> row) => Convert.ToInt32(row.Values.First(), CultureInfo.InvariantCulture);

    private static double Cosine(float[] left, float[] right)
    {
        double dot = 0, leftNorm = 0, rightNorm = 0;
        for (int i = 0; i < left.Length; i++)
        {
            dot += (double)left[i] * right[i];
            leftNorm += (double)left[i] * left[i];
            rightNorm += (double)right[i] * right[i];
        }

        return 1d - dot / Math.Sqrt(leftNorm * rightNorm);
    }

    private static float[] Vector(int seed)
    {
        var random = new Random(seed);
        var vector = new float[Dimensions];
        for (int i = 0; i < vector.Length; i++)
        {
            vector[i] = (float)(random.NextDouble() * 2 - 1);
        }

        return vector;
    }

    private static string Literal(float[] vector) =>
        "[" + string.Join(", ", vector.Select(x => x.ToString("R", CultureInfo.InvariantCulture))) + "]";

    private static void WithDocuments(StorageMode mode, string indexType, Action<DataVoContext> test)
    {
        string? directory = mode == StorageMode.Lsm ? TestStorageDirectories.Create("sql_filtered_vector") : null;
        using var context = new DataVoContext(new DataVoConfig { StorageMode = mode, DataDirectory = directory });
        Execute(context, "CREATE DATABASE FilteredVector");
        Execute(context, "USE FilteredVector");
        Execute(context, "CREATE TABLE Authors (Id INT PRIMARY KEY, Name VARCHAR(16), Country VARCHAR(4))");
        Execute(context, $"CREATE TABLE Docs (Id INT PRIMARY KEY, Category INT, Price DOUBLE, AuthorId INT, Tag VARCHAR(8), Emb VECTOR({Dimensions}))");
        Execute(context, $"CREATE INDEX vx ON Docs (Emb) USING {indexType}");
        Execute(context, "INSERT INTO Authors (Id, Name, Country) VALUES " + string.Join(", ", AuthorRows.Select(a => $"({a.Id}, '{a.Name}', '{a.Country}')")));
        foreach (Doc[] chunk in Docs.Chunk(250))
        {
            Execute(context, "INSERT INTO Docs (Id, Category, Price, AuthorId, Tag, Emb) VALUES " + string.Join(", ", chunk.Select(d =>
                $"({d.Id}, {d.Category}, {d.Price.ToString(CultureInfo.InvariantCulture)}, {d.AuthorId?.ToString(CultureInfo.InvariantCulture) ?? "NULL"}, {(d.Tag is null ? "NULL" : $"'{d.Tag}'")}, '{Literal(d.Emb)}')")));
        }

        test(context);
    }

    private static void Execute(DataVoContext context, string sql)
    {
        QueryResult result = context.Execute(sql)[^1];
        Assert.False(result.IsError, sql[..Math.Min(sql.Length, 120)] + ": " + string.Join("; ", result.Messages));
    }
}
