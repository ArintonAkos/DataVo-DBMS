using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Sql;

/// <summary>
/// <c>HAVING</c> may use aggregates the SELECT list does not project. Each is computed on
/// its own — never read from another aggregate of the same function — on every execution path: the typed scan,
/// the index-ordered group walk, the general (join) path and inside a transaction. Results are checked against
/// LINQ over the same rows.
/// </summary>
public sealed class SqlHavingHiddenAggregateTests : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"datavo_having_{Guid.NewGuid():N}");
    private readonly List<(int Id, int Customer, string? Tag, int? V)> _rows = [];

    public static TheoryData<StorageMode, bool> Shapes => new()
    {
        { StorageMode.InMemory, false },
        { StorageMode.InMemory, true },
        { StorageMode.Lsm, false },
        { StorageMode.Lsm, true },
        { StorageMode.Disk, true },
    };

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }

    [Theory]
    [MemberData(nameof(Shapes))]
    public void HavingOnAnAggregateMissingFromTheSelectList_MatchesLinq(StorageMode mode, bool indexed)
    {
        using DataVoContext db = Seed(mode, indexed);

        foreach ((string having, Func<IGrouping<int, (int Id, int Customer, string? Tag, int? V)>, bool> keep) in Predicates())
        {
            var expected = _rows.GroupBy(row => row.Customer).Where(keep)
                .OrderBy(group => group.Key)
                .Select(group => $"{group.Key}:{group.LongCount()}")
                .ToList();

            foreach (string sql in new[]
            {
                $"SELECT CustomerId, COUNT(*) AS C FROM T GROUP BY CustomerId HAVING {having} ORDER BY CustomerId",
                $"SELECT CustomerId, COUNT(*) AS C FROM T WHERE Id > 0 GROUP BY CustomerId HAVING {having} ORDER BY CustomerId",
                $"SELECT t.CustomerId, COUNT(*) AS C FROM T t JOIN Customers c ON t.CustomerId = c.Id GROUP BY t.CustomerId HAVING {having} ORDER BY t.CustomerId",
            })
            {
                List<string> actual = [.. Query(db, sql).Data.Select(row => $"{row.Values.First()}:{row["C"]}")];
                Assert.True(expected.SequenceEqual(actual), $"{sql}\nexpected {string.Join(" ", expected)}\nactual   {string.Join(" ", actual)}");
            }
        }
    }

    [Theory]
    [MemberData(nameof(Shapes))]
    public void HavingInsideATransaction_MatchesLinq(StorageMode mode, bool indexed)
    {
        using DataVoContext db = Seed(mode, indexed);
        Guid tx = Guid.NewGuid();
        Query(db, "USE Hv", tx);
        Query(db, "BEGIN", tx);
        Query(db, "INSERT INTO T (Id, CustomerId, Tag, V) VALUES (10001, 3, NULL, 5)", tx);
        Query(db, "INSERT INTO T (Id, CustomerId, Tag, V) VALUES (10002, 3, 'x', NULL)", tx);
        _rows.Add((10001, 3, null, 5));
        _rows.Add((10002, 3, "x", null));

        var expected = _rows.GroupBy(row => row.Customer)
            .Where(group => group.Count(row => row.Tag is not null) >= 2)
            .OrderBy(group => group.Key)
            .Select(group => $"{group.Key}:{group.LongCount()}")
            .ToList();
        List<string> actual = [.. Query(db, "SELECT CustomerId, COUNT(*) AS C FROM T GROUP BY CustomerId HAVING COUNT(Tag) >= 2 ORDER BY CustomerId", tx)
            .Data.Select(row => $"{row["CustomerId"]}:{row["C"]}")];

        Assert.Equal(expected, actual);
        Query(db, "ROLLBACK", tx);
    }

    [Fact]
    public void ReportedCase_DropsTheNullTagGroup()
    {
        using var db = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        Query(db, "CREATE DATABASE R");
        Query(db, "USE R");
        Query(db, "CREATE TABLE T (Id INT PRIMARY KEY, CustomerId INT, Tag VARCHAR(10))");
        Query(db, "INSERT INTO T (Id, CustomerId, Tag) VALUES (1, 1, 'a'), (2, 1, 'b'), (3, 2, NULL), (4, 2, NULL), (5, 3, 'c')");

        QueryResult result = Query(db, "SELECT CustomerId, COUNT(*) FROM T GROUP BY CustomerId HAVING COUNT(Tag) >= 2");

        Dictionary<string, object?> row = Assert.Single(result.Data);
        Assert.Equal(1, row["CustomerId"]);
        Assert.Equal(2L, row.Values.Last());
    }

    private static IEnumerable<(string Having, Func<IGrouping<int, (int Id, int Customer, string? Tag, int? V)>, bool> Keep)> Predicates()
    {
        yield return ("COUNT(Tag) >= 2", group => group.Count(row => row.Tag is not null) >= 2);
        yield return ("COUNT(V) < 3", group => group.Count(row => row.V is not null) < 3);
        yield return ("SUM(V) > 40", group => group.Sum(row => (long?)row.V) is > 40);
        yield return ("MIN(V) <= 2", group => group.Min(row => row.V) is <= 2);
        yield return ("MAX(V) >= 9", group => group.Max(row => row.V) is >= 9);
        yield return ("COUNT(*) >= 4 AND COUNT(Tag) < 3", group => group.Count() >= 4 && group.Count(row => row.Tag is not null) < 3);
        yield return ("COUNT(DISTINCT Tag) = 1", group => group.Where(row => row.Tag is not null).Select(row => row.Tag).Distinct().Count() == 1);
    }

    private DataVoContext Seed(StorageMode mode, bool indexed)
    {
        var db = new DataVoContext(new DataVoConfig
        {
            StorageMode = mode,
            DiskStoragePath = mode == StorageMode.InMemory ? null! : _directory,
            LsmStrictFsync = false,
        });
        Query(db, "CREATE DATABASE Hv");
        Query(db, "USE Hv");
        Query(db, "CREATE TABLE T (Id INT PRIMARY KEY, CustomerId INT, Tag VARCHAR(10), V INT)");
        Query(db, "CREATE TABLE Customers (Id INT PRIMARY KEY)");
        if (indexed)
        {
            Query(db, "CREATE INDEX ix_customer ON T (CustomerId)");
        }

        var random = new Random(42);
        string[] tags = ["a", "b", "c"];
        for (int id = 1; id <= 400; id++)
        {
            int customer = random.Next(1, 30);
            string? tag = random.Next(3) == 0 ? null : tags[random.Next(tags.Length)];
            int? v = random.Next(4) == 0 ? null : random.Next(0, 12);
            _rows.Add((id, customer, tag, v));
            Query(db, $"INSERT INTO T (Id, CustomerId, Tag, V) VALUES ({id}, {customer}, {(tag is null ? "NULL" : $"'{tag}'")}, {(v is null ? "NULL" : v.ToString())})");
        }

        for (int customer = 1; customer < 30; customer++)
        {
            Query(db, $"INSERT INTO Customers (Id) VALUES ({customer})");
        }

        return db;
    }

    private static QueryResult Query(DataVoContext db, string sql, Guid? session = null)
    {
        QueryResult result = (session is { } id ? db.Execute(sql, id) : db.Execute(sql))[^1];
        Assert.False(result.IsError, $"{sql} => {string.Join(" | ", result.Messages)}");
        return result;
    }
}
