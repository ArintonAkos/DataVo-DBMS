using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Sql;

/// <summary>
/// <c>COUNT</c> is a 64-bit integer (<see cref="long"/>) on every execution path and backend: typed scans, the general
/// path (joins, derived expressions), grouped and ungrouped queries, empty inputs, transactions and prepared statements.
/// </summary>
public sealed class SqlCountTypeTests : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"datavo_count_{Guid.NewGuid():N}");

    public static TheoryData<StorageMode> Modes => new() { StorageMode.InMemory, StorageMode.Lsm };

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void Count_IsInt64_OnEveryPath(StorageMode mode)
    {
        using DataVoContext db = Seed(mode);
        string[] queries =
        [
            "SELECT COUNT(*) AS C FROM T",
            "SELECT COUNT(V) AS C FROM T",
            "SELECT COUNT(DISTINCT G) AS C FROM T",
            "SELECT COUNT(*) AS C FROM T WHERE V > 1000",
            "SELECT COUNT(*) AS C FROM E",
            "SELECT G, COUNT(*) AS C FROM T GROUP BY G",
            "SELECT G, COUNT(V) AS C FROM T GROUP BY G HAVING COUNT(V) >= 0",
            "SELECT COUNT(*) AS C FROM T t JOIN U u ON t.G = u.Id",
            "SELECT u.Id, COUNT(t.Id) AS C FROM U u LEFT JOIN T t ON t.G = u.Id GROUP BY u.Id",
            "SELECT G, COUNT(*) AS C FROM T WHERE V + 0 >= 0 GROUP BY G",
            "SELECT COUNT(*) AS C FROM T WHERE Id IN (SELECT Id FROM T WHERE G = 1)",
            "SELECT COUNT(*) AS C FROM T GROUP BY G HAVING COUNT(*) > 100",
        ];

        foreach (string sql in queries)
        {
            AssertInt64Counts(Query(db, sql), sql);
        }

        Guid tx = Guid.NewGuid();
        Query(db, "USE Ct", tx);
        Query(db, "BEGIN", tx);
        Query(db, "INSERT INTO T (Id, G, V) VALUES (999, 1, NULL)", tx);
        foreach (string sql in queries)
        {
            AssertInt64Counts(Query(db, sql, tx), $"{sql} (in a transaction)");
        }

        Query(db, "ROLLBACK", tx);

        DataVoPreparedStatement prepared = db.Prepare("SELECT COUNT(*) AS C FROM T WHERE G = @g");
        AssertInt64Counts(prepared.Execute(("g", 1)), "prepared");
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void EmptyAndUngrouped_CountIsZeroAsInt64(StorageMode mode)
    {
        using DataVoContext db = Seed(mode);

        Assert.Equal(0L, Assert.Single(Query(db, "SELECT COUNT(*) AS C FROM E").Data)["C"]);
        Assert.Equal(0L, Assert.Single(Query(db, "SELECT COUNT(*) AS C FROM T WHERE V > 1000").Data)["C"]);
        Assert.Equal(0L, Assert.Single(Query(db, "SELECT COUNT(*) AS C FROM T t JOIN U u ON t.G = u.Id WHERE u.Id > 1000").Data)["C"]);
    }

    private static void AssertInt64Counts(QueryResult result, string because)
    {
        Assert.False(result.IsError, $"{because} => {string.Join(" | ", result.Messages)}");
        foreach (Dictionary<string, object?> row in result.Data)
        {
            Assert.True(row["C"] is long, $"{because}: COUNT returned {row["C"]?.GetType().Name ?? "null"}");
        }
    }

    private DataVoContext Seed(StorageMode mode)
    {
        var db = new DataVoContext(new DataVoConfig
        {
            StorageMode = mode,
            DataDirectory = mode == StorageMode.InMemory ? null! : _directory,
            LsmStrictFsync = false,
        });
        Query(db, "CREATE DATABASE Ct");
        Query(db, "USE Ct");
        Query(db, "CREATE TABLE T (Id INT PRIMARY KEY, G INT, V INT)");
        Query(db, "CREATE TABLE U (Id INT PRIMARY KEY)");
        Query(db, "CREATE TABLE E (Id INT PRIMARY KEY)");
        for (int id = 1; id <= 30; id++)
        {
            Query(db, $"INSERT INTO T (Id, G, V) VALUES ({id}, {id % 3}, {(id % 4 == 0 ? "NULL" : id.ToString())})");
        }

        Query(db, "INSERT INTO U (Id) VALUES (0), (1), (2), (5)");
        return db;
    }

    private static QueryResult Query(DataVoContext db, string sql, Guid? session = null)
    {
        QueryResult result = (session is { } id ? db.Execute(sql, id) : db.Execute(sql))[^1];
        Assert.False(result.IsError, $"{sql} => {string.Join(" | ", result.Messages)}");
        return result;
    }
}
