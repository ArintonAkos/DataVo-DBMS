using DataVo.Core;
using DataVo.Core.Execution;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Sql;

/// <summary>
/// NOT in every form (prefix NOT, NOT LIKE, NOT BETWEEN, NOT IN lists) with SQL's NULL rules, and SUM's
/// result type: an integer for integer columns, a double for FLOAT, NULL over no values, on every
/// aggregate path (single table, GROUP BY, join) and in both storage modes.
/// </summary>
public sealed class SqlNotAndSumTests : IDisposable
{
    private readonly string? _directory;
    private readonly DataVoContext _db;

    public SqlNotAndSumTests()
    {
        _directory = Path.Combine(Path.GetTempPath(), "datavo-not-sum-" + Guid.NewGuid().ToString("N"));
        _db = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.Lsm, DiskStoragePath = _directory, LsmStrictFsync = false });
        Seed(_db);
    }

    public void Dispose()
    {
        _db.Dispose();
        try { Directory.Delete(_directory!, recursive: true); } catch (IOException) { }
    }

    private static void Seed(DataVoContext db)
    {
        Run(db, "CREATE DATABASE D");
        Run(db, "USE D");
        Run(db, "CREATE TABLE P (Id INT PRIMARY KEY, A INT, B INT, S VARCHAR(10), F FLOAT)");
        Run(db, "CREATE TABLE C (Id INT PRIMARY KEY, PId INT, V INT)");
        Run(db, "INSERT INTO P (Id, A, B, S, F) VALUES (1, 5, NULL, 'x', 1.5), (2, 10, 1, NULL, 2.5), (3, NULL, 2, 'y', NULL), (4, 7, 3, 'xy', 0.25)");
        Run(db, "INSERT INTO C (Id, PId, V) VALUES (10, 1, 3), (11, 1, 4), (12, 2, 5), (13, 9, 6)");
    }

    [Theory]
    [InlineData("NOT A > 6", new[] { 1 })]
    [InlineData("NOT (A > 6)", new[] { 1 })]
    [InlineData("NOT NOT A > 6", new[] { 2, 4 })]
    [InlineData("NOT (A > 6 AND B = 1)", new[] { 1, 3, 4 })] // row 3: A is NULL but B = 1 is false
    [InlineData("NOT (A > 6 OR B = 2)", new int[0])] // row 1: A > 6 is false but B = 2 is unknown, so NOT stays unknown
    [InlineData("NOT A > 6 AND B > 0", new int[0])]
    [InlineData("NOT S LIKE 'x%'", new[] { 3 })]
    [InlineData("S NOT LIKE 'x%'", new[] { 3 })]
    [InlineData("A NOT IN (5, 7)", new[] { 2 })]
    [InlineData("A NOT IN (5, NULL)", new int[0])] // a NULL in the list makes every row unknown
    [InlineData("A NOT BETWEEN 6 AND 8", new[] { 1, 2 })]
    [InlineData("NOT (A IS NULL)", new[] { 1, 2, 4 })]
    [InlineData("NOT (Id IN (SELECT PId FROM C))", new[] { 3, 4 })]
    [InlineData("Id NOT IN (SELECT PId FROM C)", new[] { 3, 4 })]
    [InlineData("Id NOT IN (SELECT PId FROM C WHERE V > 100)", new[] { 1, 2, 3, 4 })] // empty subquery
    public void Not_FollowsSqlThreeValuedLogic(string condition, int[] expected)
    {
        Assert.Equal(expected, Ids($"SELECT Id FROM P WHERE {condition} ORDER BY Id"));
    }

    [Fact]
    public void NotIn_ASubqueryWithANull_IsUnknownForEveryRow()
    {
        Run(_db, "INSERT INTO C (Id, PId, V) VALUES (14, NULL, 7)");
        Assert.Empty(Ids("SELECT Id FROM P WHERE Id NOT IN (SELECT PId FROM C)"));
        Assert.Equal([1, 2], Ids("SELECT Id FROM P WHERE Id IN (SELECT PId FROM C) ORDER BY Id"));
    }

    [Fact]
    public void Not_OnANonCondition_IsAParseError()
    {
        Assert.True(_db.Execute("SELECT Id FROM P WHERE NOT A").Last().IsError);
    }

    [Fact]
    public void Sum_OfIntegers_IsAnInteger_OnEveryPath()
    {
        Assert.Equal(22L, Single("SELECT SUM(A) AS s FROM P")["s"]);
        Assert.Equal(17L, Single("SELECT SUM(A) AS s FROM P WHERE Id >= 2")["s"]); // 10 + NULL + 7

        var groups = _db.Execute("SELECT B, SUM(A) AS s FROM P GROUP BY B").Last().Data;
        Assert.All(groups.Where(row => row["s"] is not null), row => Assert.IsType<long>(row["s"]));

        var joined = Single("SELECT SUM(C.V) AS s, SUM(P.A) AS t FROM P JOIN C ON P.Id = C.PId");
        Assert.Equal(12L, joined["s"]);
        Assert.Equal(20L, joined["t"]);
    }

    [Fact]
    public void Sum_OfFloats_IsADouble_AndOfNothing_IsNull()
    {
        Assert.Equal(4.25d, Assert.IsType<double>(Single("SELECT SUM(F) AS s FROM P")["s"]), 6);
        Assert.Null(Single("SELECT SUM(A) AS s FROM P WHERE Id > 100")["s"]);
        Assert.Null(Single("SELECT SUM(A) AS s FROM P WHERE Id = 3")["s"]);
    }

    [Fact]
    public void Sum_InMemory_MatchesLsm()
    {
        using var memory = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        Seed(memory);
        Assert.Equal(22L, memory.Execute("SELECT SUM(A) AS s FROM P").Last().Data.Single()["s"]);
    }

    [Fact]
    public void SumAccumulator_StaysExact_AndFallsBackToDoubleOnOverflow()
    {
        var exact = new SqlSumAccumulator();
        exact.Add(long.MaxValue - 1);
        exact.Add(1L);
        Assert.Equal(long.MaxValue, exact.Result);

        var overflow = new SqlSumAccumulator();
        overflow.Add(long.MaxValue);
        overflow.Add(1L);
        Assert.IsType<double>(overflow.Result);

        var merged = new SqlSumAccumulator();
        merged.Add(2L);
        var other = new SqlSumAccumulator();
        other.Add(3L);
        merged.Merge(other);
        Assert.Equal(5L, merged.Result);
        Assert.Equal(2, merged.Count);

        Assert.Null(new SqlSumAccumulator().Result);
    }

    private int[] Ids(string sql)
    {
        var result = _db.Execute(sql).Last();
        Assert.False(result.IsError, string.Join("; ", result.Messages));
        return result.Data.Select(row => Convert.ToInt32(row["Id"])).ToArray();
    }

    private Dictionary<string, object?> Single(string sql)
    {
        var result = _db.Execute(sql).Last();
        Assert.False(result.IsError, string.Join("; ", result.Messages));
        return Assert.Single(result.Data);
    }

    private static void Run(DataVoContext db, string sql)
    {
        var result = db.Execute(sql).Last();
        Assert.False(result.IsError, $"{sql}: {string.Join("; ", result.Messages)}");
    }
}
