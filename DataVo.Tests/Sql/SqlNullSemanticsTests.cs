using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Sql;

/// <summary>
/// SQL NULL and empty-input semantics that the SQLite differential test found broken: a comparison with
/// NULL is unknown (never true), NULL joins nothing, and aggregates over no rows return one row.
/// </summary>
public sealed class SqlNullSemanticsTests : IDisposable
{
    private readonly DataVoContext _db = new(new DataVoConfig { StorageMode = StorageMode.InMemory });

    public SqlNullSemanticsTests()
    {
        Run("CREATE DATABASE D");
        Run("USE D");
        Run("CREATE TABLE P (Id INT PRIMARY KEY, A INT, B INT)");
    }

    public void Dispose() => _db.Dispose();

    [Fact]
    public void NotEquals_IsNeverTrue_ForNull()
    {
        Run("INSERT INTO P (Id, A, B) VALUES (1, 7, NULL), (2, NULL, 8), (3, 1, 2)");
        Assert.Equal([3], Ids("SELECT Id FROM P WHERE B <> 8"));

        // The data-loss case: DELETE must keep rows whose B is NULL.
        Run("DELETE FROM P WHERE B <> 8");
        Assert.Equal([1, 2], Ids("SELECT Id FROM P"));
    }

    [Fact]
    public void Join_DoesNotMatchNullToNull()
    {
        Run("CREATE TABLE C (Id INT PRIMARY KEY, PId INT)");
        Run("INSERT INTO P (Id, A, B) VALUES (1, NULL, 0), (2, 2, 0)");
        Run("INSERT INTO C (Id, PId) VALUES (10, NULL), (20, 2)");
        var rows = _db.Execute("SELECT P.Id AS p, C.Id AS c FROM P JOIN C ON P.A = C.PId")[0].Data;
        Assert.Single(rows);
        Assert.Equal(20, Convert.ToInt32(rows[0]["c"]));
    }

    [Fact]
    public void Aggregates_OverNoRows_ReturnOneRow()
    {
        var empty = Single("SELECT COUNT(*) AS c, COUNT(A) AS n, SUM(A) AS s, MIN(A) AS lo, MAX(A) AS hi, AVG(A) AS av FROM P");
        Assert.Equal(0, Convert.ToInt32(empty["c"]));
        Assert.Equal(0, Convert.ToInt32(empty["n"]));
        Assert.Null(empty["s"]);
        Assert.Null(empty["lo"]);
        Assert.Null(empty["hi"]);
        Assert.Null(empty["av"]);

        Run("INSERT INTO P (Id, A, B) VALUES (1, 5, 1)");
        Assert.Equal(0, Convert.ToInt32(Single("SELECT COUNT(*) AS c FROM P WHERE Id > 5")["c"]));
    }

    [Fact]
    public void Sum_OfOnlyNulls_IsNull()
    {
        Run("INSERT INTO P (Id, A, B) VALUES (1, NULL, 1), (2, NULL, 1), (3, 4, 2)");
        Assert.Null(Single("SELECT SUM(A) AS s FROM P WHERE B = 1")["s"]);
        Assert.Equal(4d, Convert.ToDouble(Single("SELECT SUM(A) AS s FROM P WHERE B = 2")["s"]));
    }

    private void Run(string sql)
    {
        var result = _db.Execute(sql)[0];
        Assert.False(result.IsError, $"{sql}: {string.Join(" | ", result.Messages)}");
    }

    private Dictionary<string, object?> Single(string sql)
    {
        var result = _db.Execute(sql)[0];
        Assert.False(result.IsError, $"{sql}: {string.Join(" | ", result.Messages)}");
        return Assert.Single(result.Data);
    }

    private List<int> Ids(string sql)
    {
        var result = _db.Execute(sql)[0];
        Assert.False(result.IsError, $"{sql}: {string.Join(" | ", result.Messages)}");
        return [.. result.Data.Select(row => Convert.ToInt32(row["Id"])).Order()];
    }
}
