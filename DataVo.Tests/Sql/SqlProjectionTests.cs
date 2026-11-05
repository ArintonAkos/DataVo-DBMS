using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Sql;

/// <summary>
/// Output columns are named by their alias ("Id AS x" was returned as "Id AS x") and come in the order
/// the SELECT list wrote them (they were grouped by table, with aggregates and expressions last).
/// </summary>
public sealed class SqlProjectionTests : IDisposable
{
    private readonly DataVoContext _db = new(new DataVoConfig { StorageMode = StorageMode.InMemory });

    public SqlProjectionTests()
    {
        foreach (string sql in new[]
        {
            "CREATE DATABASE D", "USE D",
            "CREATE TABLE P (Id INT PRIMARY KEY, A INT)", "CREATE TABLE C (Id INT PRIMARY KEY, PId INT)",
            "INSERT INTO P (Id, A) VALUES (2, 2)", "INSERT INTO C (Id, PId) VALUES (20, 2)",
        })
        {
            Assert.False(_db.Execute(sql)[0].IsError, sql);
        }
    }

    public void Dispose() => _db.Dispose();

    [Theory]
    [InlineData("SELECT Id AS x, A FROM P", "x", "A")]
    [InlineData("SELECT P.Id AS x FROM P", "x")]
    [InlineData("SELECT Id AS x FROM P WHERE Id = 2", "x")]
    [InlineData("SELECT C.Id AS c, P.Id AS p FROM P JOIN C ON P.A = C.PId", "c", "p")]
    [InlineData("SELECT P.Id AS p, COUNT(*) AS n FROM P JOIN C ON P.A = C.PId GROUP BY P.Id", "p", "n")]
    [InlineData("SELECT P.Id AS p, C.Id AS c, A FROM P JOIN C ON P.A = C.PId", "p", "c", "P.A")]
    [InlineData("SELECT COUNT(*) AS n, A FROM P GROUP BY A", "n", "A")]
    [InlineData("SELECT MAX(Id) AS m, A, COUNT(*) AS n FROM P GROUP BY A", "m", "A", "n")]
    [InlineData("SELECT Id + 1 AS nxt, Id FROM P", "nxt", "Id")]
    [InlineData("SELECT A, Id FROM P", "A", "Id")]
    [InlineData("SELECT C.Id, P.* FROM P JOIN C ON P.A = C.PId", "C.Id", "P.Id", "P.A")]
    public void Columns_AreNamedAndOrderedLikeTheSelectList(string sql, params string[] columns)
    {
        var result = _db.Execute(sql)[0];
        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Equal(columns, Assert.Single(result.Data).Keys);
    }
}
