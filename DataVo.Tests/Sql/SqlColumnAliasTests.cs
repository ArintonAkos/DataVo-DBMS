using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Sql;

/// <summary>A plain column's alias names the output column ("Id AS x" was returned as "Id AS x").</summary>
public sealed class SqlColumnAliasTests : IDisposable
{
    private readonly DataVoContext _db = new(new DataVoConfig { StorageMode = StorageMode.InMemory });

    public SqlColumnAliasTests()
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
    public void Alias_NamesTheOutputColumn(string sql, params string[] columns)
    {
        var result = _db.Execute(sql)[0];
        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Equal(columns, Assert.Single(result.Data).Keys);
    }
}
