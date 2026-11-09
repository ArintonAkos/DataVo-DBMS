using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E.DQL;

/// <summary>A SELECT list may contain constants, with or without a FROM clause.</summary>
public sealed class LiteralProjectionTests : IDisposable
{
    private readonly DataVoContext _db = new(new DataVoConfig { StorageMode = StorageMode.InMemory });

    public LiteralProjectionTests()
    {
        Ok("CREATE DATABASE L");
        Ok("USE L");
        Ok("CREATE TABLE T (Id INT PRIMARY KEY, Name VARCHAR(20))");
        Ok("INSERT INTO T (Id, Name) VALUES (1, 'a')");
        Ok("INSERT INTO T (Id, Name) VALUES (2, 'b')");
    }

    public void Dispose() => _db.Dispose();

    [Theory]
    [InlineData("'x'", "x")]
    [InlineData("'it''s'", "it's")]
    [InlineData("''", "")]
    [InlineData("7", 7)]
    [InlineData("2.5", 2.5)]
    public void WithoutFrom_ReturnsOneRow(string literal, object expected)
    {
        QueryResult result = Ok($"SELECT {literal} AS y");

        Assert.Equal(["y"], result.Fields);
        Assert.Equal(expected, Assert.Single(result.Data)["y"]);
    }

    [Fact]
    public void WithoutFrom_AcceptsSeveralConstantsAndNull()
    {
        Dictionary<string, object?> row = Assert.Single(Ok("SELECT 'a' AS First, 2 AS Second, NULL AS Third").Data);

        Assert.Equal("a", row["First"]);
        Assert.Equal(2, row["Second"]);
        Assert.Null(row["Third"]);
    }

    [Fact]
    public void WithoutFrom_AndWithoutAlias_NamesTheColumnAfterTheExpression()
    {
        Assert.Single(Ok("SELECT 1").Data);
    }

    [Fact]
    public void WithFrom_RepeatsTheConstantOnEveryRow()
    {
        QueryResult result = Ok("SELECT 'x' AS Tag, Id FROM T");

        Assert.Equal(2, result.Data.Count);
        Assert.All(result.Data, row => Assert.Equal("x", row["Tag"]));
        Assert.Equal(["Tag", "Id"], result.Fields);
    }

    [Fact]
    public void WithFrom_RespectsWhereAndLimit()
    {
        Assert.Equal(1, Assert.Single(Ok("SELECT 1 AS One FROM T WHERE Id = 2").Data)["One"]);
        Assert.Single(Ok("SELECT 'x' AS Tag FROM T LIMIT 1").Data);
    }

    [Fact]
    public void Parameter_CanBeProjected()
    {
        QueryResult result = _db.Prepare("SELECT @v AS Echo").Execute(("v", "O'Brien"));

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Equal("O'Brien", Assert.Single(result.Data)["Echo"]);
    }

    [Fact]
    public void WithoutFrom_ColumnReferenceIsAnError()
    {
        Assert.True(_db.Execute("SELECT Id AS y")[^1].IsError);
    }

    [Fact]
    public void BadColumns_StillFailWithFrom()
    {
        Assert.True(_db.Execute("SELECT Missing FROM T")[^1].IsError);
    }

    private QueryResult Ok(string sql)
    {
        QueryResult result = _db.Execute(sql)[^1];
        Assert.False(result.IsError, $"{sql} => {string.Join(" | ", result.Messages)}");
        return result;
    }
}
