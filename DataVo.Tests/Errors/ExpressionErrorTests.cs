using DataVo.Core;
using DataVo.Core.Contracts.Results;

namespace DataVo.Tests.Errors;

/// <summary>Integer division by zero is an error (as in PostgreSQL and SQL Server), not infinity.</summary>
public sealed class DivisionByZeroTests
{
    [Theory]
    [InlineData("SELECT Id / 0 AS X FROM T")]
    [InlineData("SELECT * FROM T WHERE Code / 0 > 1")]
    [InlineData("SELECT Code / (Id - 1) AS X FROM T")]
    [InlineData("UPDATE T SET Code = Code / 0 WHERE Id = 1")]
    public void IntegerDivisionByZero_IsAnError(string sql)
    {
        using DataVoContext db = ErrorContractTests.Seeded();
        string before = MalformedStatementTests.Snapshot(db);

        QueryResult result = db.Execute(sql).Single();

        Assert.True(result.IsError, string.Join(" / ", result.Messages));
        Assert.Equal(DataVoErrorCodes.DivisionByZero, result.ErrorCode);
        ErrorContractTests.AssertClean(result);
        Assert.Equal(before, MalformedStatementTests.Snapshot(db));
    }

    [Fact]
    public void IntegerDivision_ByNonZero_StillWorks()
    {
        using DataVoContext db = ErrorContractTests.Seeded();

        QueryResult result = db.Execute("SELECT Code / 4 AS X FROM T WHERE Id = 1").Single();

        Assert.False(result.IsError);
        Assert.Equal(25d, Convert.ToDouble(result.Data.Single()["X"]));
    }
}

/// <summary>Comparing a numeric column with non-numeric text is a type error, not an empty result.</summary>
public sealed class ComparisonTypeMismatchTests
{
    [Theory]
    [InlineData("SELECT * FROM T WHERE Id = 'abc'")]
    [InlineData("SELECT * FROM T WHERE 'abc' = Code")]
    [InlineData("SELECT * FROM T WHERE Id > 'x' AND Code = 100")]
    [InlineData("DELETE FROM T WHERE Id = 'abc'")]
    [InlineData("UPDATE T SET Name = 'z' WHERE Code = 'abc'")]
    public void NumericColumnComparedWithText_IsATypeMismatch(string sql)
    {
        using DataVoContext db = ErrorContractTests.Seeded();
        string before = MalformedStatementTests.Snapshot(db);

        QueryResult result = db.Execute(sql).Single();

        Assert.True(result.IsError, string.Join(" / ", result.Messages));
        Assert.Equal(DataVoErrorCodes.TypeMismatch, result.ErrorCode);
        Assert.Equal(before, MalformedStatementTests.Snapshot(db));
    }

    [Theory]
    [InlineData("SELECT * FROM T WHERE Id = '1'", 1)]
    [InlineData("SELECT * FROM T WHERE Name = 'a'", 1)]
    [InlineData("SELECT * FROM T WHERE Code >= '150'", 1)]
    [InlineData("SELECT * FROM T WHERE Code < '150'", 1)]
    [InlineData("SELECT * FROM T WHERE Code != '100'", 1)]
    public void ComparableValues_StillWork(string sql, int expectedRows)
    {
        using DataVoContext db = ErrorContractTests.Seeded();

        QueryResult result = db.Execute(sql).Single();

        Assert.False(result.IsError, string.Join(" / ", result.Messages));
        Assert.Equal(expectedRows, result.Data.Count);
    }
}
