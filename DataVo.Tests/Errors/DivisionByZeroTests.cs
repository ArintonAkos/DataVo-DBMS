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
