using DataVo.Core;
using DataVo.Core.Contracts.Results;

namespace DataVo.Tests.Errors;

/// <summary>Statements naming objects that do not exist must fail and must not change session or data.</summary>
public sealed class UnknownObjectTests
{
    [Fact]
    public void UseUnknownDatabase_Fails_AndKeepsTheCurrentDatabase()
    {
        using DataVoContext db = ErrorContractTests.Seeded();

        QueryResult result = db.Execute("USE NoSuchDb").Single();

        Assert.True(result.IsError);
        Assert.Equal(DataVoErrorCodes.UnknownDatabase, result.ErrorCode);
        ErrorContractTests.AssertClean(result);
        Assert.False(db.Execute("SELECT * FROM T").Single().IsError); // still on database A
    }

    [Theory]
    [InlineData("UPDATE T SET NoCol = 1 WHERE Id = 1")]
    [InlineData("UPDATE T SET Name = 'z', NoCol = 1 WHERE Id = 1")]
    [InlineData("UPDATE T SET NoCol = 1")]
    public void UpdateUnknownColumn_Fails_AndChangesNothing(string sql)
    {
        using DataVoContext db = ErrorContractTests.Seeded();
        string before = MalformedStatementTests.Snapshot(db);

        QueryResult result = db.Execute(sql).Single();

        Assert.True(result.IsError, string.Join(" / ", result.Messages));
        Assert.Equal(DataVoErrorCodes.UnknownColumn, result.ErrorCode);
        ErrorContractTests.AssertClean(result);
        Assert.Equal(before, MalformedStatementTests.Snapshot(db));
    }

    [Fact]
    public void UpdateUnknownTable_ReportsUnknownTable()
    {
        using DataVoContext db = ErrorContractTests.Seeded();

        QueryResult result = db.Execute("UPDATE NoSuchTable SET Code = 1 WHERE Id = 1").Single();

        Assert.Equal(DataVoErrorCodes.UnknownTable, result.ErrorCode);
    }
}
