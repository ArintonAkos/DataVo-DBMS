using DataVo.Core;
using DataVo.Core.Contracts.Results;

namespace DataVo.Tests.Errors;

/// <summary>Malformed SQL must be rejected before it executes, and must leave the data untouched.</summary>
public sealed class MalformedStatementTests
{
    public static TheoryData<string> Incomplete => new()
    {
        "DELETE FROM T WHERE",
        "DELETE FROM T WHERE   ",
        "UPDATE T SET Code = 5 WHERE",
        "SELECT * FROM T WHERE",
        "INSERT INTO T (Id, Name, Code) VALUES (9, 'x', 9",
        "INSERT INTO T (Id, Name, Code) VALUES (9, 'x', 9), (10, 'y', 10",
        "INSERT INTO T (Id, Name, Code VALUES (9, 'x', 9)",
        "CREATE TABLE X (Id INT PRIMARY KEY,",
        "CREATE TABLE X (Id INT PRIMARY KEY",
        "UPDATE T SET",
        "DELETE FROM",
    };

    [Theory]
    [MemberData(nameof(Incomplete))]
    public void IncompleteStatement_IsASyntaxError_AndChangesNothing(string sql)
    {
        using DataVoContext db = ErrorContractTests.Seeded();
        string before = Snapshot(db);

        QueryResult result = db.Execute(sql).Single();

        Assert.True(result.IsError, $"'{sql}' was accepted: {string.Join(" / ", result.Messages)}");
        Assert.Equal(DataVoErrorCodes.Syntax, result.ErrorCode);
        Assert.NotNull(result.ErrorLine);
        ErrorContractTests.AssertClean(result);
        Assert.Equal(before, Snapshot(db));
        Assert.Contains(db.Execute("SELECT * FROM X"), r => r.IsError); // no half-created table
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData(";")]
    public void EmptyInput_ReportsEmptyStatement(string sql)
    {
        using DataVoContext db = ErrorContractTests.Seeded();

        QueryResult result = Assert.Single(db.Execute(sql));

        Assert.True(result.IsError);
        Assert.Equal(DataVoErrorCodes.EmptyStatement, result.ErrorCode);
    }

    internal static string Snapshot(DataVoContext db) =>
        string.Join("|", new[] { "T", "Parent", "Child" }.Select(table =>
            string.Join(";", db.Execute($"SELECT * FROM {table}")[0].Data
                .Select(row => string.Join(",", row.OrderBy(c => c.Key).Select(c => $"{c.Key}={c.Value}")))
                .Order())));
}
