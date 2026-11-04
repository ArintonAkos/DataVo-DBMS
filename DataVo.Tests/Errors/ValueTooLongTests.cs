using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Exceptions;
using DataVo.Core.Runtime.Reactive;

namespace DataVo.Tests.Errors;

/// <summary>A string longer than its VARCHAR(n) column is rejected on every write path, never truncated.</summary>
public sealed class ValueTooLongTests
{
    [Theory]
    [InlineData("INSERT INTO T (Id, Name, Code) VALUES (9, 'muchtoolong', 9)")]
    [InlineData("INSERT INTO T (Id, Name, Code) VALUES (9, 'ok', 9), (10, 'muchtoolong', 10)")]
    [InlineData("UPDATE T SET Name = 'muchtoolong' WHERE Id = 1")]
    public void Sql_RejectsOverLongValue_AndChangesNothing(string sql)
    {
        using DataVoContext db = ErrorContractTests.Seeded();
        string before = MalformedStatementTests.Snapshot(db);

        QueryResult result = db.Execute(sql).Single();

        Assert.True(result.IsError, string.Join(" / ", result.Messages));
        Assert.Equal(DataVoErrorCodes.ValueTooLong, result.ErrorCode);
        Assert.Contains("Name", result.Messages[0], StringComparison.Ordinal);
        ErrorContractTests.AssertClean(result);
        Assert.Equal(before, MalformedStatementTests.Snapshot(db));
    }

    [Fact]
    public void ValueAtTheLimit_IsStoredUnchanged()
    {
        using DataVoContext db = ErrorContractTests.Seeded();

        Assert.False(db.Execute("INSERT INTO T (Id, Name, Code) VALUES (9, 'abcde', 9)").Single().IsError);

        Assert.Equal("abcde", db.Execute("SELECT Name FROM T WHERE Id = 9").Single().Data.Single()["Name"]);
    }

    [Fact]
    public void TypedInsert_RejectsOverLongValue()
    {
        using DataVoContext db = ErrorContractTests.Seeded();
        var schema = new ReactiveRowSchema("Id", "Name", "Code");

        var ex = Assert.Throws<ConstraintViolationException>(() =>
            db.InsertTyped("T", schema, [CellValue.From(9), CellValue.From("muchtoolong"), CellValue.From(9)]));

        Assert.Equal(DataVoErrorCodes.ValueTooLong, ex.ErrorCode);
    }

    [Fact]
    public void BulkInsert_RejectsOverLongValue()
    {
        using DataVoContext db = ErrorContractTests.Seeded();
        string before = MalformedStatementTests.Snapshot(db);

        var ex = Assert.Throws<ConstraintViolationException>(() => db.BulkInsert("T",
        [
            new Dictionary<string, object?> { ["Id"] = 9, ["Name"] = "muchtoolong", ["Code"] = 9 },
        ]));

        Assert.Equal(DataVoErrorCodes.ValueTooLong, ex.ErrorCode);
        Assert.Equal(before, MalformedStatementTests.Snapshot(db));
    }
}
