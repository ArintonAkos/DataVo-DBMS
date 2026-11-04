using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Exceptions;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Errors;

/// <summary>A statement that fails for one row must not keep any of its other rows.</summary>
public sealed class AllOrNothingStatementTests
{
    public static TheoryData<StorageMode> Modes => new() { StorageMode.InMemory, StorageMode.Lsm };

    [Theory]
    [MemberData(nameof(Modes))]
    public void MultiRowInsert_WithOneViolation_InsertsNothing(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        string before = MalformedStatementTests.Snapshot(db);

        QueryResult result = db.Execute("INSERT INTO T (Id, Name, Code) VALUES (10, 'a', 10), (11, 'b', 11), (1, 'c', 12)").Single();

        Assert.True(result.IsError);
        Assert.Equal(DataVoErrorCodes.PrimaryKeyViolation, result.ErrorCode);
        Assert.Equal(before, MalformedStatementTests.Snapshot(db));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void MultiRowInsert_WithDuplicateInsideTheStatement_InsertsNothing(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        string before = MalformedStatementTests.Snapshot(db);

        QueryResult result = db.Execute("INSERT INTO T (Id, Name, Code) VALUES (20, 'a', 20), (20, 'b', 21)").Single();

        Assert.Equal(DataVoErrorCodes.PrimaryKeyViolation, result.ErrorCode);
        Assert.Equal(before, MalformedStatementTests.Snapshot(db));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void TypedBatch_WithOneViolation_InsertsNothing(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        string before = MalformedStatementTests.Snapshot(db);
        var schema = new ReactiveRowSchema("Id", "Name", "Code");

        var ex = Assert.Throws<ConstraintViolationException>(() => db.InsertTypedBatch("T", schema,
        [
            [CellValue.From(30), CellValue.From("a"), CellValue.From(30)],
            [CellValue.From(31), CellValue.From("b"), CellValue.From(100)], // Code 100 already exists
        ]));

        Assert.Equal(DataVoErrorCodes.UniqueViolation, ex.ErrorCode);
        Assert.Equal(before, MalformedStatementTests.Snapshot(db));
    }

    private static DataVoContext Open(StorageMode mode) => mode == StorageMode.InMemory
        ? ErrorContractTests.Seeded()
        : ErrorContractTests.Seeded(StorageMode.Lsm, TestStorageDirectories.Create("all_or_nothing"));
}
