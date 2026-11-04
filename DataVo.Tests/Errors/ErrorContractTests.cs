using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Errors;

/// <summary>
/// The public error contract: a failed statement reports IsError with a stable code and one clean
/// sentence (no exception type names, stack frames, or source paths); syntax errors carry a position.
/// </summary>
public sealed class ErrorContractTests
{
    public static TheoryData<string, string> DetectedFailures => new()
    {
        { "SELEC * FROM T", DataVoErrorCodes.Syntax },
        { "SELECT * FROM", DataVoErrorCodes.Syntax },
        { "INSERT INTO T (Id, Name, Code) VALUES (9, 'abc, 9)", DataVoErrorCodes.Syntax },
        { "SELECT * FROM T garbage garbage", DataVoErrorCodes.Syntax },
        { "SELECT * FROM NoSuchTable", DataVoErrorCodes.UnknownTable },
        { "DELETE FROM NoSuchTable WHERE Id = 1", DataVoErrorCodes.UnknownTable },
        { "DROP TABLE NoSuchTable", DataVoErrorCodes.UnknownTable },
        { "SELECT NoCol FROM T", DataVoErrorCodes.UnknownColumn },
        { "SELECT * FROM T WHERE NoCol = 1", DataVoErrorCodes.UnknownColumn },
        { "INSERT INTO T (Id, NoCol) VALUES (9, 2)", DataVoErrorCodes.UnknownColumn },
        { "CREATE INDEX ix_bad ON T (NoCol)", DataVoErrorCodes.UnknownColumn },
        { "CREATE DATABASE A", DataVoErrorCodes.DatabaseExists },
        { "CREATE TABLE T (Id INT PRIMARY KEY)", DataVoErrorCodes.TableExists },
        { "INSERT INTO T (Id, Name, Code) VALUES (9, 'x')", DataVoErrorCodes.ValueCountMismatch },
        { "INSERT INTO T (Id, Name, Code) VALUES (9, 'x', 9, 10)", DataVoErrorCodes.ValueCountMismatch },
        { "DELETE FROM Parent WHERE Id = 1", DataVoErrorCodes.ForeignKeyViolation },
        { "UPDATE T SET Id = 2 WHERE Id = 1", DataVoErrorCodes.UniqueViolation },
        { "UPDATE T SET Code = 100 WHERE Id >= 1", DataVoErrorCodes.UniqueViolation },
        { "INSERT INTO T (Id, Name, Code) VALUES (1, 'dup', 999)", DataVoErrorCodes.PrimaryKeyViolation },
        { "INSERT INTO T (Id, Name, Code) VALUES (NULL, 'n', 998)", DataVoErrorCodes.NullPrimaryKey },
        { "INSERT INTO T (Id, Name, Code) VALUES (9, 'u', 100)", DataVoErrorCodes.UniqueViolation },
        { "INSERT INTO Child (Id, ParentId) VALUES (9, 42)", DataVoErrorCodes.ForeignKeyViolation },
        { "INSERT INTO T (Id, Name, Code) VALUES ('abc', 'x', 9)", DataVoErrorCodes.TypeMismatch },
        { "INSERT INTO E (Id, V) VALUES (9, '[1,2]')", DataVoErrorCodes.TypeMismatch },
        { "USE NoSuchDb", DataVoErrorCodes.UnknownDatabase },
        { "UPDATE T SET NoCol = 1 WHERE Id = 1", DataVoErrorCodes.UnknownColumn },
        { "DELETE FROM T WHERE", DataVoErrorCodes.Syntax },
        { "", DataVoErrorCodes.EmptyStatement },
        { "COMMIT", DataVoErrorCodes.NoActiveTransaction },
        { "ROLLBACK", DataVoErrorCodes.NoActiveTransaction },
    };

    [Theory]
    [MemberData(nameof(DetectedFailures))]
    public void Failure_ReportsCodeAndCleanMessage(string sql, string expectedCode)
    {
        using DataVoContext db = Seeded();

        QueryResult result = db.Execute(sql).Single();

        Assert.True(result.IsError, string.Join(" / ", result.Messages));
        Assert.Equal(expectedCode, result.ErrorCode);
        AssertClean(result);
    }

    [Fact]
    public void SyntaxError_ReportsLineAndColumn()
    {
        using DataVoContext db = Seeded();

        // "WHER" parses as a table alias, so the first token the parser cannot use is "Id".
        QueryResult result = db.Execute("SELECT *\nFROM T\nWHER Id = 1").Single();

        Assert.Equal(DataVoErrorCodes.Syntax, result.ErrorCode);
        Assert.Equal(3, result.ErrorLine);
        Assert.Equal(6, result.ErrorColumn);
    }

    [Fact]
    public void QueryBeforeUse_ReportsNoDatabaseSelected()
    {
        using var db = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });

        QueryResult result = db.Execute("SELECT * FROM T").Single();

        Assert.Equal(DataVoErrorCodes.NoDatabaseSelected, result.ErrorCode);
        AssertClean(result);
    }

    [Fact]
    public void BeginTwice_ReportsTransactionAlreadyActive()
    {
        using DataVoContext db = Seeded();
        Assert.False(db.Execute("BEGIN").Single().IsError);

        QueryResult result = db.Execute("BEGIN").Single();

        Assert.Equal(DataVoErrorCodes.TransactionAlreadyActive, result.ErrorCode);
        AssertClean(result);
    }

    [Fact]
    public void SuccessfulStatement_HasNoErrorCode()
    {
        using DataVoContext db = Seeded();

        QueryResult result = db.Execute("SELECT * FROM T").Single();

        Assert.False(result.IsError);
        Assert.Null(result.ErrorCode);
        Assert.Null(result.ErrorLine);
    }

    internal static DataVoContext Seeded(StorageMode mode = StorageMode.InMemory, string? path = null)
    {
        var db = new DataVoContext(new DataVoConfig { StorageMode = mode, DiskStoragePath = path, LsmStrictFsync = false });
        foreach (string sql in new[]
        {
            "CREATE DATABASE A", "USE A",
            "CREATE TABLE T (Id INT PRIMARY KEY, Name VARCHAR(5), Code INT UNIQUE)",
            "CREATE TABLE Parent (Id INT PRIMARY KEY)",
            "CREATE TABLE Child (Id INT PRIMARY KEY, ParentId INT REFERENCES Parent(Id))",
            "CREATE TABLE E (Id INT PRIMARY KEY, V VECTOR(3))",
            "INSERT INTO T (Id, Name, Code) VALUES (1, 'a', 100)",
            "INSERT INTO T (Id, Name, Code) VALUES (2, 'b', 200)",
            "INSERT INTO Parent (Id) VALUES (1)",
            "INSERT INTO Child (Id, ParentId) VALUES (1, 1)",
        })
        {
            QueryResult? error = db.Execute(sql).FirstOrDefault(r => r.IsError);
            Assert.True(error is null, $"seed failed: {sql}: {string.Join(" / ", error?.Messages ?? [])}");
        }

        return db;
    }

    internal static void AssertClean(QueryResult result)
    {
        foreach (string message in result.Messages)
        {
            Assert.DoesNotContain("Exception", message, StringComparison.Ordinal);
            Assert.DoesNotContain("   at ", message, StringComparison.Ordinal);
            Assert.DoesNotContain(".cs:line", message, StringComparison.Ordinal);
            Assert.DoesNotContain('\n', message);
            Assert.False(message.StartsWith("Error:", StringComparison.OrdinalIgnoreCase), message);
        }
    }
}
