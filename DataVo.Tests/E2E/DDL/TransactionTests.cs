using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E.DDL;

/// <summary>
/// End-to-end tests for explicit transaction boundaries (BEGIN, COMMIT, ROLLBACK).
/// Each test is fully isolated using unique table names to prevent cross-test contamination.
/// </summary>
public abstract class TransactionTestsBase : SqlExecutionTestsBase
{
    protected TransactionTestsBase(DataVoConfig config, string testDbName) : base(config, testDbName)
    {
    }

    [Fact]
    public void BeginAndCommit_InsertsAreVisibleAfterCommit()
    {
        string table = $"T_{Guid.NewGuid():N}";
        Execute($"CREATE TABLE {table} (Id INT PRIMARY KEY, Name VARCHAR(50));");

        Execute("BEGIN;");
        Execute($"INSERT INTO {table} (Id, Name) VALUES (1, 'Alice');");
        Execute($"INSERT INTO {table} (Id, Name) VALUES (2, 'Bob');");
        Execute("COMMIT;");

        var result = ExecuteAndReturn($"SELECT * FROM {table};");
        Assert.NotNull(result);
        Assert.Equal(2, result.Data.Count);
    }

    [Fact]
    public void BeginAndRollback_InsertsAreDiscarded()
    {
        string table = $"T_{Guid.NewGuid():N}";
        Execute($"CREATE TABLE {table} (Id INT PRIMARY KEY, Name VARCHAR(50));");

        Execute("BEGIN;");
        Execute($"INSERT INTO {table} (Id, Name) VALUES (1, 'Alice');");
        Execute($"INSERT INTO {table} (Id, Name) VALUES (2, 'Bob');");
        Execute("ROLLBACK;");

        var result = ExecuteAndReturn($"SELECT * FROM {table};");
        Assert.NotNull(result);
        Assert.Empty(result.Data);
    }

    [Fact]
    public void AutoCommit_InsertsWithoutBeginAreVisibleImmediately()
    {
        string table = $"T_{Guid.NewGuid():N}";
        Execute($"CREATE TABLE {table} (Id INT PRIMARY KEY, Name VARCHAR(50));");

        // No BEGIN — auto-commit mode
        Execute($"INSERT INTO {table} (Id, Name) VALUES (1, 'Alice');");

        var result = ExecuteAndReturn($"SELECT * FROM {table};");
        Assert.NotNull(result);
        Assert.Single(result.Data);
    }

    [Fact]
    public void BeginTransaction_KeywordVariant_Works()
    {
        string table = $"T_{Guid.NewGuid():N}";
        Execute($"CREATE TABLE {table} (Id INT PRIMARY KEY, Name VARCHAR(50));");

        Execute("BEGIN TRANSACTION;");
        Execute($"INSERT INTO {table} (Id, Name) VALUES (1, 'Alice');");
        Execute("COMMIT;");

        var result = ExecuteAndReturn($"SELECT * FROM {table};");
        Assert.NotNull(result);
        Assert.Single(result.Data);
    }

    [Fact]
    public void Rollback_ReturnsCorrectMessage()
    {
        Execute("BEGIN;");
        var result = ExecuteAndReturn("ROLLBACK;");
        Assert.NotNull(result);
        Assert.Contains("Transaction rolled back.", result.Messages.FirstOrDefault() ?? "");
    }

    [Fact]
    public void Commit_ReturnsCorrectMessage()
    {
        Execute("BEGIN;");
        var result = ExecuteAndReturn("COMMIT;");
        Assert.NotNull(result);
        Assert.Contains("Transaction committed.", result.Messages.FirstOrDefault() ?? "");
    }

    [Fact]
    public void DoubleBegin_ReturnsError()
    {
        Execute("BEGIN;");
        var result = ExecuteAndReturn("BEGIN;");
        Assert.NotNull(result);
        Assert.Contains("already active", result.Messages.FirstOrDefault() ?? "");
        // Cleanup
        Execute("ROLLBACK;");
    }

    [Fact]
    public void CommitWithoutBegin_ReturnsError()
    {
        var result = ExecuteAndReturn("COMMIT;");
        Assert.NotNull(result);
        Assert.Contains("No active transaction", result.Messages.FirstOrDefault() ?? "");
    }

    [Fact]
    public void RollbackWithoutBegin_ReturnsError()
    {
        var result = ExecuteAndReturn("ROLLBACK;");
        Assert.NotNull(result);
        Assert.Contains("No active transaction", result.Messages.FirstOrDefault() ?? "");
    }

    [Fact]
    public void BeginAndRollback_DeletesAreDiscarded()
    {
        string table = $"T_{Guid.NewGuid():N}";
        Execute($"CREATE TABLE {table} (Id INT PRIMARY KEY, Name VARCHAR(50));");
        Execute($"INSERT INTO {table} (Id, Name) VALUES (1, 'Alice');");
        Execute($"INSERT INTO {table} (Id, Name) VALUES (2, 'Bob');");

        Execute("BEGIN;");
        Execute($"DELETE FROM {table} WHERE Id = 1;");
        Execute("ROLLBACK;");

        var result = ExecuteAndReturn($"SELECT * FROM {table};");
        Assert.NotNull(result);
        Assert.Equal(2, result.Data.Count);
    }

    [Fact]
    public void BeginAndCommit_DeletesAreApplied()
    {
        string table = $"T_{Guid.NewGuid():N}";
        Execute($"CREATE TABLE {table} (Id INT PRIMARY KEY, Name VARCHAR(50));");
        Execute($"INSERT INTO {table} (Id, Name) VALUES (1, 'Alice');");
        Execute($"INSERT INTO {table} (Id, Name) VALUES (2, 'Bob');");

        Execute("BEGIN;");
        Execute($"DELETE FROM {table} WHERE Id = 1;");
        Execute("COMMIT;");

        var result = ExecuteAndReturn($"SELECT * FROM {table};");
        Assert.NotNull(result);
        Assert.Single(result.Data);
    }

    [Fact]
    public void BeginAndRollback_UpdatesAreDiscarded()
    {
        string table = $"T_{Guid.NewGuid():N}";
        Execute($"CREATE TABLE {table} (Id INT PRIMARY KEY, Name VARCHAR(50));");
        Execute($"INSERT INTO {table} (Id, Name) VALUES (1, 'Alice');");

        Execute("BEGIN;");
        Execute($"UPDATE {table} SET Name = 'Updated' WHERE Id = 1;");
        Execute("ROLLBACK;");

        var result = ExecuteAndReturn($"SELECT * FROM {table};");
        Assert.NotNull(result);
        Assert.Single(result.Data);
        Assert.Equal("Alice", result.Data[0]["Name"]?.ToString());
    }

    [Fact]
    public void BeginAndCommit_UpdatesAreApplied()
    {
        string table = $"T_{Guid.NewGuid():N}";
        Execute($"CREATE TABLE {table} (Id INT PRIMARY KEY, Name VARCHAR(50));");
        Execute($"INSERT INTO {table} (Id, Name) VALUES (1, 'Alice');");

        Execute("BEGIN;");
        Execute($"UPDATE {table} SET Name = 'Updated' WHERE Id = 1;");
        Execute("COMMIT;");

        var result = ExecuteAndReturn($"SELECT * FROM {table};");
        Assert.NotNull(result);
        Assert.Single(result.Data);
        Assert.Equal("Updated", result.Data[0]["Name"]?.ToString());
    }
}

public class TransactionTestsMemory : TransactionTestsBase
{
    public TransactionTestsMemory() : base(new DataVoConfig { StorageMode = StorageMode.InMemory }, "TxMemDb") { }
}

public class TransactionTestsDisk : TransactionTestsBase
{
    public TransactionTestsDisk() : base(new DataVoConfig { StorageMode = StorageMode.Disk, DiskStoragePath = "TxDiskDb" }, "TxDiskDb") { }
}
