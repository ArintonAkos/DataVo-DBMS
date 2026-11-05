using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Lsm;

/// <summary>
/// End to end through SQL: LSM-mode indexes are persisted as index tables, maintained by every DML
/// statement, served after reopen without a rebuild, and removed with their index or table.
/// </summary>
public sealed class LsmPersistedIndexSqlTests : IDisposable
{
    private readonly string _root = TestStorageDirectories.Create("lsm_persisted_index_sql");

    public void Dispose()
    {
        try
        {
            Directory.Delete(_root, recursive: true);
        }
        catch (IOException)
        {
        }
    }

    [Fact]
    public void Indexes_AreMaintainedByDml_AndServedAfterReopen()
    {
        using (DataVoContext context = Open())
        {
            Execute(context, "CREATE DATABASE Shop");
            Execute(context, "USE Shop");
            Execute(context, "CREATE TABLE Items (Id INT PRIMARY KEY, Name VARCHAR(20), Code INT)");
            Execute(context, "CREATE INDEX ix_code ON Items (Code)");
            Execute(context, "CREATE INDEX ix_name ON Items (Name)");
            Execute(context, "INSERT INTO Items (Id, Name, Code) VALUES (1, 'apple', 10), (2, 'pear', 20), (3, 'plum', 10), (4, 'fig', 30)");
            Execute(context, "UPDATE Items SET Code = 20 WHERE Id = 1");
            Execute(context, "DELETE FROM Items WHERE Id = 4");
        }

        string tableDirectory = Path.Combine(_root, "Shop", "Items", ".idx");
        Assert.True(File.Exists(Path.Combine(tableDirectory, "ix_code", "BUILT")));
        Assert.True(File.Exists(Path.Combine(tableDirectory, "_PK_Items", "BUILT")));

        using (DataVoContext reopened = Open())
        {
            Execute(reopened, "USE Shop");
            Assert.Equal([3], Ids(reopened, "SELECT Id FROM Items WHERE Code = 10"));
            Assert.Equal([1, 2], Ids(reopened, "SELECT Id FROM Items WHERE Code = 20"));
            Assert.Empty(Ids(reopened, "SELECT Id FROM Items WHERE Code = 30"));
            Assert.Equal([2], Ids(reopened, "SELECT Id FROM Items WHERE Name = 'pear'"));
            Assert.Equal([3], Ids(reopened, "SELECT Id FROM Items WHERE Id = 3"));

            // The persisted primary key still enforces uniqueness.
            var duplicate = reopened.Execute("INSERT INTO Items (Id, Name, Code) VALUES (2, 'dup', 99)");
            Assert.True(duplicate[0].IsError);
            Execute(reopened, "INSERT INTO Items (Id, Name, Code) VALUES (5, 'kiwi', 10)");
            Assert.Equal([3, 5], Ids(reopened, "SELECT Id FROM Items WHERE Code = 10"));

            Execute(reopened, "DROP INDEX ix_code ON Items");
            Assert.False(Directory.Exists(Path.Combine(tableDirectory, "ix_code")));
            Assert.Equal([3, 5], Ids(reopened, "SELECT Id FROM Items WHERE Code = 10")); // now a scan

            Execute(reopened, "DROP TABLE Items");
            Assert.False(Directory.Exists(Path.Combine(_root, "Shop", "Items")));
        }
    }

    [Fact]
    public void IndexCreatedOnAnEmptyTable_IsServedAfterReopen()
    {
        using (DataVoContext context = Open())
        {
            Execute(context, "CREATE DATABASE Shop");
            Execute(context, "USE Shop");
            Execute(context, "CREATE TABLE Items (Id INT PRIMARY KEY, Code INT)");
            Execute(context, "CREATE INDEX ix_code ON Items (Code)");
        }

        // Found by the SQLite differential test: the empty index had no data file, so reopen never
        // opened it and every query through it failed with "index does not exist" until the first write.
        using DataVoContext reopened = Open();
        Execute(reopened, "USE Shop");
        Assert.Empty(Ids(reopened, "SELECT Id FROM Items WHERE Code = 7"));
        Execute(reopened, "INSERT INTO Items (Id, Code) VALUES (1, 7)");
        Assert.Equal([1], Ids(reopened, "SELECT Id FROM Items WHERE Code = 7"));
    }

    [Fact]
    public void CreateIndex_OnPopulatedTable_BuildsThePersistedIndex()
    {
        using (DataVoContext context = Open())
        {
            Execute(context, "CREATE DATABASE Shop");
            Execute(context, "USE Shop");
            Execute(context, "CREATE TABLE Items (Id INT PRIMARY KEY, Code INT)");
            Execute(context, "INSERT INTO Items (Id, Code) VALUES (1, 7), (2, 8), (3, 7)");
            Execute(context, "CREATE INDEX ix_code ON Items (Code)");
            Assert.Equal([1, 3], Ids(context, "SELECT Id FROM Items WHERE Code = 7"));
        }

        using DataVoContext reopened = Open();
        Execute(reopened, "USE Shop");
        Assert.Equal([1, 3], Ids(reopened, "SELECT Id FROM Items WHERE Code = 7"));
    }

    private DataVoContext Open() => new(new DataVoConfig { StorageMode = StorageMode.Lsm, DiskStoragePath = _root, LsmStrictFsync = false });

    private static void Execute(DataVoContext context, string sql)
    {
        var result = context.Execute(sql);
        Assert.False(result[0].IsError, $"{sql}: {string.Join(" | ", result[0].Messages)}");
    }

    private static List<int> Ids(DataVoContext context, string sql)
    {
        var result = context.Execute(sql);
        Assert.False(result[0].IsError, $"{sql}: {string.Join(" | ", result[0].Messages)}");
        return [.. result[0].Data.Select(row => Convert.ToInt32(row["Id"])).Order()];
    }
}
