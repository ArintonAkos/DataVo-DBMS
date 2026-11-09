using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E.DDL;

/// <summary>
/// Table and database names are case-insensitive in SQL, so every statement must reach the same stored
/// table however the name is spelled, in every storage mode and across a restart.
/// </summary>
public class IdentifierCaseTests : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"datavo_case_{Guid.NewGuid():N}");

    public static TheoryData<StorageMode> StorageModes => new() { StorageMode.Lsm, StorageMode.Disk, StorageMode.InMemory };

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }

    [Theory]
    [MemberData(nameof(StorageModes))]
    public void Dml_WithDifferentlyCasedTableName_ReachesTheSameRows(StorageMode mode)
    {
        using DataVoContext context = Open(mode);
        Ok(context, "CREATE DATABASE Shop");
        Ok(context, "USE Shop");
        Ok(context, "CREATE TABLE Items (Id INT PRIMARY KEY, Qty INT)");

        Ok(context, "INSERT INTO items (Id, Qty) VALUES (1, 10)");
        Ok(context, "INSERT INTO ITEMS (Id, Qty) VALUES (2, 20)");
        Ok(context, "UPDATE iTeMs SET Qty = 11 WHERE Id = 1");
        Ok(context, "DELETE FROM ITEMS WHERE Id = 2");

        QueryResult rows = Ok(context, "SELECT Id, Qty FROM Items");
        Dictionary<string, object?> row = Assert.Single(rows.Data);
        Assert.Equal(1, row["Id"]);
        Assert.Equal(11, row["Qty"]);
    }

    [Theory]
    [MemberData(nameof(StorageModes))]
    public void AlterTable_WithDifferentlyCasedName_KeepsRowsReadable(StorageMode mode)
    {
        using DataVoContext context = Open(mode);
        Ok(context, "CREATE DATABASE Shop");
        Ok(context, "USE Shop");
        Ok(context, "CREATE TABLE Items (Id INT PRIMARY KEY, Qty INT)");
        Ok(context, "INSERT INTO Items (Id, Qty) VALUES (1, 10)");

        Ok(context, "ALTER TABLE items ADD COLUMN Extra INT DEFAULT 7");

        foreach (string spelling in new[] { "Items", "items", "ITEMS" })
        {
            Dictionary<string, object?> row = Assert.Single(Ok(context, $"SELECT Id, Qty, Extra FROM {spelling}").Data);
            Assert.Equal(10, row["Qty"]);
            Assert.Equal(7, row["Extra"]);
        }

        Ok(context, "ALTER TABLE ITEMS DROP COLUMN Extra");
        Assert.DoesNotContain("Extra", Ok(context, "SELECT * FROM iTeMs").Fields);
    }

    [Theory]
    [MemberData(nameof(StorageModes))]
    public void Index_CreatedAndDroppedWithDifferentCase_ServesTheSameTable(StorageMode mode)
    {
        using DataVoContext context = Open(mode);
        Ok(context, "CREATE DATABASE Shop");
        Ok(context, "USE Shop");
        Ok(context, "CREATE TABLE Items (Id INT PRIMARY KEY, Grp INT)");
        Ok(context, "INSERT INTO Items (Id, Grp) VALUES (1, 5)");
        Ok(context, "CREATE INDEX ix_grp ON items (Grp)");
        Ok(context, "INSERT INTO ITEMS (Id, Grp) VALUES (2, 5)");

        Assert.Equal(2, Ok(context, "SELECT Id FROM Items WHERE Grp = 5").Data.Count);

        Ok(context, "DROP INDEX ix_grp ON ITEMS");
        Assert.Equal(2, Ok(context, "SELECT Id FROM items WHERE Grp = 5").Data.Count);
    }

    [Theory]
    [MemberData(nameof(StorageModes))]
    public void Drop_WithDifferentCase_RemovesRowsSoRecreateStartsEmpty(StorageMode mode)
    {
        using DataVoContext context = Open(mode);
        Ok(context, "CREATE DATABASE Shop");
        Ok(context, "USE Shop");
        Ok(context, "CREATE TABLE Items (Id INT PRIMARY KEY)");
        Ok(context, "INSERT INTO Items (Id) VALUES (1)");

        Ok(context, "DROP TABLE ITEMS");
        Ok(context, "CREATE TABLE Items (Id INT PRIMARY KEY)");

        Assert.Empty(Ok(context, "SELECT Id FROM Items").Data);
    }

    [Theory]
    [MemberData(nameof(StorageModes))]
    public void DatabaseName_WithDifferentCase_ReachesTheSameDatabase(StorageMode mode)
    {
        using DataVoContext context = Open(mode);
        Ok(context, "CREATE DATABASE Shop");
        Ok(context, "USE shop");
        Ok(context, "CREATE TABLE Items (Id INT PRIMARY KEY)");
        Ok(context, "INSERT INTO Items (Id) VALUES (1)");

        Ok(context, "USE SHOP");
        Assert.Single(Ok(context, "SELECT Id FROM Items").Data);

        Ok(context, "USE Shop");
        Assert.Single(Ok(context, "SELECT Id FROM ITEMS").Data);
    }

    [Theory]
    [InlineData(StorageMode.Lsm)]
    [InlineData(StorageMode.Disk)]
    public void Restart_AfterWritesInMixedCase_KeepsEveryRowInOneTable(StorageMode mode)
    {
        using (DataVoContext writer = Open(mode))
        {
            Ok(writer, "CREATE DATABASE Shop");
            Ok(writer, "USE shop");
            Ok(writer, "CREATE TABLE Items (Id INT PRIMARY KEY, Qty INT)");
            Ok(writer, "INSERT INTO items (Id, Qty) VALUES (1, 10)");
            Ok(writer, "INSERT INTO ITEMS (Id, Qty) VALUES (2, 20)");
            Ok(writer, "ALTER TABLE iTeMs ADD COLUMN Extra INT DEFAULT 7");
        }

        using DataVoContext reader = Open(mode);
        Ok(reader, "USE SHOP");
        Assert.Equal(2, Ok(reader, "SELECT Id, Extra FROM items").Data.Count);
        Assert.Equal(2, Ok(reader, "SELECT Id, Extra FROM Items").Data.Count);

        // Disk mode keeps its index folders lower-cased by design, so only its table files are compared.
        string[] tableEntries = mode == StorageMode.Disk
            ? Directory.GetFiles(_directory, "*.dat", SearchOption.AllDirectories)
            : Directory.GetDirectories(_directory, "*", SearchOption.AllDirectories);
        string[] spellings = tableEntries
            .Select(Path.GetFileNameWithoutExtension)
            .Where(IsItemsEntry)
            .Distinct(StringComparer.Ordinal)
            .ToArray()!;
        Assert.Equal(["Items"], spellings);
        Assert.Equal("Shop", Path.GetFileName(ShopDirectory(mode)));
    }

    [Fact]
    public void JoinsAndSubqueries_WithDifferentlyCasedNames_ReadTheSameRows()
    {
        using DataVoContext context = Open(StorageMode.Lsm);
        Ok(context, "CREATE DATABASE Shop");
        Ok(context, "USE Shop");
        Ok(context, "CREATE TABLE Items (Id INT PRIMARY KEY, Grp INT)");
        Ok(context, "CREATE TABLE Groups (Id INT PRIMARY KEY, Name VARCHAR(20))");
        Ok(context, "INSERT INTO items (Id, Grp) VALUES (1, 5)");
        Ok(context, "INSERT INTO GROUPS (Id, Name) VALUES (5, 'five')");

        Assert.Single(Ok(context, "SELECT i.Id FROM ITEMS i JOIN groups g ON i.Grp = g.Id WHERE g.Name = 'five'").Data);
        Assert.Single(Ok(context, "SELECT Id FROM iTeMs WHERE Grp IN (SELECT Id FROM GROUPS)").Data);
    }

    [Fact]
    public void Prepared_WithDifferentlyCasedName_SeesTheSameRowsAndAlterations()
    {
        using DataVoContext context = Open(StorageMode.Lsm);
        Ok(context, "CREATE DATABASE Shop");
        Ok(context, "USE Shop");
        Ok(context, "CREATE TABLE Items (Id INT PRIMARY KEY, Qty INT)");
        Ok(context, "INSERT INTO Items (Id, Qty) VALUES (1, 10)");

        var select = context.Prepare("SELECT Id, Qty FROM items WHERE Id = @id");
        Assert.Single(select.Execute(("id", 1)).Data);

        var update = context.Prepare("UPDATE ITEMS SET Qty = @qty WHERE Id = @id");
        Assert.False(update.Execute(("qty", 99), ("id", 1)).IsError);
        Assert.Equal(99, Assert.Single(Ok(context, "SELECT Qty FROM Items").Data)["Qty"]);

        Ok(context, "ALTER TABLE iTeMs ADD COLUMN Extra INT DEFAULT 7");
        QueryResult afterAlter = select.Execute(("id", 1));
        Assert.False(afterAlter.IsError, string.Join(" | ", afterAlter.Messages));
        Assert.Single(afterAlter.Data);
    }

    [Fact]
    public void Batch_CreatesAndUsesNamesInOtherCase_ReachesOneTable()
    {
        using DataVoContext context = Open(StorageMode.Lsm);
        List<QueryResult> results = context.Execute("""
            CREATE DATABASE Shop;
            USE shop;
            CREATE TABLE Items (Id INT PRIMARY KEY, Qty INT);
            INSERT INTO items (Id, Qty) VALUES (1, 10);
            DROP TABLE items;
            CREATE TABLE ITEMS (Id INT PRIMARY KEY, Qty INT);
            INSERT INTO items (Id, Qty) VALUES (2, 20);
            """);
        Assert.False(results.Any(result => result.IsError), string.Join(" | ", results.SelectMany(result => result.Messages)));

        Assert.Equal(2, Assert.Single(Ok(context, "SELECT Id FROM iTeMs").Data)["Id"]);
    }

    [Fact]
    public void QualifiedColumns_InAnyCase_ResolveToTheTable()
    {
        using DataVoContext context = Open(StorageMode.Lsm);
        Ok(context, "CREATE DATABASE Shop");
        Ok(context, "USE Shop");
        Ok(context, "CREATE TABLE Items (Id INT PRIMARY KEY, Grp INT)");
        Ok(context, "CREATE TABLE Groups (Id INT PRIMARY KEY, Name VARCHAR(20))");
        Ok(context, "INSERT INTO Items (Id, Grp) VALUES (1, 5)");
        Ok(context, "INSERT INTO Groups (Id, Name) VALUES (5, 'five')");

        Assert.Single(Ok(context, "SELECT items.Id FROM items WHERE items.Grp = 5").Data);
        Assert.Single(Ok(context, "SELECT ITEMS.Id FROM items WHERE Items.Grp = 5").Data);
        Assert.Single(Ok(context, "SELECT items.Id, GROUPS.Name FROM items JOIN groups ON Items.Grp = Groups.Id").Data);
        Ok(context, "UPDATE ITEMS SET Grp = 6 WHERE items.Id = 1");
        Assert.Equal(6, Assert.Single(Ok(context, "SELECT Grp FROM Items").Data)["Grp"]);
    }

    [Fact]
    public void ForeignKey_DeclaredInOtherCase_IsEnforcedAgainstTheParent()
    {
        using DataVoContext context = Open(StorageMode.Lsm);
        Ok(context, "CREATE DATABASE Shop");
        Ok(context, "USE Shop");
        Ok(context, "CREATE TABLE Parents (Id INT PRIMARY KEY)");
        Ok(context, "CREATE TABLE Children (Id INT PRIMARY KEY, ParentId INT REFERENCES PARENTS (Id))");
        Ok(context, "INSERT INTO parents (Id) VALUES (1)");

        Ok(context, "INSERT INTO children (Id, ParentId) VALUES (1, 1)");
        Assert.True(context.Execute("INSERT INTO CHILDREN (Id, ParentId) VALUES (2, 99)")[^1].IsError);
    }

    [Fact]
    public void TypedApi_WithDifferentlyCasedName_WritesToTheSameTable()
    {
        using DataVoContext context = Open(StorageMode.Lsm);
        Ok(context, "CREATE DATABASE Shop");
        Ok(context, "USE Shop");
        Ok(context, "CREATE TABLE Items (Id INT PRIMARY KEY, Qty INT)");

        context.BulkInsert("items", [new Dictionary<string, object?> { ["Id"] = 1, ["Qty"] = 10 }]);

        Assert.Single(Ok(context, "SELECT Id FROM Items").Data);
    }

    [Fact]
    public void Transaction_WritesInMixedCase_CommitAndRollBackTogether()
    {
        using DataVoContext context = Open(StorageMode.Lsm);
        Ok(context, "CREATE DATABASE Shop");
        Ok(context, "USE Shop");
        Ok(context, "CREATE TABLE Items (Id INT PRIMARY KEY, Qty INT)");
        Ok(context, "INSERT INTO Items (Id, Qty) VALUES (1, 10)");

        Ok(context, "BEGIN");
        Ok(context, "UPDATE items SET Qty = 11 WHERE Id = 1");
        Ok(context, "UPDATE ITEMS SET Qty = 12 WHERE Id = 1");
        Ok(context, "ROLLBACK");

        Assert.Equal(10, Assert.Single(Ok(context, "SELECT Qty FROM Items").Data)["Qty"]);
    }

    private static bool IsItemsEntry(string? name) =>
        string.Equals(name, "Items", StringComparison.OrdinalIgnoreCase);

    private string ShopDirectory(StorageMode mode)
    {
        string[] matches = Directory.GetDirectories(_directory, "*", SearchOption.AllDirectories)
            .Where(path => string.Equals(Path.GetFileName(path), "Shop", StringComparison.OrdinalIgnoreCase))
            .ToArray();
        Assert.True(matches.Length == 1, $"Expected one database directory in {mode}, found: {string.Join(", ", matches)}");
        Assert.Equal("Shop", Path.GetFileName(matches[0]));
        return matches[0];
    }

    private DataVoContext Open(StorageMode mode) => new(new DataVoConfig
    {
        StorageMode = mode,
        DiskStoragePath = mode == StorageMode.InMemory ? null! : _directory,
    });

    private static QueryResult Ok(DataVoContext context, string sql)
    {
        QueryResult result = context.Execute(sql)[^1];
        Assert.False(result.IsError, $"{sql} => {string.Join(" | ", result.Messages)}");
        return result;
    }
}
