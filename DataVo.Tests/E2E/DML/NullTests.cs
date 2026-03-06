using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E.DML;

public abstract class NullTestsBase(DataVoConfig config, string testDbName) : SqlExecutionTestsBase(config, testDbName)
{
    [Fact]
    public void Insert_NullValues_StoredCorrectly()
    {
        Execute("CREATE TABLE NullItems (Id INT PRIMARY KEY, Val VARCHAR, Num INT)");
        Execute("INSERT INTO NullItems VALUES (1, NULL, 10)");
        Execute("INSERT INTO NullItems VALUES (2, 'two', NULL)");

        var res = ExecuteAndReturn("SELECT * FROM NullItems");
        Assert.Equal(2, res.Data.Count);

        var row1 = res.Data.First(r => (int)r["Id"] == 1);
        Assert.Null(row1["Val"]);
        Assert.Equal(10, row1["Num"]);

        var row2 = res.Data.First(r => (int)r["Id"] == 2);
        Assert.Equal("two", row2["Val"]);
        Assert.Null(row2["Num"]);
    }

    [Fact]
    public void Select_IsNull_IsNotNull_FiltersCorrectly()
    {
        Execute("CREATE TABLE NullFilters (Id INT PRIMARY KEY, Val INT)");
        Execute("INSERT INTO NullFilters VALUES (1, 100)");
        Execute("INSERT INTO NullFilters VALUES (2, NULL)");
        Execute("INSERT INTO NullFilters VALUES (3, 300)");

        var isNullRes = ExecuteAndReturn("SELECT * FROM NullFilters WHERE Val IS NULL");
        Assert.Single(isNullRes.Data);
        Assert.Equal(2, isNullRes.Data[0]["Id"]);

        var isNotNullRes = ExecuteAndReturn("SELECT * FROM NullFilters WHERE Val IS NOT NULL");
        Assert.Equal(2, isNotNullRes.Data.Count);
        Assert.Contains(isNotNullRes.Data, r => (int)r["Id"] == 1);
        Assert.Contains(isNotNullRes.Data, r => (int)r["Id"] == 3);
    }

    [Fact]
    public void SQL_NullEquality_EvaluatesToFalse()
    {
        Execute("CREATE TABLE EqItems (Id INT PRIMARY KEY, Val INT)");
        Execute("INSERT INTO EqItems VALUES (1, NULL)");
        Execute("INSERT INTO EqItems VALUES (2, 20)");

        // NULL = NULL should be false
        var eqRes = ExecuteAndReturn("SELECT * FROM EqItems WHERE Val = NULL");
        Assert.Empty(eqRes.Data);

        // NULL < 30 should be false
        var ltRes = ExecuteAndReturn("SELECT * FROM EqItems WHERE Val < 30");
        Assert.Single(ltRes.Data); // Should only match Id 2
        Assert.Equal(2, ltRes.Data[0]["Id"]);
    }

    [Fact]
    public void Update_SetNull_And_WhereIsNull()
    {
        Execute("CREATE TABLE UpdNulls (Id INT PRIMARY KEY, Status VARCHAR)");
        Execute("INSERT INTO UpdNulls VALUES (1, 'Active')");
        Execute("INSERT INTO UpdNulls VALUES (2, 'Pending')");

        // Set to NULL
        Execute("UPDATE UpdNulls SET Status = NULL WHERE Id = 2");

        var setRes = ExecuteAndReturn("SELECT * FROM UpdNulls WHERE Id = 2");
        Assert.Null(setRes.Data[0]["Status"]);

        // Update where IS NULL
        Execute("UPDATE UpdNulls SET Status = 'Resolved' WHERE Status IS NULL");

        var verifyRes = ExecuteAndReturn("SELECT * FROM UpdNulls WHERE Id = 2");
        Assert.Equal("Resolved", verifyRes.Data[0]["Status"]);
    }

    [Fact]
    public void PrimaryKey_CannotBeNull_Rejected()
    {
        Execute("CREATE TABLE PKNulls (Id INT PRIMARY KEY)");
        
        var res1 = ExecuteAndReturn("INSERT INTO PKNulls VALUES (NULL)");
        Assert.Contains(res1.Messages, m => m.Contains("Primary key cannot be null"));

        Execute("INSERT INTO PKNulls VALUES (1)");
        var res2 = ExecuteAndReturn("UPDATE PKNulls SET Id = NULL WHERE Id = 1");
        Assert.Contains(res2.Messages, m => m.Contains("cannot be null"));
    }
}

[Collection("SequentialStorageTests")]
public class InMemoryNullTests : NullTestsBase
{
    public InMemoryNullTests() : base(new DataVoConfig { StorageMode = StorageMode.InMemory }, "NullDB_Mem") { }
}

[Collection("SequentialStorageTests")]
public class DiskNullTests : NullTestsBase
{
    public DiskNullTests() : base(new DataVoConfig { StorageMode = StorageMode.Disk, DiskStoragePath = "./test_datavo_null" }, "NullDB_Disk") { }
}
