using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E.DQL;

public abstract class LimitOffsetTestsBase : SqlExecutionTestsBase
{
    protected LimitOffsetTestsBase(DataVoConfig config, string testDbName) : base(config, testDbName)
    {
        EnsureTableExists();
    }

    protected void EnsureTableExists()
    {
        Execute($"CREATE TABLE Ranking (Id INT PRIMARY KEY, Player VARCHAR(50), Score INT);");

        Execute("INSERT INTO Ranking (Id, Player, Score) VALUES (1, 'Alice', 100);");
        Execute("INSERT INTO Ranking (Id, Player, Score) VALUES (2, 'Bob', 200);");
        Execute("INSERT INTO Ranking (Id, Player, Score) VALUES (3, 'Charlie', 300);");
        Execute("INSERT INTO Ranking (Id, Player, Score) VALUES (4, 'David', 400);");
        Execute("INSERT INTO Ranking (Id, Player, Score) VALUES (5, 'Eve', 500);");
        Execute("INSERT INTO Ranking (Id, Player, Score) VALUES (6, 'Frank', 600);");
    }

    [Fact]
    public void Select_LimitOnly_ReturnsTopNRows()
    {
        var result = ExecuteAndReturn("SELECT * FROM Ranking LIMIT 2");

        Assert.NotNull(result);
        Assert.Equal(2, result.Data.Count);

        // Assert ID 1 and 2
        var ids = result.Data.Select(r => (int)r["Id"]).OrderBy(x => x).ToList();
        Assert.Equal([1, 2], ids);
    }

    [Fact]
    public void Select_LimitAndOffset_SkipsAndTakesProperly()
    {
        var result = ExecuteAndReturn("SELECT Player FROM Ranking LIMIT 2 OFFSET 2");

        Assert.NotNull(result);
        Assert.Equal(2, result.Data.Count);

        var players = result.Data.Select(r => r["Player"]?.ToString()).OrderBy(x => x).ToList();
        Assert.Equal(["Charlie", "David"], players);
    }

    [Fact]
    public void Select_OffsetThenLimit_SkipsAndTakesProperly()
    {
        var result = ExecuteAndReturn("SELECT Player FROM Ranking OFFSET 2 LIMIT 2");

        Assert.NotNull(result);
        Assert.False(result.IsError);
        Assert.Equal(2, result.Data.Count);

        var players = result.Data.Select(r => r["Player"]?.ToString()).OrderBy(x => x).ToList();
        Assert.Equal(["Charlie", "David"], players);
    }

    [Fact]
    public void Select_OffsetOnly_ExceedsBounds_ReturnsEmpty()
    {
        var result = ExecuteAndReturn("SELECT * FROM Ranking LIMIT 10 OFFSET 10");

        Assert.NotNull(result);
        Assert.Empty(result.Data);
    }

    [Fact]
    public void Select_LimitExceedsTotalRows_ReturnsAllRemaining()
    {
        var result = ExecuteAndReturn("SELECT * FROM Ranking LIMIT 10");

        Assert.NotNull(result);
        Assert.Equal(6, result.Data.Count);
    }

    [Theory]
    [InlineData("SELECT * FROM Ranking LIMIT 0")]
    [InlineData("SELECT Player FROM Ranking LIMIT 0")]
    [InlineData("SELECT Player FROM Ranking WHERE Score > 100 LIMIT 0")]
    [InlineData("SELECT Player FROM Ranking ORDER BY Score DESC LIMIT 0")]
    [InlineData("SELECT * FROM Ranking ORDER BY Score LIMIT 0 OFFSET 2")]
    public void Select_LimitZero_ReturnsNoRows(string sql)
    {
        var result = ExecuteAndReturn(sql);

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Empty(result.Data);
    }

    [Theory]
    [InlineData("SELECT Player FROM Ranking ORDER BY Score DESC OFFSET 2", new[] { "David", "Charlie", "Bob", "Alice" })]
    [InlineData("SELECT Player FROM Ranking WHERE Score >= 200 ORDER BY Score OFFSET 1", new[] { "Charlie", "David", "Eve", "Frank" })]
    [InlineData("SELECT Player FROM Ranking ORDER BY Score DESC OFFSET 6", new string[0])]
    [InlineData("SELECT Player FROM Ranking ORDER BY Score DESC LIMIT 2 OFFSET 1", new[] { "Eve", "David" })]
    [InlineData("SELECT Player FROM Ranking ORDER BY Score DESC LIMIT 2", new[] { "Frank", "Eve" })]
    public void Select_OrderByWithOffset_SortsBySkippedColumnAndPages(string sql, string[] expected)
    {
        var result = ExecuteAndReturn(sql);

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Equal(expected, result.Data.Select(r => r["Player"]?.ToString()).ToArray());
        Assert.All(result.Data, row => Assert.Single(row));
    }
}

public class LimitOffsetTestsMemory : LimitOffsetTestsBase
{
    public LimitOffsetTestsMemory() : base(new DataVoConfig { StorageMode = StorageMode.InMemory }, "LimitDB_Mem") { }
}

public class LimitOffsetTestsLsm : LimitOffsetTestsBase
{
    public LimitOffsetTestsLsm() : base(new DataVoConfig { StorageMode = StorageMode.Lsm, DiskStoragePath = "LimitDB_Lsm" }, "LimitDB_Lsm") { }
}
