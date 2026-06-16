using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E.DQL;

public abstract class ExplainTestsBase(DataVoConfig config, string testDbName) : SqlExecutionTestsBase(config, testDbName)
{
    [Fact]
    public void ExplainSelect_ReturnsPlannerDiagnosticsWithoutRows()
    {
        Execute("CREATE TABLE Items (Id INT, Name VARCHAR)");
        Execute("INSERT INTO Items (Id, Name) VALUES (1, 'Widget')");

        var result = ExecuteAndReturn("EXPLAIN SELECT * FROM Items WHERE Id = 1");

        Assert.False(result.IsError, string.Join(Environment.NewLine, result.Messages));
        Assert.Equal(["Plan", "Physical", "EstimatedCost", "Reason"], result.Fields);
        var row = Assert.Single(result.Data);
        Assert.True(row.ContainsKey("Plan"));
        Assert.True(row.ContainsKey("Physical"));
        Assert.True(row.ContainsKey("EstimatedCost"));
        Assert.True(row.ContainsKey("Reason"));
        Assert.DoesNotContain("Id", result.Fields);
        Assert.DoesNotContain("Id", row.Keys);
    }

    [Fact]
    public void ExplainSelect_WithSubquery_DoesNotExecuteSubquery()
    {
        Execute("CREATE TABLE Items (Id INT, Name VARCHAR)");

        var result = ExecuteAndReturn("EXPLAIN SELECT * FROM Items WHERE Id IN (SELECT Id FROM MissingItems)");

        Assert.False(result.IsError, string.Join(Environment.NewLine, result.Messages));
        Assert.Equal(["Plan", "Physical", "EstimatedCost", "Reason"], result.Fields);
        var row = Assert.Single(result.Data);
        Assert.True(row.ContainsKey("Plan"));
        Assert.True(row.ContainsKey("Physical"));
        Assert.True(row.ContainsKey("EstimatedCost"));
        Assert.True(row.ContainsKey("Reason"));
    }

    [Fact]
    public void ExplainSelect_DoesNotRequireReadingRowsForCostEstimate()
    {
        Execute("CREATE TABLE EmptyItems (Id INT, Name VARCHAR)");
        Execute("CREATE TABLE PopulatedItems (Id INT, Name VARCHAR)");

        string values = string.Join(", ", Enumerable.Range(1, 1000).Select(i => $"({i}, 'Item {i}')"));
        Execute($"INSERT INTO PopulatedItems (Id, Name) VALUES {values}");

        var emptyResult = ExecuteAndReturn("EXPLAIN SELECT * FROM EmptyItems WHERE Id = 1");
        var populatedResult = ExecuteAndReturn("EXPLAIN SELECT * FROM PopulatedItems WHERE Id = 1");

        Assert.False(emptyResult.IsError, string.Join(Environment.NewLine, emptyResult.Messages));
        Assert.False(populatedResult.IsError, string.Join(Environment.NewLine, populatedResult.Messages));

        var emptyRow = Assert.Single(emptyResult.Data);
        var populatedRow = Assert.Single(populatedResult.Data);

        Assert.Equal(emptyRow["Plan"], populatedRow["Plan"]);
        Assert.Equal(emptyRow["Physical"], populatedRow["Physical"]);
        Assert.Equal(emptyRow["EstimatedCost"], populatedRow["EstimatedCost"]);
    }
}

public class InMemoryExplainTests : ExplainTestsBase
{
    public InMemoryExplainTests() : base(new DataVoConfig { StorageMode = StorageMode.InMemory }, "ExplainDB_Mem") { }
}

public class DiskExplainTests : ExplainTestsBase
{
    public DiskExplainTests() : base(new DataVoConfig { StorageMode = StorageMode.Disk, DiskStoragePath = "./test_datavo_explain" }, "ExplainDB_Disk") { }
}
