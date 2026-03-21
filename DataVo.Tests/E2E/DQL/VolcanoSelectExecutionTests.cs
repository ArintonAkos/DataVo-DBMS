using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E.DQL;

public class VolcanoSelectExecutionTests : SqlExecutionTestsBase
{
    public VolcanoSelectExecutionTests()
        : base(new DataVoConfig
        {
            StorageMode = StorageMode.InMemory,
            EnableVolcanoExecution = true
        }, "VolcanoSelectDB")
    {
    }

    [Fact]
    public void Select_NoJoin_WithWhere_UsesVolcanoPathAndReturnsExpectedRows()
    {
        Execute("CREATE TABLE Users (Id INT PRIMARY KEY, Name VARCHAR, Age INT)");
        Execute("INSERT INTO Users (Id, Name, Age) VALUES (1, 'Alice', 30)");
        Execute("INSERT INTO Users (Id, Name, Age) VALUES (2, 'Bob', 25)");
        Execute("INSERT INTO Users (Id, Name, Age) VALUES (3, 'Charlie', 35)");

        var result = ExecuteAndReturn("SELECT Id, Name FROM Users WHERE Age >= 30 ORDER BY Id ASC");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Equal(2, result.Data.Count);
        Assert.Equal(1, (int)result.Data[0]["Id"]);
        Assert.Equal("Alice", (string)result.Data[0]["Name"]);
        Assert.Equal(3, (int)result.Data[1]["Id"]);
        Assert.Equal("Charlie", (string)result.Data[1]["Name"]);
    }

    [Fact]
    public void Select_NoJoin_WithWhereAndLimit_UsesVolcanoPathAndReturnsLimitedRows()
    {
        Execute("CREATE TABLE Numbers (Id INT PRIMARY KEY, Value INT)");
        Execute("INSERT INTO Numbers (Id, Value) VALUES (1, 10)");
        Execute("INSERT INTO Numbers (Id, Value) VALUES (2, 20)");
        Execute("INSERT INTO Numbers (Id, Value) VALUES (3, 30)");

        var result = ExecuteAndReturn("SELECT Id, Value FROM Numbers WHERE Value >= 10 LIMIT 2");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Equal(2, result.Data.Count);
    }

    [Fact]
    public void Select_NoJoin_WithWhereAndOffsetAndLimit_UsesVolcanoPathAndReturnsExpectedWindow()
    {
        Execute("CREATE TABLE Numbers (Id INT PRIMARY KEY, Value INT)");
        Execute("INSERT INTO Numbers (Id, Value) VALUES (1, 10)");
        Execute("INSERT INTO Numbers (Id, Value) VALUES (2, 20)");
        Execute("INSERT INTO Numbers (Id, Value) VALUES (3, 30)");

        var result = ExecuteAndReturn("SELECT Id, Value FROM Numbers WHERE Value >= 10 LIMIT 1 OFFSET 1");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Single(result.Data);
        Assert.Equal(2, (int)result.Data[0]["Id"]);
    }

    [Fact]
    public void Select_NoJoin_WithoutWhereAndOffsetAndLimit_UsesVolcanoPathAndReturnsExpectedWindow()
    {
        Execute("CREATE TABLE Numbers (Id INT PRIMARY KEY, Value INT)");
        Execute("INSERT INTO Numbers (Id, Value) VALUES (1, 10)");
        Execute("INSERT INTO Numbers (Id, Value) VALUES (2, 20)");
        Execute("INSERT INTO Numbers (Id, Value) VALUES (3, 30)");

        var result = ExecuteAndReturn("SELECT Id, Value FROM Numbers LIMIT 1 OFFSET 1");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Single(result.Data);
        Assert.Equal(2, (int)result.Data[0]["Id"]);
    }
}
