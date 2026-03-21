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

    [Fact]
    public void Select_SingleInnerJoin_WithoutWhere_UsesVolcanoJoinPathAndReturnsExpectedWindow()
    {
        Execute("CREATE TABLE Orders (Id INT PRIMARY KEY, CustomerId INT)");
        Execute("CREATE TABLE Customers (Id INT PRIMARY KEY, Name VARCHAR)");

        Execute("INSERT INTO Orders (Id, CustomerId) VALUES (1, 10)");
        Execute("INSERT INTO Orders (Id, CustomerId) VALUES (2, 11)");
        Execute("INSERT INTO Orders (Id, CustomerId) VALUES (3, 10)");
        Execute("INSERT INTO Customers (Id, Name) VALUES (10, 'Alice')");
        Execute("INSERT INTO Customers (Id, Name) VALUES (11, 'Bob')");

        var result = ExecuteAndReturn(@"
            SELECT c.Name, o.Id
            FROM Orders o
            JOIN Customers c ON o.CustomerId = c.Id
            LIMIT 1 OFFSET 1");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Single(result.Data);
        Assert.Equal("Bob", (string)result.Data[0]["c.Name"]);
        Assert.Equal(2, (int)result.Data[0]["o.Id"]);
    }

    [Fact]
    public void Select_SingleInnerJoin_WithWhere_UsesVolcanoJoinPathAndFiltersRows()
    {
        Execute("CREATE TABLE Orders (Id INT PRIMARY KEY, CustomerId INT)");
        Execute("CREATE TABLE Customers (Id INT PRIMARY KEY, Name VARCHAR)");

        Execute("INSERT INTO Orders (Id, CustomerId) VALUES (1, 10)");
        Execute("INSERT INTO Orders (Id, CustomerId) VALUES (2, 11)");
        Execute("INSERT INTO Orders (Id, CustomerId) VALUES (3, 10)");
        Execute("INSERT INTO Customers (Id, Name) VALUES (10, 'Alice')");
        Execute("INSERT INTO Customers (Id, Name) VALUES (11, 'Bob')");

        var result = ExecuteAndReturn(@"
            SELECT c.Name, o.Id
            FROM Orders o
            JOIN Customers c ON o.CustomerId = c.Id
            WHERE c.Name = 'Alice'
            LIMIT 1");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Single(result.Data);
        Assert.Equal("Alice", (string)result.Data[0]["c.Name"]);
    }

    [Fact]
    public void Select_NoJoin_OrderByWithLimitOffset_UsesVolcanoSortThenWindow()
    {
        Execute("CREATE TABLE Scores (Id INT PRIMARY KEY, Score INT)");
        Execute("INSERT INTO Scores (Id, Score) VALUES (1, 70)");
        Execute("INSERT INTO Scores (Id, Score) VALUES (2, 95)");
        Execute("INSERT INTO Scores (Id, Score) VALUES (3, 80)");

        var result = ExecuteAndReturn("SELECT Id, Score FROM Scores ORDER BY Score DESC LIMIT 1 OFFSET 1");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Single(result.Data);
        Assert.Equal(3, (int)result.Data[0]["Id"]);
        Assert.Equal(80, (int)result.Data[0]["Score"]);
    }

    [Fact]
    public void Select_MultiInnerJoin_UsesVolcanoJoinPipeline()
    {
        Execute("CREATE TABLE Orders (Id INT PRIMARY KEY, CustomerId INT)");
        Execute("CREATE TABLE Customers (Id INT PRIMARY KEY, CityId INT, Name VARCHAR)");
        Execute("CREATE TABLE Cities (Id INT PRIMARY KEY, CityName VARCHAR)");

        Execute("INSERT INTO Orders (Id, CustomerId) VALUES (1, 10)");
        Execute("INSERT INTO Orders (Id, CustomerId) VALUES (2, 11)");
        Execute("INSERT INTO Orders (Id, CustomerId) VALUES (3, 12)");

        Execute("INSERT INTO Customers (Id, CityId, Name) VALUES (10, 100, 'Alice')");
        Execute("INSERT INTO Customers (Id, CityId, Name) VALUES (11, 101, 'Bob')");
        Execute("INSERT INTO Customers (Id, CityId, Name) VALUES (12, 100, 'Cara')");

        Execute("INSERT INTO Cities (Id, CityName) VALUES (100, 'Athens')");
        Execute("INSERT INTO Cities (Id, CityName) VALUES (101, 'Rome')");

        var result = ExecuteAndReturn(@"
            SELECT o.Id, c.Name, ci.CityName
            FROM Orders o
            JOIN Customers c ON o.CustomerId = c.Id
            JOIN Cities ci ON c.CityId = ci.Id
            WHERE ci.CityName = 'Athens'
            LIMIT 2");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Equal(2, result.Data.Count);
        Assert.All(result.Data, row => Assert.Equal("Athens", (string)row["ci.CityName"]));
    }
}
