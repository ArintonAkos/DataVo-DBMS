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
    public void Select_SingleInnerJoin_WithPerTableAndPredicate_PreservesSemantics()
    {
        Execute("CREATE TABLE Orders (Id INT PRIMARY KEY, CustomerId INT)");
        Execute("CREATE TABLE Customers (Id INT PRIMARY KEY, Name VARCHAR)");

        Execute("INSERT INTO Orders (Id, CustomerId) VALUES (1, 10)");
        Execute("INSERT INTO Orders (Id, CustomerId) VALUES (2, 11)");
        Execute("INSERT INTO Orders (Id, CustomerId) VALUES (3, 10)");
        Execute("INSERT INTO Customers (Id, Name) VALUES (10, 'Alice')");
        Execute("INSERT INTO Customers (Id, Name) VALUES (11, 'Bob')");

        var result = ExecuteAndReturn(@"
            SELECT o.Id, c.Name
            FROM Orders o
            JOIN Customers c ON o.CustomerId = c.Id
            WHERE o.Id >= 2 AND c.Name = 'Alice'
            ORDER BY o.Id ASC");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Single(result.Data);
        Assert.Equal(3, (int)result.Data[0]["o.Id"]);
        Assert.Equal("Alice", (string)result.Data[0]["c.Name"]);
    }

    [Fact]
    public void Select_SingleInnerJoin_WithCrossTableOrPredicate_PreservesSemantics()
    {
        Execute("CREATE TABLE Orders (Id INT PRIMARY KEY, CustomerId INT)");
        Execute("CREATE TABLE Customers (Id INT PRIMARY KEY, Name VARCHAR)");

        Execute("INSERT INTO Orders (Id, CustomerId) VALUES (1, 10)");
        Execute("INSERT INTO Orders (Id, CustomerId) VALUES (2, 11)");
        Execute("INSERT INTO Orders (Id, CustomerId) VALUES (3, 10)");
        Execute("INSERT INTO Customers (Id, Name) VALUES (10, 'Alice')");
        Execute("INSERT INTO Customers (Id, Name) VALUES (11, 'Bob')");

        var result = ExecuteAndReturn(@"
            SELECT o.Id, c.Name
            FROM Orders o
            JOIN Customers c ON o.CustomerId = c.Id
            WHERE o.Id = 1 OR c.Name = 'Bob'
            ORDER BY o.Id ASC");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Equal(2, result.Data.Count);
        Assert.Equal(1, (int)result.Data[0]["o.Id"]);
        Assert.Equal("Alice", (string)result.Data[0]["c.Name"]);
        Assert.Equal(2, (int)result.Data[1]["o.Id"]);
        Assert.Equal("Bob", (string)result.Data[1]["c.Name"]);
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

    [Fact]
    public void Select_SingleInnerJoin_OrderByWithLimitOffset_UsesVolcanoJoinSortWindow()
    {
        Execute("CREATE TABLE Orders (Id INT PRIMARY KEY, CustomerId INT)");
        Execute("CREATE TABLE Customers (Id INT PRIMARY KEY, Name VARCHAR)");

        Execute("INSERT INTO Orders (Id, CustomerId) VALUES (1, 10)");
        Execute("INSERT INTO Orders (Id, CustomerId) VALUES (2, 11)");
        Execute("INSERT INTO Orders (Id, CustomerId) VALUES (3, 10)");
        Execute("INSERT INTO Customers (Id, Name) VALUES (10, 'Alice')");
        Execute("INSERT INTO Customers (Id, Name) VALUES (11, 'Bob')");

        var result = ExecuteAndReturn(@"
            SELECT o.Id, c.Name
            FROM Orders o
            JOIN Customers c ON o.CustomerId = c.Id
            ORDER BY o.Id DESC
            LIMIT 1 OFFSET 1");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Single(result.Data);
        Assert.Equal(2, (int)result.Data[0]["o.Id"]);
        Assert.Equal("Bob", (string)result.Data[0]["c.Name"]);
    }

    [Fact]
    public void Select_NoJoin_MultiColumnOrderByWithLimitOffset_UsesVolcanoSortWindow()
    {
        Execute("CREATE TABLE Scores (Id INT PRIMARY KEY, Bucket INT, Score INT)");
        Execute("INSERT INTO Scores (Id, Bucket, Score) VALUES (1, 1, 80)");
        Execute("INSERT INTO Scores (Id, Bucket, Score) VALUES (2, 1, 70)");
        Execute("INSERT INTO Scores (Id, Bucket, Score) VALUES (3, 2, 90)");
        Execute("INSERT INTO Scores (Id, Bucket, Score) VALUES (4, 2, 60)");

        var result = ExecuteAndReturn(@"
            SELECT Id, Bucket, Score
            FROM Scores
            ORDER BY Bucket DESC, Score ASC
            LIMIT 1 OFFSET 1");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Single(result.Data);
        Assert.Equal(3, (int)result.Data[0]["Id"]);
    }

    [Fact]
    public void Select_Join_MultiColumnOrderByWithLimitOffset_UsesVolcanoSortWindow()
    {
        Execute("CREATE TABLE Orders (Id INT PRIMARY KEY, CustomerId INT)");
        Execute("CREATE TABLE Customers (Id INT PRIMARY KEY, Name VARCHAR)");

        Execute("INSERT INTO Orders (Id, CustomerId) VALUES (1, 10)");
        Execute("INSERT INTO Orders (Id, CustomerId) VALUES (2, 11)");
        Execute("INSERT INTO Orders (Id, CustomerId) VALUES (3, 10)");
        Execute("INSERT INTO Customers (Id, Name) VALUES (10, 'Alice')");
        Execute("INSERT INTO Customers (Id, Name) VALUES (11, 'Bob')");

        var result = ExecuteAndReturn(@"
            SELECT o.Id, c.Name
            FROM Orders o
            JOIN Customers c ON o.CustomerId = c.Id
            ORDER BY c.Name ASC, o.Id DESC
            LIMIT 1 OFFSET 1");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Single(result.Data);
        Assert.Equal(1, (int)result.Data[0]["o.Id"]);
        Assert.Equal("Alice", (string)result.Data[0]["c.Name"]);
    }

    [Fact]
    public void Select_NoJoin_ProjectSubsetWithOrderByDifferentColumn_UsesVolcanoProjectionSafely()
    {
        Execute("CREATE TABLE Scores (Id INT PRIMARY KEY, Name VARCHAR, Score INT)");
        Execute("INSERT INTO Scores (Id, Name, Score) VALUES (1, 'A', 70)");
        Execute("INSERT INTO Scores (Id, Name, Score) VALUES (2, 'B', 95)");
        Execute("INSERT INTO Scores (Id, Name, Score) VALUES (3, 'C', 80)");

        var result = ExecuteAndReturn("SELECT Name FROM Scores ORDER BY Score DESC LIMIT 1 OFFSET 1");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Single(result.Data);
        Assert.Equal("C", (string)result.Data[0]["Name"]);
    }

    [Fact]
    public void Select_NoJoin_Distinct_UsesVolcanoDistinctPushdownSafely()
    {
        Execute("CREATE TABLE Scores (Id INT PRIMARY KEY, Name VARCHAR, Score INT)");
        Execute("INSERT INTO Scores (Id, Name, Score) VALUES (1, 'A', 70)");
        Execute("INSERT INTO Scores (Id, Name, Score) VALUES (2, 'A', 95)");
        Execute("INSERT INTO Scores (Id, Name, Score) VALUES (3, 'B', 80)");

        var result = ExecuteAndReturn("SELECT DISTINCT Name FROM Scores ORDER BY Name ASC");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Equal(2, result.Data.Count);
        Assert.Equal("A", (string)result.Data[0]["Name"]);
        Assert.Equal("B", (string)result.Data[1]["Name"]);
    }

    [Fact]
    public void Select_Join_Distinct_UsesVolcanoDistinctPushdownSafely()
    {
        Execute("CREATE TABLE Orders (Id INT PRIMARY KEY, CustomerId INT)");
        Execute("CREATE TABLE Customers (Id INT PRIMARY KEY, Name VARCHAR)");

        Execute("INSERT INTO Orders (Id, CustomerId) VALUES (1, 10)");
        Execute("INSERT INTO Orders (Id, CustomerId) VALUES (2, 10)");
        Execute("INSERT INTO Orders (Id, CustomerId) VALUES (3, 11)");
        Execute("INSERT INTO Customers (Id, Name) VALUES (10, 'Alice')");
        Execute("INSERT INTO Customers (Id, Name) VALUES (11, 'Bob')");

        var result = ExecuteAndReturn(@"
            SELECT DISTINCT c.Name
            FROM Orders o
            JOIN Customers c ON o.CustomerId = c.Id");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Equal(2, result.Data.Count);

        var names = result.Data.Select(row => (string)row["c.Name"]).OrderBy(x => x).ToList();
        Assert.Equal("Alice", names[0]);
        Assert.Equal("Bob", names[1]);
    }

    [Fact]
    public void Select_NoJoin_DistinctWithLimit_UsesVolcanoDistinctAndWindowPushdownSafely()
    {
        Execute("CREATE TABLE Scores (Id INT PRIMARY KEY, Name VARCHAR, Score INT)");
        Execute("INSERT INTO Scores (Id, Name, Score) VALUES (1, 'A', 70)");
        Execute("INSERT INTO Scores (Id, Name, Score) VALUES (2, 'A', 95)");
        Execute("INSERT INTO Scores (Id, Name, Score) VALUES (3, 'B', 80)");

        var result = ExecuteAndReturn("SELECT DISTINCT Name FROM Scores LIMIT 1");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Single(result.Data);
        Assert.Equal("A", (string)result.Data[0]["Name"]);
    }

    [Fact]
    public void Select_Join_DistinctWithLimit_UsesVolcanoDistinctAndWindowPushdownSafely()
    {
        Execute("CREATE TABLE Orders (Id INT PRIMARY KEY, CustomerId INT)");
        Execute("CREATE TABLE Customers (Id INT PRIMARY KEY, Name VARCHAR)");

        Execute("INSERT INTO Orders (Id, CustomerId) VALUES (1, 10)");
        Execute("INSERT INTO Orders (Id, CustomerId) VALUES (2, 10)");
        Execute("INSERT INTO Orders (Id, CustomerId) VALUES (3, 11)");
        Execute("INSERT INTO Customers (Id, Name) VALUES (10, 'Alice')");
        Execute("INSERT INTO Customers (Id, Name) VALUES (11, 'Bob')");

        var result = ExecuteAndReturn(@"
            SELECT DISTINCT c.Name
            FROM Orders o
            JOIN Customers c ON o.CustomerId = c.Id
            LIMIT 1");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Single(result.Data);
        Assert.Equal("Alice", (string)result.Data[0]["c.Name"]);
    }

    [Fact]
    public void Select_NoJoin_DistinctWithOrderAndWindow_UsesVolcanoDistinctSortWindowPushdownSafely()
    {
        Execute("CREATE TABLE Scores (Id INT PRIMARY KEY, Name VARCHAR, Score INT)");
        Execute("INSERT INTO Scores (Id, Name, Score) VALUES (1, 'B', 70)");
        Execute("INSERT INTO Scores (Id, Name, Score) VALUES (2, 'A', 95)");
        Execute("INSERT INTO Scores (Id, Name, Score) VALUES (3, 'A', 80)");

        var result = ExecuteAndReturn("SELECT DISTINCT Name FROM Scores ORDER BY Name ASC LIMIT 1 OFFSET 1");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Single(result.Data);
        Assert.Equal("B", (string)result.Data[0]["Name"]);
    }

    [Fact]
    public void Select_Join_DistinctWithOrderAndWindow_UsesVolcanoDistinctSortWindowPushdownSafely()
    {
        Execute("CREATE TABLE Orders (Id INT PRIMARY KEY, CustomerId INT)");
        Execute("CREATE TABLE Customers (Id INT PRIMARY KEY, Name VARCHAR)");

        Execute("INSERT INTO Orders (Id, CustomerId) VALUES (1, 10)");
        Execute("INSERT INTO Orders (Id, CustomerId) VALUES (2, 10)");
        Execute("INSERT INTO Orders (Id, CustomerId) VALUES (3, 11)");
        Execute("INSERT INTO Customers (Id, Name) VALUES (10, 'Bob')");
        Execute("INSERT INTO Customers (Id, Name) VALUES (11, 'Alice')");

        var result = ExecuteAndReturn(@"
            SELECT DISTINCT c.Name
            FROM Orders o
            JOIN Customers c ON o.CustomerId = c.Id
            ORDER BY c.Name ASC
            LIMIT 1 OFFSET 1");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Single(result.Data);
        Assert.Equal("Bob", (string)result.Data[0]["c.Name"]);
    }

    [Fact]
    public void Select_Join_ProjectSubsetWithOrderByDifferentColumn_UsesVolcanoJoinProjectionSafely()
    {
        Execute("CREATE TABLE Orders (Id INT PRIMARY KEY, CustomerId INT)");
        Execute("CREATE TABLE Customers (Id INT PRIMARY KEY, Name VARCHAR)");

        Execute("INSERT INTO Orders (Id, CustomerId) VALUES (1, 10)");
        Execute("INSERT INTO Orders (Id, CustomerId) VALUES (2, 11)");
        Execute("INSERT INTO Orders (Id, CustomerId) VALUES (3, 10)");
        Execute("INSERT INTO Customers (Id, Name) VALUES (10, 'Bob')");
        Execute("INSERT INTO Customers (Id, Name) VALUES (11, 'Alice')");

        var result = ExecuteAndReturn(@"
            SELECT o.Id
            FROM Orders o
            JOIN Customers c ON o.CustomerId = c.Id
            ORDER BY c.Name ASC, o.Id ASC
            LIMIT 1 OFFSET 1");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Single(result.Data);
        Assert.Equal(1, (int)result.Data[0]["o.Id"]);
    }

    [Fact]
    public void Select_Join_DistinctProjectWithOrderAndWindow_UsesVolcanoJoinProjectionSafely()
    {
        Execute("CREATE TABLE Orders (Id INT PRIMARY KEY, CustomerId INT)");
        Execute("CREATE TABLE Customers (Id INT PRIMARY KEY, Name VARCHAR)");

        Execute("INSERT INTO Orders (Id, CustomerId) VALUES (1, 10)");
        Execute("INSERT INTO Orders (Id, CustomerId) VALUES (2, 10)");
        Execute("INSERT INTO Orders (Id, CustomerId) VALUES (3, 11)");
        Execute("INSERT INTO Customers (Id, Name) VALUES (10, 'Bob')");
        Execute("INSERT INTO Customers (Id, Name) VALUES (11, 'Alice')");

        var result = ExecuteAndReturn(@"
            SELECT DISTINCT o.CustomerId
            FROM Orders o
            JOIN Customers c ON o.CustomerId = c.Id
            ORDER BY o.CustomerId ASC
            LIMIT 1 OFFSET 1");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Single(result.Data);
        Assert.Equal(11, (int)result.Data[0]["o.CustomerId"]);
    }

    [Fact]
    public void Select_NoJoin_DistinctWithIncompatibleOrderKey_PreservesLegacyWindowSemantics()
    {
        Execute("CREATE TABLE Scores (Id INT PRIMARY KEY, Name VARCHAR, Score INT)");
        Execute("INSERT INTO Scores (Id, Name, Score) VALUES (1, 'A', 70)");
        Execute("INSERT INTO Scores (Id, Name, Score) VALUES (2, 'A', 95)");
        Execute("INSERT INTO Scores (Id, Name, Score) VALUES (3, 'B', 80)");

        var result = ExecuteAndReturn("SELECT DISTINCT Name FROM Scores ORDER BY Score DESC LIMIT 1 OFFSET 1");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Single(result.Data);
        Assert.Equal("B", (string)result.Data[0]["Name"]);
    }

    [Fact]
    public void Select_Join_DistinctWithIncompatibleOrderKey_PreservesLegacyWindowSemantics()
    {
        Execute("CREATE TABLE Orders (Id INT PRIMARY KEY, CustomerId INT)");
        Execute("CREATE TABLE Customers (Id INT PRIMARY KEY, Name VARCHAR)");

        Execute("INSERT INTO Orders (Id, CustomerId) VALUES (1, 10)");
        Execute("INSERT INTO Orders (Id, CustomerId) VALUES (2, 11)");
        Execute("INSERT INTO Orders (Id, CustomerId) VALUES (3, 10)");
        Execute("INSERT INTO Customers (Id, Name) VALUES (10, 'Alice')");
        Execute("INSERT INTO Customers (Id, Name) VALUES (11, 'Bob')");

        var result = ExecuteAndReturn(@"
            SELECT DISTINCT c.Name
            FROM Orders o
            JOIN Customers c ON o.CustomerId = c.Id
            ORDER BY o.Id DESC
            LIMIT 1 OFFSET 1");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Single(result.Data);
        Assert.Equal("Bob", (string)result.Data[0]["c.Name"]);
    }

    [Fact]
    public void Select_NoJoin_GroupByWithoutAggregates_UsesVolcanoGroupByPushdownSafely()
    {
        Execute("CREATE TABLE Items (Id INT PRIMARY KEY, Category VARCHAR)");
        Execute("INSERT INTO Items (Id, Category) VALUES (1, 'A')");
        Execute("INSERT INTO Items (Id, Category) VALUES (2, 'A')");
        Execute("INSERT INTO Items (Id, Category) VALUES (3, 'B')");

        var result = ExecuteAndReturn("SELECT Category FROM Items GROUP BY Category ORDER BY Category ASC");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Equal(2, result.Data.Count);
        Assert.Equal("A", (string)result.Data[0]["Category"]);
        Assert.Equal("B", (string)result.Data[1]["Category"]);
    }

    [Fact]
    public void Select_NoJoin_GroupByWithoutAggregatesWithOrderWindow_UsesVolcanoGroupWindowPushdownSafely()
    {
        Execute("CREATE TABLE Items (Id INT PRIMARY KEY, Category VARCHAR)");
        Execute("INSERT INTO Items (Id, Category) VALUES (1, 'A')");
        Execute("INSERT INTO Items (Id, Category) VALUES (2, 'A')");
        Execute("INSERT INTO Items (Id, Category) VALUES (3, 'B')");
        Execute("INSERT INTO Items (Id, Category) VALUES (4, 'C')");

        var result = ExecuteAndReturn("SELECT Category FROM Items GROUP BY Category ORDER BY Category ASC LIMIT 1 OFFSET 1");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Single(result.Data);
        Assert.Equal("B", (string)result.Data[0]["Category"]);
    }

    [Fact]
    public void Select_NoJoin_GlobalCount_UsesVolcanoAggregatePushdownSafely()
    {
        Execute("CREATE TABLE Items (Id INT PRIMARY KEY, Category VARCHAR)");
        Execute("INSERT INTO Items (Id, Category) VALUES (1, 'A')");
        Execute("INSERT INTO Items (Id, Category) VALUES (2, 'A')");
        Execute("INSERT INTO Items (Id, Category) VALUES (3, 'B')");

        var result = ExecuteAndReturn("SELECT COUNT(*) AS C FROM Items WHERE Category = 'A'");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Single(result.Data);
        Assert.Equal(2L, Convert.ToInt64(result.Data[0]["C"]));
    }

    [Fact]
    public void Select_NoJoin_GroupedCount_UsesVolcanoAggregatePushdownSafely()
    {
        Execute("CREATE TABLE Items (Id INT PRIMARY KEY, Category VARCHAR)");
        Execute("INSERT INTO Items (Id, Category) VALUES (1, 'A')");
        Execute("INSERT INTO Items (Id, Category) VALUES (2, 'A')");
        Execute("INSERT INTO Items (Id, Category) VALUES (3, 'B')");

        var result = ExecuteAndReturn("SELECT Category, COUNT(*) AS C FROM Items GROUP BY Category ORDER BY Category ASC");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Equal(2, result.Data.Count);
        Assert.Equal("A", (string)result.Data[0]["Category"]);
        Assert.Equal(2L, Convert.ToInt64(result.Data[0]["C"]));
        Assert.Equal("B", (string)result.Data[1]["Category"]);
        Assert.Equal(1L, Convert.ToInt64(result.Data[1]["C"]));
    }

    [Fact]
    public void Select_NoJoin_GlobalSum_UsesVolcanoAggregatePushdownSafely()
    {
        Execute("CREATE TABLE Sales (Id INT PRIMARY KEY, Amount INT)");
        Execute("INSERT INTO Sales (Id, Amount) VALUES (1, 10)");
        Execute("INSERT INTO Sales (Id, Amount) VALUES (2, 25)");
        Execute("INSERT INTO Sales (Id, Amount) VALUES (3, 5)");

        var result = ExecuteAndReturn("SELECT SUM(Amount) AS Total FROM Sales WHERE Amount >= 10");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Single(result.Data);
        Assert.Equal(35d, Convert.ToDouble(result.Data[0]["Total"]));
    }

    [Fact]
    public void Select_NoJoin_GroupedAvg_UsesVolcanoAggregatePushdownSafely()
    {
        Execute("CREATE TABLE Sales (Id INT PRIMARY KEY, Category VARCHAR, Amount INT)");
        Execute("INSERT INTO Sales (Id, Category, Amount) VALUES (1, 'A', 10)");
        Execute("INSERT INTO Sales (Id, Category, Amount) VALUES (2, 'A', 20)");
        Execute("INSERT INTO Sales (Id, Category, Amount) VALUES (3, 'B', 30)");

        var result = ExecuteAndReturn("SELECT Category, AVG(Amount) AS AvgAmount FROM Sales GROUP BY Category ORDER BY Category ASC");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Equal(2, result.Data.Count);
        Assert.Equal("A", (string)result.Data[0]["Category"]);
        Assert.Equal(15d, Convert.ToDouble(result.Data[0]["AvgAmount"]));
        Assert.Equal("B", (string)result.Data[1]["Category"]);
        Assert.Equal(30d, Convert.ToDouble(result.Data[1]["AvgAmount"]));
    }

    [Fact]
    public void Select_NoJoin_GroupedAggregateWithHaving_UsesVolcanoAggregatePushdownSafely()
    {
        Execute("CREATE TABLE Sales (Id INT PRIMARY KEY, Category VARCHAR, Amount INT)");
        Execute("INSERT INTO Sales (Id, Category, Amount) VALUES (1, 'A', 10)");
        Execute("INSERT INTO Sales (Id, Category, Amount) VALUES (2, 'A', 20)");
        Execute("INSERT INTO Sales (Id, Category, Amount) VALUES (3, 'B', 30)");

        var result = ExecuteAndReturn(@"
            SELECT Category, SUM(Amount) AS Total
            FROM Sales
            GROUP BY Category
            HAVING SUM(Amount) >= 30
            ORDER BY Category ASC");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Equal(2, result.Data.Count);
        Assert.Equal("A", (string)result.Data[0]["Category"]);
        Assert.Equal(30d, Convert.ToDouble(result.Data[0]["Total"]));
        Assert.Equal("B", (string)result.Data[1]["Category"]);
        Assert.Equal(30d, Convert.ToDouble(result.Data[1]["Total"]));
    }

    [Fact]
    public void Select_NoJoin_AggregateExpressionArgument_UsesVolcanoAggregatePushdownSafely()
    {
        Execute("CREATE TABLE Sales (Id INT PRIMARY KEY, Amount INT)");
        Execute("INSERT INTO Sales (Id, Amount) VALUES (1, 10)");
        Execute("INSERT INTO Sales (Id, Amount) VALUES (2, 25)");

        var result = ExecuteAndReturn("SELECT SUM(Amount * 2) AS TotalDouble FROM Sales");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Single(result.Data);
        Assert.Equal(70d, Convert.ToDouble(result.Data[0]["TotalDouble"]));
    }

    [Fact]
    public void Select_NoJoin_GroupByHavingOrderAndWindow_PreservesSemantics()
    {
        Execute("CREATE TABLE Sales (Id INT PRIMARY KEY, Category VARCHAR, Amount INT)");
        Execute("INSERT INTO Sales (Id, Category, Amount) VALUES (1, 'A', 10)");
        Execute("INSERT INTO Sales (Id, Category, Amount) VALUES (2, 'A', 20)");
        Execute("INSERT INTO Sales (Id, Category, Amount) VALUES (3, 'B', 15)");
        Execute("INSERT INTO Sales (Id, Category, Amount) VALUES (4, 'B', 10)");
        Execute("INSERT INTO Sales (Id, Category, Amount) VALUES (5, 'C', 7)");

        var result = ExecuteAndReturn(@"
            SELECT Category, SUM(Amount) AS Total
            FROM Sales
            GROUP BY Category
            HAVING SUM(Amount) >= 20
            ORDER BY Total DESC
            LIMIT 1 OFFSET 1");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Single(result.Data);
        Assert.Equal("B", (string)result.Data[0]["Category"]);
        Assert.Equal(25d, Convert.ToDouble(result.Data[0]["Total"]));
    }

    [Fact]
    public void Select_LeftJoinShape_FallsBackToLegacyAndReturnsExpectedRows()
    {
        Execute("CREATE TABLE Orders (Id INT PRIMARY KEY, CustomerId INT)");
        Execute("CREATE TABLE Customers (Id INT PRIMARY KEY, Name VARCHAR)");

        Execute("INSERT INTO Orders (Id, CustomerId) VALUES (1, 10)");
        Execute("INSERT INTO Orders (Id, CustomerId) VALUES (2, 11)");
        Execute("INSERT INTO Customers (Id, Name) VALUES (10, 'Alice')");

        var result = ExecuteAndReturn(@"
            SELECT o.Id, c.Name
            FROM Orders o
            LEFT JOIN Customers c ON o.CustomerId = c.Id
            ORDER BY o.Id ASC");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Equal(2, result.Data.Count);
        Assert.Equal(1, (int)result.Data[0]["o.Id"]);
        Assert.Equal("Alice", (string)result.Data[0]["c.Name"]);
        Assert.Equal(2, (int)result.Data[1]["o.Id"]);
        Assert.Null(result.Data[1]["c.Name"]);
    }

    [Fact]
    public void Select_NoJoin_ComputedWhereExpression_FallsBackAndReturnsExpectedRows()
    {
        Execute("CREATE TABLE Numbers (Id INT PRIMARY KEY, Value INT)");
        Execute("INSERT INTO Numbers (Id, Value) VALUES (1, 3)");
        Execute("INSERT INTO Numbers (Id, Value) VALUES (2, 6)");

        var result = ExecuteAndReturn("SELECT Id FROM Numbers WHERE Value * 2 > 10 ORDER BY Id ASC");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Single(result.Data);
        Assert.Equal(2, (int)result.Data[0]["Id"]);
    }
}

public class VolcanoSpillGuardrailTests : SqlExecutionTestsBase
{
    public VolcanoSpillGuardrailTests()
        : base(new DataVoConfig
        {
            StorageMode = StorageMode.InMemory,
            EnableVolcanoExecution = true,
            EnableVolcanoSpillGuardrails = true,
            VolcanoSortSpillThresholdRows = 2,
            VolcanoAggregateSpillThresholdRows = 2
        }, "VolcanoSpillGuardrailDB")
    {
    }

    [Fact]
    public void Select_NoJoin_OrderByAboveThreshold_PreservesResultSemantics()
    {
        Execute("CREATE TABLE Scores (Id INT PRIMARY KEY, Score INT)");
        Execute("INSERT INTO Scores (Id, Score) VALUES (1, 50)");
        Execute("INSERT INTO Scores (Id, Score) VALUES (2, 90)");
        Execute("INSERT INTO Scores (Id, Score) VALUES (3, 70)");

        var result = ExecuteAndReturn("SELECT Id FROM Scores ORDER BY Score DESC LIMIT 1 OFFSET 1");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Single(result.Data);
        Assert.Equal(3, (int)result.Data[0]["Id"]);
    }

    [Fact]
    public void Select_NoJoin_AggregateAboveThreshold_PreservesResultSemantics()
    {
        Execute("CREATE TABLE Sales (Id INT PRIMARY KEY, Category VARCHAR, Amount INT)");
        Execute("INSERT INTO Sales (Id, Category, Amount) VALUES (1, 'A', 10)");
        Execute("INSERT INTO Sales (Id, Category, Amount) VALUES (2, 'A', 15)");
        Execute("INSERT INTO Sales (Id, Category, Amount) VALUES (3, 'B', 30)");

        var result = ExecuteAndReturn(@"
            SELECT Category, SUM(Amount) AS Total
            FROM Sales
            GROUP BY Category
            HAVING SUM(Amount) >= 25
            ORDER BY Category ASC");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Equal(2, result.Data.Count);
        Assert.Equal("A", (string)result.Data[0]["Category"]);
        Assert.Equal(25d, Convert.ToDouble(result.Data[0]["Total"]));
        Assert.Equal("B", (string)result.Data[1]["Category"]);
        Assert.Equal(30d, Convert.ToDouble(result.Data[1]["Total"]));
    }

    [Fact]
    public void Select_Join_OrderByAboveThreshold_PreservesResultSemantics()
    {
        Execute("CREATE TABLE Orders (Id INT PRIMARY KEY, CustomerId INT)");
        Execute("CREATE TABLE Customers (Id INT PRIMARY KEY, Name VARCHAR)");

        Execute("INSERT INTO Orders (Id, CustomerId) VALUES (1, 10)");
        Execute("INSERT INTO Orders (Id, CustomerId) VALUES (2, 11)");
        Execute("INSERT INTO Orders (Id, CustomerId) VALUES (3, 12)");
        Execute("INSERT INTO Customers (Id, Name) VALUES (10, 'Cara')");
        Execute("INSERT INTO Customers (Id, Name) VALUES (11, 'Alice')");
        Execute("INSERT INTO Customers (Id, Name) VALUES (12, 'Bob')");

        var result = ExecuteAndReturn(@"
            SELECT o.Id, c.Name
            FROM Orders o
            JOIN Customers c ON o.CustomerId = c.Id
            ORDER BY c.Name ASC
            LIMIT 1 OFFSET 1");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Single(result.Data);
        Assert.Equal(3, (int)result.Data[0]["o.Id"]);
        Assert.Equal("Bob", (string)result.Data[0]["c.Name"]);
    }
}

public class VolcanoExternalSpillExecutionTests : SqlExecutionTestsBase
{
    public VolcanoExternalSpillExecutionTests()
        : base(new DataVoConfig
        {
            StorageMode = StorageMode.InMemory,
            EnableVolcanoExecution = true,
            EnableVolcanoSpillGuardrails = false,
            EnableVolcanoExternalSortSpill = true,
            VolcanoExternalSortThresholdRows = 2,
            VolcanoExternalSortRunSizeRows = 2,
            EnableVolcanoExternalAggregateSpill = true,
            VolcanoExternalAggregateThresholdRows = 2,
            VolcanoExternalAggregatePartitionCount = 2
        }, "VolcanoExternalSpillDB")
    {
    }

    [Fact]
    public void ExternalSpillConfig_IsPreservedByTestBaseClone()
    {
        Assert.True(Config.EnableVolcanoExternalSortSpill);
        Assert.Equal(2, Config.VolcanoExternalSortThresholdRows);
        Assert.Equal(2, Config.VolcanoExternalSortRunSizeRows);

        Assert.True(Config.EnableVolcanoExternalAggregateSpill);
        Assert.Equal(2, Config.VolcanoExternalAggregateThresholdRows);
        Assert.Equal(2, Config.VolcanoExternalAggregatePartitionCount);
    }

    [Fact]
    public void Select_NoJoin_OrderBy_ExternalSortSpillEnabled_PreservesResultSemantics()
    {
        Execute("CREATE TABLE Scores (Id INT PRIMARY KEY, Score INT)");
        Execute("INSERT INTO Scores (Id, Score) VALUES (1, 40)");
        Execute("INSERT INTO Scores (Id, Score) VALUES (2, 10)");
        Execute("INSERT INTO Scores (Id, Score) VALUES (3, 30)");
        Execute("INSERT INTO Scores (Id, Score) VALUES (4, 20)");

        var result = ExecuteAndReturn("SELECT Id FROM Scores ORDER BY Score ASC LIMIT 1 OFFSET 1");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Single(result.Data);
        Assert.Equal(4, (int)result.Data[0]["Id"]);
    }

    [Fact]
    public void Select_NoJoin_GroupedAggregate_ExternalAggregateSpillEnabled_PreservesResultSemantics()
    {
        Execute("CREATE TABLE Sales (Id INT PRIMARY KEY, Category VARCHAR, Amount INT)");
        Execute("INSERT INTO Sales (Id, Category, Amount) VALUES (1, 'A', 10)");
        Execute("INSERT INTO Sales (Id, Category, Amount) VALUES (2, 'A', 20)");
        Execute("INSERT INTO Sales (Id, Category, Amount) VALUES (3, 'B', 7)");
        Execute("INSERT INTO Sales (Id, Category, Amount) VALUES (4, 'B', 9)");

        var result = ExecuteAndReturn(@"
            SELECT Category, SUM(Amount) AS Total
            FROM Sales
            GROUP BY Category
            HAVING SUM(Amount) >= 15
            ORDER BY Category ASC");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Equal(2, result.Data.Count);
        Assert.Equal("A", (string)result.Data[0]["Category"]);
        Assert.Equal(30d, Convert.ToDouble(result.Data[0]["Total"]));
        Assert.Equal("B", (string)result.Data[1]["Category"]);
        Assert.Equal(16d, Convert.ToDouble(result.Data[1]["Total"]));
    }
}
