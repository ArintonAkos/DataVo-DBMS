using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E.DQL;

public abstract class ExpressionAggregateTestsBase(DataVoConfig config, string testDbName) : SqlExecutionTestsBase(config, testDbName)
{
    [Fact]
    public void Select_ArithmeticInProjection_AddsColumns()
    {
        Execute("CREATE TABLE Users (Id INT, A INT, B INT)");
        Execute("INSERT INTO Users (Id, A, B) VALUES (1, 2, 3)");
        Execute("INSERT INTO Users (Id, A, B) VALUES (2, 5, 7)");

        var result = ExecuteAndReturn("SELECT Id, A + B AS SumAB FROM Users ORDER BY Id");

        Assert.True(!result.IsError, string.Join(Environment.NewLine, result.Messages));
        Assert.Equal(2, result.Data.Count);
        Assert.Equal(5L, Convert.ToInt64(result.Data[0]["SumAB"]));
        Assert.Equal(12L, Convert.ToInt64(result.Data[1]["SumAB"]));
    }

    [Fact]
    public void Select_ArithmeticInWhere_FiltersCorrectly()
    {
        Execute("CREATE TABLE Items (Id INT, Qty INT)");
        Execute("INSERT INTO Items (Id, Qty) VALUES (1, 3)");
        Execute("INSERT INTO Items (Id, Qty) VALUES (2, 6)");

        var result = ExecuteAndReturn("SELECT * FROM Items WHERE Qty * 2 > 10");

        Assert.True(!result.IsError, string.Join(Environment.NewLine, result.Messages));
        Assert.Single(result.Data);
        Assert.Equal(2L, Convert.ToInt64(result.Data[0]["Id"]));
    }

    [Fact]
    public void Select_Having_CountGreaterThanOne_FiltersGroups()
    {
        Execute("CREATE TABLE T (Category VARCHAR)");
        Execute("INSERT INTO T (Category) VALUES ('A')");
        Execute("INSERT INTO T (Category) VALUES ('A')");
        Execute("INSERT INTO T (Category) VALUES ('B')");

        var result = ExecuteAndReturn("SELECT Category, COUNT(*) AS C FROM T GROUP BY Category HAVING COUNT(*) > 1 ORDER BY Category");

        Assert.True(!result.IsError, string.Join(Environment.NewLine, result.Messages));
        Assert.Single(result.Data);
        Assert.Equal("A", result.Data[0]["Category"]);
        // Count may be numeric type
        Assert.Equal(2L, Convert.ToInt64(result.Data[0]["C"]));
    }

    [Fact]
    public void Select_SumOfExpression_ComputesTotal()
    {
        Execute("CREATE TABLE Orders (Id INT, Price FLOAT, Qty INT)");
        Execute("INSERT INTO Orders (Id, Price, Qty) VALUES (1, 10.0, 2)");
        Execute("INSERT INTO Orders (Id, Price, Qty) VALUES (2, 5.5, 3)");

        var result = ExecuteAndReturn("SELECT SUM(Price * Qty) AS Total FROM Orders");

        Assert.True(!result.IsError, string.Join(Environment.NewLine, result.Messages));
        // 10*2 + 5.5*3 = 20 + 16.5 = 36.5
        Assert.Equal(36.5, Convert.ToDouble(result.Data[0]["Total"]));
    }

    [Fact]
    public void Select_OrderByAggregateAlias_WithOffsetLimit_Works()
    {
        Execute("CREATE TABLE Sales (UserName VARCHAR, Amount INT)");
        Execute("INSERT INTO Sales (UserName, Amount) VALUES ('Alice', 10)");
        Execute("INSERT INTO Sales (UserName, Amount) VALUES ('Alice', 20)");
        Execute("INSERT INTO Sales (UserName, Amount) VALUES ('Bob', 5)");

        var result = ExecuteAndReturn("SELECT UserName, SUM(Amount) AS Income FROM Sales GROUP BY UserName ORDER BY Income DESC OFFSET 1 LIMIT 1");

        Assert.True(!result.IsError, string.Join(Environment.NewLine, result.Messages));
        Assert.Single(result.Data);
        Assert.Equal("Bob", result.Data[0]["UserName"]);
        Assert.Equal(5L, Convert.ToInt64(result.Data[0]["Income"]));
    }

    [Fact]
    public void Select_RankFunction_ReportsUnsupportedParserError()
    {
        Execute("CREATE TABLE Products (ProductName VARCHAR, Category VARCHAR, Price FLOAT)");
        Execute("INSERT INTO Products (ProductName, Category, Price) VALUES ('A', 'X', 10.0)");

        var result = ExecuteAndReturn("SELECT ProductName, RANK() AS PriceRank FROM Products");

        Assert.True(result.IsError);
        Assert.Contains(result.Messages, m => m.Contains("Parser Error") && m.Contains("Function 'RANK' is not supported yet"));
    }

    [Fact]
    public void Select_WindowOverClause_ReportsUnsupportedParserError()
    {
        Execute("CREATE TABLE Products (ProductName VARCHAR, Category VARCHAR, Price FLOAT)");
        Execute("INSERT INTO Products (ProductName, Category, Price) VALUES ('A', 'X', 10.0)");

        var result = ExecuteAndReturn("SELECT ProductName, RANK() OVER(PARTITION BY Category ORDER BY Price DESC) AS PriceRank FROM Products");

        Assert.False(result.IsError, string.Join(Environment.NewLine, result.Messages));
        Assert.Single(result.Data);
        Assert.Equal("A", result.Data[0]["ProductName"]);
        Assert.Equal(1L, Convert.ToInt64(result.Data[0]["PriceRank"]));
    }

    [Fact]
    public void Select_CteWithRankOver_FilterTopPerCategory_Works()
    {
        Execute("CREATE TABLE Products (ProductName VARCHAR, Category VARCHAR, Price FLOAT)");
        Execute("INSERT INTO Products (ProductName, Category, Price) VALUES ('A', 'X', 10.0)");
        Execute("INSERT INTO Products (ProductName, Category, Price) VALUES ('B', 'X', 20.0)");
        Execute("INSERT INTO Products (ProductName, Category, Price) VALUES ('C', 'Y', 30.0)");
        Execute("INSERT INTO Products (ProductName, Category, Price) VALUES ('D', 'Y', 25.0)");

        var result = ExecuteAndReturn(@"
WITH RankedProducts AS (
    SELECT ProductName, Category, Price,
           RANK() OVER(PARTITION BY Category ORDER BY Price DESC) AS PriceRank
    FROM Products
)
SELECT ProductName, Category, Price, PriceRank
FROM RankedProducts
WHERE PriceRank = 1
ORDER BY Category");

        Assert.False(result.IsError, string.Join(Environment.NewLine, result.Messages));
        Assert.Equal(2, result.Data.Count);
        Assert.Equal("B", result.Data[0]["ProductName"]);
        Assert.Equal("X", result.Data[0]["Category"]);
        Assert.Equal(1L, Convert.ToInt64(result.Data[0]["PriceRank"]));
        Assert.Equal("C", result.Data[1]["ProductName"]);
        Assert.Equal("Y", result.Data[1]["Category"]);
        Assert.Equal(1L, Convert.ToInt64(result.Data[1]["PriceRank"]));
    }
}

public class InMemoryExpressionAggregateTests : ExpressionAggregateTestsBase
{
    public InMemoryExpressionAggregateTests() : base(new DataVoConfig { StorageMode = StorageMode.InMemory }, "ExprAgg_Mem") { }
}

public class DiskExpressionAggregateTests : ExpressionAggregateTestsBase
{
    public DiskExpressionAggregateTests() : base(new DataVoConfig { StorageMode = StorageMode.Disk, DiskStoragePath = "./test_datavo_expragg" }, "ExprAgg_Disk") { }
}
