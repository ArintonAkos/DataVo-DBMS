using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E;

public class ReactiveJoinAggregateTests
{
    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Disk)]
    public void VipExposureByMarketCategory_IsMaintainedIncrementally(StorageMode mode)
    {
        using var ctx = ChangeCaptureIntegrationTests.NewContext(mode, out _);
        ctx.Execute("CREATE TABLE Accounts (Id INT PRIMARY KEY, IsVip BIT)");
        ctx.Execute("CREATE TABLE Markets (Id INT PRIMARY KEY, Category VARCHAR(20))");
        ctx.Execute("CREATE TABLE Orders (Id INT PRIMARY KEY, AccountId INT, MarketId INT, Stake INT)");

        var live = new Dictionary<string, decimal>(StringComparer.OrdinalIgnoreCase);
        using var sub = ctx.Subscribe("""
            SELECT m.Category, SUM(o.Stake) AS TotalExposure
            FROM Orders o
            JOIN Accounts a ON o.AccountId = a.Id
            JOIN Markets m ON o.MarketId = m.Id
            WHERE a.IsVip = true
            GROUP BY m.Category
            """, qc =>
        {
            foreach (IReadOnlyDictionary<string, object?> row in qc.Removed)
            {
                live.Remove((string)row["Category"]!);
            }

            foreach (IReadOnlyDictionary<string, object?> row in qc.Added.Concat(qc.Updated))
            {
                live[(string)row["Category"]!] = Convert.ToDecimal(row["TotalExposure"]);
            }
        });

        ctx.Execute("INSERT INTO Accounts VALUES (1, true)");
        ctx.Execute("INSERT INTO Accounts VALUES (2, false)");
        ctx.Execute("INSERT INTO Markets VALUES (10, 'sports')");
        ctx.Execute("INSERT INTO Markets VALUES (11, 'casino')");
        ctx.Execute("INSERT INTO Orders VALUES (100, 1, 10, 25)");
        ctx.Execute("INSERT INTO Orders VALUES (101, 2, 10, 99)");
        ctx.Execute("INSERT INTO Orders VALUES (102, 1, 11, 10)");
        ctx.DispatchPendingNotifications();

        Assert.Equal(25m, live["sports"]);
        Assert.Equal(10m, live["casino"]);
        Assert.Equal(2, live.Count);

        ctx.Execute("INSERT INTO Orders VALUES (103, 1, 10, 5)");
        ctx.DispatchPendingNotifications();

        Assert.Equal(30m, live["sports"]);
    }
}
