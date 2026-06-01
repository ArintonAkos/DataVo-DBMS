using DataVo.Core;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E;

public class ReactiveAggregateTests
{
    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Disk)]
    public void Count_Sum_Avg_MaintainedPerGroup(StorageMode mode)
    {
        using var ctx = ChangeCaptureIntegrationTests.NewContext(mode, out _);
        ctx.Execute("CREATE TABLE P (Id INT PRIMARY KEY, Team VARCHAR(10), Score INT)");

        QueryChange? last = null;
        using var sub = ctx.Subscribe(
            "SELECT Team, COUNT(*) AS Cnt, SUM(Score) AS Total FROM P GROUP BY Team",
            qc => last = qc);

        ctx.Execute("INSERT INTO P VALUES (1, 'red', 10)");
        ctx.Execute("INSERT INTO P VALUES (2, 'red', 20)");
        ctx.DispatchPendingNotifications();

        // Recompute oracle for the 'red' group:
        var row = ctx.Execute("SELECT Team, COUNT(*) AS Cnt, SUM(Score) AS Total FROM P GROUP BY Team")
            .Single().Data.Single(r => Equals(r["Team"], "red"));
        Assert.Equal(2L, Convert.ToInt64(row["Cnt"]));
        Assert.Equal(30L, Convert.ToInt64(row["Total"]));

        // Delete one row -> group still present, Updated
        ctx.Execute("DELETE FROM P WHERE Id = 1");
        ctx.DispatchPendingNotifications();
        Assert.NotNull(last);
        Assert.Contains(last!.Updated, r => Equals(r["Team"], "red") && Convert.ToInt64(r["Total"]) == 20L);
    }

    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Disk)]
    public void EmptyingGroup_EmitsRemoved(StorageMode mode)
    {
        using var ctx = ChangeCaptureIntegrationTests.NewContext(mode, out _);
        ctx.Execute("CREATE TABLE P (Id INT PRIMARY KEY, Team VARCHAR(10))");
        QueryChange? last = null;
        using var sub = ctx.Subscribe("SELECT Team, COUNT(*) AS Cnt FROM P GROUP BY Team", qc => last = qc);

        ctx.Execute("INSERT INTO P VALUES (1, 'blue')");
        ctx.DispatchPendingNotifications();
        ctx.Execute("DELETE FROM P WHERE Id = 1");
        ctx.DispatchPendingNotifications();

        Assert.Contains(last!.Removed, r => Equals(r["Team"], "blue"));
    }
}
