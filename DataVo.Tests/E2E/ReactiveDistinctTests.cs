using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E;

public class ReactiveDistinctTests
{
    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Disk)]
    public void Distinct_CollapsesAndRetracts(StorageMode mode)
    {
        using var ctx = ChangeCaptureIntegrationTests.NewContext(mode, out _);
        ctx.Execute("CREATE TABLE T (Id INT PRIMARY KEY, City VARCHAR(20))");
        var live = new Dictionary<string, int>();
        using var sub = ctx.Subscribe("SELECT DISTINCT City FROM T", qc =>
        {
            foreach (var r in qc.Added)
            {
                string city = (string)r["City"]!;
                live[city] = live.GetValueOrDefault(city) + 1;
            }

            foreach (var r in qc.Removed)
            {
                string city = (string)r["City"]!;
                live[city] = live.GetValueOrDefault(city) - 1;
            }
        });

        ctx.Execute("INSERT INTO T VALUES (1,'NYC')");
        ctx.Execute("INSERT INTO T VALUES (2,'NYC')");
        ctx.Execute("INSERT INTO T VALUES (3,'LA')");
        ctx.DispatchPendingNotifications();
        Assert.Equal(1, live["NYC"]);
        Assert.Equal(1, live["LA"]);

        ctx.Execute("DELETE FROM T WHERE Id=1");
        ctx.DispatchPendingNotifications();
        Assert.Equal(1, live["NYC"]);

        ctx.Execute("DELETE FROM T WHERE Id=2");
        ctx.DispatchPendingNotifications();
        Assert.Equal(0, live["NYC"]);
    }
}
