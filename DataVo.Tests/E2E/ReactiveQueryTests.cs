using DataVo.Core;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E;

public class ReactiveQueryTests
{
    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Disk)]
    public void Subscribe_DeliversAddedOnDrain(StorageMode mode)
    {
        using var ctx = ChangeCaptureIntegrationTests.NewContext(mode, out _);
        ctx.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Health INT)");

        var batches = new List<QueryChange>();
        using var sub = ctx.Subscribe("SELECT Id, Health FROM Players WHERE Health < 20", batches.Add);

        ctx.Execute("INSERT INTO Players VALUES (1, 10)");
        Assert.Empty(batches);                  // pull-drain: nothing until drain
        ctx.DispatchPendingNotifications();

        QueryChange qc = Assert.Single(batches);
        Assert.Single(qc.Added);
        Assert.Equal(1, qc.Added[0]["Id"]);
    }

    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Disk)]
    public void Dispose_StopsDelivery(StorageMode mode)
    {
        using var ctx = ChangeCaptureIntegrationTests.NewContext(mode, out _);
        ctx.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Health INT)");
        var batches = new List<QueryChange>();
        var sub = ctx.Subscribe("SELECT * FROM Players WHERE Health < 20", batches.Add);
        sub.Dispose();

        ctx.Execute("INSERT INTO Players VALUES (1, 5)");
        ctx.DispatchPendingNotifications();
        Assert.Empty(batches);
    }

    [Fact]
    public void Reentrancy_WriteInCallback_SurfacesNextDrain()
    {
        using var ctx = ChangeCaptureIntegrationTests.NewContext(StorageMode.InMemory, out _);
        ctx.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Health INT)");
        int callbacks = 0;
        using var sub = ctx.Subscribe("SELECT * FROM Players WHERE Health < 20", _ =>
        {
            if (callbacks++ == 0)
                ctx.Execute("INSERT INTO Players VALUES (2, 1)"); // write inside callback
        });

        ctx.Execute("INSERT INTO Players VALUES (1, 1)");
        ctx.DispatchPendingNotifications(); // delivers row 1; row 2 buffered, NOT recursively dispatched
        Assert.Equal(1, callbacks);
        ctx.DispatchPendingNotifications(); // now delivers row 2
        Assert.Equal(2, callbacks);
    }

    [Fact]
    public void SubscriptionCap_Throws()
    {
        using var ctx = ChangeCaptureIntegrationTests.NewContext(StorageMode.InMemory, out _);
        ctx.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Health INT)");
        ctx.SetMaxReactiveSubscriptions(1);
        using var a = ctx.Subscribe("SELECT * FROM Players WHERE Health < 20", _ => { });
        Assert.Throws<InvalidOperationException>(() =>
            ctx.Subscribe("SELECT * FROM Players WHERE Health < 10", _ => { }));
    }
}
