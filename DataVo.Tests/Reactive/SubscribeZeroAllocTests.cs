using DataVo.Core;
using DataVo.Core.Runtime.Changes;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Reactive;

public class SubscribeZeroAllocTests
{
    // Exact shape of the benchmark's ComplexVipSql.Query (proven to route to VipExposureReactiveQuery).
    private const string VipSql =
        "SELECT m.Category, SUM(o.Stake) AS TotalExposure " +
        "FROM Orders o " +
        "JOIN Accounts a ON o.AccountId = a.Id " +
        "JOIN Markets m ON o.MarketId = m.Id " +
        "WHERE a.IsVip = true " +
        "GROUP BY m.Category";

    private static DataVoContext NewVipContext()
    {
        var ctx = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        ctx.Execute("CREATE DATABASE VipDb");
        ctx.Execute("USE VipDb");
        ctx.Execute("CREATE TABLE Accounts (Id INT, IsVip BIT)");
        ctx.Execute("CREATE TABLE Markets (Id INT, Category VARCHAR(40))");
        ctx.Execute("CREATE TABLE Orders (Id INT, AccountId INT, MarketId INT, Stake INT)");
        ctx.Execute("INSERT INTO Accounts VALUES (1, true)");
        ctx.Execute("INSERT INTO Markets VALUES (1, 'sports')");
        return ctx;
    }

    [Fact]
    public void SubscribeZeroAlloc_DeliversBorrowedDelta_ForVipOrderInsert()
    {
        using DataVoContext ctx = NewVipContext();

        string? category = null;
        decimal total = 0m;
        using IDisposable sub = ctx.SubscribeZeroAlloc(VipSql, (in QueryChangeRef change) =>
        {
            for (int i = 0; i < change.Added.Count; i++)
            {
                category = change.Added[i]["Category"].AsString();
                total = change.Added[i]["TotalExposure"].AsDecimal();
            }
        });

        ctx.Execute("INSERT INTO Orders VALUES (1, 1, 1, 250)");
        ctx.DispatchPendingNotifications();

        Assert.Equal("sports", category);
        Assert.Equal(250m, total);
    }

    [Fact]
    public void SubscribeZeroAlloc_OnUnsupportedShape_Throws_WithNoSubscription()
    {
        using DataVoContext ctx = NewVipContext();

        Assert.Throws<NotSupportedException>(() =>
            ctx.SubscribeZeroAlloc("SELECT Id FROM Orders WHERE Stake > 0", (in QueryChangeRef _) => { }));

        // No side effect: a subsequent owned Subscribe + dispatch still works (cap not consumed, no
        // stale registration delivering). One insert, one delivered owned change.
        int deliveries = 0;
        using IDisposable sub = ctx.Subscribe("SELECT Id FROM Orders WHERE Stake > 0", _ => deliveries++);
        ctx.Execute("INSERT INTO Orders VALUES (5, 1, 1, 10)");
        ctx.DispatchPendingNotifications();
        Assert.Equal(1, deliveries);
    }

    [Fact]
    public void BorrowedDispatch_IsAllocationLight_OnSteadyState()
    {
        using DataVoContext ctx = NewVipContext();
        using IDisposable sub = ctx.SubscribeZeroAlloc(VipSql, (in QueryChangeRef _) => { });

        // A reused committed ChangeSet: one Orders insert. Re-publishing the same object each iteration
        // does not allocate a new change/dict.
        var after = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
        {
            ["Id"] = 1, ["AccountId"] = 1, ["MarketId"] = 1, ["Stake"] = 250L,
        };
        var set = new ChangeSet(1, "VipDb",
            new[] { new RowChange("Orders", 1, ChangeKind.Insert, before: null, after: after) });

        for (int i = 0; i < 2_000; i++) // warm up JIT + one-time scratch/queue growth
        {
            ctx.Changes.Publish(set);
            ctx.DispatchPendingNotifications();
        }

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 5_000; i++)
        {
            ctx.Changes.Publish(set);
            ctx.DispatchPendingNotifications();
        }
        long perIteration = (GC.GetAllocatedBytesForCurrentThread() - before) / 5_000;

        // The borrowed dispatch path allocates only the two deliberately-retained per-drain snapshot
        // arrays (pending/_subscriptions ToArray); no LINQ/closure/per-row allocation. Bound is generous
        // headroom over those two small arrays; the LINQ version would allocate far more.
        Assert.True(perIteration <= 256, $"borrowed dispatch allocated {perIteration} B/iter (expected <= 256)");
    }
}
