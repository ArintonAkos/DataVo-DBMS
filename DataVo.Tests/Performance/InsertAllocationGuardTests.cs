using DataVo.Core;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Performance;

public class InsertAllocationGuardTests
{
    // No-subscriber (capture-off) warm insert path. Tightened per phase against the real measured value
    // (kept tight so regressions are caught early), not the plan's looser round-number targets:
    //   baseline ~4,700 -> P1 ~3,800 -> P2 measured ~1,262 -> P3 measured ~1,090 (Task 3.1 constraint-
    //   free fast-path removed the messages List + accepted PK/UK collections). Variance ~5 B.
    private const long PerInsertCeilingBytes = 1_200; // P3: -validation scaffolding for constraint-free tables

    [Fact]
    public void InsertTyped_WarmPerInsertAllocation_StaysUnderCeiling()
    {
        using var ctx = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        foreach (var sql in new[] { "CREATE DATABASE GuardDb", "USE GuardDb",
            "CREATE TABLE Orders (Id INT, AccountId INT, MarketId INT, Stake INT)" })
        {
            Assert.False(ctx.Execute(sql).Last().IsError);
        }

        var schema = new ReactiveRowSchema("Id", "AccountId", "MarketId", "Stake");
        var cells = new CellValue[4];
        long Insert(int id)
        {
            cells[0] = CellValue.From(id);
            cells[1] = CellValue.From(id % 1000);
            cells[2] = CellValue.From(id % 50);
            cells[3] = CellValue.From(id);
            long r = ctx.InsertTyped("Orders", schema, cells);
            ctx.DispatchPendingNotifications();
            return r;
        }

        for (int i = 1; i <= 2_000; i++) Insert(i); // warm up (JIT + first-touch + dict growth)

        const int measured = 20_000;
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 2_001; i <= 2_000 + measured; i++) Insert(i);
        long perInsert = (GC.GetAllocatedBytesForCurrentThread() - before) / measured;

        Assert.True(perInsert <= PerInsertCeilingBytes,
            $"per-insert allocation {perInsert} B exceeds ceiling {PerInsertCeilingBytes} B");
    }

    // Task 3.3 fence: with a borrowed VIP subscriber active (capture on), an Orders insert must not
    // allocate the owned after-image dict, because VipExposureReactiveQuery reads TypedAfter (the typed
    // lane) and never touches RowChange.After. VipExposure is the only production borrowed operator that
    // reads the typed image, so it is the shape that exercises the dual-image collapse.
    //
    // Honest numbers (measured, low variance ~8 B): collapsing the dual after-image drops the warm
    // per-insert from ~2,551 B (eager dict + TypedRow clone) to ~2,370 B — a ~180 B win. The absolute is
    // dominated by inherent per-insert costs (storage row retention, MVCC version, ChangeSet/RowChange,
    // per-drain dispatch), NOT the after-image; the plan's "~1,900 / ~600 B" estimate was based on a
    // query shape (SELECT Id, Stake) that does not route to a typed-lane borrowed operator. The ceiling
    // sits below the pre-collapse baseline so a regression re-adding the eager dict is caught.
    private const long VipBorrowedPerInsertCeilingBytes = 2_450;

    // The benchmark's complex-vip query shape (proven to route to VipExposureReactiveQuery).
    private const string VipSql =
        "SELECT m.Category, SUM(o.Stake) AS TotalExposure " +
        "FROM Orders o " +
        "JOIN Accounts a ON o.AccountId = a.Id " +
        "JOIN Markets m ON o.MarketId = m.Id " +
        "WHERE a.IsVip = true " +
        "GROUP BY m.Category";

    [Fact]
    public void InsertTyped_VipBorrowedSubscriber_AllocatesNoOwnedAfterImageDict()
    {
        using var ctx = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        foreach (var sql in new[] { "CREATE DATABASE VipNoDictDb", "USE VipNoDictDb",
            "CREATE TABLE Accounts (Id INT, IsVip BIT)",
            "CREATE TABLE Markets (Id INT, Category VARCHAR(40))",
            "CREATE TABLE Orders (Id INT, AccountId INT, MarketId INT, Stake INT)",
            "INSERT INTO Accounts VALUES (1, true)",
            "INSERT INTO Markets VALUES (1, 'sports')" })
        {
            Assert.False(ctx.Execute(sql).Last().IsError);
        }

        decimal sink = 0m;
        using IDisposable sub = ctx.SubscribeZeroAlloc(VipSql, (in QueryChangeRef change) =>
        {
            for (int i = 0; i < change.Added.Count; i++) sink += change.Added[i]["TotalExposure"].AsDecimal();
            for (int i = 0; i < change.Updated.Count; i++) sink += change.Updated[i]["TotalExposure"].AsDecimal();
        });

        var schema = new ReactiveRowSchema("Id", "AccountId", "MarketId", "Stake");
        var cells = new CellValue[4];
        void Insert(int id)
        {
            cells[0] = CellValue.From(id);
            cells[1] = CellValue.From(1);   // VIP account
            cells[2] = CellValue.From(1);   // sports market
            cells[3] = CellValue.From(id);
            ctx.InsertTyped("Orders", schema, cells);
            ctx.DispatchPendingNotifications();
        }

        for (int i = 1; i <= 2_000; i++) Insert(i); // warm up (JIT + operator dict growth)

        const int measured = 20_000;
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 2_001; i <= 2_000 + measured; i++) Insert(i);
        long perInsert = (GC.GetAllocatedBytesForCurrentThread() - before) / measured;

        Assert.True(sink > 0m); // prove the borrowed VIP callback actually ran
        Assert.True(perInsert <= VipBorrowedPerInsertCeilingBytes,
            $"per-insert {perInsert} B exceeds VIP borrowed ceiling {VipBorrowedPerInsertCeilingBytes} B — owned after-image dict not collapsed");
    }
}
