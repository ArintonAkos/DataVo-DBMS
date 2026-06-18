using DataVo.Core;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Performance;

public class InsertAllocationGuardTests
{
    // Tightened per phase: baseline ~4700 -> P1 ~3800 -> P2 ~2200 -> P3 ~1800.
    private const long PerInsertCeilingBytes = 3_900; // P1: -serializer stream (~520) -StoredRow clone (216)

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
}
