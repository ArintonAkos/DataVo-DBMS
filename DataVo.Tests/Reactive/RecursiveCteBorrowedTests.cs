using DataVo.Core;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Reactive;

/// <summary>
/// Behavior/parity coverage for the borrowed fast-lane (emit-side) migration of
/// <c>RecursiveCteReactiveQuery</c> (GC Reduction Slice 3, Step 1 only — deep purification is
/// formally deferred because retraction recomputes the closure from scratch). Drives a
/// <c>WITH RECURSIVE</c> reachability query through <c>SubscribeZeroAlloc</c> and asserts the
/// borrowed delta carries the transitive closure (additions) and exact retractions.
/// </summary>
public class RecursiveCteBorrowedTests
{
    private const string ReachSql = @"WITH RECURSIVE Reach AS (
        SELECT Src, Dst FROM Edge
        UNION ALL
        SELECT r.Src, e.Dst FROM Reach r INNER JOIN Edge e ON e.Src = r.Dst)
        SELECT Src, Dst FROM Reach";

    private static DataVoContext NewContext()
    {
        var ctx = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        ctx.Execute("CREATE DATABASE CteDb");
        ctx.Execute("USE CteDb");
        ctx.Execute("CREATE TABLE Edge (Src INT, Dst INT, Id INT PRIMARY KEY)");
        return ctx;
    }

    private sealed record Pair(string Kind, long Src, long Dst);

    private static long Num(CellValue cell) => cell.Type switch
    {
        CellType.Int64 => cell.AsInt64(),
        CellType.Int32 => cell.AsInt32(),
        CellType.Decimal => (long)cell.AsDecimal(),
        CellType.Double => (long)cell.AsDouble(),
        _ => throw new InvalidOperationException($"non-numeric cell: {cell.Type}"),
    };

    private static void Record(List<Pair> sink, in QueryChangeRef change)
    {
        for (int i = 0; i < change.Added.Count; i++)
        {
            sink.Add(new Pair("Added", Num(change.Added[i]["Src"]), Num(change.Added[i]["Dst"])));
        }

        for (int i = 0; i < change.Removed.Count; i++)
        {
            sink.Add(new Pair("Removed", Num(change.Removed[i]["Src"]), Num(change.Removed[i]["Dst"])));
        }
    }

    [Fact]
    public void SubscribeZeroAlloc_RecursiveCte_EmitsTransitiveClosure()
    {
        using DataVoContext ctx = NewContext();
        var sink = new List<Pair>();
        using IDisposable sub = ctx.SubscribeZeroAlloc(ReachSql, (in QueryChangeRef change) => Record(sink, change));

        ctx.Execute("INSERT INTO Edge VALUES (1, 2, 1)");
        ctx.Execute("INSERT INTO Edge VALUES (2, 3, 2)");
        ctx.DispatchPendingNotifications();

        Assert.Contains(new Pair("Added", 1, 2), sink);
        Assert.Contains(new Pair("Added", 2, 3), sink);
        Assert.Contains(new Pair("Added", 1, 3), sink); // transitive 1 -> 2 -> 3
    }

    [Fact]
    public void SubscribeZeroAlloc_RecursiveCte_RetractsOnDelete()
    {
        using DataVoContext ctx = NewContext();
        var sink = new List<Pair>();
        using IDisposable sub = ctx.SubscribeZeroAlloc(ReachSql, (in QueryChangeRef change) => Record(sink, change));

        ctx.Execute("INSERT INTO Edge VALUES (1, 2, 1)");
        ctx.Execute("INSERT INTO Edge VALUES (2, 3, 2)");
        ctx.DispatchPendingNotifications();
        sink.Clear();

        ctx.Execute("DELETE FROM Edge WHERE Id = 2"); // removes 2->3, so 1->3 and 2->3 retract
        ctx.DispatchPendingNotifications();

        Assert.Contains(new Pair("Removed", 1, 3), sink);
        Assert.Contains(new Pair("Removed", 2, 3), sink);
        Assert.DoesNotContain(new Pair("Removed", 1, 2), sink);
    }
}
