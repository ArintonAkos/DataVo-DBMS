using DataVo.Core;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Reactive;

/// <summary>
/// Behavior/parity coverage for the borrowed fast-lane (emit-side) migration of
/// <c>JoinReactiveQuery</c> (GC Reduction Slice 3, the final operator). Drives a two-table inner
/// equi-join through <c>SubscribeZeroAlloc</c> and asserts the borrowed delta for match / retract /
/// value-update transitions, including the update before-image. Output rows carry the qualified
/// projection columns (e.g. <c>R.Id</c>, <c>S.Kind</c>).
/// </summary>
public class JoinBorrowedTests
{
    private const string JoinSql = "SELECT R.Id, S.Kind FROM R INNER JOIN S ON R.Gid = S.Id";

    private static DataVoContext NewContext()
    {
        var ctx = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        ctx.Execute("CREATE DATABASE JoinDb");
        ctx.Execute("USE JoinDb");
        ctx.Execute("CREATE TABLE R (Id INT PRIMARY KEY, Gid INT)");
        ctx.Execute("CREATE TABLE S (Id INT PRIMARY KEY, Kind VARCHAR(20))");
        return ctx;
    }

    private sealed record Delivered(string Op, long RId, string? SKind);

    private static long Num(CellValue cell) => cell.Type switch
    {
        CellType.Int64 => cell.AsInt64(),
        CellType.Int32 => cell.AsInt32(),
        CellType.Decimal => (long)cell.AsDecimal(),
        CellType.Double => (long)cell.AsDouble(),
        _ => throw new InvalidOperationException($"non-numeric cell: {cell.Type}"),
    };

    private static void Record(List<Delivered> sink, in QueryChangeRef change)
    {
        for (int i = 0; i < change.Added.Count; i++)
        {
            sink.Add(new Delivered("Added", Num(change.Added[i]["R.Id"]), change.Added[i]["S.Kind"].AsString()));
        }

        for (int i = 0; i < change.Updated.Count; i++)
        {
            sink.Add(new Delivered("Updated", Num(change.Updated[i]["R.Id"]), change.Updated[i]["S.Kind"].AsString()));
        }

        for (int i = 0; i < change.UpdatedBefore.Count; i++)
        {
            sink.Add(new Delivered("UpdatedBefore", Num(change.UpdatedBefore[i]["R.Id"]), change.UpdatedBefore[i]["S.Kind"].AsString()));
        }

        for (int i = 0; i < change.Removed.Count; i++)
        {
            sink.Add(new Delivered("Removed", Num(change.Removed[i]["R.Id"]), change.Removed[i]["S.Kind"].AsString()));
        }
    }

    [Fact]
    public void SubscribeZeroAlloc_Join_EmitsAddedOnMatch()
    {
        using DataVoContext ctx = NewContext();
        var sink = new List<Delivered>();
        using IDisposable sub = ctx.SubscribeZeroAlloc(JoinSql, (in QueryChangeRef change) => Record(sink, change));

        ctx.Execute("INSERT INTO S VALUES (100, 'gold')");
        ctx.Execute("INSERT INTO R VALUES (1, 100)"); // R.Gid = 100 matches S.Id = 100
        ctx.Execute("INSERT INTO R VALUES (2, 999)"); // no match
        ctx.DispatchPendingNotifications();

        Delivered d = Assert.Single(sink);
        Assert.Equal(new Delivered("Added", 1, "gold"), d);
    }

    [Fact]
    public void SubscribeZeroAlloc_Join_EmitsRemovedOnRetract()
    {
        using DataVoContext ctx = NewContext();
        var sink = new List<Delivered>();
        using IDisposable sub = ctx.SubscribeZeroAlloc(JoinSql, (in QueryChangeRef change) => Record(sink, change));

        ctx.Execute("INSERT INTO S VALUES (100, 'gold')");
        ctx.Execute("INSERT INTO R VALUES (1, 100)");
        ctx.DispatchPendingNotifications();
        sink.Clear();

        ctx.Execute("DELETE FROM S WHERE Id = 100"); // retracts the joined row
        ctx.DispatchPendingNotifications();

        Delivered d = Assert.Single(sink);
        Assert.Equal(new Delivered("Removed", 1, "gold"), d);
    }

    [Fact]
    public void SubscribeZeroAlloc_Join_EmitsUpdatedWithBeforeImage_WhenJoinedValueChanges()
    {
        using DataVoContext ctx = NewContext();
        var sink = new List<Delivered>();
        using IDisposable sub = ctx.SubscribeZeroAlloc(JoinSql, (in QueryChangeRef change) => Record(sink, change));

        ctx.Execute("INSERT INTO S VALUES (100, 'gold')");
        ctx.Execute("INSERT INTO R VALUES (1, 100)");
        ctx.DispatchPendingNotifications();
        sink.Clear();

        ctx.Execute("UPDATE S SET Kind = 'silver' WHERE Id = 100");
        ctx.DispatchPendingNotifications();

        Assert.Contains(new Delivered("Updated", 1, "silver"), sink);
        Assert.Contains(new Delivered("UpdatedBefore", 1, "gold"), sink);
    }
}
