using DataVo.Core;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Reactive;

/// <summary>
/// Behavior/parity coverage for the borrowed fast-lane migration of <c>TopKReactiveQuery</c>
/// (GC Reduction Slice 3). Drives an <c>ORDER BY … LIMIT</c> through the zero-allocation
/// <c>SubscribeZeroAlloc</c> path and asserts the borrowed window delta for enter/displace/update
/// transitions, including the update before-image. A <c>SELECT *</c> case exercises the
/// catalog-derived output schema.
/// </summary>
public class TopKBorrowedTests
{
    private const string TopKSql = "SELECT Id, Score FROM Players ORDER BY Score DESC LIMIT 2";

    private static DataVoContext NewContext()
    {
        var ctx = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        ctx.Execute("CREATE DATABASE TopKDb");
        ctx.Execute("USE TopKDb");
        ctx.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Score INT)");
        return ctx;
    }

    private sealed record Delivered(string Kind, long Id, long Score);

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
            sink.Add(new Delivered("Added", Num(change.Added[i]["Id"]), Num(change.Added[i]["Score"])));
        }

        for (int i = 0; i < change.Updated.Count; i++)
        {
            sink.Add(new Delivered("Updated", Num(change.Updated[i]["Id"]), Num(change.Updated[i]["Score"])));
        }

        for (int i = 0; i < change.UpdatedBefore.Count; i++)
        {
            sink.Add(new Delivered("UpdatedBefore", Num(change.UpdatedBefore[i]["Id"]), Num(change.UpdatedBefore[i]["Score"])));
        }

        for (int i = 0; i < change.Removed.Count; i++)
        {
            sink.Add(new Delivered("Removed", Num(change.Removed[i]["Id"]), Num(change.Removed[i]["Score"])));
        }
    }

    [Fact]
    public void SubscribeZeroAlloc_TopK_EmitsAddedWithinLimit()
    {
        using DataVoContext ctx = NewContext();
        var sink = new List<Delivered>();
        using IDisposable sub = ctx.SubscribeZeroAlloc(TopKSql, (in QueryChangeRef change) => Record(sink, change));

        ctx.Execute("INSERT INTO Players VALUES (1, 50)");
        ctx.DispatchPendingNotifications();
        sink.Clear();

        ctx.Execute("INSERT INTO Players VALUES (2, 70)");
        ctx.DispatchPendingNotifications();

        Delivered d = Assert.Single(sink);
        Assert.Equal(new Delivered("Added", 2, 70), d);
    }

    [Fact]
    public void SubscribeZeroAlloc_TopK_DisplacesLowestWhenFull()
    {
        using DataVoContext ctx = NewContext();
        var sink = new List<Delivered>();
        using IDisposable sub = ctx.SubscribeZeroAlloc(TopKSql, (in QueryChangeRef change) => Record(sink, change));

        ctx.Execute("INSERT INTO Players VALUES (1, 50)");
        ctx.Execute("INSERT INTO Players VALUES (2, 70)");
        ctx.DispatchPendingNotifications();
        sink.Clear();

        ctx.Execute("INSERT INTO Players VALUES (3, 90)");
        ctx.DispatchPendingNotifications();

        Assert.Contains(new Delivered("Added", 3, 90), sink);
        Assert.Contains(new Delivered("Removed", 1, 50), sink);
        Assert.Equal(2, sink.Count);
    }

    [Fact]
    public void SubscribeZeroAlloc_TopK_EmitsUpdatedWithBeforeImage_WhenWindowedValueChanges()
    {
        using DataVoContext ctx = NewContext();
        var sink = new List<Delivered>();
        using IDisposable sub = ctx.SubscribeZeroAlloc(TopKSql, (in QueryChangeRef change) => Record(sink, change));

        ctx.Execute("INSERT INTO Players VALUES (1, 90)");
        ctx.Execute("INSERT INTO Players VALUES (2, 70)");
        ctx.DispatchPendingNotifications();
        sink.Clear();

        ctx.Execute("UPDATE Players SET Score = 80 WHERE Id = 2");
        ctx.DispatchPendingNotifications();

        Assert.Contains(new Delivered("Updated", 2, 80), sink);
        Assert.Contains(new Delivered("UpdatedBefore", 2, 70), sink);
    }

    [Fact]
    public void SubscribeZeroAlloc_TopK_SelectStar_UsesCatalogSchema()
    {
        using DataVoContext ctx = NewContext();
        var sink = new List<Delivered>();
        using IDisposable sub = ctx.SubscribeZeroAlloc(
            "SELECT * FROM Players ORDER BY Score DESC LIMIT 2",
            (in QueryChangeRef change) => Record(sink, change));

        ctx.Execute("INSERT INTO Players VALUES (1, 50)");
        ctx.DispatchPendingNotifications();

        Delivered d = Assert.Single(sink);
        Assert.Equal(new Delivered("Added", 1, 50), d);
    }
}
