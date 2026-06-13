using DataVo.Core;
using DataVo.Core.Parser.AST;
using DataVo.Core.Runtime;
using DataVo.Core.Runtime.Changes;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Reactive;

/// <summary>
/// Behavior/parity coverage for the borrowed fast-lane migration of <c>AggregateReactiveQuery</c>
/// (GC Reduction Slice 3, Step 1). Drives a single-table GROUP BY through the zero-allocation
/// <c>SubscribeZeroAlloc</c> path and asserts the borrowed delta carries the correct per-group
/// aggregate results for add / update / remove transitions.
/// </summary>
public class AggregateBorrowedTests
{
    private const string GroupBySql =
        "SELECT Category, SUM(Stake) AS Total, COUNT(*) AS Cnt FROM Orders GROUP BY Category";

    private static DataVoContext NewContext()
    {
        var ctx = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        ctx.Execute("CREATE DATABASE AggDb");
        ctx.Execute("USE AggDb");
        ctx.Execute("CREATE TABLE Orders (Id INT PRIMARY KEY, Category VARCHAR(20), Stake INT)");
        return ctx;
    }

    private sealed record Delivered(string Kind, string? Category, long Total, long Cnt);

    // Reads a numeric aggregate cell type-agnostically: SUM of an INT column surfaces as Decimal,
    // COUNT as Int64 (mirrors the owned path, whose tests use Convert.ToInt64 defensively).
    private static long Num(CellValue cell) => cell.Type switch
    {
        CellType.Int64 => cell.AsInt64(),
        CellType.Int32 => cell.AsInt32(),
        CellType.Decimal => (long)cell.AsDecimal(),
        CellType.Double => (long)cell.AsDouble(),
        _ => throw new InvalidOperationException($"non-numeric aggregate cell: {cell.Type}"),
    };

    // Reads the borrowed delta inside the callback (a QueryChangeRef cannot escape) into owned records.
    private static void Record(List<Delivered> sink, in QueryChangeRef change)
    {
        for (int i = 0; i < change.Added.Count; i++)
        {
            sink.Add(new Delivered("Added", change.Added[i]["Category"].AsString(),
                Num(change.Added[i]["Total"]), Num(change.Added[i]["Cnt"])));
        }

        for (int i = 0; i < change.Updated.Count; i++)
        {
            sink.Add(new Delivered("Updated", change.Updated[i]["Category"].AsString(),
                Num(change.Updated[i]["Total"]), Num(change.Updated[i]["Cnt"])));
        }

        // Removed groups carry null aggregate cells; only the group key is meaningful.
        for (int i = 0; i < change.Removed.Count; i++)
        {
            sink.Add(new Delivered("Removed", change.Removed[i]["Category"].AsString(), 0, 0));
        }
    }

    private static IReadOnlyDictionary<string, object?> Row(params (string Key, object? Value)[] cells)
    {
        var d = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach ((string key, object? value) in cells)
        {
            d[key] = value;
        }

        return d;
    }

    // Operator-level construction: parse the GROUP BY into a SelectStatement and seed one existing
    // "sports" group, so the steady-state loop only ever updates an existing group (no group creation).
    private static AggregateReactiveQuery SeededSportsOperator(out DataVoEngine engine)
    {
        var select = (SelectStatement)ReactiveQueryParser.ParseSingleStatement(GroupBySql);
        engine = DataVoEngine.Initialize(new DataVoConfig { StorageMode = StorageMode.InMemory });
        var op = new AggregateReactiveQuery(select, engine, "AggDb");
        op.Seed("Orders", new[] { (1L, Row(("Id", 1), ("Category", "sports"), ("Stake", 100))) });
        return op;
    }

    [Fact]
    public void AggregateDispatch_IsAllocationFree_OnSteadyState()
    {
        AggregateReactiveQuery op = SeededSportsOperator(out DataVoEngine engine);
        using (engine)
        {
            var builder = new QueryChangeBuilder(op.OutputSchema);
            var batch = new RowChange[]
            {
                new("Orders", 1, ChangeKind.Insert, before: null,
                    after: Row(("Id", 1), ("Category", "sports"), ("Stake", 100))),
            };

            long sink = 0;
            for (int i = 0; i < 2_000; i++) // warm up JIT + buffer growth + Added->Updated transition
            {
                builder.Reset();
                op.ApplyInto(batch, builder);
                QueryChangeRef c = builder.Build();
                for (int r = 0; r < c.Added.Count; r++) sink += c.Added[r][0].AsString()!.Length;
                for (int r = 0; r < c.Updated.Count; r++) sink += c.Updated[r][0].AsString()!.Length;
            }

            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int i = 0; i < 5_000; i++)
            {
                builder.Reset();
                op.ApplyInto(batch, builder);
                QueryChangeRef c = builder.Build();
                for (int r = 0; r < c.Updated.Count; r++) sink += c.Updated[r][0].AsString()!.Length;
            }
            long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

            Assert.True(sink > 0);
            Assert.Equal(0L, allocated);
        }
    }

    [Fact]
    public void SubscribeZeroAlloc_GroupBy_EmitsAddedWithAggregatesForNewGroup()
    {
        using DataVoContext ctx = NewContext();
        var sink = new List<Delivered>();
        using IDisposable sub = ctx.SubscribeZeroAlloc(GroupBySql,
            (in QueryChangeRef change) => Record(sink, change));

        ctx.Execute("INSERT INTO Orders VALUES (1, 'sports', 100)");
        ctx.DispatchPendingNotifications();

        Delivered d = Assert.Single(sink);
        Assert.Equal("Added", d.Kind);
        Assert.Equal("sports", d.Category);
        Assert.Equal(100, d.Total);
        Assert.Equal(1, d.Cnt);
    }

    [Fact]
    public void SubscribeZeroAlloc_GroupBy_EmitsUpdatedWhenExistingGroupGrows()
    {
        using DataVoContext ctx = NewContext();
        var sink = new List<Delivered>();
        using IDisposable sub = ctx.SubscribeZeroAlloc(GroupBySql,
            (in QueryChangeRef change) => Record(sink, change));

        ctx.Execute("INSERT INTO Orders VALUES (1, 'sports', 100)");
        ctx.DispatchPendingNotifications();
        sink.Clear();

        ctx.Execute("INSERT INTO Orders VALUES (2, 'sports', 50)");
        ctx.DispatchPendingNotifications();

        Delivered d = Assert.Single(sink);
        Assert.Equal("Updated", d.Kind);
        Assert.Equal("sports", d.Category);
        Assert.Equal(150, d.Total);
        Assert.Equal(2, d.Cnt);
    }

    [Fact]
    public void SubscribeZeroAlloc_GroupBy_EmitsRemovedWhenGroupEmptied()
    {
        using DataVoContext ctx = NewContext();
        var sink = new List<Delivered>();
        using IDisposable sub = ctx.SubscribeZeroAlloc(GroupBySql,
            (in QueryChangeRef change) => Record(sink, change));

        ctx.Execute("INSERT INTO Orders VALUES (1, 'sports', 100)");
        ctx.DispatchPendingNotifications();
        sink.Clear();

        ctx.Execute("DELETE FROM Orders WHERE Id = 1");
        ctx.DispatchPendingNotifications();

        Delivered d = Assert.Single(sink);
        Assert.Equal("Removed", d.Kind);
        Assert.Equal("sports", d.Category);
    }

    [Fact]
    public void SubscribeZeroAlloc_GroupBy_SeparatesDistinctGroups()
    {
        using DataVoContext ctx = NewContext();
        var sink = new List<Delivered>();
        using IDisposable sub = ctx.SubscribeZeroAlloc(GroupBySql,
            (in QueryChangeRef change) => Record(sink, change));

        ctx.Execute("INSERT INTO Orders VALUES (1, 'sports', 100)");
        ctx.DispatchPendingNotifications();
        sink.Clear();

        ctx.Execute("INSERT INTO Orders VALUES (2, 'music', 30)");
        ctx.DispatchPendingNotifications();

        Delivered d = Assert.Single(sink);
        Assert.Equal("Added", d.Kind);
        Assert.Equal("music", d.Category);
        Assert.Equal(30, d.Total);
        Assert.Equal(1, d.Cnt);
    }
}
