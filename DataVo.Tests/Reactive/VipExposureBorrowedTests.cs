using DataVo.Core.Runtime.Changes;
using DataVo.Core.Runtime.Reactive;

namespace DataVo.Tests.Reactive;

public class VipExposureBorrowedTests
{
    private static IReadOnlyDictionary<string, object?> Row(params (string Key, object? Value)[] cells)
    {
        var d = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach ((string key, object? value) in cells) d[key] = value;
        return d;
    }

    private static VipExposureReactiveQuery SeededOperator()
    {
        var op = new VipExposureReactiveQuery();
        op.Seed("Accounts", new[] { (1L, Row(("Id", 1), ("IsVip", true))) });
        op.Seed("Markets", new[] { (1L, Row(("Id", 1), ("Category", "sports"))) });
        return op;
    }

    [Fact]
    public void ApplyInto_BuildsBorrowedDelta_ForVipOrderInsert()
    {
        VipExposureReactiveQuery op = SeededOperator();
        var builder = new QueryChangeBuilder(op.OutputSchema);

        var insert = new RowChange("Orders", 1, ChangeKind.Insert, before: null,
            after: Row(("Id", 1), ("AccountId", 1), ("MarketId", 1), ("Stake", 250L)));

        op.ApplyInto(new[] { insert }, builder);
        QueryChangeRef change = builder.Build();

        Assert.Equal(1, change.Added.Count);
        Assert.Equal("sports", change.Added[0]["Category"].AsString());
        Assert.Equal(250m, change.Added[0]["TotalExposure"].AsDecimal());
    }

    [Fact]
    public void ApplyInto_UsesTypedAfter_ForOrderInsert_WhenPresent()
    {
        VipExposureReactiveQuery op = SeededOperator();
        var builder = new QueryChangeBuilder(op.OutputSchema);
        var schema = new ReactiveRowSchema("Id", "AccountId", "MarketId", "Stake");
        var typedAfter = new TypedRow(schema,
            [CellValue.From(1), CellValue.From(1), CellValue.From(1), CellValue.From(250)]);

        var misleadingDictAfter = Row(
            ("Id", 1),
            ("AccountId", 1),
            ("MarketId", 1),
            ("Stake", 1L));

        var insert = new RowChange("Orders", 1, ChangeKind.Insert, before: null, after: misleadingDictAfter, typedAfter);

        op.ApplyInto([insert], builder);
        QueryChangeRef change = builder.Build();

        Assert.Equal(1, change.Added.Count);
        Assert.Equal("sports", change.Added[0]["Category"].AsString());
        Assert.Equal(250m, change.Added[0]["TotalExposure"].AsDecimal());
    }

    [Fact]
    public void ApplyInto_FallsBackToAfter_ForOrderInsert_WhenTypedAfterSchemaOrderDiffers()
    {
        VipExposureReactiveQuery op = SeededOperator();
        var builder = new QueryChangeBuilder(op.OutputSchema);
        var schema = new ReactiveRowSchema("AccountId", "Id", "MarketId", "Stake");
        var typedAfter = new TypedRow(schema,
            [CellValue.From(1), CellValue.From(9), CellValue.From(1), CellValue.From(250)]);

        var dictAfter = Row(
            ("Id", 9),
            ("AccountId", 1),
            ("MarketId", 1),
            ("Stake", 250L));

        var insert = new RowChange("Orders", 9, ChangeKind.Insert, before: null, after: dictAfter, typedAfter);

        op.ApplyInto([insert], builder);
        QueryChangeRef change = builder.Build();

        Assert.Equal(1, change.Added.Count);
        Assert.Equal("sports", change.Added[0]["Category"].AsString());
        Assert.Equal(250m, change.Added[0]["TotalExposure"].AsDecimal());
    }

    [Fact]
    public void OrderInsertDispatch_IsAllocationFree_OnSteadyState()
    {
        VipExposureReactiveQuery op = SeededOperator();
        var builder = new QueryChangeBuilder(op.OutputSchema);
        var insert = new RowChange("Orders", 1, ChangeKind.Insert, before: null,
            after: Row(("Id", 1), ("AccountId", 1), ("MarketId", 1), ("Stake", 250L)));
        var batch = new RowChange[] { insert };

        decimal sink = 0m;
        for (int i = 0; i < 2_000; i++) // warm up JIT + arena/touched growth
        {
            builder.Reset();
            op.ApplyInto(batch, builder);
            QueryChangeRef c = builder.Build();
            for (int r = 0; r < c.Updated.Count; r++) sink += c.Updated[r][1].AsDecimal();
            for (int r = 0; r < c.Added.Count; r++) sink += c.Added[r][1].AsDecimal();
        }

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 5_000; i++)
        {
            builder.Reset();
            op.ApplyInto(batch, builder);
            QueryChangeRef c = builder.Build();
            for (int r = 0; r < c.Updated.Count; r++) sink += c.Updated[r][1].AsDecimal();
        }
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.True(sink > 0m);
        Assert.Equal(0L, allocated);
    }
}
