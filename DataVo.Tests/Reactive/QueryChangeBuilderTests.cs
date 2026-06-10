using DataVo.Core.Runtime.Reactive;

namespace DataVo.Tests.Reactive;

public class QueryChangeBuilderTests
{
    private static readonly ReactiveRowSchema Schema = new("Category", "TotalExposure");

    [Fact]
    public void BuildsDelta_FromAppendedRows()
    {
        var builder = new QueryChangeBuilder(Schema);
        CellValue[] row = new CellValue[2];

        row[0] = CellValue.From("sports");
        row[1] = CellValue.From(10m);
        builder.AddAddedRow(row);

        row[0] = CellValue.From("casino");
        row[1] = CellValue.From(20m);
        builder.AddAddedRow(row);

        QueryChangeRef change = builder.Build();
        Assert.Equal(2, change.Added.Count);
        Assert.Equal("sports", change.Added[0]["Category"].AsString());
        Assert.Equal(20m, change.Added[1]["TotalExposure"].AsDecimal());
        Assert.Equal(0, change.Removed.Count);
    }

    [Fact]
    public void AddRow_RejectsWrongWidth()
    {
        var builder = new QueryChangeBuilder(Schema);
        CellValue[] tooShort = [CellValue.From("x")];
        Assert.Throws<ArgumentException>(() => builder.AddAddedRow(tooShort));
    }

    [Fact]
    public void MaterializedDelta_IsStable_AfterBuilderResetAndReuse()
    {
        var builder = new QueryChangeBuilder(Schema);
        CellValue[] row = new CellValue[2];

        row[0] = CellValue.From("sports");
        row[1] = CellValue.From(10m);
        builder.AddAddedRow(row);

        // Materialize delta N (copies out of the arena).
        QueryChange first = builder.Build().Materialize();

        // Reuse the builder for delta N+1 with different data.
        builder.Reset();
        row[0] = CellValue.From("casino");
        row[1] = CellValue.From(999m);
        builder.AddAddedRow(row);
        QueryChange second = builder.Build().Materialize();

        // The first materialized delta must be unaffected by reuse.
        Assert.Equal("sports", first.Added[0]["Category"]);
        Assert.Equal(10m, first.Added[0]["TotalExposure"]);
        Assert.Equal("casino", second.Added[0]["Category"]);
        Assert.Equal(999m, second.Added[0]["TotalExposure"]);
    }

    [Fact]
    public void GrowsBeyondInitialCapacity_WithoutLoss()
    {
        var builder = new QueryChangeBuilder(Schema, initialRowCapacity: 2);
        CellValue[] row = new CellValue[2];
        for (int i = 0; i < 50; i++)
        {
            row[0] = CellValue.From("g" + i);
            row[1] = CellValue.From((decimal)i);
            builder.AddAddedRow(row);
        }

        QueryChangeRef change = builder.Build();
        Assert.Equal(50, change.Added.Count);
        Assert.Equal(49m, change.Added[49]["TotalExposure"].AsDecimal());
    }

    [Fact]
    public void BuildAndRead_IsAllocationFree_OnSteadyState()
    {
        var builder = new QueryChangeBuilder(Schema, initialRowCapacity: 8);
        CellValue[] row = new CellValue[2]; // reused; allocated once, before measurement

        // Warm up: JIT + one-time arena growth.
        for (int i = 0; i < 2_000; i++)
        {
            builder.Reset();
            row[0] = CellValue.From("sports");
            row[1] = CellValue.From(123.45m);
            builder.AddAddedRow(row);
            QueryChangeRef w = builder.Build();
            for (int r = 0; r < w.Added.Count; r++)
            {
                _ = w.Added[r][1].AsDecimal();
            }
        }

        long before = GC.GetAllocatedBytesForCurrentThread();
        decimal sink = 0m;
        for (int i = 0; i < 5_000; i++)
        {
            builder.Reset();
            row[0] = CellValue.From("sports");
            row[1] = CellValue.From(123.45m);
            builder.AddAddedRow(row);
            QueryChangeRef change = builder.Build();
            for (int r = 0; r < change.Added.Count; r++)
            {
                sink += change.Added[r][1].AsDecimal();
            }
        }
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.True(sink > 0m); // keep sink live
        Assert.Equal(0L, allocated);
    }
}
