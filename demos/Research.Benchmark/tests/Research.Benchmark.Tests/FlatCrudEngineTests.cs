using Research.Benchmark.Abstractions;
using Research.Benchmark.Runners.FlatCrud;
using Xunit.Abstractions;

namespace Research.Benchmark.Tests;

/// <summary>
/// Correctness contract for the Scenario A (Flat CRUD) engines, so the benchmark measures implementations
/// that actually round-trip records faithfully.
/// </summary>
public sealed class FlatCrudEngineTests
{
    private const int AllocationIterations = 20_000;
    private const long DataVoGetByIdAllocationCeilingBytes = 192;
    private readonly ITestOutputHelper _output;

    public FlatCrudEngineTests(ITestOutputHelper output) => _output = output;

    public static IEnumerable<object[]> Engines()
    {
        yield return [new DataVoFlatCrudEngine()];
        yield return [new LiteDbFlatCrudEngine()];
        yield return [new SqliteFlatCrudEngine()];
    }

    [Theory]
    [MemberData(nameof(Engines))]
    public void InsertedRecordsRoundTripByPrimaryKey(IFlatCrudEngine engine)
    {
        using (engine)
        {
            engine.Initialize();

            for (int i = 1; i <= 100; i++)
            {
                engine.Insert(new FlatRecord(i, $"name-{i}", i * 2, i + 0.5d));
            }

            FlatRecord? first = engine.GetById(1);
            Assert.NotNull(first);
            Assert.Equal(1, first!.Id);
            Assert.Equal("name-1", first.Name);
            Assert.Equal(2, first.Value);
            Assert.Equal(1.5d, first.Score);

            FlatRecord? mid = engine.GetById(57);
            Assert.NotNull(mid);
            Assert.Equal("name-57", mid!.Name);
            Assert.Equal(114, mid.Value);

            Assert.Null(engine.GetById(9999));
        }
    }

    [Fact]
    public void DataVoGetById_WarmPointLookup_StaysNearMaterializationFloor()
    {
        using var engine = new DataVoFlatCrudEngine();
        engine.Initialize();

        for (int i = 1; i <= 1_000; i++)
        {
            engine.Insert(new FlatRecord(i, $"name-{i}", i * 2, i + 0.5d));
        }

        AllocationSample sample = Measure(id => engine.GetById(id));
        _output.WriteLine($"DataVo GetById warm point lookup: {sample.BytesPerCall:N1} B/call");

        Assert.True(sample.BytesPerCall <= DataVoGetByIdAllocationCeilingBytes,
            $"DataVo GetById point lookup {sample.BytesPerCall:N1} B/call exceeds {DataVoGetByIdAllocationCeilingBytes} B/call");
    }

    private static AllocationSample Measure(Func<int, object?> action)
    {
        object? sink = null;
        for (int i = 0; i < 1_000; i++)
        {
            sink = action((i % 1_000) + 1);
        }

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < AllocationIterations; i++)
        {
            sink = action((i % 1_000) + 1);
        }

        long bytes = GC.GetAllocatedBytesForCurrentThread() - before;
        GC.KeepAlive(sink);
        return new AllocationSample(bytes, bytes / (double)AllocationIterations);
    }

    private readonly record struct AllocationSample(long TotalBytes, double BytesPerCall);
}
