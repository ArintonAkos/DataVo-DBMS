using Research.Benchmark.Abstractions;
using Research.Benchmark.Runners.FlatCrud;

namespace Research.Benchmark.Tests;

/// <summary>
/// Correctness contract for the Scenario A (Flat CRUD) engines, so the benchmark measures implementations
/// that actually round-trip records faithfully.
/// </summary>
public sealed class FlatCrudEngineTests
{
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
}
