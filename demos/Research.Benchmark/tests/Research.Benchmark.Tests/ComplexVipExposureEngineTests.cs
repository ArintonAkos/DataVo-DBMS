using Research.Benchmark.Abstractions;
using Research.Benchmark.Runners.ComplexVip;

namespace Research.Benchmark.Tests;

public sealed class ComplexVipExposureEngineTests
{
    public static IEnumerable<object[]> Engines()
    {
        yield return [new DataVoComplexVipExposureEngine()];
        yield return [new DuckDbComplexVipExposureEngine()];
        yield return [new SqliteComplexVipExposureEngine()];
    }

    [Theory]
    [MemberData(nameof(Engines))]
    public async Task ComputesVipExposureGroupedByMarketCategory(IComplexVipExposureEngine engine)
    {
        await using (engine)
        {
            var scenario = new ComplexVipExposureScenario(
                InitialOrderCount: 0,
                AccountCount: 5,
                MarketCount: 3,
                VipRatio: 0.2d);

            await engine.InitializeAsync(scenario);

            await engine.IngestOrderAsync(new ComplexOrderTick(1, AccountId: 5, MarketId: 1, Stake: 10m));
            await engine.IngestOrderAsync(new ComplexOrderTick(2, AccountId: 1, MarketId: 1, Stake: 99m));
            await engine.IngestOrderAsync(new ComplexOrderTick(3, AccountId: 5, MarketId: 2, Stake: 7m));

            IReadOnlyList<CategoryExposure> exposure = await engine.QueryExposureAsync();

            Assert.Equal(
                [new CategoryExposure("Category-1", 10m), new CategoryExposure("Category-2", 7m)],
                exposure.OrderBy(row => row.Category).ToArray());
        }
    }
}
