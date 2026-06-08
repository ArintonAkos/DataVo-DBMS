namespace Research.Benchmark.Abstractions;

public sealed record ComplexVipExposureScenario(
    int InitialOrderCount,
    int AccountCount,
    int MarketCount,
    double VipRatio);

public sealed record ComplexOrderTick(
    long Id,
    int AccountId,
    int MarketId,
    decimal Stake);

public sealed record CategoryExposure(
    string Category,
    decimal TotalExposure);

public interface IComplexVipExposureEngine : IAsyncDisposable
{
    string Name { get; }

    EngineArchitecture Architecture { get; }

    ValueTask InitializeAsync(ComplexVipExposureScenario scenario, CancellationToken cancellationToken = default);

    ValueTask IngestOrderAsync(ComplexOrderTick order, CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyList<CategoryExposure>> QueryExposureAsync(CancellationToken cancellationToken = default);
}
