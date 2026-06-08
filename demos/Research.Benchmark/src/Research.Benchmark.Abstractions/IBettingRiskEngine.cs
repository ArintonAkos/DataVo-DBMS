namespace Research.Benchmark.Abstractions;

public interface IBettingRiskEngine : IAsyncDisposable
{
    string Name { get; }

    EngineArchitecture Architecture { get; }

    ValueTask InitializeAsync(BettingRiskScenario scenario, CancellationToken cancellationToken = default);

    ValueTask IngestTickAsync(MarketTick tick, CancellationToken cancellationToken = default);

    ValueTask IngestBatchAsync(IReadOnlyList<MarketTick> ticks, CancellationToken cancellationToken = default);

    ValueTask<RiskReadModel> QueryRiskAsync(RiskQuery query, CancellationToken cancellationToken = default);

    ValueTask ResetAsync(CancellationToken cancellationToken = default);
}
