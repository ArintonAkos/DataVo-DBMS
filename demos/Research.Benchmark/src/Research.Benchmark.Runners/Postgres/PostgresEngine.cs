using Research.Benchmark.Abstractions;

namespace Research.Benchmark.Runners.Postgres;

public sealed class PostgresEngine : IBettingRiskEngine
{
    public string Name => "PostgreSQL";

    public EngineArchitecture Architecture => EngineArchitecture.ClientServerPolling;

    public ValueTask InitializeAsync(BettingRiskScenario scenario, CancellationToken cancellationToken = default) =>
        throw new NotImplementedException();

    public ValueTask IngestTickAsync(MarketTick tick, CancellationToken cancellationToken = default) =>
        throw new NotImplementedException();

    public ValueTask IngestBatchAsync(IReadOnlyList<MarketTick> ticks, CancellationToken cancellationToken = default) =>
        throw new NotImplementedException();

    public ValueTask<RiskReadModel> QueryRiskAsync(RiskQuery query, CancellationToken cancellationToken = default) =>
        throw new NotImplementedException();

    public ValueTask ResetAsync(CancellationToken cancellationToken = default) =>
        throw new NotImplementedException();

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
