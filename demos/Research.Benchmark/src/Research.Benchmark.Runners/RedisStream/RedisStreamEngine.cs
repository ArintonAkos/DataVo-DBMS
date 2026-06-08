using Research.Benchmark.Abstractions;

namespace Research.Benchmark.Runners.RedisStream;

public sealed class RedisStreamEngine : IBettingRiskEngine
{
    public string Name => "Redis Streams";

    public EngineArchitecture Architecture => EngineArchitecture.ClientServerPubSubStreaming;

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
