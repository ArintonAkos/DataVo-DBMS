namespace Research.Benchmark.Abstractions;

public enum EngineArchitecture
{
    EmbeddedReactiveIvm,
    EmbeddedPollingRecompute,
    ClientServerPolling,
    ClientServerPubSubStreaming
}

public enum TickKind
{
    OrderPlaced,
    OrderUpdated,
    OrderCancelled,
    BetMatched,
    MarketSuspended,
    MarketReopened
}

public sealed record BettingRiskScenario(
    int MarketCount,
    int RunnersPerMarket,
    int AccountCount,
    int InitialOrderCount,
    int SubscriberCount);

public sealed record MarketTick(
    long Sequence,
    DateTimeOffset Timestamp,
    TickKind Kind,
    int MarketId,
    int RunnerId,
    int AccountId,
    string Side,
    decimal Price,
    decimal Stake);

public sealed record RiskQuery(
    int? MarketId = null,
    int? AccountId = null,
    int TopMarkets = 10,
    int? RunnerId = null);

public sealed record RiskReadModel(
    IReadOnlyList<RunnerExposure> RunnerExposure,
    IReadOnlyList<AccountExposure> AccountExposure,
    IReadOnlyList<MarketRiskSummary> MarketRisk,
    DateTimeOffset AsOf);

public sealed record RunnerExposure(
    int MarketId,
    int RunnerId,
    decimal BestBack,
    decimal BestLay,
    decimal MatchedVolume,
    decimal OpenExposure);

public sealed record AccountExposure(
    int AccountId,
    int MarketId,
    decimal OpenExposure,
    decimal MatchedStake);

public sealed record MarketRiskSummary(
    int MarketId,
    decimal TotalOpenExposure,
    decimal TotalMatchedVolume,
    int ActiveSubscriberCount);
