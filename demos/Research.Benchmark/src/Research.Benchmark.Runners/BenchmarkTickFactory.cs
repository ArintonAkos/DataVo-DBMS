using Research.Benchmark.Abstractions;

namespace Research.Benchmark.Runners;

public static class BenchmarkTickFactory
{
    public static MarketTick CreateBaselineOrder(long sequence, BettingRiskScenario scenario) =>
        CreateOrder(sequence, scenario, DateTimeOffset.UnixEpoch.AddMilliseconds(sequence));

    public static MarketTick CreateLiveTick(long sequence, BettingRiskScenario scenario) =>
        CreateOrder(sequence, scenario, DateTimeOffset.UnixEpoch.AddMilliseconds(sequence));

    private static MarketTick CreateOrder(long sequence, BettingRiskScenario scenario, DateTimeOffset timestamp)
    {
        int marketCount = Math.Max(1, scenario.MarketCount);
        int runnersPerMarket = Math.Max(1, scenario.RunnersPerMarket);
        int accountCount = Math.Max(1, scenario.AccountCount);

        return new MarketTick(
            Sequence: sequence,
            Timestamp: timestamp,
            Kind: TickKind.OrderPlaced,
            MarketId: 1 + (int)(sequence % marketCount),
            RunnerId: 1 + (int)((sequence / marketCount) % runnersPerMarket),
            AccountId: 1 + (int)((sequence * 17) % accountCount),
            Side: sequence % 2 == 0 ? "BACK" : "LAY",
            Price: 100m + sequence % 50,
            Stake: 10m + sequence % 90);
    }
}
