using Research.Benchmark.Abstractions;

namespace Research.Benchmark.Runners;

public static class ComplexVipTickFactory
{
    public static ComplexOrderTick CreateOrder(long id, ComplexVipExposureScenario scenario)
    {
        int accountCount = Math.Max(1, scenario.AccountCount);
        int marketCount = Math.Max(1, scenario.MarketCount);

        return new ComplexOrderTick(
            id,
            AccountId: 1 + (int)((id * 17) % accountCount),
            MarketId: 1 + (int)((id * 31) % marketCount),
            Stake: 10m + id % 90);
    }

    public static bool IsVipAccount(int accountId, ComplexVipExposureScenario scenario)
    {
        int vipModulo = Math.Max(1, (int)Math.Round(1d / Math.Clamp(scenario.VipRatio, 0.01d, 1d)));
        return accountId % vipModulo == 0;
    }

    public static string CategoryForMarket(int marketId) =>
        "Category-" + (marketId % 10).ToString(System.Globalization.CultureInfo.InvariantCulture);
}
