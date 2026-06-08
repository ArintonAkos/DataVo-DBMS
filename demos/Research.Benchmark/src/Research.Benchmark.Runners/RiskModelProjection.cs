using Research.Benchmark.Abstractions;

namespace Research.Benchmark.Runners;

internal readonly record struct RunnerExposureKey(int MarketId, int RunnerId);

internal readonly record struct AccountExposureKey(int AccountId, int MarketId);

internal readonly record struct RunnerAccountExposureKey(int RunnerId, int AccountId);

internal static class RiskModelProjection
{
    public static RiskReadModel Build(
        IEnumerable<RunnerExposure> runnerExposure,
        IEnumerable<AccountExposure> accountExposure,
        RiskQuery query,
        int subscriberCount)
    {
        List<RunnerExposure> runners = runnerExposure
            .Where(row => query.MarketId is null || row.MarketId == query.MarketId.Value)
            .OrderBy(row => row.MarketId)
            .ThenBy(row => row.RunnerId)
            .ToList();

        List<AccountExposure> accounts = accountExposure
            .Where(row => query.MarketId is null || row.MarketId == query.MarketId.Value)
            .Where(row => query.AccountId is null || row.AccountId == query.AccountId.Value)
            .OrderBy(row => row.AccountId)
            .ThenBy(row => row.MarketId)
            .ToList();

        List<MarketRiskSummary> markets = runners
            .GroupBy(row => row.MarketId)
            .Select(group => new MarketRiskSummary(
                group.Key,
                group.Sum(row => row.OpenExposure),
                group.Sum(row => row.MatchedVolume),
                subscriberCount))
            .OrderByDescending(row => row.TotalOpenExposure)
            .ThenBy(row => row.MarketId)
            .Take(Math.Max(1, query.TopMarkets))
            .ToList();

        return new RiskReadModel(runners, accounts, markets, DateTimeOffset.UtcNow);
    }

    public static RiskReadModel BuildPoint(int runnerId, int accountId, decimal totalExposure, int subscriberCount)
    {
        var runner = new RunnerExposure(0, runnerId, 0m, 0m, 0m, totalExposure);
        var account = new AccountExposure(accountId, 0, totalExposure, 0m);
        var market = new MarketRiskSummary(0, totalExposure, 0m, subscriberCount);
        return new RiskReadModel([runner], [account], [market], DateTimeOffset.UtcNow);
    }

    public static decimal ToDecimal(object? value)
    {
        return value switch
        {
            null => 0m,
            decimal d => d,
            double d => Convert.ToDecimal(d),
            float f => Convert.ToDecimal(f),
            int i => i,
            long l => l,
            short s => s,
            byte b => b,
            string s when decimal.TryParse(s, out decimal parsed) => parsed,
            _ => Convert.ToDecimal(value)
        };
    }

    public static int ToInt32(object? value)
    {
        return value switch
        {
            int i => i,
            long l => checked((int)l),
            short s => s,
            byte b => b,
            string s when int.TryParse(s, out int parsed) => parsed,
            _ => Convert.ToInt32(value)
        };
    }
}
