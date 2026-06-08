using Research.Benchmark.Abstractions;
using Research.Benchmark.Runners.DataVo;
using Research.Benchmark.Runners.DuckDb;
using Research.Benchmark.Runners.Sqlite;

namespace Research.Benchmark.Tests;

public sealed class EmbeddedEngineContractTests
{
    public static IEnumerable<object[]> EmbeddedEngines()
    {
        yield return [new DataVoEngine()];
        yield return [new DuckDbEngine()];
        yield return [new SqliteEngine()];
    }

    [Theory]
    [MemberData(nameof(EmbeddedEngines))]
    public async Task IngestedTickIsVisibleInGroupedRiskReadModel(IBettingRiskEngine engine)
    {
        await using (engine)
        {
            var scenario = new BettingRiskScenario(
                MarketCount: 2,
                RunnersPerMarket: 4,
                AccountCount: 3,
                InitialOrderCount: 0,
                SubscriberCount: 25);

            await engine.InitializeAsync(scenario);

            await engine.IngestTickAsync(new MarketTick(
                Sequence: 1,
                Timestamp: DateTimeOffset.UnixEpoch,
                Kind: TickKind.OrderPlaced,
                MarketId: 1,
                RunnerId: 2,
                AccountId: 3,
                Side: "BACK",
                Price: 123m,
                Stake: 25m));

            RiskReadModel model = await engine.QueryRiskAsync(new RiskQuery());

            RunnerExposure runner = Assert.Single(model.RunnerExposure);
            Assert.Equal(1, runner.MarketId);
            Assert.Equal(2, runner.RunnerId);
            Assert.Equal(123m, runner.BestBack);
            Assert.Equal(123m, runner.BestLay);
            Assert.Equal(25m, runner.OpenExposure);

            AccountExposure account = Assert.Single(model.AccountExposure);
            Assert.Equal(3, account.AccountId);
            Assert.Equal(1, account.MarketId);
            Assert.Equal(25m, account.OpenExposure);

            MarketRiskSummary market = Assert.Single(model.MarketRisk);
            Assert.Equal(1, market.MarketId);
            Assert.Equal(25m, market.TotalOpenExposure);
            Assert.Equal(25, market.ActiveSubscriberCount);
        }
    }

    [Theory]
    [MemberData(nameof(EmbeddedEngines))]
    public async Task InitialOrdersAreVisibleBeforeMeasuredIngestLoop(IBettingRiskEngine engine)
    {
        await using (engine)
        {
            var scenario = new BettingRiskScenario(
                MarketCount: 2,
                RunnersPerMarket: 2,
                AccountCount: 4,
                InitialOrderCount: 8,
                SubscriberCount: 5);

            await engine.InitializeAsync(scenario);

            RiskReadModel model = await engine.QueryRiskAsync(new RiskQuery(TopMarkets: 10));

            Assert.Equal(4, model.RunnerExposure.Count);
            Assert.Equal(116m, model.RunnerExposure.Sum(row => row.OpenExposure));
            Assert.Equal(4, model.AccountExposure.Count);
            Assert.Equal(116m, model.AccountExposure.Sum(row => row.OpenExposure));
            Assert.Equal(2, model.MarketRisk.Count);
            Assert.Equal(116m, model.MarketRisk.Sum(row => row.TotalOpenExposure));
            Assert.All(model.MarketRisk, row => Assert.Equal(5, row.ActiveSubscriberCount));
        }
    }

    [Theory]
    [MemberData(nameof(EmbeddedEngines))]
    public async Task PointRiskQueryReturnsMaintainedRunnerAccountExposure(IBettingRiskEngine engine)
    {
        await using (engine)
        {
            var scenario = new BettingRiskScenario(
                MarketCount: 2,
                RunnersPerMarket: 4,
                AccountCount: 3,
                InitialOrderCount: 0,
                SubscriberCount: 25);

            await engine.InitializeAsync(scenario);

            await engine.IngestTickAsync(new MarketTick(
                Sequence: 1,
                Timestamp: DateTimeOffset.UnixEpoch,
                Kind: TickKind.OrderPlaced,
                MarketId: 1,
                RunnerId: 2,
                AccountId: 3,
                Side: "BACK",
                Price: 123m,
                Stake: 25m));

            await engine.IngestTickAsync(new MarketTick(
                Sequence: 2,
                Timestamp: DateTimeOffset.UnixEpoch,
                Kind: TickKind.OrderPlaced,
                MarketId: 2,
                RunnerId: 2,
                AccountId: 3,
                Side: "LAY",
                Price: 124m,
                Stake: 10m));

            RiskReadModel model = await engine.QueryRiskAsync(new RiskQuery(AccountId: 3, RunnerId: 2));

            RunnerExposure runner = Assert.Single(model.RunnerExposure);
            Assert.Equal(2, runner.RunnerId);
            Assert.Equal(35m, runner.OpenExposure);

            AccountExposure account = Assert.Single(model.AccountExposure);
            Assert.Equal(3, account.AccountId);
            Assert.Equal(35m, account.OpenExposure);

            MarketRiskSummary market = Assert.Single(model.MarketRisk);
            Assert.Equal(35m, market.TotalOpenExposure);
        }
    }

    [Fact]
    public async Task DataVoDefaultRiskQueryReturnsMaintainedSnapshotWithoutRebuilding()
    {
        await using var engine = new DataVoEngine();
        var scenario = new BettingRiskScenario(
            MarketCount: 2,
            RunnersPerMarket: 2,
            AccountCount: 4,
            InitialOrderCount: 8,
            SubscriberCount: 5);

        await engine.InitializeAsync(scenario);

        RiskReadModel first = await engine.QueryRiskAsync(new RiskQuery());
        RiskReadModel second = await engine.QueryRiskAsync(new RiskQuery());

        Assert.Same(first, second);
    }

    [Fact]
    public async Task DataVoIngestUsesStructuredInsertWithoutSqlParserOutput()
    {
        await using var engine = new DataVoEngine();
        var scenario = new BettingRiskScenario(
            MarketCount: 2,
            RunnersPerMarket: 2,
            AccountCount: 4,
            InitialOrderCount: 0,
            SubscriberCount: 5);

        await engine.InitializeAsync(scenario);

        TextWriter originalOut = Console.Out;
        using var captured = new StringWriter();
        Console.SetOut(captured);

        try
        {
            await engine.IngestTickAsync(new MarketTick(
                Sequence: 1,
                Timestamp: DateTimeOffset.UnixEpoch,
                Kind: TickKind.OrderPlaced,
                MarketId: 1,
                RunnerId: 1,
                AccountId: 1,
                Side: "BACK",
                Price: 101m,
                Stake: 10m));
        }
        finally
        {
            Console.SetOut(originalOut);
        }

        Assert.DoesNotContain("Parser", captured.ToString());
        Assert.DoesNotContain("Rows", captured.ToString());
    }
}
