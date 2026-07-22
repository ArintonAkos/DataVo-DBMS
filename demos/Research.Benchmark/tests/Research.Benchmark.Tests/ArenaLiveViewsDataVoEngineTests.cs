using Research.Benchmark.Abstractions;
using Research.Benchmark.Runners.ArenaLiveViews;

namespace Research.Benchmark.Tests;

public sealed class ArenaLiveViewsDataVoEngineTests
{
    private const int OracleBatchCount = 3;

    public static IEnumerable<object[]> ApprovedProfiles()
    {
        yield return [ArenaLiveViewsProfile.Gameplay];
        yield return [ArenaLiveViewsProfile.IvmStress];
    }

    [Fact]
    public async Task ReactiveAndPolling_ProduceEqualHashesAfterTheSameBatches()
    {
        ArenaLiveViewsProfile profile = ArenaLiveViewsProfile.Gameplay;
        await using IArenaLiveViewsEngine reactive = new DataVoReactiveArenaLiveViewsEngine();
        await using IArenaLiveViewsEngine polling = new DataVoPollingArenaLiveViewsEngine();
        await reactive.InitializeAsync(profile);
        await polling.InitializeAsync(profile);

        for (int batch = 0; batch < 8; batch++)
        {
            IReadOnlyList<ArenaLiveViewsMutation> mutations = ArenaLiveViewsFixture.CreateBatch(profile, batch);
            await reactive.ApplyBatchAsync(mutations);
            await polling.ApplyBatchAsync(mutations);
        }

        Assert.Equal(ArenaLiveViewsHasher.Compute((await reactive.ReadViewsAsync()).Views),
            ArenaLiveViewsHasher.Compute((await polling.ReadViewsAsync()).Views));
    }

    [Fact]
    public async Task ReactiveViews_AreCanonicallyOrderedAndHashEqualToPolling()
    {
        ArenaLiveViewsProfile profile = ArenaLiveViewsProfile.Gameplay;
        await using IArenaLiveViewsEngine reactive = new DataVoReactiveArenaLiveViewsEngine();
        await using IArenaLiveViewsEngine polling = new DataVoPollingArenaLiveViewsEngine();
        await reactive.InitializeAsync(profile);
        await polling.InitializeAsync(profile);

        await reactive.ApplyBatchAsync(ArenaLiveViewsFixture.CreateBatch(profile, 0));
        await polling.ApplyBatchAsync(ArenaLiveViewsFixture.CreateBatch(profile, 0));

        ArenaLiveViewsViews reactiveViews = (await reactive.ReadViewsAsync()).Views;
        ArenaLiveViewsViews pollingViews = (await polling.ReadViewsAsync()).Views;

        Assert.Equal(
            reactiveViews.Leaderboard
                .OrderByDescending(row => row.Score)
                .ThenBy(row => row.Id),
            reactiveViews.Leaderboard);
        Assert.Equal(
            reactiveViews.AliveByTeam.OrderBy(row => row.Team, StringComparer.Ordinal),
            reactiveViews.AliveByTeam);
        Assert.Equal(
            reactiveViews.ZoneInventory
                .OrderBy(row => row.PlayerId)
                .ThenBy(row => row.ItemId),
            reactiveViews.ZoneInventory);
        Assert.Equal(pollingViews.ZoneInventory, reactiveViews.ZoneInventory);
        Assert.Equal(ArenaLiveViewsHasher.Compute(reactiveViews), ArenaLiveViewsHasher.Compute(pollingViews));
    }

    [Theory]
    [MemberData(nameof(ApprovedProfiles))]
    public async Task EachDataVoEngine_MatchesFixtureOracleForApprovedProfiles(ArenaLiveViewsProfile profile)
    {
        ArenaLiveViewsHashes expected = ArenaLiveViewsHasher.Compute(
            ArenaLiveViewsFixture.CreateExpectedViews(profile, OracleBatchCount));

        await AssertMatchesFixtureAsync(new DataVoReactiveArenaLiveViewsEngine(), profile, expected);
        await AssertMatchesFixtureAsync(new DataVoPollingArenaLiveViewsEngine(), profile, expected);
    }

    private static async Task AssertMatchesFixtureAsync(
        IArenaLiveViewsEngine engine,
        ArenaLiveViewsProfile profile,
        ArenaLiveViewsHashes expected)
    {
        await using (engine)
        {
            await engine.InitializeAsync(profile);
            for (int batch = 0; batch < OracleBatchCount; batch++)
            {
                await engine.ApplyBatchAsync(ArenaLiveViewsFixture.CreateBatch(profile, batch));
            }

            Assert.Equal(expected, ArenaLiveViewsHasher.Compute((await engine.ReadViewsAsync()).Views));
        }
    }

    [Fact]
    public async Task ReactiveRead_ReturnsOwnedViewsWithMeasuredPerViewDurations()
    {
        await using IArenaLiveViewsEngine reactive = new DataVoReactiveArenaLiveViewsEngine();
        await reactive.InitializeAsync(ArenaLiveViewsProfile.Gameplay);

        ArenaLiveViewsReadResult first = await reactive.ReadViewsAsync();
        ArenaLiveViewsReadResult second = await reactive.ReadViewsAsync();

        Assert.NotSame(first.Views.Leaderboard, second.Views.Leaderboard);
        Assert.NotSame(first.Views.AliveByTeam, second.Views.AliveByTeam);
        Assert.NotSame(first.Views.ZoneInventory, second.Views.ZoneInventory);
        Assert.True(first.LeaderboardMilliseconds >= 0d);
        Assert.True(first.AliveByTeamMilliseconds >= 0d);
        Assert.True(first.ZoneInventoryMilliseconds >= 0d);
    }
}
