using Research.Benchmark.Abstractions;
using Research.Benchmark.Runners.ArenaLiveViews;

namespace Research.Benchmark.Tests;

public sealed class ArenaLiveViewsPollingEngineTests
{
    public static IEnumerable<object[]> PollingEngines()
    {
        yield return [new SqliteArenaLiveViewsEngine()];
        yield return [new DuckDbArenaLiveViewsEngine()];
    }

    [Theory]
    [MemberData(nameof(PollingEngines))]
    public async Task PollingEngine_MatchesDataVoReactiveHashes(IArenaLiveViewsEngine engine)
    {
        await using IArenaLiveViewsEngine baseline = new DataVoReactiveArenaLiveViewsEngine();
        await using (engine)
        {
            ArenaLiveViewsProfile profile = ArenaLiveViewsProfile.Gameplay;
            await baseline.InitializeAsync(profile);
            await engine.InitializeAsync(profile);
            IReadOnlyList<ArenaLiveViewsMutation> batch = ArenaLiveViewsFixture.CreateBatch(profile, 4);
            await baseline.ApplyBatchAsync(batch);
            await engine.ApplyBatchAsync(batch);

            Assert.Equal(
                ArenaLiveViewsHasher.Compute((await baseline.ReadViewsAsync()).Views),
                ArenaLiveViewsHasher.Compute((await engine.ReadViewsAsync()).Views));
        }
    }

    [Theory]
    [MemberData(nameof(PollingEngines))]
    public async Task PollingRead_ReportsIndependentPerViewDurations(IArenaLiveViewsEngine engine)
    {
        await using (engine)
        {
            await engine.InitializeAsync(ArenaLiveViewsProfile.Gameplay);

            ArenaLiveViewsReadResult read = await engine.ReadViewsAsync();

            Assert.NotEmpty(read.Views.Leaderboard);
            Assert.True(read.LeaderboardMilliseconds >= 0d);
            Assert.True(read.AliveByTeamMilliseconds >= 0d);
            Assert.True(read.ZoneInventoryMilliseconds >= 0d);
        }
    }
}
