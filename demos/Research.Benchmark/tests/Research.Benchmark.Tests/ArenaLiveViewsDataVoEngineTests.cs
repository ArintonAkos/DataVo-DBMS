using Research.Benchmark.Abstractions;
using Research.Benchmark.Runners.ArenaLiveViews;

namespace Research.Benchmark.Tests;

public sealed class ArenaLiveViewsDataVoEngineTests
{
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

        Assert.Equal(ArenaLiveViewsHasher.Compute(await reactive.ReadViewsAsync()),
            ArenaLiveViewsHasher.Compute(await polling.ReadViewsAsync()));
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

        ArenaLiveViewsViews reactiveViews = await reactive.ReadViewsAsync();
        ArenaLiveViewsViews pollingViews = await polling.ReadViewsAsync();

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
}
