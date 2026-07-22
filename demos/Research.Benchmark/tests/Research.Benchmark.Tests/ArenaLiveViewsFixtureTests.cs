using Research.Benchmark.Abstractions;
using Research.Benchmark.Runners.ArenaLiveViews;

namespace Research.Benchmark.Tests;

public sealed class ArenaLiveViewsFixtureTests
{
    [Fact]
    public void GameplayProfile_HasApprovedRates()
    {
        ArenaLiveViewsProfile profile = ArenaLiveViewsProfile.Gameplay;

        Assert.Equal(
            (1_000, 4, 30, 300, 10),
            (profile.LogicalNpcCount, profile.MutationsPerBatch, profile.WarmupBatches,
                profile.MeasuredBatches, profile.SimulatedCadenceHz));
    }

    [Fact]
    public void IvmStressProfile_HasApprovedRates()
    {
        ArenaLiveViewsProfile profile = ArenaLiveViewsProfile.IvmStress;

        Assert.Equal(
            (10_000, 24, 120, 1_200, 60),
            (profile.LogicalNpcCount, profile.MutationsPerBatch, profile.WarmupBatches,
                profile.MeasuredBatches, profile.SimulatedCadenceHz));
    }

    [Theory]
    [InlineData("gameplay")]
    [InlineData("ivm-stress")]
    public void EveryProfile_DemandsAllThreePublishedViews(string profileId)
    {
        ArenaLiveViewsProfile profile = ArenaLiveViewsProfile.Parse(profileId);

        Assert.NotNull(profile);
        Assert.Equal(3, ArenaLiveViewsSql.All.Count);
    }

    [Fact]
    public void SameSeedAndBatchIndex_ProduceIdenticalMutationsAndHashes()
    {
        ArenaLiveViewsProfile profile = ArenaLiveViewsProfile.Gameplay;
        ArenaLiveViewsMutation[] batch = ArenaLiveViewsFixture.CreateBatch(profile, 17);

        Assert.Equal(batch, ArenaLiveViewsFixture.CreateBatch(profile, 17));
        Assert.Equal(new ArenaLiveViewsMutation(209, 9, 2, "arena-5", 120, 5), batch[0]);
        Assert.Equal(
            64,
            ArenaLiveViewsHasher.Compute(new ArenaLiveViewsViews([], [], [])).Leaderboard.Length);
    }
}
