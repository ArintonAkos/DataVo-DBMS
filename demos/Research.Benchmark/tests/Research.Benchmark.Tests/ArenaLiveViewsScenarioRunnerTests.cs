using System.Diagnostics;
using Research.Benchmark.Abstractions;
using Research.Benchmark.Runners.ArenaLiveViews;

namespace Research.Benchmark.Tests;

public sealed class ArenaLiveViewsScenarioRunnerTests
{
    [Fact]
    public async Task SevenRepetitions_WriteRawArtifactsAndMedianSummary()
    {
        string output = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));
        try
        {
            ArenaLiveViewsSummary summary = await ArenaLiveViewsScenarioRunner.RunAsync(
                ArenaLiveViewsProfile.Gameplay,
                new[] { () => new FixtureOracleEngine() },
                7,
                output);

            ArenaLiveViewsEngineSummary engine = summary.Engines.Single();
            Assert.Equal(7, engine.Runs.Count);
            Assert.True(File.Exists(Path.Combine(output, "summary.json")));
            Assert.True(File.Exists(Path.Combine(output, "summary.md")));
            Assert.Equal(64, engine.CanonicalHashes.ZoneInventory.Length);
            Assert.Equal(
                engine.Runs.Select(run => run.TotalMilliseconds).OrderBy(milliseconds => milliseconds).ElementAt(3),
                engine.MedianTotalMilliseconds);

            foreach (ArenaLiveViewsRepetition run in engine.Runs)
            {
                string artifactPath = Path.Combine(output, run.ArtifactPath);
                Assert.True(File.Exists(artifactPath));
                Assert.Contains("\"success\": true", await File.ReadAllTextAsync(artifactPath));
            }
        }
        finally
        {
            if (Directory.Exists(output))
            {
                Directory.Delete(output, recursive: true);
            }
        }
    }

    [Fact]
    public async Task Repetition_UsesEngineReportedPerViewTimings()
    {
        string output = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));
        try
        {
            ArenaLiveViewsSummary summary = await ArenaLiveViewsScenarioRunner.RunAsync(
                ArenaLiveViewsProfile.Gameplay,
                new[] { () => new TimedFixtureEngine() },
                1,
                output);

            ArenaLiveViewsRepetition run = summary.Engines.Single().Runs.Single();
            Assert.Equal(1.25d, run.Leaderboard.P50Milliseconds);
            Assert.Equal(2.5d, run.AliveByTeam.P50Milliseconds);
            Assert.Equal(3.75d, run.ZoneInventory.P50Milliseconds);
        }
        finally
        {
            if (Directory.Exists(output))
            {
                Directory.Delete(output, recursive: true);
            }
        }
    }

    [Fact]
    public async Task Runner_SuppressesEngineConsoleOutput()
    {
        string output = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));
        TextWriter originalOut = Console.Out;
        var captured = new StringWriter();
        Console.SetOut(captured);
        try
        {
            await ArenaLiveViewsScenarioRunner.RunAsync(
                ArenaLiveViewsProfile.Gameplay,
                new[] { () => new TimedFixtureEngine(writeToConsole: true) },
                1,
                output);

            Assert.Equal(string.Empty, captured.ToString());
        }
        finally
        {
            Console.SetOut(originalOut);
            if (Directory.Exists(output))
            {
                Directory.Delete(output, recursive: true);
            }
        }
    }

    private sealed class TimedFixtureEngine : IArenaLiveViewsEngine
    {
        private readonly bool _writeToConsole;
        private ArenaLiveViewsProfile? _profile;
        private int _appliedBatchCount;

        public TimedFixtureEngine(bool writeToConsole = false)
        {
            _writeToConsole = writeToConsole;
        }

        public string EngineId => "timed-fixture";

        public EngineArchitecture Architecture => EngineArchitecture.EmbeddedReactiveIvm;

        public ValueTask InitializeAsync(ArenaLiveViewsProfile profile, CancellationToken cancellationToken = default)
        {
            _profile = profile;
            _appliedBatchCount = 0;
            return ValueTask.CompletedTask;
        }

        public ValueTask ApplyBatchAsync(
            IReadOnlyList<ArenaLiveViewsMutation> batch,
            CancellationToken cancellationToken = default)
        {
            _appliedBatchCount++;
            if (_writeToConsole)
            {
                Console.Write("engine-output");
            }

            return ValueTask.CompletedTask;
        }

        public ValueTask<ArenaLiveViewsReadResult> ReadViewsAsync(CancellationToken cancellationToken = default) =>
            ValueTask.FromResult(new ArenaLiveViewsReadResult(
                ArenaLiveViewsFixture.CreateExpectedViews(_profile!, _appliedBatchCount),
                1.25d,
                2.5d,
                3.75d));

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class FixtureOracleEngine : IArenaLiveViewsEngine
    {
        private ArenaLiveViewsProfile? _profile;
        private int _appliedBatchCount;

        public string EngineId => "fixture-oracle";

        public EngineArchitecture Architecture => EngineArchitecture.EmbeddedPollingRecompute;

        public ValueTask InitializeAsync(ArenaLiveViewsProfile profile, CancellationToken cancellationToken = default)
        {
            _profile = profile;
            _appliedBatchCount = 0;
            return ValueTask.CompletedTask;
        }

        public ValueTask ApplyBatchAsync(
            IReadOnlyList<ArenaLiveViewsMutation> batch,
            CancellationToken cancellationToken = default)
        {
            ArenaLiveViewsProfile profile = _profile ?? throw new InvalidOperationException("Fixture engine has not been initialized.");
            IReadOnlyList<ArenaLiveViewsMutation> expected = ArenaLiveViewsFixture.CreateBatch(profile, _appliedBatchCount);
            if (!expected.SequenceEqual(batch))
            {
                throw new InvalidOperationException($"Unexpected fixture batch {_appliedBatchCount}.");
            }

            _appliedBatchCount++;
            return ValueTask.CompletedTask;
        }

        public ValueTask<ArenaLiveViewsReadResult> ReadViewsAsync(CancellationToken cancellationToken = default)
        {
            ArenaLiveViewsProfile profile = _profile ?? throw new InvalidOperationException("Fixture engine has not been initialized.");
            ArenaLiveViewsViews expected = ArenaLiveViewsFixture.CreateExpectedViews(profile, _appliedBatchCount);

            long leaderboardStart = Stopwatch.GetTimestamp();
            ArenaLeaderboardRow[] leaderboard = expected.Leaderboard.ToArray();
            long aliveByTeamStart = Stopwatch.GetTimestamp();
            ArenaAliveByTeamRow[] aliveByTeam = expected.AliveByTeam.ToArray();
            long zoneInventoryStart = Stopwatch.GetTimestamp();
            ArenaZoneInventoryRow[] zoneInventory = expected.ZoneInventory.ToArray();
            long end = Stopwatch.GetTimestamp();

            return ValueTask.FromResult(new ArenaLiveViewsReadResult(
                new ArenaLiveViewsViews(leaderboard, aliveByTeam, zoneInventory),
                ElapsedMilliseconds(leaderboardStart, aliveByTeamStart),
                ElapsedMilliseconds(aliveByTeamStart, zoneInventoryStart),
                ElapsedMilliseconds(zoneInventoryStart, end)));
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        private static double ElapsedMilliseconds(long start, long end) =>
            (end - start) * 1000d / Stopwatch.Frequency;
    }
}
