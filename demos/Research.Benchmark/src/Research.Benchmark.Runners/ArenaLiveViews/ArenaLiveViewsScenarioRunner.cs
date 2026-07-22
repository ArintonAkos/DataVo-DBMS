using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Research.Benchmark.Abstractions;

namespace Research.Benchmark.Runners.ArenaLiveViews;

public static class ArenaLiveViewsScenarioRunner
{
    public const string SchemaQueryRevision = "arena-live-views-v1";

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = true,
        Converters = { new JsonStringEnumConverter() }
    };

    public static async Task<ArenaLiveViewsSummary> RunAsync(
        ArenaLiveViewsProfile profile,
        IEnumerable<Func<IArenaLiveViewsEngine>> engineFactories,
        int repetitions,
        string outputDirectory,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(profile);
        ArgumentNullException.ThrowIfNull(engineFactories);
        ArgumentException.ThrowIfNullOrWhiteSpace(outputDirectory);
        if (repetitions <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(repetitions), "Repetitions must be positive.");
        }

        ValidateProfile(profile);
        Func<IArenaLiveViewsEngine>[] factories = engineFactories.ToArray();
        if (factories.Length == 0)
        {
            throw new ArgumentException("At least one arena live-views engine must be requested.", nameof(engineFactories));
        }

        TextWriter originalOut = Console.Out;
        Console.SetOut(TextWriter.Null);
        try
        {
            Directory.CreateDirectory(outputDirectory);
            await VerifyDataVoReactiveProbeAsync(profile, cancellationToken);

            var summaries = new List<ArenaLiveViewsEngineSummary>(factories.Length);
            var engineIds = new HashSet<string>(StringComparer.Ordinal);

            foreach (Func<IArenaLiveViewsEngine> factory in factories)
            {
                ArgumentNullException.ThrowIfNull(factory);
                cancellationToken.ThrowIfCancellationRequested();

                IArenaLiveViewsEngine first = factory()
                    ?? throw new InvalidOperationException("Arena live-views engine factory returned null.");
                string engineId = first.EngineId;
                EngineArchitecture architecture = first.Architecture;
                await first.DisposeAsync();

                if (string.IsNullOrWhiteSpace(engineId))
                {
                    throw new ArgumentException("Arena live-views engines must expose a non-empty engine ID.", nameof(engineFactories));
                }

                if (!engineIds.Add(engineId))
                {
                    throw new ArgumentException($"Arena live-views engine '{engineId}' was requested more than once.", nameof(engineFactories));
                }

                var runs = new List<ArenaLiveViewsRepetition>(repetitions);
                for (int repetition = 1; repetition <= repetitions; repetition++)
                {
                    ArenaLiveViewsRepetition run = await RunRepetitionAsync(
                        profile,
                        factory,
                        engineId,
                        architecture,
                        repetition,
                        outputDirectory,
                        cancellationToken);
                    runs.Add(run);
                }

                ArenaLiveViewsHashes canonicalHashes = runs[^1].Hashes;
                summaries.Add(new ArenaLiveViewsEngineSummary(
                    engineId,
                    architecture,
                    runs,
                    canonicalHashes,
                    Median(runs.Select(run => run.TotalMilliseconds))));
            }

            var summary = new ArenaLiveViewsSummary(SchemaQueryRevision, profile, summaries);
            await WriteTextFileAsync(Path.Combine(outputDirectory, "summary.json"), FormatJson(summary), cancellationToken);
            await WriteTextFileAsync(Path.Combine(outputDirectory, "summary.md"), FormatMarkdown(summary), cancellationToken);
            return summary;
        }
        finally
        {
            Console.SetOut(originalOut);
        }
    }

    public static string FormatJson(ArenaLiveViewsSummary summary)
    {
        ArgumentNullException.ThrowIfNull(summary);
        return JsonSerializer.Serialize(summary, JsonOptions);
    }

    public static string FormatMarkdown(ArenaLiveViewsSummary summary)
    {
        ArgumentNullException.ThrowIfNull(summary);

        var builder = new StringBuilder();
        builder.AppendLine("# Arena live-views benchmark");
        builder.AppendLine();
        builder.Append("Schema/query revision: `").Append(summary.SchemaRevision).AppendLine("`");
        builder.Append("Profile: `").Append(summary.Profile.Id).Append("` (seed ")
            .Append(summary.Profile.Seed.ToString(CultureInfo.InvariantCulture)).AppendLine(")");
        builder.AppendLine();
        builder.AppendLine("| Engine | Architecture | Repetitions | Median total (ms) |");
        builder.AppendLine("|---|---|---:|---:|");

        foreach (ArenaLiveViewsEngineSummary engine in summary.Engines)
        {
            builder.Append("| ").Append(engine.EngineId)
                .Append(" | ").Append(engine.Architecture)
                .Append(" | ").Append(engine.Runs.Count.ToString(CultureInfo.InvariantCulture))
                .Append(" | ").Append(engine.MedianTotalMilliseconds.ToString("F3", CultureInfo.InvariantCulture))
                .AppendLine(" |");
        }

        foreach (ArenaLiveViewsEngineSummary engine in summary.Engines)
        {
            builder.AppendLine();
            builder.Append("## ").AppendLine(engine.EngineId);
            builder.AppendLine();
            builder.AppendLine("| Repetition | Total (ms) | Artifact |");
            builder.AppendLine("|---:|---:|---|");
            foreach (ArenaLiveViewsRepetition run in engine.Runs)
            {
                builder.Append("| ").Append(run.Repetition.ToString(CultureInfo.InvariantCulture))
                    .Append(" | ").Append(run.TotalMilliseconds.ToString("F3", CultureInfo.InvariantCulture))
                    .Append(" | ").Append(run.ArtifactPath.Replace(Path.DirectorySeparatorChar, '/'))
                    .AppendLine(" |");
            }
        }

        return builder.ToString();
    }

    private static async Task<ArenaLiveViewsRepetition> RunRepetitionAsync(
        ArenaLiveViewsProfile profile,
        Func<IArenaLiveViewsEngine> factory,
        string expectedEngineId,
        EngineArchitecture expectedArchitecture,
        int repetition,
        string outputDirectory,
        CancellationToken cancellationToken)
    {
        IArenaLiveViewsEngine engine = factory()
            ?? throw new InvalidOperationException("Arena live-views engine factory returned null.");
        await using (engine)
        {
            if (!string.Equals(engine.EngineId, expectedEngineId, StringComparison.Ordinal)
                || engine.Architecture != expectedArchitecture)
            {
                throw new InvalidOperationException("Arena live-views engine factory changed its identity between repetitions.");
            }

            await engine.InitializeAsync(profile, cancellationToken);
            for (int warmupIndex = 0; warmupIndex < profile.WarmupBatches; warmupIndex++)
            {
                await engine.ApplyBatchAsync(ArenaLiveViewsFixture.CreateBatch(profile, warmupIndex), cancellationToken);
            }

            ArenaLiveViewsViews previousViews = (await engine.ReadViewsAsync(cancellationToken)).Views;
            ForceGcBeforeMeasuredInterval();

            var batchSamples = new List<double>(profile.MeasuredBatches);
            var leaderboardSamples = new List<double>(profile.MeasuredBatches);
            var aliveByTeamSamples = new List<double>(profile.MeasuredBatches);
            var zoneInventorySamples = new List<double>(profile.MeasuredBatches);
            long writesApplied = 0;
            long reactiveChanges = 0;
            long pollingRows = 0;
            ArenaLiveViewsHashes? finalHashes = null;

            for (int measuredIndex = 0; measuredIndex < profile.MeasuredBatches; measuredIndex++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                int batchIndex = profile.WarmupBatches + measuredIndex;
                long batchStart = Stopwatch.GetTimestamp();
                await engine.ApplyBatchAsync(ArenaLiveViewsFixture.CreateBatch(profile, batchIndex), cancellationToken);
                ArenaLiveViewsReadResult read = await engine.ReadViewsAsync(cancellationToken);
                long batchEnd = Stopwatch.GetTimestamp();
                ArenaLiveViewsViews views = read.Views;

                batchSamples.Add(ElapsedMilliseconds(batchStart, batchEnd));
                leaderboardSamples.Add(read.LeaderboardMilliseconds);
                aliveByTeamSamples.Add(read.AliveByTeamMilliseconds);
                zoneInventorySamples.Add(read.ZoneInventoryMilliseconds);

                ArenaLiveViewsHashes expected = ArenaLiveViewsHasher.Compute(
                    ArenaLiveViewsFixture.CreateExpectedViews(profile, batchIndex + 1));
                ArenaLiveViewsHashes actual = ArenaLiveViewsHasher.Compute(views);
                AssertHashes(expected, actual, engine.EngineId, batchIndex);
                finalHashes = actual;

                writesApplied += profile.MutationsPerBatch;
                if (engine.Architecture == EngineArchitecture.EmbeddedReactiveIvm)
                {
                    reactiveChanges += CountViewChanges(previousViews, views);
                }
                else
                {
                    pollingRows += views.Leaderboard.Count + views.AliveByTeam.Count + views.ZoneInventory.Count;
                }

                previousViews = views;
            }

            ArenaLiveViewsRepetition run = new(
                expectedEngineId,
                repetition,
                batchSamples.Sum(),
                CalculatePercentiles(batchSamples),
                CalculatePercentiles(leaderboardSamples),
                CalculatePercentiles(aliveByTeamSamples),
                CalculatePercentiles(zoneInventorySamples),
                writesApplied,
                reactiveChanges,
                pollingRows,
                finalHashes ?? throw new InvalidOperationException("No measured arena batches were executed."),
                Path.Combine(expectedEngineId, $"repetition-{repetition}.json"));

            await WriteArtifactAsync(outputDirectory, profile, engine.Architecture, run, cancellationToken);
            return run;
        }
    }

    private static async Task VerifyDataVoReactiveProbeAsync(
        ArenaLiveViewsProfile profile,
        CancellationToken cancellationToken)
    {
        await using IArenaLiveViewsEngine reactive = new DataVoReactiveArenaLiveViewsEngine();
        await reactive.InitializeAsync(profile, cancellationToken);
        await reactive.ApplyBatchAsync(ArenaLiveViewsFixture.CreateBatch(profile, 0), cancellationToken);

        ArenaLiveViewsHashes expected = ArenaLiveViewsHasher.Compute(ArenaLiveViewsFixture.CreateExpectedViews(profile, 1));
        ArenaLiveViewsHashes actual = ArenaLiveViewsHasher.Compute((await reactive.ReadViewsAsync(cancellationToken)).Views);
        AssertHashes(expected, actual, reactive.EngineId, 0);
    }

    private static async Task WriteArtifactAsync(
        string outputDirectory,
        ArenaLiveViewsProfile profile,
        EngineArchitecture architecture,
        ArenaLiveViewsRepetition run,
        CancellationToken cancellationToken)
    {
        var artifact = new ArenaLiveViewsArtifact(
            profile.Seed,
            profile,
            run.EngineId,
            architecture,
            SchemaQueryRevision,
            run.TotalMilliseconds,
            run.Batch,
            run.Leaderboard,
            run.AliveByTeam,
            run.ZoneInventory,
            run.WritesApplied,
            run.ReactiveChanges,
            run.PollingRows,
            run.Hashes,
            RuntimeInformation.OSDescription,
            $"{RuntimeInformation.ProcessArchitecture}; {Environment.ProcessorCount.ToString(CultureInfo.InvariantCulture)} logical processors",
            RuntimeInformation.FrameworkDescription,
            Success: true);

        string path = Path.Combine(outputDirectory, run.ArtifactPath);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        await WriteTextFileAsync(path, JsonSerializer.Serialize(artifact, JsonOptions), cancellationToken);
    }

    private static async Task WriteTextFileAsync(string path, string contents, CancellationToken cancellationToken) =>
        await File.WriteAllTextAsync(path, contents, Encoding.UTF8, cancellationToken);

    private static void ValidateProfile(ArenaLiveViewsProfile profile)
    {
        ArenaLiveViewsProfile parsed = ArenaLiveViewsProfile.Parse(profile.Id);
        if (profile != parsed)
        {
            throw new ArgumentException("Profile must exactly match the gameplay or ivm-stress definition.", nameof(profile));
        }
    }

    private static void ForceGcBeforeMeasuredInterval()
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
    }

    private static void AssertHashes(
        ArenaLiveViewsHashes expected,
        ArenaLiveViewsHashes actual,
        string engineId,
        int batchIndex)
    {
        if (expected != actual)
        {
            throw new InvalidOperationException(
                $"Arena live-views hash mismatch for engine '{engineId}' after batch {batchIndex.ToString(CultureInfo.InvariantCulture)}. " +
                $"Expected [{expected.Leaderboard}, {expected.AliveByTeam}, {expected.ZoneInventory}], " +
                $"actual [{actual.Leaderboard}, {actual.AliveByTeam}, {actual.ZoneInventory}].");
        }
    }

    private static long CountViewChanges(ArenaLiveViewsViews previous, ArenaLiveViewsViews current) =>
        CountChangedRows(previous.Leaderboard, current.Leaderboard, row => row.Id)
        + CountChangedRows(previous.AliveByTeam, current.AliveByTeam, row => row.Team)
        + CountChangedRows(previous.ZoneInventory, current.ZoneInventory, row => (row.PlayerId, row.ItemId));

    private static long CountChangedRows<TRow, TKey>(
        IReadOnlyList<TRow> previous,
        IReadOnlyList<TRow> current,
        Func<TRow, TKey> keySelector)
        where TRow : notnull
        where TKey : notnull
    {
        var before = previous.ToDictionary(keySelector);
        var after = current.ToDictionary(keySelector);
        long changes = 0;

        foreach ((TKey key, TRow row) in before)
        {
            if (!after.TryGetValue(key, out TRow? updated))
            {
                changes++;
            }
            else if (!EqualityComparer<TRow>.Default.Equals(row, updated))
            {
                changes++;
            }
        }

        foreach (TKey key in after.Keys)
        {
            if (!before.ContainsKey(key))
            {
                changes++;
            }
        }

        return changes;
    }

    private static ArenaLiveViewsPercentiles CalculatePercentiles(IReadOnlyList<double> samples)
    {
        if (samples.Count == 0)
        {
            return new ArenaLiveViewsPercentiles(0, 0d, 0d, 0d, 0d, 0d);
        }

        double[] sorted = samples.ToArray();
        Array.Sort(sorted);
        return new ArenaLiveViewsPercentiles(
            sorted.Length,
            NearestRank(sorted, 0.50d),
            NearestRank(sorted, 0.95d),
            NearestRank(sorted, 0.99d),
            sorted[^1],
            sorted.Average());
    }

    private static double Median(IEnumerable<double> samples)
    {
        double[] sorted = samples.OrderBy(value => value).ToArray();
        if (sorted.Length == 0)
        {
            return 0d;
        }

        return sorted[(sorted.Length - 1) / 2];
    }

    private static double NearestRank(double[] sorted, double percentile)
    {
        int rank = (int)Math.Ceiling(percentile * sorted.Length);
        return sorted[Math.Clamp(rank - 1, 0, sorted.Length - 1)];
    }

    private static double ElapsedMilliseconds(long start, long end) =>
        (end - start) * 1000d / Stopwatch.Frequency;

    private sealed record ArenaLiveViewsArtifact(
        int Seed,
        ArenaLiveViewsProfile Profile,
        string Engine,
        EngineArchitecture Architecture,
        string SchemaQueryRevision,
        double TotalMilliseconds,
        ArenaLiveViewsPercentiles Batch,
        ArenaLiveViewsPercentiles Leaderboard,
        ArenaLiveViewsPercentiles AliveByTeam,
        ArenaLiveViewsPercentiles ZoneInventory,
        long WritesApplied,
        long ReactiveChanges,
        long PollingRows,
        ArenaLiveViewsHashes Hashes,
        string OperatingSystem,
        string Cpu,
        string DotNetVersion,
        bool Success);
}
