using System.Globalization;
using Research.Benchmark.Abstractions;
using Research.Benchmark.Runners.ArenaLiveViews;

namespace Research.Benchmark.Host;

public enum ArenaLiveViewsOutputFormat
{
    Markdown,
    Json
}

public sealed record ArenaLiveViewsCommandOptions(
    ArenaLiveViewsProfile Profile,
    string Engine,
    int Repetitions,
    string OutputDirectory,
    ArenaLiveViewsOutputFormat Format)
{
    public IReadOnlyList<Func<IArenaLiveViewsEngine>> CreateEngineFactories() => Engine switch
    {
        "all" =>
        [
            static () => new DataVoReactiveArenaLiveViewsEngine(),
            static () => new DataVoPollingArenaLiveViewsEngine(),
            static () => new SqliteArenaLiveViewsEngine(),
            static () => new DuckDbArenaLiveViewsEngine()
        ],
        "datavo-reactive" => [static () => new DataVoReactiveArenaLiveViewsEngine()],
        "datavo-polling" => [static () => new DataVoPollingArenaLiveViewsEngine()],
        "sqlite" => [static () => new SqliteArenaLiveViewsEngine()],
        "duckdb" => [static () => new DuckDbArenaLiveViewsEngine()],
        _ => throw new InvalidOperationException($"Unsupported arena live-views engine '{Engine}'.")
    };
}

public static class ArenaLiveViewsCommand
{
    private const int DefaultRepetitions = 7;

    public static ArenaLiveViewsCommandOptions Parse(string[] args)
    {
        ArgumentNullException.ThrowIfNull(args);

        var values = new Dictionary<string, string>(StringComparer.Ordinal);
        for (int index = 0; index < args.Length; index++)
        {
            string option = args[index];
            if (option is not "--profile" and not "--engine" and not "--repetitions" and not "--output" and not "--format")
            {
                throw new ArgumentException($"Unknown arena-live-views option '{option}'.", nameof(args));
            }

            if (index + 1 >= args.Length || args[index + 1].StartsWith("--", StringComparison.Ordinal))
            {
                throw new ArgumentException($"Arena-live-views option '{option}' requires a value.", nameof(args));
            }

            if (!values.TryAdd(option, args[++index]))
            {
                throw new ArgumentException($"Arena-live-views option '{option}' may be specified only once.", nameof(args));
            }
        }

        if (!values.TryGetValue("--profile", out string? profileId))
        {
            throw new ArgumentException("Arena-live-views requires --profile gameplay or ivm-stress.", nameof(args));
        }

        ArenaLiveViewsProfile profile = ArenaLiveViewsProfile.Parse(profileId);
        string engine = values.TryGetValue("--engine", out string? requestedEngine) ? requestedEngine : "all";
        if (engine is not "all" and not "datavo-reactive" and not "datavo-polling" and not "sqlite" and not "duckdb")
        {
            throw new ArgumentException(
                "Arena-live-views engine must be all, datavo-reactive, datavo-polling, sqlite, or duckdb.",
                nameof(args));
        }

        int repetitions = DefaultRepetitions;
        if (values.TryGetValue("--repetitions", out string? repetitionsText)
            && (!int.TryParse(repetitionsText, NumberStyles.None, CultureInfo.InvariantCulture, out repetitions) || repetitions <= 0))
        {
            throw new ArgumentException("Arena-live-views repetitions must be a positive integer.", nameof(args));
        }

        ArenaLiveViewsOutputFormat format = values.TryGetValue("--format", out string? formatText) ? formatText switch
        {
            "markdown" => ArenaLiveViewsOutputFormat.Markdown,
            "json" => ArenaLiveViewsOutputFormat.Json,
            _ => throw new ArgumentException("Arena-live-views format must be markdown or json.", nameof(args))
        } : ArenaLiveViewsOutputFormat.Markdown;

        string output = values.TryGetValue("--output", out string? outputText)
            ? outputText
            : Path.Combine(
                "artifacts",
                "arena-live-views",
                $"{profile.Id}-{DateTimeOffset.UtcNow:yyyyMMddTHHmmssfffZ}");
        if (string.IsNullOrWhiteSpace(output))
        {
            throw new ArgumentException("Arena-live-views output must be a non-empty path.", nameof(args));
        }

        return new ArenaLiveViewsCommandOptions(
            profile,
            engine,
            repetitions,
            Path.GetFullPath(output),
            format);
    }
}
