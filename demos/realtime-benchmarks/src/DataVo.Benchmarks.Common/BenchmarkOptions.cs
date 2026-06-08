namespace DataVo.Benchmarks.Common;

public sealed class BenchmarkOptions
{
    public string Mode { get; init; } = "both";
    public string Storage { get; init; } = "in-memory";
    public int Rows { get; init; } = 10_000;
    public int Ticks { get; init; } = 500;
    public int WarmupTicks { get; init; } = 50;
    public int MutationsPerTick { get; init; } = 100;
    public int Seed { get; init; } = 20260620;
    public string? OutputPath { get; init; }

    public static BenchmarkOptions Parse(string[] args, BenchmarkOptions defaults)
    {
        var values = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < args.Length; i++)
        {
            string arg = args[i];
            if (!arg.StartsWith("--", StringComparison.Ordinal))
            {
                continue;
            }

            string key = arg[2..];
            string value = i + 1 < args.Length && !args[i + 1].StartsWith("--", StringComparison.Ordinal)
                ? args[++i]
                : "true";
            values[key] = value;
        }

        return new BenchmarkOptions
        {
            Mode = GetString(values, "mode", defaults.Mode) ?? "both",
            Storage = GetString(values, "storage", defaults.Storage) ?? "in-memory",
            Rows = GetInt(values, "rows", defaults.Rows),
            Ticks = GetInt(values, "ticks", defaults.Ticks),
            WarmupTicks = GetInt(values, "warmup", defaults.WarmupTicks),
            MutationsPerTick = GetInt(values, "mutations", defaults.MutationsPerTick),
            Seed = GetInt(values, "seed", defaults.Seed),
            OutputPath = GetString(values, "out", defaults.OutputPath)
        };
    }

    private static string? GetString(Dictionary<string, string> values, string key, string? fallback) =>
        values.TryGetValue(key, out string? value) ? value : fallback;

    private static int GetInt(Dictionary<string, string> values, string key, int fallback) =>
        values.TryGetValue(key, out string? value) && int.TryParse(value, out int parsed) ? parsed : fallback;
}
