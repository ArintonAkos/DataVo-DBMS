using System.Text.Json;
using System.Text.Json.Serialization;

namespace DataVo.Benchmarks.Common;

public sealed record ScenarioRunResult(
    string Scenario,
    string Architecture,
    string Storage,
    int Rows,
    int Ticks,
    int WarmupTicks,
    int MutationsPerTick,
    int Seed,
    DateTimeOffset StartedAtUtc,
    TimeSpan Duration,
    LatencySummary TickLatency,
    LatencySummary MutationLatency,
    LatencySummary ViewMaintenanceLatency,
    GcSummary Gc,
    DeltaSummary? ReactiveDeltas,
    long PollingRowsReturned,
    double FrameBudgetMissRate60Hz,
    double FrameBudgetMissRate120Hz,
    IReadOnlyDictionary<string, string> Notes)
{
    public string ToJson()
    {
        var options = new JsonSerializerOptions
        {
            WriteIndented = true,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
        };

        return JsonSerializer.Serialize(this, options);
    }
}
