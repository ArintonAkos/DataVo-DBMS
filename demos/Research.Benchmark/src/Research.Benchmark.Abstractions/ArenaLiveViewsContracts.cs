using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace Research.Benchmark.Abstractions;

public sealed record ArenaLiveViewsProfile(
    string Id,
    int Seed,
    int LogicalNpcCount,
    int MutationsPerBatch,
    int WarmupBatches,
    int MeasuredBatches,
    int SimulatedCadenceHz)
{
    public static ArenaLiveViewsProfile Gameplay { get; } =
        new("gameplay", 20260721, 1_000, 4, 30, 300, 10);

    public static ArenaLiveViewsProfile IvmStress { get; } =
        new("ivm-stress", 20260721, 10_000, 24, 120, 1_200, 60);

    public static ArenaLiveViewsProfile Parse(string id) => id switch
    {
        "gameplay" => Gameplay,
        "ivm-stress" => IvmStress,
        _ => throw new ArgumentException("Profile must be gameplay or ivm-stress.", nameof(id))
    };
}

public sealed record ArenaLiveViewsMutation(
    int PlayerId,
    int ScoreDelta,
    int ReplacementHealth,
    string ReplacementZone,
    int InventoryItemId,
    int InventorySlot);

public sealed record ArenaLiveViewsPlayer(int Id, string Team, string Zone, int Health, int Score);

public sealed record ArenaLiveViewsInventory(int InventoryId, int PlayerId, int ItemId, int Slot);

public sealed record ArenaLeaderboardRow(int Id, int Score);

public sealed record ArenaAliveByTeamRow(string Team, int Alive);

public sealed record ArenaZoneInventoryRow(int PlayerId, int ItemId);

public sealed record ArenaLiveViewsViews(
    IReadOnlyList<ArenaLeaderboardRow> Leaderboard,
    IReadOnlyList<ArenaAliveByTeamRow> AliveByTeam,
    IReadOnlyList<ArenaZoneInventoryRow> ZoneInventory);

public sealed record ArenaLiveViewsHashes(string Leaderboard, string AliveByTeam, string ZoneInventory);

public sealed record ArenaLiveViewsPercentiles(
    int Count,
    double P50Milliseconds,
    double P95Milliseconds,
    double P99Milliseconds,
    double MaxMilliseconds,
    double MeanMilliseconds);

public sealed record ArenaLiveViewsRepetition(
    string EngineId,
    int Repetition,
    double TotalMilliseconds,
    ArenaLiveViewsPercentiles Batch,
    ArenaLiveViewsPercentiles Leaderboard,
    ArenaLiveViewsPercentiles AliveByTeam,
    ArenaLiveViewsPercentiles ZoneInventory,
    long WritesApplied,
    long ReactiveChanges,
    long PollingRows,
    ArenaLiveViewsHashes Hashes,
    string ArtifactPath);

public sealed record ArenaLiveViewsEngineSummary(
    string EngineId,
    EngineArchitecture Architecture,
    IReadOnlyList<ArenaLiveViewsRepetition> Runs,
    ArenaLiveViewsHashes CanonicalHashes,
    double MedianTotalMilliseconds);

public sealed record ArenaLiveViewsSummary(
    string SchemaRevision,
    ArenaLiveViewsProfile Profile,
    IReadOnlyList<ArenaLiveViewsEngineSummary> Engines);

public interface IArenaLiveViewsEngine : IAsyncDisposable
{
    string EngineId { get; }

    EngineArchitecture Architecture { get; }

    ValueTask InitializeAsync(ArenaLiveViewsProfile profile, CancellationToken cancellationToken = default);

    ValueTask ApplyBatchAsync(
        IReadOnlyList<ArenaLiveViewsMutation> batch,
        CancellationToken cancellationToken = default);

    ValueTask<ArenaLiveViewsViews> ReadViewsAsync(CancellationToken cancellationToken = default);
}

public static class ArenaLiveViewsHasher
{
    public static ArenaLiveViewsHashes Compute(ArenaLiveViewsViews views)
    {
        ArgumentNullException.ThrowIfNull(views);
        ArgumentNullException.ThrowIfNull(views.Leaderboard);
        ArgumentNullException.ThrowIfNull(views.AliveByTeam);
        ArgumentNullException.ThrowIfNull(views.ZoneInventory);

        return new ArenaLiveViewsHashes(
            Hash(views.Leaderboard.Select(row => string.Concat(
                row.Id.ToString(CultureInfo.InvariantCulture), "|",
                row.Score.ToString(CultureInfo.InvariantCulture), "\n"))),
            Hash(views.AliveByTeam.Select(row => string.Concat(
                row.Team, "|", row.Alive.ToString(CultureInfo.InvariantCulture), "\n"))),
            Hash(views.ZoneInventory.Select(row => string.Concat(
                row.PlayerId.ToString(CultureInfo.InvariantCulture), "|",
                row.ItemId.ToString(CultureInfo.InvariantCulture), "\n"))));
    }

    private static string Hash(IEnumerable<string> lines)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(string.Concat(lines));
        return Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant();
    }
}
