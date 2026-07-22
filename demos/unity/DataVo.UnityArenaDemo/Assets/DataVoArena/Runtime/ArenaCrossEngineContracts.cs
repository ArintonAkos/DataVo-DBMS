#nullable enable

using System;
using System.Collections.Generic;

namespace DataVo.UnityArena
{
    public enum ArenaCrossEngineKind
    {
        DataVoReactive,
        DataVoPolling,
        SqlitePolling
    }

    public sealed record ArenaCrossEngineProfile(
        string id,
        int seed,
        int logicalNpcCount,
        int mutationsPerBatch,
        int warmupBatches,
        int measuredBatches,
        int simulatedCadenceHz)
    {
        public static ArenaCrossEngineProfile Gameplay { get; } =
            new("gameplay", 20260721, 1_000, 4, 30, 300, 10);

        public static ArenaCrossEngineProfile IvmStress { get; } =
            new("ivm-stress", 20260721, 10_000, 24, 120, 1_200, 60);

        public static ArenaCrossEngineProfile Parse(string id) => id switch
        {
            "gameplay" => Gameplay,
            "ivm-stress" => IvmStress,
            _ => throw new ArgumentException("Profile must be gameplay or ivm-stress.", nameof(id))
        };
    }

    public sealed record ArenaCrossEngineMutation(
        int playerId,
        int scoreDelta,
        int replacementHealth,
        string replacementZone,
        int inventoryItemId,
        int inventorySlot);

    public sealed record ArenaCrossEnginePlayer(int id, string team, string zone, int health, int score);

    public sealed record ArenaCrossEngineInventory(int inventoryId, int playerId, int itemId, int slot);

    public sealed record ArenaCrossEngineLeaderboardRow(int id, int score);

    public sealed record ArenaCrossEngineAggregateRow(string team, int alive);

    public sealed record ArenaCrossEngineZoneInventoryRow(int playerId, int itemId);

    public sealed record ArenaCrossEngineViews(
        IReadOnlyList<ArenaCrossEngineLeaderboardRow> leaderboard,
        IReadOnlyList<ArenaCrossEngineAggregateRow> aggregate,
        IReadOnlyList<ArenaCrossEngineZoneInventoryRow> zoneInventory);

    public sealed record ArenaCrossEngineHashes(string Leaderboard, string Aggregate, string ZoneInventory);

    public sealed record ArenaCrossEngineReadResult(
        ArenaCrossEngineViews views,
        double leaderboardMilliseconds,
        double aggregateMilliseconds,
        double zoneInventoryMilliseconds);

    [Serializable]
    public sealed class ArenaCrossEngineLatencySummary
    {
        public int count;
        public double p50Milliseconds;
        public double p95Milliseconds;
        public double p99Milliseconds;
        public double maxMilliseconds;
        public double meanMilliseconds;
    }

    [Serializable]
    public sealed class ArenaCrossEngineRepetition
    {
        public int index;
        public double totalMilliseconds;
        public long writesApplied;
        public long reactiveChanges;
        public long pollingRows;
        public ArenaCrossEngineLatencySummary batch = new();
        public ArenaCrossEngineLatencySummary leaderboard = new();
        public ArenaCrossEngineLatencySummary aggregate = new();
        public ArenaCrossEngineLatencySummary zoneInventory = new();
    }

    [Serializable]
    public sealed class ArenaCrossEngineResult
    {
        public int schemaVersion = 1;
        public string schemaRevision = string.Empty;
        public string status = string.Empty;
        public string engine = string.Empty;
        public string profile = string.Empty;
        public int seed;
        public int measuredBatches;
        public ArenaCrossEngineRepetition[] repetitions = Array.Empty<ArenaCrossEngineRepetition>();
        public string leaderboardChecksum = string.Empty;
        public string aggregateChecksum = string.Empty;
        public string zoneInventoryChecksum = string.Empty;
        public string unityVersion = string.Empty;
        public string platform = string.Empty;
        public string scriptingBackend = string.Empty;
        public string sqliteVersion = string.Empty;
        public string error = string.Empty;
    }
}
