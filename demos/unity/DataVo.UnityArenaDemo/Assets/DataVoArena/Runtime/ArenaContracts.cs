#nullable enable

// Unity's netstandard2.1 profile supplies the C# 9 compiler but not this marker
// type. Keeping it local lets the arena use immutable records without changing
// the portable DataVo package or relying on a newer Unity BCL.
namespace System.Runtime.CompilerServices
{
    internal static class IsExternalInit
    {
    }
}

namespace DataVo.UnityArena
{
    using System;
    using System.Collections.Generic;
    using UnityEngine;

    public enum ArenaDeliveryMode
    {
        Reactive,
        Polling
    }

    public sealed record ArenaRunOptions
    {
        public int Seed { get; init; } = 20260721;
        public int LogicalNpcCount { get; init; } = 1_000;
        public int VisibleNpcCount { get; init; } = 48;
        public int MemoriesPerNpc { get; init; } = 8;
        public int WarmupTicks { get; init; } = 30;
        public int MeasuredTicks { get; init; } = 300;
        public int MutationsPerTick { get; init; } = 24;
        public int MemorySearchEveryTicks { get; init; } = 12;
        public ArenaDeliveryMode Mode { get; init; }

        public void Validate()
        {
            if (LogicalNpcCount <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(LogicalNpcCount), "Logical NPC count must be positive.");
            }

            if (VisibleNpcCount <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(VisibleNpcCount), "Visible NPC count must be positive.");
            }

            if (VisibleNpcCount > LogicalNpcCount)
            {
                throw new ArgumentOutOfRangeException(nameof(VisibleNpcCount), "Visible NPC count cannot exceed logical NPC count.");
            }

            if (MemoriesPerNpc <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(MemoriesPerNpc), "Memories per NPC must be positive.");
            }

            if ((long)MemoriesPerNpc * LogicalNpcCount > int.MaxValue)
            {
                throw new ArgumentOutOfRangeException(nameof(MemoriesPerNpc), "The total number of NPC memories exceeds the supported range.");
            }

            if (WarmupTicks < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(WarmupTicks), "Warmup ticks cannot be negative.");
            }

            if (MeasuredTicks <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(MeasuredTicks), "Measured ticks must be positive.");
            }

            if (MutationsPerTick <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(MutationsPerTick), "Mutations per tick must be positive.");
            }

            if (MemorySearchEveryTicks <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(MemorySearchEveryTicks), "Memory search interval must be positive.");
            }
        }
    }

    public sealed record ArenaNpcSeed(
        int id,
        string team,
        string zone,
        Vector3 position,
        int health,
        int score);

    public sealed record ArenaMemorySeed(
        int id,
        int npcId,
        string tag,
        float[] embedding);

    public sealed record ArenaMutation(
        int playerId,
        int scoreDelta,
        int healthDelta,
        Vector3 position,
        string zone,
        int inventoryId,
        int inventoryItemId,
        int inventorySlot);

    public sealed record ArenaLeaderboardEntry(int id, int score);

    public sealed record ArenaTeamAggregate(string team, int alive);

    public sealed record ArenaMemoryMatch(int id, int npcId, string tag, float distance);

    public sealed record ArenaFrameSnapshot(
        int tick,
        IReadOnlyList<ArenaLeaderboardEntry> leaderboard,
        IReadOnlyList<ArenaTeamAggregate> aliveByTeam,
        IReadOnlyList<ArenaMemoryMatch> selectedMemoryMatches,
        IReadOnlyList<ArenaNpcSeed> visibleNpcs,
        int selectedNpcId)
    {
        public static ArenaFrameSnapshot Empty { get; } = new(
            tick: 0,
            leaderboard: Array.Empty<ArenaLeaderboardEntry>(),
            aliveByTeam: Array.Empty<ArenaTeamAggregate>(),
            selectedMemoryMatches: Array.Empty<ArenaMemoryMatch>(),
            visibleNpcs: Array.Empty<ArenaNpcSeed>(),
            selectedNpcId: 0);
    }

    [Serializable]
    public sealed class ArenaLatencySummary
    {
        public int count;
        public double p50Milliseconds;
        public double p95Milliseconds;
        public double p99Milliseconds;
        public double maxMilliseconds;
        public double meanMilliseconds;
    }

    [Serializable]
    public sealed class ArenaRunResult
    {
        public int schemaVersion;
        public string status = string.Empty;
        public string mode = string.Empty;
        public int seed;
        public int measuredTicks;
        public string leaderboardChecksum = string.Empty;
        public string aggregateChecksum = string.Empty;
        public long reactiveChanges;
        public long pollingRows;
        public ArenaLatencySummary tick = new();
        public ArenaLatencySummary leaderboard = new();
        public ArenaLatencySummary vectorSearch = new();
        public string unityVersion = string.Empty;
        public string platform = string.Empty;
        public string scriptingBackend = string.Empty;
        public string error = string.Empty;
    }
}
