#nullable enable

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using DataVo.Core.Runtime.Reactive;

namespace DataVo.UnityArena
{
    public sealed class ArenaDeliveryRunner : IDisposable
    {
        private readonly ArenaRunOptions options;
        private readonly ArenaDataSession session;
        private readonly List<IDisposable> subscriptions = new();
        private readonly Dictionary<int, ArenaLeaderboardEntry> leaderboard = new();
        private readonly Dictionary<string, ArenaTeamAggregate> aliveByTeam = new(StringComparer.Ordinal);
        private readonly Dictionary<int, int> zoneInventory = new();
        private readonly Dictionary<int, ArenaNpcSeed> visibleNpcs = new();
        private readonly ArenaLatencyRecorder tickLatency = new();
        private readonly ArenaLatencyRecorder leaderboardLatency = new();
        private readonly ArenaLatencyRecorder vectorSearchLatency = new();

        private IReadOnlyList<ArenaMemoryMatch> selectedMemoryMatches = Array.Empty<ArenaMemoryMatch>();
        private ArenaFrameSnapshot latestSnapshot = ArenaFrameSnapshot.Empty;
        private int selectedNpcId = 1;
        private int nextTick;
        private long reactiveChanges;
        private long pollingRows;
        private bool measuringCurrentDelivery;
        private bool warmedUp;
        private bool disposed;

        public ArenaDeliveryRunner(ArenaRunOptions options)
        {
            this.options = options ?? throw new ArgumentNullException(nameof(options));
            ValidateRunnerOptions(options);
            session = new ArenaDataSession(options);
            SeedVisibleNpcs();
            RebuildPresentation(recordPollingRows: false);

            if (options.Mode == ArenaDeliveryMode.Reactive)
            {
                subscriptions.Add(session.Context.Subscribe(ArenaDataSession.LeaderboardSql, ApplyLeaderboardChange));
                subscriptions.Add(session.Context.Subscribe(ArenaDataSession.AliveByTeamSql, ApplyAliveByTeamChange));
                subscriptions.Add(session.Context.Subscribe(ArenaDataSession.ZoneInventorySql, ApplyZoneInventoryChange));
            }

            selectedMemoryMatches = session.FindNearestMemories(selectedNpcId, topK: 3);
            latestSnapshot = BuildSnapshot();
        }

        public ArenaRunOptions Options => options;

        public void Warmup()
        {
            ThrowIfDisposed();

            if (warmedUp)
            {
                return;
            }

            for (int tick = 0; tick < options.WarmupTicks; tick++)
            {
                StepCore(recordMeasurements: false);
            }

            warmedUp = true;
        }

        public ArenaFrameSnapshot Step()
        {
            ThrowIfDisposed();

            if (!warmedUp)
            {
                throw new InvalidOperationException("Warmup must complete before measured arena ticks.");
            }

            return StepCore(recordMeasurements: true);
        }

        public ArenaRunResult Complete()
        {
            ThrowIfDisposed();

            IReadOnlyList<ArenaLeaderboardEntry> finalLeaderboard = OrderedLeaderboard();
            IReadOnlyList<ArenaTeamAggregate> finalAliveByTeam = OrderedAliveByTeam();

            return new ArenaRunResult
            {
                schemaVersion = 1,
                status = "passed",
                mode = options.Mode == ArenaDeliveryMode.Reactive ? "reactive" : "polling",
                seed = options.Seed,
                measuredTicks = options.MeasuredTicks,
                leaderboardChecksum = ChecksumLeaderboard(finalLeaderboard),
                aggregateChecksum = ChecksumAggregates(finalAliveByTeam),
                reactiveChanges = reactiveChanges,
                pollingRows = pollingRows,
                tick = tickLatency.Snapshot(),
                leaderboard = leaderboardLatency.Snapshot(),
                vectorSearch = vectorSearchLatency.Snapshot()
            };
        }

        public void Dispose()
        {
            if (disposed)
            {
                return;
            }

            disposed = true;

            foreach (IDisposable subscription in subscriptions)
            {
                subscription.Dispose();
            }

            session.Dispose();
        }

        private ArenaFrameSnapshot StepCore(bool recordMeasurements)
        {
            long tickStart = recordMeasurements ? tickLatency.Start() : 0L;
            IReadOnlyList<ArenaMutation> mutations = ArenaSeed.CreateMutations(
                options.Seed,
                nextTick,
                options.LogicalNpcCount,
                options.MutationsPerTick);

            session.ApplyMutations(mutations);
            ApplyVisibleMutations(mutations);

            long deliveryStart = recordMeasurements ? leaderboardLatency.Start() : 0L;
            measuringCurrentDelivery = recordMeasurements;

            try
            {
                if (options.Mode == ArenaDeliveryMode.Reactive)
                {
                    session.Context.DispatchPendingNotifications();
                }
                else
                {
                    RebuildPresentation(recordPollingRows: recordMeasurements);
                }
            }
            finally
            {
                measuringCurrentDelivery = false;
            }

            if (recordMeasurements)
            {
                leaderboardLatency.AddElapsed(deliveryStart);
            }

            if (nextTick % options.MemorySearchEveryTicks == 0)
            {
                selectedNpcId = nextTick % options.LogicalNpcCount + 1;
                long vectorStart = recordMeasurements ? vectorSearchLatency.Start() : 0L;
                selectedMemoryMatches = session.FindNearestMemories(selectedNpcId, topK: 3);

                if (recordMeasurements)
                {
                    vectorSearchLatency.AddElapsed(vectorStart);
                }
            }

            nextTick++;
            latestSnapshot = BuildSnapshot();

            if (recordMeasurements)
            {
                tickLatency.AddElapsed(tickStart);
            }

            return latestSnapshot;
        }

        private void SeedVisibleNpcs()
        {
            IReadOnlyList<ArenaNpcSeed> allNpcs = ArenaSeed.CreatePlayers(options.LogicalNpcCount);
            int visibleCount = Math.Min(options.VisibleNpcCount, allNpcs.Count);

            for (int index = 0; index < visibleCount; index++)
            {
                ArenaNpcSeed npc = allNpcs[index];
                visibleNpcs.Add(npc.id, npc);
            }
        }

        private void ApplyVisibleMutations(IReadOnlyList<ArenaMutation> mutations)
        {
            foreach (ArenaMutation mutation in mutations)
            {
                if (!visibleNpcs.TryGetValue(mutation.playerId, out ArenaNpcSeed? current))
                {
                    continue;
                }

                visibleNpcs[mutation.playerId] = current with
                {
                    score = current.score + mutation.scoreDelta,
                    health = current.health + mutation.healthDelta,
                    position = mutation.position,
                    zone = mutation.zone
                };
            }
        }

        private void RebuildPresentation(bool recordPollingRows)
        {
            IReadOnlyList<Dictionary<string, object?>> leaderboardRows = session.QueryRows(ArenaDataSession.LeaderboardSql);
            IReadOnlyList<Dictionary<string, object?>> aggregateRows = session.QueryRows(ArenaDataSession.AliveByTeamSql);
            IReadOnlyList<Dictionary<string, object?>> zoneRows = session.QueryRows(ArenaDataSession.ZoneInventorySql);

            ReplaceLeaderboard(leaderboardRows);
            ReplaceAliveByTeam(aggregateRows);
            ReplaceZoneInventory(zoneRows);

            if (recordPollingRows)
            {
                pollingRows += leaderboardRows.Count + aggregateRows.Count + zoneRows.Count;
            }
        }

        private void ApplyLeaderboardChange(QueryChange change)
        {
            CountReactiveRows(change);
            ApplyRows(change.Added, UpdateLeaderboard);
            ApplyRows(change.Updated, UpdateLeaderboard);

            foreach (IReadOnlyDictionary<string, object?> row in change.Removed)
            {
                leaderboard.Remove(ReadInt(row, "Id"));
            }
        }

        private void ApplyAliveByTeamChange(QueryChange change)
        {
            CountReactiveRows(change);
            ApplyRows(change.Added, UpdateAliveByTeam);
            ApplyRows(change.Updated, UpdateAliveByTeam);

            foreach (IReadOnlyDictionary<string, object?> row in change.Removed)
            {
                aliveByTeam.Remove(ReadString(row, "Team"));
            }
        }

        private void ApplyZoneInventoryChange(QueryChange change)
        {
            CountReactiveRows(change);
            ApplyRows(change.Added, UpdateZoneInventory);
            ApplyRows(change.Updated, UpdateZoneInventory);

            foreach (IReadOnlyDictionary<string, object?> row in change.Removed)
            {
                zoneInventory.Remove(ReadInt(row, "p.Id"));
            }
        }

        private void ReplaceLeaderboard(IReadOnlyList<Dictionary<string, object?>> rows)
        {
            leaderboard.Clear();

            foreach (IReadOnlyDictionary<string, object?> row in rows)
            {
                UpdateLeaderboard(row);
            }
        }

        private void ReplaceAliveByTeam(IReadOnlyList<Dictionary<string, object?>> rows)
        {
            aliveByTeam.Clear();

            foreach (IReadOnlyDictionary<string, object?> row in rows)
            {
                UpdateAliveByTeam(row);
            }
        }

        private void ReplaceZoneInventory(IReadOnlyList<Dictionary<string, object?>> rows)
        {
            zoneInventory.Clear();

            foreach (IReadOnlyDictionary<string, object?> row in rows)
            {
                UpdateZoneInventory(row);
            }
        }

        private void UpdateLeaderboard(IReadOnlyDictionary<string, object?> row)
        {
            int id = ReadInt(row, "Id");
            leaderboard[id] = new ArenaLeaderboardEntry(id, ReadInt(row, "Score"));
        }

        private void UpdateAliveByTeam(IReadOnlyDictionary<string, object?> row)
        {
            string team = ReadString(row, "Team");
            aliveByTeam[team] = new ArenaTeamAggregate(team, ReadInt(row, "Alive"));
        }

        private void UpdateZoneInventory(IReadOnlyDictionary<string, object?> row)
        {
            zoneInventory[ReadInt(row, "p.Id")] = ReadInt(row, "i.ItemId");
        }

        private ArenaFrameSnapshot BuildSnapshot() => new(
            tick: nextTick,
            leaderboard: OrderedLeaderboard(),
            aliveByTeam: OrderedAliveByTeam(),
            selectedMemoryMatches: selectedMemoryMatches.ToArray(),
            visibleNpcs: visibleNpcs.Values.OrderBy(npc => npc.id).ToArray(),
            selectedNpcId: selectedNpcId);

        private IReadOnlyList<ArenaLeaderboardEntry> OrderedLeaderboard() => leaderboard.Values
            .OrderByDescending(entry => entry.score)
            .ThenBy(entry => entry.id)
            .ToArray();

        private IReadOnlyList<ArenaTeamAggregate> OrderedAliveByTeam() => aliveByTeam.Values
            .OrderBy(entry => entry.team, StringComparer.Ordinal)
            .ToArray();

        private static void ApplyRows(
            IReadOnlyList<IReadOnlyDictionary<string, object?>> rows,
            Action<IReadOnlyDictionary<string, object?>> apply)
        {
            foreach (IReadOnlyDictionary<string, object?> row in rows)
            {
                apply(row);
            }
        }

        private static long CountRows(QueryChange change) =>
            change.Added.Count + change.Updated.Count + change.Removed.Count;

        private void CountReactiveRows(QueryChange change)
        {
            if (measuringCurrentDelivery)
            {
                reactiveChanges += CountRows(change);
            }
        }

        private static int ReadInt(IReadOnlyDictionary<string, object?> row, string column)
        {
            if (!row.TryGetValue(column, out object? value) || value is null)
            {
                throw new InvalidOperationException("Arena query result does not contain " + column + ".");
            }

            return Convert.ToInt32(value, CultureInfo.InvariantCulture);
        }

        private static string ReadString(IReadOnlyDictionary<string, object?> row, string column)
        {
            if (!row.TryGetValue(column, out object? value) || value is not string text || text.Length == 0)
            {
                throw new InvalidOperationException("Arena query result does not contain " + column + ".");
            }

            return text;
        }

        private static string ChecksumLeaderboard(IReadOnlyList<ArenaLeaderboardEntry> rows)
        {
            var builder = new StringBuilder();

            foreach (ArenaLeaderboardEntry row in rows.OrderBy(row => row.id))
            {
                builder.Append(row.id.ToString(CultureInfo.InvariantCulture));
                builder.Append(':');
                builder.Append(row.score.ToString(CultureInfo.InvariantCulture));
                builder.Append('\n');
            }

            return Sha256(builder.ToString());
        }

        private static string ChecksumAggregates(IReadOnlyList<ArenaTeamAggregate> rows)
        {
            var builder = new StringBuilder();

            foreach (ArenaTeamAggregate row in rows.OrderBy(row => row.team, StringComparer.Ordinal))
            {
                builder.Append(row.team);
                builder.Append(':');
                builder.Append(row.alive.ToString(CultureInfo.InvariantCulture));
                builder.Append('\n');
            }

            return Sha256(builder.ToString());
        }

        private static string Sha256(string value)
        {
            using SHA256 hasher = SHA256.Create();
            byte[] hash = hasher.ComputeHash(Encoding.UTF8.GetBytes(value));
            var result = new StringBuilder(hash.Length * 2);

            foreach (byte valueByte in hash)
            {
                result.Append(valueByte.ToString("x2", CultureInfo.InvariantCulture));
            }

            return result.ToString();
        }

        private static void ValidateRunnerOptions(ArenaRunOptions options)
        {
            // Headless runs can use a smaller logical set than the visual default.
            // ArenaRunOptions.Validate() remains the strict renderer-facing contract.
            if (options.LogicalNpcCount <= 0 || options.MemoriesPerNpc <= 0 ||
                options.MeasuredTicks <= 0 || options.MutationsPerTick <= 0 ||
                options.MemorySearchEveryTicks <= 0 || options.WarmupTicks < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(options), "Arena run options contain an invalid count or interval.");
            }

            if ((long)options.LogicalNpcCount * options.MemoriesPerNpc > int.MaxValue)
            {
                throw new ArgumentOutOfRangeException(nameof(options), "Arena run options exceed the supported memory range.");
            }
        }

        private void ThrowIfDisposed()
        {
            if (disposed)
            {
                throw new ObjectDisposedException(nameof(ArenaDeliveryRunner));
            }
        }
    }

    public static class ArenaBenchmark
    {
        public static ArenaRunResult Run(ArenaRunOptions options)
        {
            using var runner = new ArenaDeliveryRunner(options);
            runner.Warmup();

            for (int tick = 0; tick < options.MeasuredTicks; tick++)
            {
                runner.Step();
            }

            return runner.Complete();
        }

        public static void RequireEquivalent(ArenaRunResult reactive, ArenaRunResult polling)
        {
            if (reactive is null)
            {
                throw new ArgumentNullException(nameof(reactive));
            }

            if (polling is null)
            {
                throw new ArgumentNullException(nameof(polling));
            }

            if (!string.Equals(reactive.leaderboardChecksum, polling.leaderboardChecksum, StringComparison.Ordinal) ||
                !string.Equals(reactive.aggregateChecksum, polling.aggregateChecksum, StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    "Reactive maintained views and polling full-query views produced different final checksums.");
            }
        }
    }
}
