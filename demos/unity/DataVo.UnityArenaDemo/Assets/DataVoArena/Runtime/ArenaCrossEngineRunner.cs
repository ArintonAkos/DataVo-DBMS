#nullable enable

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using DataVo.Core;
using DataVo.Core.CompiledQueries;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;
using DataVo.UnityArena.Sqlite;
using UnityEngine;

namespace DataVo.UnityArena
{
    public static class ArenaCrossEngineRunner
    {
        public static ArenaCrossEngineResult Run(
            ArenaCrossEngineProfile profile,
            ArenaCrossEngineKind engine,
            int repetitions)
        {
            ArenaCrossEngineWorkload.ValidateProfile(profile);
            if (repetitions <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(repetitions), "Repetitions must be positive.");
            }

            var completedRuns = new ArenaCrossEngineRepetition[repetitions];
            ArenaCrossEngineHashes? canonicalHashes = null;
            string sqliteVersion = string.Empty;

            for (int repetition = 1; repetition <= repetitions; repetition++)
            {
                long expectedHashStart = Stopwatch.GetTimestamp();
                ArenaCrossEngineHashes[] expectedHashes = ArenaCrossEngineWorkload.CreateExpectedHashSequence(
                    profile,
                    profile.warmupBatches + profile.measuredBatches);
                ArenaCrossEngineDiagnostics.LogPhase(
                    profile,
                    engine,
                    repetition,
                    "expected-hashes",
                    ElapsedMilliseconds(expectedHashStart, Stopwatch.GetTimestamp()));

                long sessionStart = Stopwatch.GetTimestamp();
                using ICrossEngineSession session = CreateSession(profile, engine);
                ArenaCrossEngineDiagnostics.LogPhase(
                    profile,
                    engine,
                    repetition,
                    "session-create",
                    ElapsedMilliseconds(sessionStart, Stopwatch.GetTimestamp()));

                var warmupDiagnostics = new ArenaCrossEngineDiagnosticWindow();
                for (int batchIndex = 0; batchIndex < profile.warmupBatches; batchIndex++)
                {
                    ArenaCrossEngineApplyTiming apply = session.ApplyBatch(
                        ArenaCrossEngineWorkload.CreateBatch(profile, batchIndex));
                    long demandStart = Stopwatch.GetTimestamp();
                    ArenaCrossEngineReadResult read = session.DemandViews();
                    long demandEnd = Stopwatch.GetTimestamp();
                    long hashStart = Stopwatch.GetTimestamp();
                    RequireExpectedHashes(expectedHashes, batchIndex + 1, read.views, engine);
                    long hashEnd = Stopwatch.GetTimestamp();
                    warmupDiagnostics.Add(
                        apply,
                        ElapsedMilliseconds(demandStart, demandEnd),
                        ElapsedMilliseconds(hashStart, hashEnd));
                    if (ArenaCrossEngineDiagnostics.IsProgressBoundary(batchIndex + 1, profile.warmupBatches))
                    {
                        ArenaCrossEngineDiagnostics.LogBatchWindow(
                            profile,
                            engine,
                            repetition,
                            "warmup",
                            warmupDiagnostics);
                        warmupDiagnostics.Reset();
                    }
                }

                session.BeginMeasuredInterval();
                var batchLatency = new ArenaLatencyRecorder();
                var leaderboardLatency = new ArenaLatencyRecorder();
                var aggregateLatency = new ArenaLatencyRecorder();
                var zoneInventoryLatency = new ArenaLatencyRecorder();
                var measuredDiagnostics = new ArenaCrossEngineDiagnosticWindow();
                double totalMilliseconds = 0d;
                ArenaCrossEngineHashes? finalHashes = null;

                for (int measuredIndex = 0; measuredIndex < profile.measuredBatches; measuredIndex++)
                {
                    int batchIndex = profile.warmupBatches + measuredIndex;
                    long batchStart = Stopwatch.GetTimestamp();
                    ArenaCrossEngineApplyTiming apply = session.ApplyBatch(
                        ArenaCrossEngineWorkload.CreateBatch(profile, batchIndex));
                    long demandStart = Stopwatch.GetTimestamp();
                    ArenaCrossEngineReadResult read = session.DemandViews();
                    long demandEnd = Stopwatch.GetTimestamp();
                    long hashStart = Stopwatch.GetTimestamp();
                    ArenaCrossEngineHashes actualHashes = RequireExpectedHashes(expectedHashes, batchIndex + 1, read.views, engine);
                    long hashEnd = Stopwatch.GetTimestamp();
                    double elapsedMilliseconds = ElapsedMilliseconds(batchStart, demandEnd);

                    batchLatency.AddMilliseconds(elapsedMilliseconds);
                    leaderboardLatency.AddMilliseconds(read.leaderboardMilliseconds);
                    aggregateLatency.AddMilliseconds(read.aggregateMilliseconds);
                    zoneInventoryLatency.AddMilliseconds(read.zoneInventoryMilliseconds);
                    totalMilliseconds += elapsedMilliseconds;
                    finalHashes = actualHashes;
                    measuredDiagnostics.Add(
                        apply,
                        ElapsedMilliseconds(demandStart, demandEnd),
                        ElapsedMilliseconds(hashStart, hashEnd));
                    if (ArenaCrossEngineDiagnostics.IsProgressBoundary(measuredIndex + 1, profile.measuredBatches))
                    {
                        ArenaCrossEngineDiagnostics.LogBatchWindow(
                            profile,
                            engine,
                            repetition,
                            "measured",
                            measuredDiagnostics);
                        measuredDiagnostics.Reset();
                    }
                }

                ArenaCrossEngineHashes hashes = finalHashes
                    ?? throw new InvalidOperationException("No measured arena batches were executed.");
                if (canonicalHashes is null)
                {
                    canonicalHashes = hashes;
                }
                else if (canonicalHashes != hashes)
                {
                    throw new InvalidOperationException(
                        "Cross-engine arena hashes changed between repetitions for engine '" + EngineId(engine) + "'.");
                }

                if (engine == ArenaCrossEngineKind.SqlitePolling)
                {
                    if (string.IsNullOrWhiteSpace(session.SqliteVersion))
                    {
                        throw new InvalidOperationException("SQLite session did not report a library version.");
                    }

                    if (sqliteVersion.Length == 0)
                    {
                        sqliteVersion = session.SqliteVersion;
                    }
                    else if (!string.Equals(sqliteVersion, session.SqliteVersion, StringComparison.Ordinal))
                    {
                        throw new InvalidOperationException("SQLite library version changed between arena repetitions.");
                    }
                }

                completedRuns[repetition - 1] = new ArenaCrossEngineRepetition
                {
                    index = repetition,
                    totalMilliseconds = totalMilliseconds,
                    writesApplied = (long)profile.mutationsPerBatch * profile.measuredBatches,
                    reactiveChanges = session.ReactiveChanges,
                    pollingRows = session.PollingRows,
                    batch = ToCrossEngineSummary(batchLatency.Snapshot()),
                    leaderboard = ToCrossEngineSummary(leaderboardLatency.Snapshot()),
                    aggregate = ToCrossEngineSummary(aggregateLatency.Snapshot()),
                    zoneInventory = ToCrossEngineSummary(zoneInventoryLatency.Snapshot())
                };
            }

            ArenaCrossEngineHashes resultHashes = canonicalHashes
                ?? throw new InvalidOperationException("No cross-engine repetitions were executed.");
            return new ArenaCrossEngineResult
            {
                schemaVersion = 1,
                schemaRevision = ArenaCrossEngineWorkload.SchemaRevision,
                status = "passed",
                engine = EngineId(engine),
                profile = profile.id,
                seed = profile.seed,
                measuredBatches = profile.measuredBatches,
                repetitions = completedRuns,
                leaderboardChecksum = resultHashes.Leaderboard,
                aggregateChecksum = resultHashes.Aggregate,
                zoneInventoryChecksum = resultHashes.ZoneInventory,
                unityVersion = Application.unityVersion,
                platform = Application.platform.ToString(),
                scriptingBackend = ScriptingBackendName(),
                sqliteVersion = engine == ArenaCrossEngineKind.SqlitePolling ? sqliteVersion : string.Empty,
                error = string.Empty
            };
        }

        private static ICrossEngineSession CreateSession(ArenaCrossEngineProfile profile, ArenaCrossEngineKind engine) => engine switch
        {
            ArenaCrossEngineKind.DataVoReactive => new DataVoReactiveCrossEngineSession(profile),
            ArenaCrossEngineKind.DataVoPolling => new DataVoPollingCrossEngineSession(profile),
            ArenaCrossEngineKind.SqlitePolling => new SqliteCrossEngineSession(profile),
            _ => throw new ArgumentOutOfRangeException(nameof(engine), "Unknown arena cross-engine kind.")
        };

        private static ArenaCrossEngineHashes RequireExpectedHashes(
            ArenaCrossEngineHashes[] expectedHashes,
            int appliedBatchCount,
            ArenaCrossEngineViews actualViews,
            ArenaCrossEngineKind engine)
        {
            if (appliedBatchCount < 1 || appliedBatchCount > expectedHashes.Length)
            {
                throw new InvalidOperationException(
                    "Cross-engine arena expected hash sequence does not contain batch " +
                    appliedBatchCount.ToString(CultureInfo.InvariantCulture) + ".");
            }

            ArenaCrossEngineHashes expected = expectedHashes[appliedBatchCount - 1];
            ArenaCrossEngineHashes actual = ArenaCrossEngineWorkload.Hash(actualViews);
            if (expected != actual)
            {
                throw new InvalidOperationException(
                    "Cross-engine arena hash mismatch for engine '" + EngineId(engine) + "' after batch " +
                    (appliedBatchCount - 1).ToString(CultureInfo.InvariantCulture) + ". Expected [" +
                    expected.Leaderboard + ", " + expected.Aggregate + ", " + expected.ZoneInventory +
                    "], actual [" + actual.Leaderboard + ", " + actual.Aggregate + ", " + actual.ZoneInventory + "].");
            }

            return actual;
        }

        private static ArenaCrossEngineLatencySummary ToCrossEngineSummary(ArenaLatencySummary summary) => new()
        {
            count = summary.count,
            p50Milliseconds = summary.p50Milliseconds,
            p95Milliseconds = summary.p95Milliseconds,
            p99Milliseconds = summary.p99Milliseconds,
            maxMilliseconds = summary.maxMilliseconds,
            meanMilliseconds = summary.meanMilliseconds
        };

        private static string EngineId(ArenaCrossEngineKind engine) => engine switch
        {
            ArenaCrossEngineKind.DataVoReactive => "datavo-reactive",
            ArenaCrossEngineKind.DataVoPolling => "datavo-polling",
            ArenaCrossEngineKind.SqlitePolling => "sqlite",
            _ => throw new ArgumentOutOfRangeException(nameof(engine))
        };

        private static double ElapsedMilliseconds(long start, long end) =>
            (end - start) * 1000d / Stopwatch.Frequency;

        private static string ScriptingBackendName()
        {
#if ENABLE_IL2CPP
            return "IL2CPP";
#elif ENABLE_MONO
            return "Mono";
#else
            return "Unknown";
#endif
        }

        // The timing boundary deliberately wraps ApplyMutations rather than trying to split the compiled
        // plan executor: each arena UPDATE is one compiled-plan operation.
        // Reactive notification is measured separately immediately afterwards.
        private readonly struct ArenaCrossEngineApplyTiming
        {
            public ArenaCrossEngineApplyTiming(double updateExecuteMilliseconds, double reactiveNotificationMilliseconds)
            {
                UpdateExecuteMilliseconds = updateExecuteMilliseconds;
                ReactiveNotificationMilliseconds = reactiveNotificationMilliseconds;
            }

            public double UpdateExecuteMilliseconds { get; }

            public double ReactiveNotificationMilliseconds { get; }
        }

        private sealed class ArenaCrossEngineDiagnosticWindow
        {
            private int batchCount;
            private double updateExecuteMilliseconds;
            private double reactiveNotificationMilliseconds;
            private double demandViewsMilliseconds;
            private double hashMilliseconds;

            public int BatchCount => batchCount;

            public double UpdateExecuteMilliseconds => updateExecuteMilliseconds;

            public double ReactiveNotificationMilliseconds => reactiveNotificationMilliseconds;

            public double DemandViewsMilliseconds => demandViewsMilliseconds;

            public double HashMilliseconds => hashMilliseconds;

            public void Add(
                ArenaCrossEngineApplyTiming apply,
                double demandViewsElapsedMilliseconds,
                double hashElapsedMilliseconds)
            {
                batchCount++;
                updateExecuteMilliseconds += apply.UpdateExecuteMilliseconds;
                reactiveNotificationMilliseconds += apply.ReactiveNotificationMilliseconds;
                demandViewsMilliseconds += demandViewsElapsedMilliseconds;
                hashMilliseconds += hashElapsedMilliseconds;
            }

            public void Reset()
            {
                batchCount = 0;
                updateExecuteMilliseconds = 0d;
                reactiveNotificationMilliseconds = 0d;
                demandViewsMilliseconds = 0d;
                hashMilliseconds = 0d;
            }
        }

        private static class ArenaCrossEngineDiagnostics
        {
            private static readonly bool enabled = HasArgument("-datavoArenaDiagnostics");

            public static bool IsProgressBoundary(int completedBatchCount, int totalBatchCount) =>
                enabled && (completedBatchCount == totalBatchCount || completedBatchCount % 100 == 0);

            public static void LogPhase(
                ArenaCrossEngineProfile profile,
                ArenaCrossEngineKind engine,
                int repetition,
                string phase,
                double elapsedMilliseconds)
            {
                if (!enabled)
                {
                    return;
                }

                UnityEngine.Debug.Log(
                    "DATAVO_ARENA_CROSS_ENGINE_DIAGNOSTIC profile=" + profile.id +
                    " engine=" + EngineId(engine) +
                    " repetition=" + repetition.ToString(CultureInfo.InvariantCulture) +
                    " phase=" + phase +
                    " milliseconds=" + elapsedMilliseconds.ToString("F3", CultureInfo.InvariantCulture));
            }

            public static void LogBatchWindow(
                ArenaCrossEngineProfile profile,
                ArenaCrossEngineKind engine,
                int repetition,
                string phase,
                ArenaCrossEngineDiagnosticWindow window)
            {
                if (!enabled || window.BatchCount == 0)
                {
                    return;
                }

                UnityEngine.Debug.Log(
                    "DATAVO_ARENA_CROSS_ENGINE_DIAGNOSTIC profile=" + profile.id +
                    " engine=" + EngineId(engine) +
                    " repetition=" + repetition.ToString(CultureInfo.InvariantCulture) +
                    " phase=" + phase +
                    " batches=" + window.BatchCount.ToString(CultureInfo.InvariantCulture) +
                    " updateExecuteMilliseconds=" +
                    window.UpdateExecuteMilliseconds.ToString("F3", CultureInfo.InvariantCulture) +
                    " reactiveNotificationMilliseconds=" +
                    window.ReactiveNotificationMilliseconds.ToString("F3", CultureInfo.InvariantCulture) +
                    " demandViewsMilliseconds=" +
                    window.DemandViewsMilliseconds.ToString("F3", CultureInfo.InvariantCulture) +
                    " hashMilliseconds=" + window.HashMilliseconds.ToString("F3", CultureInfo.InvariantCulture));
            }

            private static bool HasArgument(string target)
            {
                foreach (string argument in Environment.GetCommandLineArgs())
                {
                    if (argument == target)
                    {
                        return true;
                    }
                }

                return false;
            }
        }

        private interface ICrossEngineSession : IDisposable
        {
            long ReactiveChanges { get; }
            long PollingRows { get; }
            string SqliteVersion { get; }

            void BeginMeasuredInterval();
            ArenaCrossEngineApplyTiming ApplyBatch(IReadOnlyList<ArenaCrossEngineMutation> batch);
            ArenaCrossEngineReadResult DemandViews();
        }

        private static readonly DataVoCompiledQueryPlan PlayerMutationUpdatePlan = DataVoCompiledQueryPlan.Update(
            tableName: "Players",
            assignments: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["Score"] = "score",
                ["Health"] = "health",
                ["Zone"] = "zone"
            },
            whereColumn: "Id",
            whereParameterName: "id");

        private static readonly DataVoCompiledQueryPlan InventoryMutationUpdatePlan = DataVoCompiledQueryPlan.Update(
            tableName: "Inventory",
            assignments: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["ItemId"] = "itemId",
                ["Slot"] = "slot"
            },
            whereColumn: "InventoryId",
            whereParameterName: "inventoryId");

        private sealed class SqliteCrossEngineSession : ICrossEngineSession
        {
            private readonly ArenaSqlitePollingSession session;

            public SqliteCrossEngineSession(ArenaCrossEngineProfile profile)
            {
                session = new ArenaSqlitePollingSession(profile);
            }

            public long ReactiveChanges => 0;

            public long PollingRows => session.PollingRows;

            public string SqliteVersion => session.SqliteVersion;

            public void BeginMeasuredInterval() => session.BeginMeasuredInterval();

            public ArenaCrossEngineApplyTiming ApplyBatch(IReadOnlyList<ArenaCrossEngineMutation> batch)
            {
                long updateStart = Stopwatch.GetTimestamp();
                session.ApplyBatch(batch);
                return new ArenaCrossEngineApplyTiming(
                    ElapsedMilliseconds(updateStart, Stopwatch.GetTimestamp()),
                    0d);
            }

            public ArenaCrossEngineReadResult DemandViews() => session.DemandViews();

            public void Dispose() => session.Dispose();
        }

        private sealed class DataVoPollingCrossEngineSession : ICrossEngineSession
        {
            private readonly DataVoContext context;
            private readonly DataVoMutationState mutationState;
            private bool recordPollingRows;
            private bool disposed;

            public DataVoPollingCrossEngineSession(ArenaCrossEngineProfile profile)
            {
                context = CreateContext();
                Seed(context, profile);
                mutationState = new DataVoMutationState(profile);
            }

            public long ReactiveChanges => 0;

            public long PollingRows { get; private set; }

            public string SqliteVersion => string.Empty;

            public void BeginMeasuredInterval()
            {
                ThrowIfDisposed();
                recordPollingRows = true;
                PollingRows = 0;
            }

            public ArenaCrossEngineApplyTiming ApplyBatch(IReadOnlyList<ArenaCrossEngineMutation> batch)
            {
                ThrowIfDisposed();
                long updateStart = Stopwatch.GetTimestamp();
                ApplyMutations(context, mutationState, batch);
                return new ArenaCrossEngineApplyTiming(
                    ElapsedMilliseconds(updateStart, Stopwatch.GetTimestamp()),
                    0d);
            }

            public ArenaCrossEngineReadResult DemandViews()
            {
                ThrowIfDisposed();
                long leaderboardStart = Stopwatch.GetTimestamp();
                IReadOnlyList<ArenaCrossEngineLeaderboardRow> leaderboard = ReadLeaderboard(context);
                long aggregateStart = Stopwatch.GetTimestamp();
                IReadOnlyList<ArenaCrossEngineAggregateRow> aggregate = ReadAggregate(context);
                long zoneInventoryStart = Stopwatch.GetTimestamp();
                IReadOnlyList<ArenaCrossEngineZoneInventoryRow> zoneInventory = ReadZoneInventory(context);
                long readEnd = Stopwatch.GetTimestamp();

                if (recordPollingRows)
                {
                    PollingRows += leaderboard.Count + aggregate.Count + zoneInventory.Count;
                }

                return new ArenaCrossEngineReadResult(
                    new ArenaCrossEngineViews(leaderboard, aggregate, zoneInventory),
                    ElapsedMilliseconds(leaderboardStart, aggregateStart),
                    ElapsedMilliseconds(aggregateStart, zoneInventoryStart),
                    ElapsedMilliseconds(zoneInventoryStart, readEnd));
            }

            public void Dispose()
            {
                if (disposed)
                {
                    return;
                }

                disposed = true;
                context.Dispose();
            }

            private void ThrowIfDisposed()
            {
                if (disposed)
                {
                    throw new ObjectDisposedException(nameof(DataVoPollingCrossEngineSession));
                }
            }
        }

        private sealed class DataVoReactiveCrossEngineSession : ICrossEngineSession
        {
            private readonly DataVoContext context;
            private readonly DataVoMutationState mutationState;
            private readonly List<IDisposable> subscriptions = new();
            private readonly Dictionary<int, ArenaCrossEngineLeaderboardRow> leaderboard = new();
            private readonly Dictionary<string, ArenaCrossEngineAggregateRow> aggregate = new(StringComparer.Ordinal);
            private readonly Dictionary<int, ArenaCrossEngineZoneInventoryRow> zoneInventory = new();
            private bool recordReactiveChanges;
            private bool disposed;

            public DataVoReactiveCrossEngineSession(ArenaCrossEngineProfile profile)
            {
                context = CreateContext();
                mutationState = new DataVoMutationState(profile);
                subscriptions.Add(context.SubscribeZeroAlloc(ArenaCrossEngineWorkload.LeaderboardSql, ApplyLeaderboardChange));
                subscriptions.Add(context.SubscribeZeroAlloc(ArenaCrossEngineWorkload.ReactiveAggregateSql, ApplyAggregateChange));
                subscriptions.Add(context.SubscribeZeroAlloc(ArenaCrossEngineWorkload.ReactiveZoneInventorySql, ApplyZoneInventoryChange));
                Seed(context, profile);
                context.DispatchPendingNotifications();
            }

            public long ReactiveChanges { get; private set; }

            public long PollingRows => 0;

            public string SqliteVersion => string.Empty;

            public void BeginMeasuredInterval()
            {
                ThrowIfDisposed();
                recordReactiveChanges = true;
                ReactiveChanges = 0;
            }

            public ArenaCrossEngineApplyTiming ApplyBatch(IReadOnlyList<ArenaCrossEngineMutation> batch)
            {
                ThrowIfDisposed();
                long updateStart = Stopwatch.GetTimestamp();
                ApplyMutations(context, mutationState, batch);
                long notificationStart = Stopwatch.GetTimestamp();
                context.DispatchPendingNotifications();
                return new ArenaCrossEngineApplyTiming(
                    ElapsedMilliseconds(updateStart, notificationStart),
                    ElapsedMilliseconds(notificationStart, Stopwatch.GetTimestamp()));
            }

            public ArenaCrossEngineReadResult DemandViews()
            {
                ThrowIfDisposed();
                long leaderboardStart = Stopwatch.GetTimestamp();
                ArenaCrossEngineLeaderboardRow[] leaderboardRows = leaderboard.Values
                    .OrderByDescending(row => row.score)
                    .ThenBy(row => row.id)
                    .ToArray();
                long aggregateStart = Stopwatch.GetTimestamp();
                ArenaCrossEngineAggregateRow[] aggregateRows = aggregate.Values
                    .OrderBy(row => row.team, StringComparer.Ordinal)
                    .ToArray();
                long zoneInventoryStart = Stopwatch.GetTimestamp();
                ArenaCrossEngineZoneInventoryRow[] zoneInventoryRows = zoneInventory.Values
                    .OrderBy(row => row.playerId)
                    .ThenBy(row => row.itemId)
                    .ToArray();
                long readEnd = Stopwatch.GetTimestamp();

                return new ArenaCrossEngineReadResult(
                    new ArenaCrossEngineViews(leaderboardRows, aggregateRows, zoneInventoryRows),
                    ElapsedMilliseconds(leaderboardStart, aggregateStart),
                    ElapsedMilliseconds(aggregateStart, zoneInventoryStart),
                    ElapsedMilliseconds(zoneInventoryStart, readEnd));
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

                subscriptions.Clear();
                context.Dispose();
            }

            private void ApplyLeaderboardChange(in QueryChangeRef change)
            {
                CountReactiveRows(change);
                RemoveLeaderboardRows(change.Removed);
                UpsertLeaderboardRows(change.Added);
                UpsertLeaderboardRows(change.Updated);
            }

            private void ApplyAggregateChange(in QueryChangeRef change)
            {
                CountReactiveRows(change);
                RemoveAggregateRows(change.Removed);
                UpsertAggregateRows(change.Added);
                UpsertAggregateRows(change.Updated);
            }

            private void ApplyZoneInventoryChange(in QueryChangeRef change)
            {
                CountReactiveRows(change);
                RemoveZoneInventoryRows(change.Removed);
                RemoveZoneInventoryRows(change.UpdatedBefore);
                UpsertZoneInventoryRows(change.Added);
                UpsertZoneInventoryRows(change.Updated);
            }

            private void RemoveLeaderboardRows(RowSet rows)
            {
                for (int index = 0; index < rows.Count; index++)
                {
                    leaderboard.Remove(ReadInt(rows[index]["Id"]));
                }
            }

            private void UpsertLeaderboardRows(RowSet rows)
            {
                for (int index = 0; index < rows.Count; index++)
                {
                    RowRef row = rows[index];
                    int id = ReadInt(row["Id"]);
                    leaderboard[id] = new ArenaCrossEngineLeaderboardRow(id, ReadInt(row["Score"]));
                }
            }

            private void RemoveAggregateRows(RowSet rows)
            {
                for (int index = 0; index < rows.Count; index++)
                {
                    aggregate.Remove(ReadString(rows[index]["Team"]));
                }
            }

            private void UpsertAggregateRows(RowSet rows)
            {
                for (int index = 0; index < rows.Count; index++)
                {
                    RowRef row = rows[index];
                    string team = ReadString(row["Team"]);
                    aggregate[team] = new ArenaCrossEngineAggregateRow(team, ReadInt(row["Alive"]));
                }
            }

            private void RemoveZoneInventoryRows(RowSet rows)
            {
                for (int index = 0; index < rows.Count; index++)
                {
                    zoneInventory.Remove(ReadInt(rows[index]["p.Id"]));
                }
            }

            private void UpsertZoneInventoryRows(RowSet rows)
            {
                for (int index = 0; index < rows.Count; index++)
                {
                    RowRef row = rows[index];
                    int playerId = ReadInt(row["p.Id"]);
                    zoneInventory[playerId] = new ArenaCrossEngineZoneInventoryRow(playerId, ReadInt(row["i.ItemId"]));
                }
            }

            private void CountReactiveRows(in QueryChangeRef change)
            {
                if (recordReactiveChanges)
                {
                    ReactiveChanges += change.Added.Count + change.Removed.Count + change.Updated.Count;
                }
            }

            private static int ReadInt(CellValue value) => value.Type switch
            {
                CellType.Int32 => value.AsInt32(),
                CellType.Int64 => checked((int)value.AsInt64()),
                CellType.Decimal => checked((int)value.AsDecimal()),
                CellType.Double => checked((int)value.AsDouble()),
                _ => throw new InvalidOperationException("Expected numeric DataVo cell but received " + value.Type + ".")
            };

            private static string ReadString(CellValue value) =>
                value.AsString() ?? throw new InvalidOperationException("Expected a non-null DataVo string cell.");

            private void ThrowIfDisposed()
            {
                if (disposed)
                {
                    throw new ObjectDisposedException(nameof(DataVoReactiveCrossEngineSession));
                }
            }
        }

        private sealed class DataVoMutationState
        {
            private readonly Dictionary<int, int> scores;

            public DataVoMutationState(ArenaCrossEngineProfile profile)
            {
                ArenaCrossEnginePlayer[] players = ArenaCrossEngineWorkload.CreateInitialPlayers(profile);
                scores = new Dictionary<int, int>(players.Length);
                foreach (ArenaCrossEnginePlayer player in players)
                {
                    scores.Add(player.id, player.score);
                }
            }

            public int GetUpdatedScore(ArenaCrossEngineMutation mutation)
            {
                if (!scores.TryGetValue(mutation.playerId, out int currentScore))
                {
                    throw new InvalidOperationException(
                        "Arena mutation score state does not contain player " +
                        mutation.playerId.ToString(CultureInfo.InvariantCulture) + ".");
                }

                return checked(currentScore + mutation.scoreDelta);
            }

            public void SetScore(ArenaCrossEngineMutation mutation, int updatedScore) =>
                scores[mutation.playerId] = updatedScore;
        }

        private static DataVoContext CreateContext()
        {
            var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
            ExecuteOk(context, "CREATE DATABASE ArenaCrossEngine");
            ExecuteOk(context, "USE ArenaCrossEngine");
            ExecuteOk(context, ArenaCrossEngineWorkload.CreatePlayersSql);
            ExecuteOk(context, ArenaCrossEngineWorkload.CreateInventorySql);
            return context;
        }

        private static void Seed(DataVoContext context, ArenaCrossEngineProfile profile)
        {
            ArenaCrossEnginePlayer[] players = ArenaCrossEngineWorkload.CreateInitialPlayers(profile);
            var playerRows = new IReadOnlyDictionary<string, object?>[players.Length];
            for (int index = 0; index < players.Length; index++)
            {
                ArenaCrossEnginePlayer player = players[index];
                playerRows[index] = new Dictionary<string, object?>
                {
                    ["Id"] = player.id,
                    ["Team"] = player.team,
                    ["Zone"] = player.zone,
                    ["Health"] = player.health,
                    ["Score"] = player.score
                };
            }

            context.BulkInsert("Players", playerRows);

            ArenaCrossEngineInventory[] inventory = ArenaCrossEngineWorkload.CreateInitialInventory(profile);
            var inventoryRows = new IReadOnlyDictionary<string, object?>[inventory.Length];
            for (int index = 0; index < inventory.Length; index++)
            {
                ArenaCrossEngineInventory item = inventory[index];
                inventoryRows[index] = new Dictionary<string, object?>
                {
                    ["InventoryId"] = item.inventoryId,
                    ["PlayerId"] = item.playerId,
                    ["ItemId"] = item.itemId,
                    ["Slot"] = item.slot
                };
            }

            context.BulkInsert("Inventory", inventoryRows);
        }

        private static void ApplyMutations(
            DataVoContext context,
            DataVoMutationState mutationState,
            IReadOnlyList<ArenaCrossEngineMutation> batch)
        {
            if (batch is null)
            {
                throw new ArgumentNullException(nameof(batch));
            }

            foreach (ArenaCrossEngineMutation mutation in batch)
            {
                int updatedScore = mutationState.GetUpdatedScore(mutation);
                int playerRowsAffected = DataVoCompiledQuery.Update(
                    context,
                    PlayerMutationUpdatePlan,
                    new DataVoCompiledQueryParameter[]
                    {
                        new DataVoCompiledQueryParameter("score", updatedScore),
                        new DataVoCompiledQueryParameter("health", mutation.replacementHealth),
                        new DataVoCompiledQueryParameter("zone", mutation.replacementZone),
                        new DataVoCompiledQueryParameter("id", mutation.playerId)
                    });
                RequireSingleAffectedRow(playerRowsAffected, "Players", mutation.playerId);

                int inventoryRowsAffected = DataVoCompiledQuery.Update(
                    context,
                    InventoryMutationUpdatePlan,
                    new DataVoCompiledQueryParameter[]
                    {
                        new DataVoCompiledQueryParameter("itemId", mutation.inventoryItemId),
                        new DataVoCompiledQueryParameter("slot", mutation.inventorySlot),
                        new DataVoCompiledQueryParameter("inventoryId", mutation.playerId)
                    });
                RequireSingleAffectedRow(inventoryRowsAffected, "Inventory", mutation.playerId);
                mutationState.SetScore(mutation, updatedScore);
            }
        }

        private static void RequireSingleAffectedRow(int rowsAffected, string tableName, int primaryKey)
        {
            if (rowsAffected != 1)
            {
                throw new InvalidOperationException(
                    "Arena compiled update expected exactly one " + tableName + " row for primary key " +
                    primaryKey.ToString(CultureInfo.InvariantCulture) + ", but affected " +
                    rowsAffected.ToString(CultureInfo.InvariantCulture) + ".");
            }
        }

        private static IReadOnlyList<ArenaCrossEngineLeaderboardRow> ReadLeaderboard(DataVoContext context) =>
            ReadRows(context, ArenaCrossEngineWorkload.LeaderboardSql)
                .Select(row => new ArenaCrossEngineLeaderboardRow(ReadInt(row, "Id"), ReadInt(row, "Score")))
                .ToArray();

        private static IReadOnlyList<ArenaCrossEngineAggregateRow> ReadAggregate(DataVoContext context) =>
            ReadRows(context, ArenaCrossEngineWorkload.AggregateSql)
                .Select(row => new ArenaCrossEngineAggregateRow(ReadString(row, "Team"), ReadInt(row, "Alive")))
                .ToArray();

        private static IReadOnlyList<ArenaCrossEngineZoneInventoryRow> ReadZoneInventory(DataVoContext context) =>
            ReadRows(context, ArenaCrossEngineWorkload.ZoneInventorySql)
                .Select(row => new ArenaCrossEngineZoneInventoryRow(ReadInt(row, "p.Id"), ReadInt(row, "i.ItemId")))
                .ToArray();

        private static IReadOnlyList<Dictionary<string, object?>> ReadRows(DataVoContext context, string sql)
        {
            List<DataVo.Core.Contracts.Results.QueryResult> results = context.Execute(sql);
            foreach (DataVo.Core.Contracts.Results.QueryResult result in results)
            {
                if (result.IsError)
                {
                    throw new InvalidOperationException("DataVo rejected arena SQL '" + sql + "': " + string.Join(" | ", result.Messages));
                }
            }

            if (results.Count != 1)
            {
                throw new InvalidOperationException("Expected one DataVo query result for SQL: " + sql);
            }

            return results[0].Data;
        }

        private static void ExecuteOk(DataVoContext context, string sql)
        {
            List<DataVo.Core.Contracts.Results.QueryResult> results = context.Execute(sql);
            foreach (DataVo.Core.Contracts.Results.QueryResult result in results)
            {
                if (result.IsError)
                {
                    throw new InvalidOperationException("DataVo rejected arena SQL '" + sql + "': " + string.Join(" | ", result.Messages));
                }
            }
        }

        private static void RemoveRows(
            IReadOnlyList<IReadOnlyDictionary<string, object?>> rows,
            Action<IReadOnlyDictionary<string, object?>> remove)
        {
            foreach (IReadOnlyDictionary<string, object?> row in rows)
            {
                remove(row);
            }
        }

        private static void UpsertRows(
            IReadOnlyList<IReadOnlyDictionary<string, object?>> rows,
            Action<IReadOnlyDictionary<string, object?>> upsert)
        {
            foreach (IReadOnlyDictionary<string, object?> row in rows)
            {
                upsert(row);
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

    }
}
