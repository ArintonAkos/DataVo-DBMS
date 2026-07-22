#nullable enable

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using DataVo.Core;
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
                ArenaCrossEngineHashes[] expectedHashes = ArenaCrossEngineWorkload.CreateExpectedHashSequence(
                    profile,
                    profile.warmupBatches + profile.measuredBatches);
                using ICrossEngineSession session = CreateSession(profile, engine);
                for (int batchIndex = 0; batchIndex < profile.warmupBatches; batchIndex++)
                {
                    session.ApplyBatch(ArenaCrossEngineWorkload.CreateBatch(profile, batchIndex));
                    ArenaCrossEngineReadResult read = session.DemandViews();
                    RequireExpectedHashes(expectedHashes, batchIndex + 1, read.views, engine);
                }

                session.BeginMeasuredInterval();
                var batchLatency = new ArenaLatencyRecorder();
                var leaderboardLatency = new ArenaLatencyRecorder();
                var aggregateLatency = new ArenaLatencyRecorder();
                var zoneInventoryLatency = new ArenaLatencyRecorder();
                double totalMilliseconds = 0d;
                ArenaCrossEngineHashes? finalHashes = null;

                for (int measuredIndex = 0; measuredIndex < profile.measuredBatches; measuredIndex++)
                {
                    int batchIndex = profile.warmupBatches + measuredIndex;
                    long batchStart = Stopwatch.GetTimestamp();
                    session.ApplyBatch(ArenaCrossEngineWorkload.CreateBatch(profile, batchIndex));
                    ArenaCrossEngineReadResult read = session.DemandViews();
                    long batchEnd = Stopwatch.GetTimestamp();
                    double elapsedMilliseconds = ElapsedMilliseconds(batchStart, batchEnd);

                    batchLatency.AddMilliseconds(elapsedMilliseconds);
                    leaderboardLatency.AddMilliseconds(read.leaderboardMilliseconds);
                    aggregateLatency.AddMilliseconds(read.aggregateMilliseconds);
                    zoneInventoryLatency.AddMilliseconds(read.zoneInventoryMilliseconds);
                    totalMilliseconds += elapsedMilliseconds;
                    finalHashes = RequireExpectedHashes(expectedHashes, batchIndex + 1, read.views, engine);
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

        private interface ICrossEngineSession : IDisposable
        {
            long ReactiveChanges { get; }
            long PollingRows { get; }
            string SqliteVersion { get; }

            void BeginMeasuredInterval();
            void ApplyBatch(IReadOnlyList<ArenaCrossEngineMutation> batch);
            ArenaCrossEngineReadResult DemandViews();
        }

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

            public void ApplyBatch(IReadOnlyList<ArenaCrossEngineMutation> batch) => session.ApplyBatch(batch);

            public ArenaCrossEngineReadResult DemandViews() => session.DemandViews();

            public void Dispose() => session.Dispose();
        }

        private sealed class DataVoPollingCrossEngineSession : ICrossEngineSession
        {
            private readonly DataVoContext context;
            private bool recordPollingRows;
            private bool disposed;

            public DataVoPollingCrossEngineSession(ArenaCrossEngineProfile profile)
            {
                context = CreateContext();
                Seed(context, profile);
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

            public void ApplyBatch(IReadOnlyList<ArenaCrossEngineMutation> batch)
            {
                ThrowIfDisposed();
                ApplyMutations(context, batch);
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
            private readonly List<IDisposable> subscriptions = new();
            private readonly Dictionary<int, ArenaCrossEngineLeaderboardRow> leaderboard = new();
            private readonly Dictionary<string, ArenaCrossEngineAggregateRow> aggregate = new(StringComparer.Ordinal);
            private readonly Dictionary<int, ArenaCrossEngineZoneInventoryRow> zoneInventory = new();
            private bool recordReactiveChanges;
            private bool disposed;

            public DataVoReactiveCrossEngineSession(ArenaCrossEngineProfile profile)
            {
                context = CreateContext();
                subscriptions.Add(context.Subscribe(ArenaCrossEngineWorkload.LeaderboardSql, ApplyLeaderboardChange));
                subscriptions.Add(context.Subscribe(ArenaCrossEngineWorkload.ReactiveAggregateSql, ApplyAggregateChange));
                subscriptions.Add(context.Subscribe(ArenaCrossEngineWorkload.ReactiveZoneInventorySql, ApplyZoneInventoryChange));
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

            public void ApplyBatch(IReadOnlyList<ArenaCrossEngineMutation> batch)
            {
                ThrowIfDisposed();
                ApplyMutations(context, batch);
                context.DispatchPendingNotifications();
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

            private void ApplyLeaderboardChange(QueryChange change)
            {
                CountReactiveRows(change);
                RemoveRows(change.Removed, row => leaderboard.Remove(ReadInt(row, "Id")));
                UpsertRows(change.Added, row =>
                {
                    int id = ReadInt(row, "Id");
                    leaderboard[id] = new ArenaCrossEngineLeaderboardRow(id, ReadInt(row, "Score"));
                });
                UpsertRows(change.Updated, row =>
                {
                    int id = ReadInt(row, "Id");
                    leaderboard[id] = new ArenaCrossEngineLeaderboardRow(id, ReadInt(row, "Score"));
                });
            }

            private void ApplyAggregateChange(QueryChange change)
            {
                CountReactiveRows(change);
                RemoveRows(change.Removed, row => aggregate.Remove(ReadString(row, "Team")));
                UpsertRows(change.Added, row =>
                {
                    string team = ReadString(row, "Team");
                    aggregate[team] = new ArenaCrossEngineAggregateRow(team, ReadInt(row, "Alive"));
                });
                UpsertRows(change.Updated, row =>
                {
                    string team = ReadString(row, "Team");
                    aggregate[team] = new ArenaCrossEngineAggregateRow(team, ReadInt(row, "Alive"));
                });
            }

            private void ApplyZoneInventoryChange(QueryChange change)
            {
                CountReactiveRows(change);
                RemoveRows(change.Removed, row => zoneInventory.Remove(ReadInt(row, "p.Id")));
                RemoveRows(change.UpdatedBefore, row => zoneInventory.Remove(ReadInt(row, "p.Id")));
                UpsertRows(change.Added, UpsertZoneInventory);
                UpsertRows(change.Updated, UpsertZoneInventory);
            }

            private void UpsertZoneInventory(IReadOnlyDictionary<string, object?> row)
            {
                int playerId = ReadInt(row, "p.Id");
                zoneInventory[playerId] = new ArenaCrossEngineZoneInventoryRow(playerId, ReadInt(row, "i.ItemId"));
            }

            private void CountReactiveRows(QueryChange change)
            {
                if (recordReactiveChanges)
                {
                    ReactiveChanges += change.Added.Count + change.Removed.Count + change.Updated.Count;
                }
            }

            private void ThrowIfDisposed()
            {
                if (disposed)
                {
                    throw new ObjectDisposedException(nameof(DataVoReactiveCrossEngineSession));
                }
            }
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

        private static void ApplyMutations(DataVoContext context, IReadOnlyList<ArenaCrossEngineMutation> batch)
        {
            if (batch is null)
            {
                throw new ArgumentNullException(nameof(batch));
            }

            foreach (ArenaCrossEngineMutation mutation in batch)
            {
                ExecuteOk(
                    context,
                    "UPDATE Players SET Score = Score + " + mutation.scoreDelta.ToString(CultureInfo.InvariantCulture) +
                    ", Health = " + mutation.replacementHealth.ToString(CultureInfo.InvariantCulture) +
                    ", Zone = " + SqlStringLiteral(mutation.replacementZone) +
                    " WHERE Id = " + mutation.playerId.ToString(CultureInfo.InvariantCulture));
                ExecuteOk(
                    context,
                    "UPDATE Inventory SET ItemId = " + mutation.inventoryItemId.ToString(CultureInfo.InvariantCulture) +
                    ", Slot = " + mutation.inventorySlot.ToString(CultureInfo.InvariantCulture) +
                    " WHERE InventoryId = " + mutation.playerId.ToString(CultureInfo.InvariantCulture));
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

        private static string SqlStringLiteral(string value)
        {
            if (string.IsNullOrEmpty(value))
            {
                throw new ArgumentException("SQL text value is required.", nameof(value));
            }

            return "'" + value.Replace("'", "''") + "'";
        }
    }
}
