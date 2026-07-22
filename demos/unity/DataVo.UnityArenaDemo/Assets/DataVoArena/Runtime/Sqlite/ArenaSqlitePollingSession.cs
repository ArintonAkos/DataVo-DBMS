#nullable enable

using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace DataVo.UnityArena.Sqlite
{
    public sealed class ArenaSqlitePollingSession : IDisposable
    {
        private IntPtr database;
        private readonly string sqliteVersion;
        private bool recordPollingRows;
        private bool disposed;

        public ArenaSqlitePollingSession(ArenaCrossEngineProfile profile)
        {
            ArenaCrossEngineWorkload.ValidateProfile(profile);
            sqliteVersion = ArenaSqliteNative.GetLibraryVersion();

            try
            {
                int openStatus = ArenaSqliteNative.sqlite3_open_v2(
                    ":memory:",
                    out database,
                    ArenaSqliteNative.SQLITE_OPEN_READWRITE |
                    ArenaSqliteNative.SQLITE_OPEN_CREATE |
                    ArenaSqliteNative.SQLITE_OPEN_MEMORY,
                    IntPtr.Zero);
                ArenaSqliteNative.RequireStatus(database, openStatus, "sqlite3_open_v2");
                Execute(ArenaCrossEngineWorkload.CreatePlayersSql);
                Execute(ArenaCrossEngineWorkload.CreateInventorySql);
                Execute("CREATE INDEX IX_Players_Zone ON Players (Zone)");
                Execute("CREATE INDEX IX_Inventory_PlayerId ON Inventory (PlayerId)");
                Seed(profile);
            }
            catch
            {
                Dispose();
                throw;
            }
        }

        public string SqliteVersion => sqliteVersion;

        public long PollingRows { get; private set; }

        public void BeginMeasuredInterval()
        {
            ThrowIfDisposed();
            recordPollingRows = true;
            PollingRows = 0;
        }

        public void ApplyBatch(IReadOnlyList<ArenaCrossEngineMutation> batch)
        {
            ThrowIfDisposed();
            if (batch is null)
            {
                throw new ArgumentNullException(nameof(batch));
            }

            Execute("BEGIN IMMEDIATE");
            try
            {
                foreach (ArenaCrossEngineMutation mutation in batch)
                {
                    UpdatePlayer(mutation);
                    UpdateInventory(mutation);
                }

                Execute("COMMIT");
            }
            catch
            {
                TryRollback();
                throw;
            }
        }

        public ArenaCrossEngineReadResult DemandViews()
        {
            ThrowIfDisposed();

            long leaderboardStart = Stopwatch.GetTimestamp();
            IReadOnlyList<ArenaCrossEngineLeaderboardRow> leaderboard = ReadLeaderboard();
            long aggregateStart = Stopwatch.GetTimestamp();
            IReadOnlyList<ArenaCrossEngineAggregateRow> aggregate = ReadAggregate();
            long zoneInventoryStart = Stopwatch.GetTimestamp();
            IReadOnlyList<ArenaCrossEngineZoneInventoryRow> zoneInventory = ReadZoneInventory();
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
            IntPtr closingDatabase = database;
            database = IntPtr.Zero;

            if (closingDatabase != IntPtr.Zero)
            {
                int closeStatus = ArenaSqliteNative.sqlite3_close_v2(closingDatabase);
                ArenaSqliteNative.RequireStatus(closingDatabase, closeStatus, "sqlite3_close_v2");
            }
        }

        private void Seed(ArenaCrossEngineProfile profile)
        {
            Execute("BEGIN IMMEDIATE");
            try
            {
                foreach (ArenaCrossEnginePlayer player in ArenaCrossEngineWorkload.CreateInitialPlayers(profile))
                {
                    InsertPlayer(player);
                }

                foreach (ArenaCrossEngineInventory inventory in ArenaCrossEngineWorkload.CreateInitialInventory(profile))
                {
                    InsertInventory(inventory);
                }

                Execute("COMMIT");
            }
            catch
            {
                TryRollback();
                throw;
            }
        }

        private void InsertPlayer(ArenaCrossEnginePlayer player)
        {
            IntPtr statement = IntPtr.Zero;
            try
            {
                Prepare("INSERT INTO Players (Id, Team, Zone, Health, Score) VALUES (?, ?, ?, ?, ?)", out statement);
                BindInt(statement, 1, player.id);
                BindText(statement, 2, player.team);
                BindText(statement, 3, player.zone);
                BindInt(statement, 4, player.health);
                BindInt(statement, 5, player.score);
                StepDone(statement);
            }
            finally
            {
                FinalizeStatement(statement);
            }
        }

        private void InsertInventory(ArenaCrossEngineInventory inventory)
        {
            IntPtr statement = IntPtr.Zero;
            try
            {
                Prepare("INSERT INTO Inventory (InventoryId, PlayerId, ItemId, Slot) VALUES (?, ?, ?, ?)", out statement);
                BindInt(statement, 1, inventory.inventoryId);
                BindInt(statement, 2, inventory.playerId);
                BindInt(statement, 3, inventory.itemId);
                BindInt(statement, 4, inventory.slot);
                StepDone(statement);
            }
            finally
            {
                FinalizeStatement(statement);
            }
        }

        private void UpdatePlayer(ArenaCrossEngineMutation mutation)
        {
            IntPtr statement = IntPtr.Zero;
            try
            {
                Prepare("UPDATE Players SET Score = Score + ?, Health = ?, Zone = ? WHERE Id = ?", out statement);
                BindInt(statement, 1, mutation.scoreDelta);
                BindInt(statement, 2, mutation.replacementHealth);
                BindText(statement, 3, mutation.replacementZone);
                BindInt(statement, 4, mutation.playerId);
                StepDone(statement);
            }
            finally
            {
                FinalizeStatement(statement);
            }
        }

        private void UpdateInventory(ArenaCrossEngineMutation mutation)
        {
            IntPtr statement = IntPtr.Zero;
            try
            {
                Prepare("UPDATE Inventory SET ItemId = ?, Slot = ? WHERE InventoryId = ?", out statement);
                BindInt(statement, 1, mutation.inventoryItemId);
                BindInt(statement, 2, mutation.inventorySlot);
                BindInt(statement, 3, mutation.playerId);
                StepDone(statement);
            }
            finally
            {
                FinalizeStatement(statement);
            }
        }

        private IReadOnlyList<ArenaCrossEngineLeaderboardRow> ReadLeaderboard()
        {
            var rows = new List<ArenaCrossEngineLeaderboardRow>();
            IntPtr statement = IntPtr.Zero;
            try
            {
                Prepare(ArenaCrossEngineWorkload.LeaderboardSql, out statement);
                while (true)
                {
                    int status = ArenaSqliteNative.sqlite3_step(statement);
                    if (status == ArenaSqliteNative.SQLITE_DONE)
                    {
                        break;
                    }

                    ArenaSqliteNative.RequireStatus(database, status, "sqlite3_step", acceptsRow: true);
                    rows.Add(new ArenaCrossEngineLeaderboardRow(
                        ArenaSqliteNative.sqlite3_column_int(statement, 0),
                        ArenaSqliteNative.sqlite3_column_int(statement, 1)));
                }
            }
            finally
            {
                FinalizeStatement(statement);
            }

            return rows;
        }

        private IReadOnlyList<ArenaCrossEngineAggregateRow> ReadAggregate()
        {
            var rows = new List<ArenaCrossEngineAggregateRow>();
            IntPtr statement = IntPtr.Zero;
            try
            {
                Prepare(ArenaCrossEngineWorkload.AggregateSql, out statement);
                while (true)
                {
                    int status = ArenaSqliteNative.sqlite3_step(statement);
                    if (status == ArenaSqliteNative.SQLITE_DONE)
                    {
                        break;
                    }

                    ArenaSqliteNative.RequireStatus(database, status, "sqlite3_step", acceptsRow: true);
                    IntPtr team = ArenaSqliteNative.sqlite3_column_text(statement, 0);
                    rows.Add(new ArenaCrossEngineAggregateRow(
                        MarshalText(team),
                        ArenaSqliteNative.sqlite3_column_int(statement, 1)));
                }
            }
            finally
            {
                FinalizeStatement(statement);
            }

            return rows;
        }

        private IReadOnlyList<ArenaCrossEngineZoneInventoryRow> ReadZoneInventory()
        {
            var rows = new List<ArenaCrossEngineZoneInventoryRow>();
            IntPtr statement = IntPtr.Zero;
            try
            {
                Prepare(ArenaCrossEngineWorkload.ZoneInventorySql, out statement);
                while (true)
                {
                    int status = ArenaSqliteNative.sqlite3_step(statement);
                    if (status == ArenaSqliteNative.SQLITE_DONE)
                    {
                        break;
                    }

                    ArenaSqliteNative.RequireStatus(database, status, "sqlite3_step", acceptsRow: true);
                    rows.Add(new ArenaCrossEngineZoneInventoryRow(
                        ArenaSqliteNative.sqlite3_column_int(statement, 0),
                        ArenaSqliteNative.sqlite3_column_int(statement, 1)));
                }
            }
            finally
            {
                FinalizeStatement(statement);
            }

            return rows;
        }

        private void Execute(string sql)
        {
            int status = ArenaSqliteNative.sqlite3_exec(database, sql, IntPtr.Zero, IntPtr.Zero, IntPtr.Zero);
            ArenaSqliteNative.RequireStatus(database, status, "sqlite3_exec");
        }

        private void Prepare(string sql, out IntPtr statement)
        {
            int status = ArenaSqliteNative.sqlite3_prepare_v2(database, sql, -1, out statement, IntPtr.Zero);
            ArenaSqliteNative.RequireStatus(database, status, "sqlite3_prepare_v2");
        }

        private void BindInt(IntPtr statement, int index, int value)
        {
            int status = ArenaSqliteNative.sqlite3_bind_int(statement, index, value);
            ArenaSqliteNative.RequireStatus(database, status, "sqlite3_bind_int");
        }

        private void BindText(IntPtr statement, int index, string value)
        {
            int status = ArenaSqliteNative.sqlite3_bind_text(
                statement,
                index,
                value,
                -1,
                ArenaSqliteNative.SQLITE_TRANSIENT);
            ArenaSqliteNative.RequireStatus(database, status, "sqlite3_bind_text");
        }

        private void StepDone(IntPtr statement)
        {
            int status = ArenaSqliteNative.sqlite3_step(statement);
            ArenaSqliteNative.RequireStatus(database, status, "sqlite3_step", acceptsDone: true);
        }

        private void FinalizeStatement(IntPtr statement)
        {
            if (statement == IntPtr.Zero)
            {
                return;
            }

            int status = ArenaSqliteNative.sqlite3_finalize(statement);
            ArenaSqliteNative.RequireStatus(database, status, "sqlite3_finalize");
        }

        private void TryRollback()
        {
            try
            {
                Execute("ROLLBACK");
            }
            catch (InvalidOperationException)
            {
                // Preserve the mutation or seed error that initiated rollback.
            }
        }

        private static string MarshalText(IntPtr text)
        {
            if (text == IntPtr.Zero)
            {
                throw new InvalidOperationException("SQLite returned a null text value for a required column.");
            }

            return System.Runtime.InteropServices.Marshal.PtrToStringUTF8(text)
                ?? throw new InvalidOperationException("SQLite returned an invalid UTF-8 text value.");
        }

        private static double ElapsedMilliseconds(long start, long end) =>
            (end - start) * 1000d / Stopwatch.Frequency;

        private void ThrowIfDisposed()
        {
            if (disposed)
            {
                throw new ObjectDisposedException(nameof(ArenaSqlitePollingSession));
            }
        }
    }
}
