#nullable enable

using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace DataVo.UnityArena.Sqlite
{
    public sealed class ArenaSqlitePollingSession : IDisposable
    {
        private IntPtr database;
        private IntPtr insertPlayerStatement;
        private IntPtr insertInventoryStatement;
        private IntPtr updatePlayerStatement;
        private IntPtr updateInventoryStatement;
        private IntPtr leaderboardStatement;
        private IntPtr aggregateStatement;
        private IntPtr zoneInventoryStatement;
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
                PrepareStatements();
                Seed(profile);
            }
            catch
            {
                try
                {
                    Dispose();
                }
                catch
                {
                    // Preserve the constructor error after attempting every statement cleanup.
                }

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
            Exception? cleanupError = null;
            FinalizeStatement(ref insertPlayerStatement, ref cleanupError);
            FinalizeStatement(ref insertInventoryStatement, ref cleanupError);
            FinalizeStatement(ref updatePlayerStatement, ref cleanupError);
            FinalizeStatement(ref updateInventoryStatement, ref cleanupError);
            FinalizeStatement(ref leaderboardStatement, ref cleanupError);
            FinalizeStatement(ref aggregateStatement, ref cleanupError);
            FinalizeStatement(ref zoneInventoryStatement, ref cleanupError);

            IntPtr closingDatabase = database;
            database = IntPtr.Zero;

            if (closingDatabase != IntPtr.Zero)
            {
                try
                {
                    int closeStatus = ArenaSqliteNative.sqlite3_close_v2(closingDatabase);
                    ArenaSqliteNative.RequireStatus(closingDatabase, closeStatus, "sqlite3_close_v2");
                }
                catch (Exception exception)
                {
                    cleanupError ??= exception;
                }
            }

            if (cleanupError is not null)
            {
                throw cleanupError;
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
            ResetAndClear(insertPlayerStatement);
            BindInt(insertPlayerStatement, 1, player.id);
            BindText(insertPlayerStatement, 2, player.team);
            BindText(insertPlayerStatement, 3, player.zone);
            BindInt(insertPlayerStatement, 4, player.health);
            BindInt(insertPlayerStatement, 5, player.score);
            StepDone(insertPlayerStatement);
        }

        private void InsertInventory(ArenaCrossEngineInventory inventory)
        {
            ResetAndClear(insertInventoryStatement);
            BindInt(insertInventoryStatement, 1, inventory.inventoryId);
            BindInt(insertInventoryStatement, 2, inventory.playerId);
            BindInt(insertInventoryStatement, 3, inventory.itemId);
            BindInt(insertInventoryStatement, 4, inventory.slot);
            StepDone(insertInventoryStatement);
        }

        private void UpdatePlayer(ArenaCrossEngineMutation mutation)
        {
            ResetAndClear(updatePlayerStatement);
            BindInt(updatePlayerStatement, 1, mutation.scoreDelta);
            BindInt(updatePlayerStatement, 2, mutation.replacementHealth);
            BindText(updatePlayerStatement, 3, mutation.replacementZone);
            BindInt(updatePlayerStatement, 4, mutation.playerId);
            StepDone(updatePlayerStatement);
        }

        private void UpdateInventory(ArenaCrossEngineMutation mutation)
        {
            ResetAndClear(updateInventoryStatement);
            BindInt(updateInventoryStatement, 1, mutation.inventoryItemId);
            BindInt(updateInventoryStatement, 2, mutation.inventorySlot);
            BindInt(updateInventoryStatement, 3, mutation.playerId);
            StepDone(updateInventoryStatement);
        }

        private IReadOnlyList<ArenaCrossEngineLeaderboardRow> ReadLeaderboard()
        {
            var rows = new List<ArenaCrossEngineLeaderboardRow>();
            ResetAndClear(leaderboardStatement);
            while (true)
            {
                int status = ArenaSqliteNative.sqlite3_step(leaderboardStatement);
                if (status == ArenaSqliteNative.SQLITE_DONE)
                {
                    break;
                }

                ArenaSqliteNative.RequireStatus(database, status, "sqlite3_step", acceptsRow: true);
                rows.Add(new ArenaCrossEngineLeaderboardRow(
                    ArenaSqliteNative.sqlite3_column_int(leaderboardStatement, 0),
                    ArenaSqliteNative.sqlite3_column_int(leaderboardStatement, 1)));
            }

            return rows;
        }

        private IReadOnlyList<ArenaCrossEngineAggregateRow> ReadAggregate()
        {
            var rows = new List<ArenaCrossEngineAggregateRow>();
            ResetAndClear(aggregateStatement);
            while (true)
            {
                int status = ArenaSqliteNative.sqlite3_step(aggregateStatement);
                if (status == ArenaSqliteNative.SQLITE_DONE)
                {
                    break;
                }

                ArenaSqliteNative.RequireStatus(database, status, "sqlite3_step", acceptsRow: true);
                IntPtr team = ArenaSqliteNative.sqlite3_column_text(aggregateStatement, 0);
                rows.Add(new ArenaCrossEngineAggregateRow(
                    MarshalText(team),
                    ArenaSqliteNative.sqlite3_column_int(aggregateStatement, 1)));
            }

            return rows;
        }

        private IReadOnlyList<ArenaCrossEngineZoneInventoryRow> ReadZoneInventory()
        {
            var rows = new List<ArenaCrossEngineZoneInventoryRow>();
            ResetAndClear(zoneInventoryStatement);
            while (true)
            {
                int status = ArenaSqliteNative.sqlite3_step(zoneInventoryStatement);
                if (status == ArenaSqliteNative.SQLITE_DONE)
                {
                    break;
                }

                ArenaSqliteNative.RequireStatus(database, status, "sqlite3_step", acceptsRow: true);
                rows.Add(new ArenaCrossEngineZoneInventoryRow(
                    ArenaSqliteNative.sqlite3_column_int(zoneInventoryStatement, 0),
                    ArenaSqliteNative.sqlite3_column_int(zoneInventoryStatement, 1)));
            }

            return rows;
        }

        private void PrepareStatements()
        {
            Prepare("INSERT INTO Players (Id, Team, Zone, Health, Score) VALUES (?, ?, ?, ?, ?)", out insertPlayerStatement);
            Prepare("INSERT INTO Inventory (InventoryId, PlayerId, ItemId, Slot) VALUES (?, ?, ?, ?)", out insertInventoryStatement);
            Prepare("UPDATE Players SET Score = Score + ?, Health = ?, Zone = ? WHERE Id = ?", out updatePlayerStatement);
            Prepare("UPDATE Inventory SET ItemId = ?, Slot = ? WHERE InventoryId = ?", out updateInventoryStatement);
            Prepare(ArenaCrossEngineWorkload.LeaderboardSql, out leaderboardStatement);
            Prepare(ArenaCrossEngineWorkload.AggregateSql, out aggregateStatement);
            Prepare(ArenaCrossEngineWorkload.ZoneInventorySql, out zoneInventoryStatement);
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

        private void ResetAndClear(IntPtr statement)
        {
            int resetStatus = ArenaSqliteNative.sqlite3_reset(statement);
            ArenaSqliteNative.RequireStatus(database, resetStatus, "sqlite3_reset");
            int clearBindingsStatus = ArenaSqliteNative.sqlite3_clear_bindings(statement);
            ArenaSqliteNative.RequireStatus(database, clearBindingsStatus, "sqlite3_clear_bindings");
        }

        private void FinalizeStatement(ref IntPtr statement, ref Exception? cleanupError)
        {
            IntPtr closingStatement = statement;
            statement = IntPtr.Zero;
            if (closingStatement == IntPtr.Zero)
            {
                return;
            }

            try
            {
                int status = ArenaSqliteNative.sqlite3_finalize(closingStatement);
                ArenaSqliteNative.RequireStatus(database, status, "sqlite3_finalize");
            }
            catch (Exception exception)
            {
                cleanupError ??= exception;
            }
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
