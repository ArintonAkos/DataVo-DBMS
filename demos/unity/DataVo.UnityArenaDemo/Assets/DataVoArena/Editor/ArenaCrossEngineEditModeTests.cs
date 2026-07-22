#nullable enable

using System;
using DataVo.UnityArena.Sqlite;
using NUnit.Framework;

namespace DataVo.UnityArena.EditorTests
{
    public sealed class ArenaCrossEngineEditModeTests
    {
        [Test]
        public void SameProfileAndBatch_ProducesTheDesktopCompatibleHashFormat()
        {
            ArenaCrossEngineProfile profile = ArenaCrossEngineProfile.Gameplay;
            ArenaCrossEngineHashes first = ArenaCrossEngineWorkload.Hash(
                ArenaCrossEngineWorkload.CreateExpectedViews(profile, batchCount: 3));
            ArenaCrossEngineHashes second = ArenaCrossEngineWorkload.Hash(
                ArenaCrossEngineWorkload.CreateExpectedViews(profile, batchCount: 3));

            Assert.That(first, Is.EqualTo(second));
            Assert.That(first.ZoneInventory.Length, Is.EqualTo(64));
        }

        [Test]
        public void IncrementalExpectedHashes_MatchFullHistoryWithoutRepeatingItPerDemand()
        {
            ArenaCrossEngineProfile profile = ArenaCrossEngineProfile.Gameplay;
            ArenaCrossEngineWorkload.ResetFullHistoryExpectedViewCallsForTest();

            ArenaCrossEngineHashes[] incremental =
                ArenaCrossEngineWorkload.CreateExpectedHashSequence(profile, batchCount: 4);

            Assert.That(incremental, Has.Length.EqualTo(4));
            Assert.That(ArenaCrossEngineWorkload.FullHistoryExpectedViewCallsForTest, Is.EqualTo(0));

            for (int batchCount = 1; batchCount <= incremental.Length; batchCount++)
            {
                ArenaCrossEngineHashes fullHistory = ArenaCrossEngineWorkload.Hash(
                    ArenaCrossEngineWorkload.CreateExpectedViews(profile, batchCount));
                Assert.That(incremental[batchCount - 1], Is.EqualTo(fullHistory));
            }

            Assert.That(ArenaCrossEngineWorkload.FullHistoryExpectedViewCallsForTest, Is.EqualTo(4));
        }

        [Test]
        public void AllEngines_ReportCompatibleGameplayHashes()
        {
            string sqliteVersion = ArenaSqliteNative.GetLibraryVersion();
            ArenaCrossEngineResult sqlite = ArenaCrossEngineRunner.Run(
                ArenaCrossEngineProfile.Gameplay, ArenaCrossEngineKind.SqlitePolling, repetitions: 1);
            ArenaCrossEngineResult dataVo = ArenaCrossEngineRunner.Run(
                ArenaCrossEngineProfile.Gameplay, ArenaCrossEngineKind.DataVoPolling, repetitions: 1);
            ArenaCrossEngineResult reactive = ArenaCrossEngineRunner.Run(
                ArenaCrossEngineProfile.Gameplay, ArenaCrossEngineKind.DataVoReactive, repetitions: 1);

            Assert.That(sqliteVersion, Is.Not.Empty);
            Assert.That(sqlite.status, Is.EqualTo("passed"));
            Assert.That(sqlite.sqliteVersion, Is.EqualTo(sqliteVersion));
            Assert.That(dataVo.status, Is.EqualTo("passed"));
            Assert.That(reactive.status, Is.EqualTo("passed"));
            Assert.That(sqlite.leaderboardChecksum, Is.EqualTo(dataVo.leaderboardChecksum));
            Assert.That(sqlite.aggregateChecksum, Is.EqualTo(dataVo.aggregateChecksum));
            Assert.That(sqlite.zoneInventoryChecksum, Is.EqualTo(dataVo.zoneInventoryChecksum));
            Assert.That(reactive.leaderboardChecksum, Is.EqualTo(dataVo.leaderboardChecksum));
            Assert.That(reactive.aggregateChecksum, Is.EqualTo(dataVo.aggregateChecksum));
            Assert.That(reactive.zoneInventoryChecksum, Is.EqualTo(dataVo.zoneInventoryChecksum));
        }

        [Test]
        public void UnavailableSqliteBoundary_ProducesAnExplicitFailure()
        {
            InvalidOperationException exception = Assert.Throws<InvalidOperationException>(() =>
                ArenaSqliteNative.RequireAvailableForTest(() => throw new DllNotFoundException("sqlite3")));

            Assert.That(exception.Message, Is.EqualTo(
                "macOS sqlite3 native library is unavailable; Unity cross-engine SQLite benchmark cannot run."));
        }
    }
}
