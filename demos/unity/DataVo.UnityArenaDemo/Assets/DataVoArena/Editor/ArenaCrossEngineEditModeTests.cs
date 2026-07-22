#nullable enable

using System;
using System.Linq;
using System.Reflection;
using DataVo.Core;
using DataVo.Core.Runtime.Reactive;
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
        public void ReactiveSession_UsesBorrowedDeltaCallbacksAndPreservesAllLiveViewHashes()
        {
            AssertBorrowedDeltaCallback("ApplyLeaderboardChange");
            AssertBorrowedDeltaCallback("ApplyAggregateChange");
            AssertBorrowedDeltaCallback("ApplyZoneInventoryChange");

            ArenaCrossEngineResult polling = ArenaCrossEngineRunner.Run(
                ArenaCrossEngineProfile.Gameplay, ArenaCrossEngineKind.DataVoPolling, repetitions: 1);
            ArenaCrossEngineResult reactive = ArenaCrossEngineRunner.Run(
                ArenaCrossEngineProfile.Gameplay, ArenaCrossEngineKind.DataVoReactive, repetitions: 1);

            Assert.That(reactive.status, Is.EqualTo("passed"));
            Assert.That(reactive.leaderboardChecksum, Is.EqualTo(polling.leaderboardChecksum));
            Assert.That(reactive.aggregateChecksum, Is.EqualTo(polling.aggregateChecksum));
            Assert.That(reactive.zoneInventoryChecksum, Is.EqualTo(polling.zoneInventoryChecksum));
        }

        [Test]
        public void DataVoMutationBatch_DoesNotExecuteRenderedSqlUpdates()
        {
            Type? sessionType = typeof(ArenaCrossEngineRunner).GetNestedType(
                "DataVoPollingCrossEngineSession",
                BindingFlags.NonPublic);
            Assert.That(sessionType, Is.Not.Null, "DataVo polling session type was not found.");

            object? session = Activator.CreateInstance(
                sessionType!,
                BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
                binder: null,
                args: new object[] { ArenaCrossEngineProfile.Gameplay },
                culture: null);
            Assert.That(session, Is.Not.Null, "DataVo polling session could not be created.");

            try
            {
                FieldInfo? contextField = sessionType!.GetField("context", BindingFlags.Instance | BindingFlags.NonPublic);
                Assert.That(contextField, Is.Not.Null, "DataVo polling session context field was not found.");
                var context = (DataVoContext)contextField!.GetValue(session!)!;
                context.Diagnostics.Enabled = true;
                context.Diagnostics.Clear();

                MethodInfo? applyBatch = sessionType.GetMethod("ApplyBatch", BindingFlags.Instance | BindingFlags.Public);
                Assert.That(applyBatch, Is.Not.Null, "DataVo polling session ApplyBatch method was not found.");
                _ = applyBatch!.Invoke(session, new object[] { ArenaCrossEngineWorkload.CreateBatch(ArenaCrossEngineProfile.Gameplay, 0) });

                Assert.That(context.Diagnostics.LastQuery, Is.Null,
                    "Arena mutations must execute compiled updates without rendering SQL statements.");
            }
            finally
            {
                ((IDisposable)session!).Dispose();
            }
        }

        [Test]
        public void DataVoMutationBatch_RepeatedPlayerIdAccumulatesScoreAndKeepsFinalValues()
        {
            Type? sessionType = typeof(ArenaCrossEngineRunner).GetNestedType(
                "DataVoPollingCrossEngineSession",
                BindingFlags.NonPublic);
            Assert.That(sessionType, Is.Not.Null, "DataVo polling session type was not found.");

            object? session = Activator.CreateInstance(
                sessionType!,
                BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
                binder: null,
                args: new object[] { ArenaCrossEngineProfile.Gameplay },
                culture: null);
            Assert.That(session, Is.Not.Null, "DataVo polling session could not be created.");

            try
            {
                FieldInfo? contextField = sessionType!.GetField("context", BindingFlags.Instance | BindingFlags.NonPublic);
                MethodInfo? applyBatch = sessionType.GetMethod("ApplyBatch", BindingFlags.Instance | BindingFlags.Public);
                Assert.That(contextField, Is.Not.Null, "DataVo polling session context field was not found.");
                Assert.That(applyBatch, Is.Not.Null, "DataVo polling session ApplyBatch method was not found.");

                _ = applyBatch!.Invoke(session, new object[]
                {
                    new ArenaCrossEngineMutation[]
                    {
                        new(1, 3, 11, "arena-1", 777, 4),
                        new(1, 5, 22, "arena-7", 888, 6)
                    }
                });

                var context = (DataVoContext)contextField!.GetValue(session!)!;
                var player = context.Execute("SELECT Score, Health, Zone FROM Players WHERE Id = 1").Single().Data.Single();
                var inventory = context.Execute("SELECT ItemId, Slot FROM Inventory WHERE InventoryId = 1").Single().Data.Single();

                Assert.That(player["Score"], Is.EqualTo(45));
                Assert.That(player["Health"], Is.EqualTo(22));
                Assert.That(player["Zone"], Is.EqualTo("arena-7"));
                Assert.That(inventory["ItemId"], Is.EqualTo(888));
                Assert.That(inventory["Slot"], Is.EqualTo(6));
            }
            finally
            {
                ((IDisposable)session!).Dispose();
            }
        }

        [Test]
        public void UnavailableSqliteBoundary_ProducesAnExplicitFailure()
        {
            InvalidOperationException exception = Assert.Throws<InvalidOperationException>(() =>
                ArenaSqliteNative.RequireAvailableForTest(() => throw new DllNotFoundException("sqlite3")));

            Assert.That(exception.Message, Is.EqualTo(
                "macOS sqlite3 native library is unavailable; Unity cross-engine SQLite benchmark cannot run."));
        }

        private static void AssertBorrowedDeltaCallback(string methodName)
        {
            Type? sessionType = typeof(ArenaCrossEngineRunner).GetNestedType(
                "DataVoReactiveCrossEngineSession",
                BindingFlags.NonPublic);
            Assert.That(sessionType, Is.Not.Null, "Reactive cross-engine session type was not found.");

            MethodInfo? method = sessionType!.GetMethod(
                methodName,
                BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.That(method, Is.Not.Null, "Reactive callback was not found: " + methodName);

            ParameterInfo[] parameters = method!.GetParameters();
            Assert.That(parameters, Has.Length.EqualTo(1));
            Assert.That(parameters[0].ParameterType.IsByRef, Is.True);
            Assert.That(parameters[0].ParameterType.GetElementType(), Is.EqualTo(typeof(QueryChangeRef)));
        }
    }
}
