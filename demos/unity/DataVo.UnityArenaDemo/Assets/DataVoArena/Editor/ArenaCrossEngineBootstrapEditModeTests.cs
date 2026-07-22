#nullable enable

using System;
using DataVo.UnityArena;
using NUnit.Framework;

namespace DataVo.UnityArena.EditorTests
{
    public sealed class ArenaCrossEngineBootstrapEditModeTests
    {
        [Test]
        public void Invocation_RequiresKnownEngineProfileAndSafeRunId()
        {
            ArenaCrossEngineInvocation invocation = ArenaCrossEngineBootstrap.ParseInvocation(new[]
            {
                "-datavoArenaEngine", "sqlite",
                "-datavoArenaProfile", "gameplay",
                "-datavoArenaRunId", "sqlite_gameplay_01",
                "-datavoArenaRepetitions", "1"
            });

            Assert.That(invocation.engine, Is.EqualTo(ArenaCrossEngineKind.SqlitePolling));
            Assert.That(invocation.profile.Id, Is.EqualTo("gameplay"));
            Assert.That(invocation.repetitions, Is.EqualTo(1));
        }

        [Test]
        public void Invocation_RejectsNonAsciiRunId()
        {
            ArgumentException exception = Assert.Throws<ArgumentException>(() =>
                ArenaCrossEngineBootstrap.ParseInvocation(new[]
                {
                    "-datavoArenaEngine", "sqlite",
                    "-datavoArenaProfile", "gameplay",
                    "-datavoArenaRunId", "sqlite_é",
                    "-datavoArenaRepetitions", "1"
                }));

            Assert.That(exception.Message, Does.Contain("-datavoArenaRunId"));
        }

        [Test]
        public void Invocation_RejectsDuplicateRepetitions()
        {
            ArgumentException exception = Assert.Throws<ArgumentException>(() =>
                ArenaCrossEngineBootstrap.ParseInvocation(new[]
                {
                    "-datavoArenaEngine", "sqlite",
                    "-datavoArenaProfile", "gameplay",
                    "-datavoArenaRunId", "sqlite_gameplay_01",
                    "-datavoArenaRepetitions", "1",
                    "-datavoArenaRepetitions", "2"
                }));

            Assert.That(exception.Message, Does.Contain("-datavoArenaRepetitions"));
        }
    }
}
