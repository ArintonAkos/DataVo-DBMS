#nullable enable

using NUnit.Framework;

namespace DataVo.UnityArena.EditorTests
{
    public sealed class ArenaHeadlessEditModeTests
    {
        [Test]
        public void ShortRun_ReportsPassedSchemaAndMeasuredTicks()
        {
            ArenaRunResult result = ArenaBenchmark.Run(new ArenaRunOptions
            {
                Mode = ArenaDeliveryMode.Reactive,
                LogicalNpcCount = 32,
                MemoriesPerNpc = 4,
                MeasuredTicks = 8
            });

            Assert.That(result.schemaVersion, Is.EqualTo(1));
            Assert.That(result.status, Is.EqualTo("passed"));
            Assert.That(result.tick.count, Is.EqualTo(8));
            Assert.That(result.error, Is.Empty);
        }

        [Test]
        public void Invocation_RequiresSafeRunIdAndRecognisedMode()
        {
            ArenaHeadlessInvocation invocation = ArenaHeadlessBootstrap.ParseInvocation(new[]
            {
                "-datavoArenaMode", "reactive",
                "-datavoArenaRunId", "editor_test_01",
                "-datavoArenaTicks", "8"
            });

            Assert.That(invocation.mode, Is.EqualTo(ArenaDeliveryMode.Reactive));
            Assert.That(invocation.runId, Is.EqualTo("editor_test_01"));
            Assert.That(invocation.measuredTicks, Is.EqualTo(8));
        }
    }
}
