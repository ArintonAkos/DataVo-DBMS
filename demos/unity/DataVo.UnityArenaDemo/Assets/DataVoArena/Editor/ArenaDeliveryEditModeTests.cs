#nullable enable

using NUnit.Framework;

namespace DataVo.UnityArena.EditorTests
{
    public sealed class ArenaDeliveryEditModeTests
    {
        [Test]
        public void ReactiveAndPolling_ProduceSameFinalView()
        {
            var common = new ArenaRunOptions
            {
                LogicalNpcCount = 64,
                MemoriesPerNpc = 4,
                WarmupTicks = 2,
                MeasuredTicks = 20,
                MutationsPerTick = 4
            };

            ArenaRunResult reactive = ArenaBenchmark.Run(common with { Mode = ArenaDeliveryMode.Reactive });
            ArenaRunResult polling = ArenaBenchmark.Run(common with { Mode = ArenaDeliveryMode.Polling });

            Assert.That(reactive.leaderboardChecksum, Is.EqualTo(polling.leaderboardChecksum));
            Assert.That(reactive.aggregateChecksum, Is.EqualTo(polling.aggregateChecksum));
        }

        [Test]
        public void DeliveryModes_ReportOnlyTheirOwnWorkCounter()
        {
            ArenaRunResult reactive = ArenaBenchmark.Run(new ArenaRunOptions
            {
                Mode = ArenaDeliveryMode.Reactive,
                LogicalNpcCount = 32,
                MemoriesPerNpc = 4,
                MeasuredTicks = 5
            });
            ArenaRunResult polling = ArenaBenchmark.Run(new ArenaRunOptions
            {
                Mode = ArenaDeliveryMode.Polling,
                LogicalNpcCount = 32,
                MemoriesPerNpc = 4,
                MeasuredTicks = 5
            });

            Assert.That(reactive.reactiveChanges, Is.GreaterThan(0));
            Assert.That(reactive.pollingRows, Is.EqualTo(0));
            Assert.That(polling.pollingRows, Is.GreaterThan(0));
            Assert.That(polling.reactiveChanges, Is.EqualTo(0));
        }
    }
}
