#nullable enable

using System.Linq;
using NUnit.Framework;

namespace DataVo.UnityArena.EditorTests
{
    public sealed class ArenaSeedEditModeTests
    {
        [Test]
        public void MemoryVector_IsAlwaysThirtyTwoFiniteComponents()
        {
            float[] vector = ArenaSeed.MemoryVector(npcId: 7, memoryIndex: 3);

            Assert.That(vector, Has.Length.EqualTo(32));
            Assert.That(vector.All(float.IsFinite), Is.True);
        }

        [Test]
        public void MutationSchedule_ReplaysForTheSameSeed()
        {
            ArenaMutation[] first = ArenaSeed.CreateMutations(42, 100, 9, 12).ToArray();
            ArenaMutation[] second = ArenaSeed.CreateMutations(42, 100, 9, 12).ToArray();

            Assert.That(second, Is.EqualTo(first));
        }

        [Test]
        public void LatencySummary_UsesNearestRankPercentiles()
        {
            var recorder = new ArenaLatencyRecorder();

            foreach (double value in new[] { 1d, 2d, 3d, 4d, 5d })
            {
                recorder.AddMilliseconds(value);
            }

            Assert.That(recorder.Snapshot().p95Milliseconds, Is.EqualTo(5d));
        }
    }
}
