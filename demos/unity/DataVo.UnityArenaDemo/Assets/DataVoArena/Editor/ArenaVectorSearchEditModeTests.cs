#nullable enable

using System.Linq;
using NUnit.Framework;
using UnityEngine;

namespace DataVo.UnityArena.EditorTests
{
    public sealed class ArenaVectorSearchEditModeTests
    {
        [Test]
        public void SeededDangerStimulus_ReturnsDangerAsNearestMemory()
        {
            using var session = new ArenaDataSession(new ArenaRunOptions
            {
                LogicalNpcCount = 8,
                MemoriesPerNpc = 4
            });

            ArenaMemoryMatch nearest = session.FindNearestMemories(1, 1).Single();

            Assert.That(nearest.tag, Is.EqualTo("danger"));
            Assert.That(nearest.npcId, Is.EqualTo(1));
            Assert.That(nearest.distance, Is.LessThan(0.0001f));
        }

        [Test]
        public void PlayerPosition_UsesRegisteredUnityVector3Converter()
        {
            using var session = new ArenaDataSession(new ArenaRunOptions
            {
                LogicalNpcCount = 8,
                MemoriesPerNpc = 4
            });

            Vector3 position = session.ReadPlayerPosition(1);

            Assert.That(position, Is.EqualTo(ArenaSeed.CreatePlayers(8).First().position));
        }
    }
}
