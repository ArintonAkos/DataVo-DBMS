#nullable enable

using NUnit.Framework;

namespace DataVo.UnityArena.EditorTests
{
    public sealed class ArenaVisualEditModeTests
    {
        [Test]
        public void BehaviourColours_AreStableAndDistinct()
        {
            Assert.That(ArenaVisualController.ColourFor("patrol"), Is.Not.EqualTo(ArenaVisualController.ColourFor("flee")));
            Assert.That(ArenaVisualController.ColourFor("assist"), Is.Not.EqualTo(ArenaVisualController.ColourFor("pursue")));
        }

        [Test]
        public void RenderSnapshot_TakesOnlyAFrameSnapshot()
        {
            Assert.That(typeof(ArenaVisualController).GetMethod("RenderSnapshot")!.GetParameters(), Has.Length.EqualTo(1));
        }
    }
}
