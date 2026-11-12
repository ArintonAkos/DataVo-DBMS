using DataVo.Core;
using DataVo.Core.Indexing.Columnar;

namespace DataVo.Tests.Indexing;

public class RowKeyPositionMapTests
{
    [Fact]
    public void SetGetAndRemove_FollowADictionary_ThroughGrowthAndChurn()
    {
        var random = new Random(31);
        var map = new RowKeyPositionMap();
        var expected = new Dictionary<RowKey, int>();
        var keys = new List<RowKey>();
        for (int step = 0; step < 200_000; step++)
        {
            int action = random.Next(10);
            if (action < 6 || keys.Count == 0)
            {
                // Sequential integers, negative integers and GUIDs: the three shapes a row key takes.
                RowKey key = (step % 3) switch
                {
                    0 => (RowKey)(long)step,
                    1 => (RowKey)(-(long)random.Next(50_000)),
                    _ => RowKey.FromGuid(new Guid(random.Next(), 0, 0, 0, 0, 0, 0, 0, 0, 0, (byte)random.Next(256))),
                };
                map.Set(key, step);
                if (expected.TryAdd(key, step))
                {
                    keys.Add(key);
                }
                else
                {
                    expected[key] = step;
                }
            }
            else
            {
                int pick = random.Next(keys.Count);
                RowKey key = keys[pick];
                keys[pick] = keys[^1];
                keys.RemoveAt(keys.Count - 1);
                Assert.True(map.Remove(key, out int position));
                Assert.Equal(expected[key], position);
                expected.Remove(key);
                Assert.False(map.Remove(key, out _));
            }
        }

        Assert.Equal(expected.Count, map.Count);
        foreach ((RowKey key, int position) in expected)
        {
            Assert.True(map.TryGet(key, out int found));
            Assert.Equal(position, found);
        }

        Assert.False(map.TryGet((RowKey)long.MinValue, out _));
    }

    [Fact]
    public void PositionZero_IsAPositionLikeAnyOther()
    {
        var map = new RowKeyPositionMap(capacity: 1_000);
        map.Set((RowKey)7L, 0);

        Assert.True(map.TryGet((RowKey)7L, out int position));
        Assert.Equal(0, position);
        Assert.True(map.Remove((RowKey)7L, out position));
        Assert.Equal(0, position);
        Assert.Equal(0, map.Count);
    }
}
