using DataVo.Core;
using DataVo.Core.StorageEngine.Lsm;

namespace DataVo.Tests.Lsm;

public sealed class LsmLatestRowStoreTests
{
    private static readonly RowKey Guid1 = RowKey.FromGuid(Guid.Parse("018f2c1e-7b3a-7cde-8f00-123456789abc"));
    private static readonly RowKey Guid2 = RowKey.FromGuid(Guid.Parse("f47ac10b-58cc-4372-a567-0e02b2c3d479"));

    // Keys on both sides of the dense range: negative, dense (engine ids), just past it, huge, GUIDs.
    private static readonly RowKey[] Keys =
    [
        long.MinValue,
        -7,
        0,
        1,
        4_097,
        LsmLatestRowStore.DenseLimit - 1,
        LsmLatestRowStore.DenseLimit,
        long.MaxValue,
        Guid1,
        Guid2,
    ];

    [Fact]
    public void EveryKind_OfKey_IsStoredReadAndDeleted()
    {
        var store = new LsmLatestRowStore();
        ulong sequence = 1;
        foreach (RowKey key in Keys)
        {
            store.Set(key, Value(key), sequence++);
        }

        Assert.Equal(Keys.Length, store.Count);
        foreach (RowKey key in Keys)
        {
            Assert.Equal(LatestRowLookup.Live, store.TryGet(key, out byte[] value));
            Assert.Equal(Value(key), value);
        }

        Assert.Equal(LatestRowLookup.Missing, store.TryGet(2, out _));
        Assert.Equal(LatestRowLookup.Missing, store.TryGet(-8, out _));
        Assert.Equal(LatestRowLookup.Missing, store.TryGet(new RowKey(1, 1), out _));

        store.SetTombstone(-7, sequence++);
        store.SetTombstone(Guid2, sequence++);
        Assert.Equal(LatestRowLookup.Tombstone, store.TryGet(-7, out _));
        Assert.Equal(LatestRowLookup.Tombstone, store.TryGet(Guid2, out _));
        Assert.Equal(Keys.Length, store.Count); // a tombstone replaces the version, it does not add one
    }

    [Fact]
    public void ForEachVersion_VisitsKeysInRowKeyOrder_AcrossDenseAndSparseKeys()
    {
        var store = new LsmLatestRowStore();
        ulong sequence = 1;
        foreach (RowKey key in Keys.Reverse())
        {
            store.Set(key, Value(key), sequence++);
        }

        store.SetTombstone(4_097, sequence);

        var visited = new List<(RowKey Key, bool Live)>();
        store.ForEachVersion(visited, static (list, key, _, value) => list.Add((key, value is not null)));

        Assert.Equal(Keys.Order(), visited.Select(v => v.Key));
        Assert.False(visited.Single(v => v.Key == 4_097).Live);
    }

    [Fact]
    public void EvictFlushed_DropsOnlyVersionsAtOrBelowTheFlushedSequence()
    {
        var store = new LsmLatestRowStore();
        store.Set(5, Value(5), 10);
        store.Set(Guid1, Value(Guid1), 10);
        store.Set(Guid2, Value(Guid2), 30);
        store.Set(-1, Value(-1), 30);

        store.EvictFlushed(5, flushedThrough: 20);
        store.EvictFlushed(Guid1, flushedThrough: 20);
        store.EvictFlushed(Guid2, flushedThrough: 20); // newer than the flush: stays
        store.EvictFlushed(-1, flushedThrough: 20);

        Assert.Equal(LatestRowLookup.Missing, store.TryGet(5, out _));
        Assert.Equal(LatestRowLookup.Missing, store.TryGet(Guid1, out _));
        Assert.Equal(LatestRowLookup.Live, store.TryGet(Guid2, out _));
        Assert.Equal(LatestRowLookup.Live, store.TryGet(-1, out _));
        Assert.Equal(2, store.Count);
    }

    [Fact]
    public void ConcurrentReaders_SeeOnlyCompleteVersions_WhileSparseKeysArePublished()
    {
        var store = new LsmLatestRowStore();
        RowKey[] keys = [.. Enumerable.Range(0, 512).Select(i => RowKey.FromGuid(new Guid(i, 0, 0, new byte[8])))];
        using var stop = new CancellationTokenSource();
        long reads = 0;
        Task[] readers = [.. Enumerable.Range(0, 3).Select(_ => Task.Run(() =>
        {
            while (!stop.IsCancellationRequested)
            {
                foreach (RowKey key in keys)
                {
                    if (store.TryGet(key, out byte[] value) == LatestRowLookup.Live)
                    {
                        Assert.Equal(Value(key), value);
                    }

                    Interlocked.Increment(ref reads);
                }
            }
        }))];

        for (ulong round = 1; round <= 50; round++)
        {
            foreach (RowKey key in keys)
            {
                store.Set(key, Value(key), round);
            }

            foreach (RowKey key in keys)
            {
                store.EvictFlushed(key, round);
            }
        }

        stop.Cancel();
        Task.WaitAll(readers);
        Assert.True(Interlocked.Read(ref reads) > 0);
        Assert.Equal(0, store.Count);
    }

    private static byte[] Value(RowKey key) => System.Text.Encoding.UTF8.GetBytes($"v-{key}");
}
