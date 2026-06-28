using DataVo.Core.StorageEngine.Lsm;

namespace DataVo.Tests.Lsm;

public class MemTableTests
{
    private static byte[] Key(long pk)
    {
        var buf = new byte[8];
        InternalKey.EncodeInt64UserKey(buf, pk);
        return buf;
    }

    private static byte[] Val(string s) => System.Text.Encoding.UTF8.GetBytes(s);

    [Fact]
    public void Put_Then_TryGet_ReturnsValue()
    {
        using var table = new MemTable();
        table.Put(Key(1), seqno: 5, LsmValueType.Put, Val("hello"));

        bool found = table.TryGet(Key(1), snapshotSeqno: 10, out ReadOnlySpan<byte> value, out bool tomb);

        Assert.True(found);
        Assert.False(tomb);
        Assert.Equal("hello", System.Text.Encoding.UTF8.GetString(value));
        Assert.Equal(1, table.Count);
    }

    [Fact]
    public void TryGet_MissingKey_ReturnsFalse()
    {
        using var table = new MemTable();
        table.Put(Key(1), 5, LsmValueType.Put, Val("a"));

        Assert.False(table.TryGet(Key(2), 10, out _, out _));
    }

    [Fact]
    public void TryGet_ReturnsNewestVersionAtOrBelowSnapshot()
    {
        using var table = new MemTable();
        table.Put(Key(1), 3, LsmValueType.Put, Val("old"));
        table.Put(Key(1), 7, LsmValueType.Put, Val("new"));

        // Snapshot above both → newest (seqno 7).
        Assert.True(table.TryGet(Key(1), 10, out ReadOnlySpan<byte> v1, out _));
        Assert.Equal("new", System.Text.Encoding.UTF8.GetString(v1));

        // Snapshot between them → only the older version is visible.
        Assert.True(table.TryGet(Key(1), 5, out ReadOnlySpan<byte> v2, out _));
        Assert.Equal("old", System.Text.Encoding.UTF8.GetString(v2));

        // Snapshot below both → not visible.
        Assert.False(table.TryGet(Key(1), 2, out _, out _));
    }

    [Fact]
    public void TryGet_ManyKeys_AllRetrievable()
    {
        using var table = new MemTable();
        for (int i = 0; i < 1000; i++)
        {
            table.Put(Key(i), (ulong)(i + 1), LsmValueType.Put, Val($"v{i}"));
        }

        Assert.Equal(1000, table.Count);
        for (int i = 0; i < 1000; i++)
        {
            Assert.True(table.TryGet(Key(i), 2000, out ReadOnlySpan<byte> v, out _), $"missing key {i}");
            Assert.Equal($"v{i}", System.Text.Encoding.UTF8.GetString(v));
        }
    }

    [Fact]
    public void ApproximateBytes_GrowsWithInserts()
    {
        using var table = new MemTable();
        long before = table.ApproximateBytes;
        table.Put(Key(1), 1, LsmValueType.Put, Val("xyz"));
        Assert.True(table.ApproximateBytes > before);
    }

    [Fact]
    public void Delete_ShadowsOlderPut_AtNewerSnapshot()
    {
        using var table = new MemTable();
        table.Put(Key(1), 3, LsmValueType.Put, Val("alive"));
        table.Delete(Key(1), 7);

        bool found = table.TryGet(Key(1), 10, out _, out bool tomb);

        Assert.False(found);
        Assert.True(tomb);
    }

    [Fact]
    public void Delete_DoesNotAffectReadsBelowTombstoneSeqno()
    {
        using var table = new MemTable();
        table.Put(Key(1), 3, LsmValueType.Put, Val("alive"));
        table.Delete(Key(1), 7);

        // A snapshot below the tombstone still sees the live value.
        Assert.True(table.TryGet(Key(1), 5, out ReadOnlySpan<byte> v, out bool tomb));
        Assert.False(tomb);
        Assert.Equal("alive", System.Text.Encoding.UTF8.GetString(v));
    }

    [Fact]
    public void Put_AfterDelete_Resurrects()
    {
        using var table = new MemTable();
        table.Put(Key(1), 3, LsmValueType.Put, Val("v3"));
        table.Delete(Key(1), 7);
        table.Put(Key(1), 9, LsmValueType.Put, Val("v9"));

        Assert.True(table.TryGet(Key(1), 10, out ReadOnlySpan<byte> v, out bool tomb));
        Assert.False(tomb);
        Assert.Equal("v9", System.Text.Encoding.UTF8.GetString(v));
    }

    [Fact]
    public void Enumerator_YieldsEntriesInInternalKeyOrder()
    {
        using var table = new MemTable();
        // Insert out of order, including two versions of key 5.
        table.Put(Key(5), 2, LsmValueType.Put, Val("five-old"));
        table.Put(Key(1), 1, LsmValueType.Put, Val("one"));
        table.Put(Key(5), 8, LsmValueType.Put, Val("five-new"));
        table.Put(Key(3), 4, LsmValueType.Put, Val("three"));

        var keys = new List<byte[]>();
        foreach (MemTableEntry entry in table)
        {
            keys.Add(entry.InternalKey.ToArray());
        }

        // Adjacent internal keys are strictly non-decreasing under InternalKey.Compare.
        for (int i = 1; i < keys.Count; i++)
        {
            Assert.True(InternalKey.Compare(keys[i - 1], keys[i]) <= 0, $"out of order at {i}");
        }

        // For user key 5, the newer seqno (8) sorts before the older (2).
        var fiveSeqnos = new List<ulong>();
        foreach (byte[] k in keys)
        {
            if (InternalKey.UserKey(k).SequenceEqual(Key(5)))
            {
                fiveSeqnos.Add(InternalKey.Sequence(k));
            }
        }

        Assert.Equal(2, fiveSeqnos.Count);
        Assert.Equal(8UL, fiveSeqnos[0]);
        Assert.Equal(2UL, fiveSeqnos[1]);
    }

    [Fact]
    public void Freeze_RejectsFurtherWrites_ButAllowsReads()
    {
        using var table = new MemTable();
        table.Put(Key(1), 1, LsmValueType.Put, Val("v"));
        table.Freeze();

        Assert.True(table.IsFrozen);
        Assert.Throws<InvalidOperationException>(() => table.Put(Key(2), 2, LsmValueType.Put, Val("x")));
        Assert.Throws<InvalidOperationException>(() => table.Delete(Key(3), 3));

        // Reads still work after freeze.
        Assert.True(table.TryGet(Key(1), 10, out _, out _));
    }

    [Fact]
    public void Put_SteadyState_IsAllocationFree()
    {
        // A large slab keeps the measured loop inside one slab (no slab rents → no GC).
        using var table = new MemTable(slabSize: 16 << 20);

        // Reuse the key buffer and value so the test harness itself allocates nothing per iteration —
        // any per-op allocation measured then comes from Put, which must be zero within a slab.
        var keyBuf = new byte[8];
        byte[] value = Val("payload");

        for (int i = 0; i < 200; i++) // warm
        {
            InternalKey.EncodeInt64UserKey(keyBuf, i);
            table.Put(keyBuf, (ulong)(i + 1), LsmValueType.Put, value);
        }

        const int n = 10_000;
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < n; i++)
        {
            InternalKey.EncodeInt64UserKey(keyBuf, 1_000_000 + i);
            table.Put(keyBuf, (ulong)(i + 1), LsmValueType.Put, value);
        }
        long perOp = (GC.GetAllocatedBytesForCurrentThread() - before) / n;

        Assert.True(perOp == 0, $"MemTable.Put steady-state allocated {perOp} B/op (expected 0)");
    }

    [Fact]
    public void ForwardPointerSlots_AreEightByteAlignedForVolatileReadsAndWrites()
    {
        Assert.Equal(0, MemTable.ForwardOffsetForTesting & 7);
    }

    [Fact]
    public async Task ConcurrentReaders_DoNotObserveCorruptSkiplistWhileSingleWriterPublishesNodes()
    {
        using var table = new MemTable(slabSize: 16 << 20);
        byte[] value = Val("v");
        using var cts = new CancellationTokenSource();
        Exception? readerFailure = null;

        Task[] readers = Enumerable.Range(0, Environment.ProcessorCount).Select(readerId => Task.Run(() =>
        {
            var key = new byte[8];
            int probe = readerId;
            try
            {
                while (Volatile.Read(ref readerFailure) is null && !cts.IsCancellationRequested)
                {
                    probe = unchecked((probe * 1103515245) + 12345);
                    int id = (probe & int.MaxValue) % 20_000;
                    InternalKey.EncodeInt64UserKey(key, id);
                    if (table.TryGet(key, InternalKey.MaxSequenceNumber, out ReadOnlySpan<byte> found, out bool tombstone))
                    {
                        if (tombstone || found.Length != value.Length || found[0] != value[0])
                        {
                            throw new InvalidOperationException("Reader observed a corrupt value.");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Interlocked.CompareExchange(ref readerFailure, ex, null);
                cts.Cancel();
            }
        })).ToArray();

        var writerKey = new byte[8];
        for (int i = 0; i < 20_000 && Volatile.Read(ref readerFailure) is null; i++)
        {
            InternalKey.EncodeInt64UserKey(writerKey, i);
            table.Put(writerKey, (ulong)(i + 1), LsmValueType.Put, value);
        }

        cts.Cancel();
        await Task.WhenAll(readers);

        if (readerFailure is not null)
        {
            throw readerFailure;
        }
    }
}
