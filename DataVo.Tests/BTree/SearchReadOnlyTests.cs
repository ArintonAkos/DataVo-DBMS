using System.Collections.Concurrent;
using DataVo.Core.BTree;

namespace DataVo.Tests.BTree;

/// <summary>
/// L3 read-path allocation tests: a point lookup must return the index's matching row ids
/// without copying (zero-allocation) while preserving the per-call isolation guarantee that
/// <see cref="JsonBTreeIndex.Search"/> previously provided via a defensive copy taken under the lock.
/// </summary>
public class SearchReadOnlyTests
{
    private static JsonBTreeIndex BuildIndex(params (string Key, long RowId)[] entries)
    {
        var index = new JsonBTreeIndex(minDegree: 50);
        foreach (var (key, rowId) in entries)
        {
            index.Insert(key, rowId);
        }

        return index;
    }

    // ---- Parity with Search -------------------------------------------------

    [Fact]
    public void SearchReadOnly_Hit_ReturnsSameRowIds_AsSearch()
    {
        var index = BuildIndex(("alice", 1), ("bob", 2), ("carol", 3));

        Assert.Equal(index.Search("bob"), index.SearchReadOnly("bob"));
    }

    [Fact]
    public void SearchReadOnly_Miss_ReturnsEmpty()
    {
        var index = BuildIndex(("alice", 1));

        Assert.Empty(index.SearchReadOnly("missing"));
    }

    [Fact]
    public void SearchReadOnly_MultiValueKey_ReturnsAllRowIds()
    {
        var index = BuildIndex(("dup", 10), ("dup", 20), ("dup", 30));

        Assert.Equal(new[] { 10L, 20L, 30L }, index.SearchReadOnly("dup"));
    }

    // ---- Isolation (copy-on-write) ------------------------------------------

    // The defining guard: a snapshot handed out by SearchReadOnly must reflect the point-in-time
    // state and must NOT observe a later in-place mutation of the same key. Returning the index's
    // internal list by reference WITHOUT copy-on-write would let this snapshot mutate underneath the
    // caller (the classic "Collection was modified" / torn-read trap), so this test fails unless
    // duplicate-key appends replace the list reference instead of mutating it in place.
    [Fact]
    public void SearchReadOnly_Snapshot_IsIsolatedFromLaterMutationOfSameKey()
    {
        var index = BuildIndex(("k", 1), ("k", 2));

        IReadOnlyList<long> snapshot = index.SearchReadOnly("k");
        Assert.Equal(new[] { 1L, 2L }, snapshot);

        index.Insert("k", 3); // in-place append would corrupt the live snapshot

        Assert.Equal(2, snapshot.Count);
        Assert.Equal(new[] { 1L, 2L }, snapshot);              // snapshot unchanged
        Assert.Equal(new[] { 1L, 2L, 3L }, index.SearchReadOnly("k")); // fresh read sees the append
    }

    // ---- Allocation ---------------------------------------------------------

    private static long BytesPerOp(int n, Action op)
    {
        for (int i = 0; i < 500; i++) op(); // warm
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < n; i++) op();
        return (GC.GetAllocatedBytesForCurrentThread() - before) / n;
    }

    [Fact]
    public void SearchReadOnly_WarmHit_DoesNotAllocate()
    {
        var index = new JsonBTreeIndex(minDegree: 50);
        for (int i = 0; i < 1000; i++) index.Insert($"key{i}", i);

        long perCall = BytesPerOp(200_000, () => { _ = index.SearchReadOnly("key500"); });

        Assert.True(perCall <= 8, $"SearchReadOnly warm hit {perCall} B exceeds 8 B (still copying the value list)");
    }

    // ---- Concurrency stress -------------------------------------------------

    // Readers enumerate SearchReadOnly snapshots in full while writers append to the same keys.
    // With copy-on-write this never throws and every snapshot is a self-consistent contiguous prefix.
    [Fact]
    public void SearchReadOnly_ConcurrentReadersAndWriters_NoExceptionAndConsistent()
    {
        var index = new JsonBTreeIndex(minDegree: 8);
        index.Insert("hot", 0);

        var errors = new ConcurrentQueue<Exception>();
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(750));

        var writer = Task.Run(() =>
        {
            long next = 1;
            while (!cts.IsCancellationRequested)
            {
                index.Insert("hot", next++);
            }
        });

        var readers = Enumerable.Range(0, 4).Select(_ => Task.Run(() =>
        {
            while (!cts.IsCancellationRequested)
            {
                try
                {
                    IReadOnlyList<long> snapshot = index.SearchReadOnly("hot");
                    long expected = 0;
                    foreach (long rowId in snapshot) // full enumeration must not throw / tear
                    {
                        if (rowId != expected)
                        {
                            errors.Enqueue(new Exception($"Torn snapshot: expected {expected}, saw {rowId}"));
                            return;
                        }

                        expected++;
                    }
                }
                catch (Exception ex)
                {
                    errors.Enqueue(ex);
                    return;
                }
            }
        })).ToArray();

        Task.WaitAll([writer, .. readers]);

        Assert.True(errors.IsEmpty, errors.TryDequeue(out var first) ? first.ToString() : "no error");
    }
}
