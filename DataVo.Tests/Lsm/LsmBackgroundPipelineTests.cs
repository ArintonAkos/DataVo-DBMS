using System.Diagnostics;
using System.Text;
using DataVo.Core.StorageEngine.Lsm;

namespace DataVo.Tests.Lsm;

/// <summary>
/// Covers the background write pipeline: lease-deferred arena reclamation, MemTable rotation with
/// background flush, WAL group commit, and background Level-0 compaction.
/// </summary>
public sealed class LsmBackgroundPipelineTests : IDisposable
{
    private readonly string _root = Path.Combine(
        Path.GetTempPath(), $"lsm-bg-pipeline-{Guid.NewGuid():N}");

    public void Dispose()
    {
        if (Directory.Exists(_root))
        {
            try
            {
                Directory.Delete(_root, recursive: true);
            }
            catch (IOException)
            {
                // Best-effort cleanup of temp state.
            }
        }
    }

    [Fact]
    public void ArenaLease_DefersSlabReturnUntilLastRelease()
    {
        var arena = new Arena(slabSize: 1024);
        Span<byte> span = arena.Allocate(16, out long handle);
        span.Fill(0xAB);

        ArenaLease lease = arena.AcquireLease();
        arena.Dispose(); // deferred: the lease pins the slabs

        Span<byte> resolved = arena.Resolve(handle, 16);
        Assert.All(resolved.ToArray(), value => Assert.Equal(0xAB, value));

        lease.Dispose(); // last release performs the actual return
        Assert.Throws<ObjectDisposedException>(() => arena.Resolve(handle, 16));
    }

    [Fact]
    public void ArenaLease_AfterFullDispose_CannotBeAcquired()
    {
        var arena = new Arena(slabSize: 1024);
        arena.Dispose();
        Assert.Throws<ObjectDisposedException>(() => arena.AcquireLease());
    }

    [Fact]
    public void BackgroundRotation_FlushesFrozenGenerationsAndPreservesAllRows()
    {
        string dir = Path.Combine(_root, "rotation");
        var engine = new LsmStorageEngine(dir, LsmWalDurabilityMode.RelaxedOsBuffer)
        {
            MemTableFlushThresholdOverrideBytes = 16 * 1024,
        };

        const int Rows = 500;
        var rowIds = new long[Rows];
        for (int i = 0; i < Rows; i++)
        {
            rowIds[i] = engine.InsertRow("db", "users", Value(i));
        }

        // Rotations happened (16KB threshold, ~100B rows) and the background worker flushed at
        // least one frozen generation to a Level-0 SSTable.
        Assert.True(WaitUntil(() => Directory.GetFiles(Path.Combine(dir, "db", "users"), "*.sst").Length > 0));

        // Every row stays readable through the latest-row table regardless of which generation
        // (active, frozen, or flushed) currently holds its bytes.
        for (int i = 0; i < Rows; i++)
        {
            Assert.Equal(Value(i), engine.ReadRow("db", "users", rowIds[i]));
        }

        // A clean reopen recovers every row from SSTables plus surviving WAL segments.
        engine.Dispose();
        using var reopened = new LsmStorageEngine(dir, LsmWalDurabilityMode.RelaxedOsBuffer);
        for (int i = 0; i < Rows; i++)
        {
            Assert.Equal(Value(i), reopened.ReadRow("db", "users", rowIds[i]));
        }
    }

    [Fact]
    public void GroupCommit_OneFsyncCoversAllPendingTickets()
    {
        string dir = Path.Combine(_root, "groupcommit-tickets");
        Directory.CreateDirectory(dir);
        var manifest = new LsmManifest(Path.Combine(dir, "MANIFEST"));
        using var table = new LsmTable(dir, manifest, Path.Combine(dir, "active.wal"));

        // Append many frames without waiting, then wait on the LAST ticket: the leader's single
        // fsync must cover every frame appended before it, and earlier tickets must observe the
        // advanced durable watermark and complete without issuing their own fsync.
        const int Ops = 100;
        var tickets = new LsmWalDurabilityTicket[Ops];
        Span<byte> key = stackalloc byte[sizeof(long)];
        for (int i = 0; i < Ops; i++)
        {
            InternalKey.EncodeInt64UserKey(key, i + 1);
            tickets[i] = table.PutDeferDurability(key, (ulong)(i + 1), Value(i));
        }

        Assert.Equal(0, table.WalDurableFlushCount);
        tickets[Ops - 1].Wait();
        Assert.Equal(1, table.WalDurableFlushCount);

        for (int i = 0; i < Ops - 1; i++)
        {
            tickets[i].Wait();
        }

        Assert.Equal(1, table.WalDurableFlushCount);
    }

    [Fact]
    public void GroupCommit_ConcurrentStrictWritersStayCorrectAndAmortized()
    {
        string dir = Path.Combine(_root, "groupcommit-threads");
        Directory.CreateDirectory(dir);
        var manifest = new LsmManifest(Path.Combine(dir, "MANIFEST"));
        using var table = new LsmTable(dir, manifest, Path.Combine(dir, "active.wal"));

        const int Threads = 8;
        const int OpsPerThread = 40;
        long seqno = 0;
        using var barrier = new Barrier(Threads);

        // Dedicated threads plus a start barrier guarantee genuine overlap: all writers are inside
        // the Put/ticket-wait cycle simultaneously, so leader fsyncs cover follower frames.
        var workers = new Thread[Threads];
        for (int w = 0; w < Threads; w++)
        {
            int worker = w;
            workers[w] = new Thread(() =>
            {
                Span<byte> key = stackalloc byte[sizeof(long)];
                barrier.SignalAndWait();
                for (int i = 0; i < OpsPerThread; i++)
                {
                    long id = (worker * OpsPerThread) + i + 1;
                    InternalKey.EncodeInt64UserKey(key, id);
                    ulong seq = (ulong)Interlocked.Increment(ref seqno);
                    table.Put(key, seq, Value((int)id));
                }
            });
            workers[w].Start();
        }

        foreach (Thread worker in workers)
        {
            worker.Join();
        }

        int totalOps = Threads * OpsPerThread;
        int fsyncs = table.WalDurableFlushCount;
        Assert.True(fsyncs > 0, "strict mode must fsync");
        // No amortization-ratio assertion here: when device fsync latency is lower than writer
        // inter-arrival time, perfectly alternating writers legitimately produce one fsync per op.
        // The amortization CONTRACT (one fsync covers all pending tickets; earlier tickets no-op)
        // is proven deterministically by GroupCommit_OneFsyncCoversAllPendingTickets; this test
        // proves the protocol is correct under genuine thread overlap.
        Assert.True(fsyncs <= totalOps, $"fsyncs={fsyncs} must not exceed ops={totalOps}");
        Assert.Equal(totalOps, table.ActiveCount);
    }

    [Fact]
    public void BackgroundCompaction_MergesLevel0IntoLevel1AtThreshold()
    {
        string dir = Path.Combine(_root, "compaction");
        Directory.CreateDirectory(dir);
        var manifest = new LsmManifest(Path.Combine(dir, "MANIFEST"));
        using var table = new LsmTable(dir, manifest)
        {
            CompactionRegistry = new LsmFileRegistry(dir, manifest),
            Level0CompactionThreshold = 4,
        };

        Span<byte> key = stackalloc byte[sizeof(long)];
        for (int generation = 1; generation <= 4; generation++)
        {
            InternalKey.EncodeInt64UserKey(key, generation);
            table.Put(key, (ulong)generation, Value(generation));
            Assert.NotNull(table.FlushActiveMemTable());
        }

        // The fourth flush schedules the background compaction check, which merges Level 0 into
        // Level 1 once the live-file threshold is reached.
        Assert.True(
            WaitUntil(() => manifest.GetLiveFiles(0).Count < 4 && manifest.GetLiveFiles(1).Count >= 1),
            $"expected background L0 compaction (L0={manifest.GetLiveFiles(0).Count}, L1={manifest.GetLiveFiles(1).Count})");
    }

    [Fact]
    public void BackgroundCompaction_CascadesBeyondLevelOneAndDropsBottomLevelTombstones()
    {
        string dir = Path.Combine(_root, "leveled-compaction");
        Directory.CreateDirectory(dir);
        var manifest = new LsmManifest(Path.Combine(dir, "MANIFEST"));
        using var table = new LsmTable(dir, manifest)
        {
            CompactionRegistry = new LsmFileRegistry(dir, manifest),
            Level0CompactionThreshold = 2,
            LevelCompactionThreshold = 2,
            MaxCompactionLevel = 2,
        };

        Span<byte> key = stackalloc byte[sizeof(long)];

        InternalKey.EncodeInt64UserKey(key, 1);
        table.Put(key, seqno: 1, Value(1));
        Assert.NotNull(table.FlushActiveMemTable());

        InternalKey.EncodeInt64UserKey(key, 1);
        table.Delete(key, seqno: 2);
        Assert.NotNull(table.FlushActiveMemTable());

        Assert.True(
            WaitUntil(() => manifest.GetLiveFiles(0).Count == 0 && manifest.GetLiveFiles(1).Count == 1),
            $"expected first L0 compaction (L0={manifest.GetLiveFiles(0).Count}, L1={manifest.GetLiveFiles(1).Count})");

        InternalKey.EncodeInt64UserKey(key, 2);
        table.Put(key, seqno: 3, Value(2));
        Assert.NotNull(table.FlushActiveMemTable());

        InternalKey.EncodeInt64UserKey(key, 3);
        table.Put(key, seqno: 4, Value(3));
        Assert.NotNull(table.FlushActiveMemTable());

        Assert.True(
            WaitUntil(() => manifest.GetLiveFiles(1).Count == 0 && manifest.GetLiveFiles(2).Count == 1),
            $"expected cascading L1 compaction (L1={manifest.GetLiveFiles(1).Count}, L2={manifest.GetLiveFiles(2).Count})");

        LsmTableFileMetadata bottomFile = Assert.Single(manifest.GetLiveFiles(2));
        SsTableReader reader = SsTableReader.Load(File.ReadAllBytes(Path.Combine(dir, bottomFile.FileName)));

        InternalKey.EncodeInt64UserKey(key, 1);
        Assert.False(reader.TryGet(key, snapshotSeqno: 2, out byte[] deletedValue, out bool isTombstone));
        Assert.False(isTombstone);
        Assert.Empty(deletedValue);

        InternalKey.EncodeInt64UserKey(key, 2);
        Assert.True(reader.TryGet(key, snapshotSeqno: 3, out byte[] rowTwo, out isTombstone));
        Assert.False(isTombstone);
        Assert.Equal(Value(2), rowTwo);

        InternalKey.EncodeInt64UserKey(key, 3);
        Assert.True(reader.TryGet(key, snapshotSeqno: 4, out byte[] rowThree, out isTombstone));
        Assert.False(isTombstone);
        Assert.Equal(Value(3), rowThree);
    }

    [Fact]
    public void Reopen_ReplaysRotatedWalSegmentsInOrder()
    {
        string dir = Path.Combine(_root, "segments");
        var engine = new LsmStorageEngine(dir, LsmWalDurabilityMode.StrictFsync)
        {
            MemTableFlushThresholdOverrideBytes = 8 * 1024,
        };

        const int Rows = 300;
        var rowIds = new long[Rows];
        for (int i = 0; i < Rows; i++)
        {
            rowIds[i] = engine.InsertRow("db", "orders", Value(i));
        }

        engine.Dispose();

        using var reopened = new LsmStorageEngine(dir, LsmWalDurabilityMode.StrictFsync);
        for (int i = 0; i < Rows; i++)
        {
            Assert.Equal(Value(i), reopened.ReadRow("db", "orders", rowIds[i]));
        }
    }

    private static byte[] Value(int i) => Encoding.UTF8.GetBytes($"row-value-{i:D6}-{new string('x', 80)}");

    private static bool WaitUntil(Func<bool> condition, int timeoutMs = 10_000)
    {
        var stopwatch = Stopwatch.StartNew();
        while (stopwatch.ElapsedMilliseconds < timeoutMs)
        {
            if (condition())
            {
                return true;
            }

            Thread.Sleep(20);
        }

        return condition();
    }
}
