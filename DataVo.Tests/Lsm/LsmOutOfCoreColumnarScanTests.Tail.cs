using DataVo.Core;
using DataVo.Core.StorageEngine.Lsm;
using DataVo.Core.StorageEngine.Lsm.V2;

namespace DataVo.Tests.Lsm;

// Rows a newer, unflushed write replaces are masked out of the files' segments. A thread keeps those masks
// for the tail and the files it scanned, and the next scan only marks the keys published since.
public sealed partial class LsmOutOfCoreColumnarScanTests
{
    [Theory]
    [MemberData(nameof(Modes))]
    public void UnflushedUpdatesAndDeletes_OfFileRows_AreMaskedScanAfterScan(LsmReadMode mode)
    {
        var model = new Dictionary<RowKey, (int Level, long? Gold)>();
        var random = new Random(mode == LsmReadMode.MemoryMapped ? 41 : 42);
        using LsmStorageEngine engine = Open(mode, flushThreshold: 1L << 30);
        Build(engine);
        Mutate(engine, model, random, 6_000);
        engine.FlushTableForTesting(Db, Table);
        Mutate(engine, model, random, 3_000);
        engine.FlushTableForTesting(Db, Table);

        // No flush from here on: every scan reads the same files and a tail that only grows.
        for (int step = 0; step < 60; step++)
        {
            Mutate(engine, model, random, random.Next(4) == 0 ? 0 : 1 + random.Next(80));
            AssertScans(engine, model);
        }
    }

    [Fact]
    public void ScansOnTwoThreads_EachKeepTheirOwnMasks_AcrossFlushesAndNewTails()
    {
        var model = new Dictionary<RowKey, (int Level, long? Gold)>();
        var random = new Random(43);
        using LsmStorageEngine engine = Open(LsmReadMode.Auto, flushThreshold: 1L << 30);
        Build(engine);
        Mutate(engine, model, random, 4_000);
        engine.FlushTableForTesting(Db, Table);

        // The second thread scans rarely, so it meets tails and versions its masks were not made for.
        using var other = new SingleThreadRunner();
        for (int step = 0; step < 40; step++)
        {
            Mutate(engine, model, random, 1 + random.Next(150));
            if (step % 7 == 3)
            {
                engine.FlushTableForTesting(Db, Table);
            }

            AssertScans(engine, model);
            if (step % 5 == 0)
            {
                other.Run(() => AssertScans(engine, model));
            }
        }
    }

    [Fact]
    public void ASecondScan_WithNothingPublishedSince_MarksNoRowAgain()
    {
        var model = new Dictionary<RowKey, (int Level, long? Gold)>();
        var random = new Random(44);
        using LsmStorageEngine engine = Open(LsmReadMode.Auto, flushThreshold: 1L << 30);
        Build(engine);
        Mutate(engine, model, random, 20_000);
        engine.FlushTableForTesting(Db, Table);

        // Rewrite rows spread over the whole file, unflushed.
        foreach (RowKey id in model.Keys.Where((_, i) => i % 7 == 0).ToList())
        {
            Assert.True(engine.OverwriteRowForTesting(Db, Table, id, Row(1, 1)));
            model[id] = (1, 1);
        }

        // A plain count reads no column, so any chunk it loads is one it loaded to mark rows.
        long before = SsTableColumnarChunkReader.LoadsOnThisThread;
        AssertCount(engine, model);
        long first = SsTableColumnarChunkReader.LoadsOnThisThread - before;
        Assert.True(first > 0);

        before = SsTableColumnarChunkReader.LoadsOnThisThread;
        AssertCount(engine, model);
        Assert.Equal(0, SsTableColumnarChunkReader.LoadsOnThisThread - before);

        // One more rewrite: only its key is marked.
        RowKey last = model.Keys.First();
        Assert.True(engine.OverwriteRowForTesting(Db, Table, last, Row(2, 2)));
        model[last] = (2, 2);
        before = SsTableColumnarChunkReader.LoadsOnThisThread;
        AssertCount(engine, model);
        Assert.InRange(SsTableColumnarChunkReader.LoadsOnThisThread - before, 0, 1);
        AssertScans(engine, model);
    }

    private static void AssertCount(LsmStorageEngine engine, Dictionary<RowKey, (int Level, long? Gold)> model)
    {
        Assert.True(engine.TryScanColumnar(Db, Table, engine.Columnar(Db, Table)!, default, default, default, 0, out int count));
        Assert.Equal(model.Count, count);
    }

    /// <summary>Runs actions on one dedicated thread, so its thread-static state lives from one call to the next.</summary>
    private sealed class SingleThreadRunner : IDisposable
    {
        private readonly System.Collections.Concurrent.BlockingCollection<(Action Action, TaskCompletionSource Done)> _work = [];
        private readonly Thread _thread;

        public SingleThreadRunner()
        {
            _thread = new Thread(() =>
            {
                foreach ((Action action, TaskCompletionSource done) in _work.GetConsumingEnumerable())
                {
                    try
                    {
                        action();
                        done.SetResult();
                    }
                    catch (Exception error)
                    {
                        done.SetException(error);
                    }
                }
            }) { IsBackground = true };
            _thread.Start();
        }

        public void Run(Action action)
        {
            var done = new TaskCompletionSource();
            _work.Add((action, done));
            done.Task.GetAwaiter().GetResult();
        }

        public void Dispose()
        {
            _work.CompleteAdding();
            _thread.Join();
            _work.Dispose();
        }
    }
}
