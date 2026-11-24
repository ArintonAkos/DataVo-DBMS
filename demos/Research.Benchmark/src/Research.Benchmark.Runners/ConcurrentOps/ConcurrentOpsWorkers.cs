using System.Diagnostics;
using Research.Benchmark.Abstractions;

namespace Research.Benchmark.Runners.ConcurrentOps;

/// <summary>Runs one worker loop until the token it is given is cancelled.</summary>
internal delegate ConcurrentWorkerResult ConcurrentWorker(int workerId, CancellationToken cancellationToken);

/// <summary>
/// Runs the reader and writer loops of a concurrent-ops measurement on dedicated threads.
/// </summary>
/// <remarks>
/// The loops never block, so running them as thread-pool work items starves the pool: on a 4-core host only four
/// of ten workers run at first, the rest (the writers, queued last) wait for the pool's slow thread injection, and
/// the <c>CancelAfter</c> timer that ends the run is itself a pool work item that fires late. That measured the
/// pool, not the engine (one Windows 4-vCPU run reported 0 writes in 9.3 s for a 5 s run). Here every worker has
/// its own thread, writers start first, all are released together by one barrier, and the coordinating thread
/// ends the run on its own clock.
/// </remarks>
internal static class ConcurrentOpsWorkers
{
    public static ConcurrentOpsResult Run(
        ConcurrentOpsOptions options,
        CancellationToken cancellationToken,
        ConcurrentWorker read,
        ConcurrentWorker write)
    {
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        using var start = new Barrier(options.ReaderWorkers + options.WriterWorkers + 1);
        var writeResults = new ConcurrentWorkerResult?[options.WriterWorkers];
        var readResults = new ConcurrentWorkerResult?[options.ReaderWorkers];
        var errors = new System.Collections.Concurrent.ConcurrentQueue<Exception>();
        var threads = new List<Thread>(options.ReaderWorkers + options.WriterWorkers);

        Thread Spawn(string name, Action body) => new(() =>
        {
            start.SignalAndWait();
            try
            {
                body();
            }
            catch (Exception ex)
            {
                errors.Enqueue(ex);
                stop.Cancel();
            }
        })
        { IsBackground = true, Name = name };

        for (int i = 0; i < options.WriterWorkers; i++)
        {
            int workerId = i;
            threads.Add(Spawn($"concurrent-ops-writer-{i}", () => writeResults[workerId] = write(workerId, stop.Token)));
        }

        for (int i = 0; i < options.ReaderWorkers; i++)
        {
            int workerId = i;
            threads.Add(Spawn($"concurrent-ops-reader-{i}", () => readResults[workerId] = read(workerId, stop.Token)));
        }

        foreach (Thread thread in threads)
        {
            thread.Start();
        }

        start.SignalAndWait();
        var stopwatch = Stopwatch.StartNew();
        cancellationToken.WaitHandle.WaitOne(options.Duration);
        stop.Cancel();
        foreach (Thread thread in threads)
        {
            thread.Join();
        }

        stopwatch.Stop();
        if (!errors.IsEmpty)
        {
            throw new AggregateException("A concurrent-ops worker failed.", errors);
        }

        cancellationToken.ThrowIfCancellationRequested();
        return ConcurrentOpsMetrics.Build(
            [.. readResults.Select(static result => result!)],
            [.. writeResults.Select(static result => result!)],
            stopwatch.Elapsed);
    }
}
