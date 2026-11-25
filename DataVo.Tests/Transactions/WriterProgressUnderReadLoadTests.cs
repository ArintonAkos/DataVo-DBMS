using System.Diagnostics;
using DataVo.Core;
using DataVo.Core.CompiledQueries;
using DataVo.Core.Contracts.Results;
using DataVo.Core.MVCC;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;
using Xunit.Abstractions;

namespace DataVo.Tests.Transactions;

/// <summary>
/// Writers keep making progress while readers hammer the same table without pause: the contention profile of the
/// concurrent-ops benchmark (8 readers + 2 writers), oversubscribed to at least 2.5 runnable threads per core.
/// Progress is checked per time window, so a writer that stalls for a whole window fails the test, not only one
/// that never gets in.
/// </summary>
public sealed class WriterProgressUnderReadLoadTests(ITestOutputHelper output)
{
    private const int InitialRows = 20_000;
    private const int Windows = 5;
    private static readonly TimeSpan WindowLength = TimeSpan.FromMilliseconds(300);
    private static readonly ReactiveRowSchema Schema = new("Id", "Name", "Value", "Score");

    private static readonly DataVoCompiledQueryPlan LookupPlan = DataVoCompiledQueryPlan.SelectSingle(
        "Records", ["Id", "Name", "Value", "Score"], whereColumn: "Id", parameterName: "id");

    private static readonly DataVoCompiledQueryPlan UpdatePlan = DataVoCompiledQueryPlan.Update(
        "Records",
        new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) { ["Value"] = "value", ["Score"] = "score" },
        whereColumn: "Id",
        whereParameterName: "id");

    /// <summary>Readers: 8, or 2.5 per core when the machine has more cores, so threads always outnumber cores.</summary>
    private static int ReaderCount => Math.Clamp((Environment.ProcessorCount * 5 / 2) - 2, 8, 40);

    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Lsm)]
    public void Writers_CompleteWritesInEveryWindow_UnderContinuousPointReads(StorageMode mode)
    {
        string? directory = mode == StorageMode.Lsm ? TestStorageDirectories.Create("writer_progress") : null;
        try
        {
            using var context = new DataVoContext(new DataVoConfig
            {
                StorageMode = mode,
                DiskStoragePath = directory,
                LsmStrictFsync = false,
            });
            ExecuteOk(context, "CREATE DATABASE Contention");
            ExecuteOk(context, "USE Contention");
            ExecuteOk(context, "CREATE TABLE Records (Id INT PRIMARY KEY, Name VARCHAR(40), Value INT, Score FLOAT)");
            context.InsertTypedBatch("Records", Schema, [.. Enumerable.Range(1, InitialRows).Select(Row)]);
            DataVoPreparedSelectSingle<int> lookup = DataVoCompiledQuery.PrepareSelectSingleTyped(context, LookupPlan, static row => row.GetInt32(2));

            long nextId = InitialRows;
            ContentionResult result = RunContention(
                ReaderCount,
                writers: 2,
                read: random => lookup.Execute(random.Next(1, InitialRows + 1)),
                write: (writer, random) =>
                {
                    if (writer == 0)
                    {
                        int id = checked((int)Interlocked.Increment(ref nextId));
                        context.InsertTyped("Records", Schema, Row(id));
                    }
                    else
                    {
                        int value = random.Next();
                        DataVoCompiledQuery.Update(context, UpdatePlan,
                        [
                            new DataVoCompiledQueryParameter("id", random.Next(1, InitialRows + 1)),
                            new DataVoCompiledQueryParameter("value", value),
                            new DataVoCompiledQueryParameter("score", value * 0.5d),
                        ]);
                    }
                });

            AssertSteadyProgress(result);
        }
        finally
        {
            if (directory is not null)
            {
                TryDelete(directory);
            }
        }
    }

    [Fact]
    public void SnapshotGate_Writers_EnterInEveryWindow_UnderContinuousSnapshotReaders()
    {
        var gate = new SnapshotPublicationGate(waitTimeoutMs: 30_000);
        ContentionResult result = RunContention(
            ReaderCount,
            writers: 2,
            read: _ =>
            {
                gate.EnterRead();
                Thread.SpinWait(50); // a short snapshot statement
                gate.Exit();
                return 0;
            },
            write: (_, _) =>
            {
                gate.EnterWrite(holdsLocks: false);
                Thread.SpinWait(50);
                gate.Exit();
            });

        AssertSteadyProgress(result);
    }

    /// <summary>What one contention run measured: each writer's completed writes per window, and what a failure report needs.</summary>
    /// <param name="WritesPerWindow">Completed writes, per writer and window.</param>
    /// <param name="LongestWriteMs">Each writer's longest single write, in milliseconds.</param>
    /// <param name="LongestWriteEndWindow">The window in which each writer's longest write finished (Windows when after the run).</param>
    /// <param name="Reads">Completed reads over the whole run.</param>
    /// <param name="Readers">Number of reader threads.</param>
    /// <param name="Collections">Gen0, gen1 and gen2 collections during the run.</param>
    /// <param name="GcPause">Total time the runtime was paused for GC during the run.</param>
    private sealed record ContentionResult(
        long[][] WritesPerWindow,
        double[] LongestWriteMs,
        int[] LongestWriteEndWindow,
        long Reads,
        int Readers,
        int[] Collections,
        TimeSpan GcPause)
    {
        /// <summary>One line with every measurement, for the test output and failure messages.</summary>
        public string Describe()
        {
            var text = new System.Text.StringBuilder();
            text.Append($"{Readers} readers, {Reads:N0} reads; writes per {WindowLength.TotalMilliseconds} ms window:");
            for (int writer = 0; writer < WritesPerWindow.Length; writer++)
            {
                text.Append($" writer {writer} [{string.Join(", ", WritesPerWindow[writer])}], longest write {LongestWriteMs[writer]:F1} ms ending in window {LongestWriteEndWindow[writer]};");
            }

            text.Append($" GC collections gen0/gen1/gen2 {Collections[0]}/{Collections[1]}/{Collections[2]}, GC pause {GcPause.TotalMilliseconds:F1} ms");
            return text.ToString();
        }
    }

    /// <summary>
    /// Runs readers and writers on dedicated threads (never the thread pool, whose slow thread injection would
    /// itself starve late workers) released together by one barrier, and counts each writer's completed writes
    /// per window.
    /// </summary>
    private ContentionResult RunContention(int readers, int writers, Func<Random, int> read, Action<int, Random> write)
    {
        long[][] writesPerWindow = [.. Enumerable.Range(0, writers).Select(_ => new long[Windows])];
        long[] longestWriteTicks = new long[writers];
        int[] longestWriteEndWindow = new int[writers];
        long reads = 0;
        var errors = new System.Collections.Concurrent.ConcurrentQueue<Exception>();
        using var start = new Barrier(readers + writers + 1);
        var stopwatch = new Stopwatch();
        long endTicks = 0;
        int stop = 0;

        var threads = new List<Thread>();
        for (int r = 0; r < readers; r++)
        {
            int seed = r;
            threads.Add(new Thread(() =>
            {
                var random = new Random(seed);
                long local = 0;
                start.SignalAndWait();
                try
                {
                    while (Volatile.Read(ref stop) == 0)
                    {
                        read(random);
                        local++;
                    }
                }
                catch (Exception ex)
                {
                    errors.Enqueue(ex);
                }

                Interlocked.Add(ref reads, local);
            }) { IsBackground = true, Name = $"reader-{r}" });
        }

        for (int w = 0; w < writers; w++)
        {
            int writer = w;
            threads.Add(new Thread(() =>
            {
                var random = new Random(1_000 + writer);
                start.SignalAndWait();
                try
                {
                    while (Volatile.Read(ref stop) == 0)
                    {
                        long started = stopwatch.ElapsedTicks;
                        write(writer, random);
                        long elapsed = stopwatch.ElapsedTicks;
                        int window = elapsed < Volatile.Read(ref endTicks) ? (int)(elapsed * Windows / endTicks) : Windows;
                        if (window < Windows)
                        {
                            writesPerWindow[writer][window]++;
                        }

                        if (elapsed - started > longestWriteTicks[writer])
                        {
                            longestWriteTicks[writer] = elapsed - started;
                            longestWriteEndWindow[writer] = window;
                        }
                    }
                }
                catch (Exception ex)
                {
                    errors.Enqueue(ex);
                }
            }) { IsBackground = true, Name = $"writer-{w}" });
        }

        foreach (Thread thread in threads)
        {
            thread.Start();
        }

        int[] collectionsBefore = [GC.CollectionCount(0), GC.CollectionCount(1), GC.CollectionCount(2)];
        TimeSpan pauseBefore = GC.GetTotalPauseDuration();
        Volatile.Write(ref endTicks, (long)(WindowLength.TotalSeconds * Windows * Stopwatch.Frequency));
        stopwatch.Start();
        start.SignalAndWait();
        Thread.Sleep(WindowLength * Windows);
        Volatile.Write(ref stop, 1);
        foreach (Thread thread in threads)
        {
            Assert.True(thread.Join(TimeSpan.FromSeconds(60)), $"{thread.Name} did not stop");
        }

        var result = new ContentionResult(
            writesPerWindow,
            [.. longestWriteTicks.Select(ticks => ticks * 1000d / Stopwatch.Frequency)],
            longestWriteEndWindow,
            reads,
            readers,
            [.. Enumerable.Range(0, 3).Select(generation => GC.CollectionCount(generation) - collectionsBefore[generation])],
            GC.GetTotalPauseDuration() - pauseBefore);
        output.WriteLine(result.Describe());
        Assert.Empty(errors);
        Assert.True(reads > 0, $"readers made no progress: {result.Describe()}");
        return result;
    }

    private static void AssertSteadyProgress(ContentionResult result)
    {
        for (int writer = 0; writer < result.WritesPerWindow.Length; writer++)
        {
            for (int window = 0; window < Windows; window++)
            {
                Assert.True(result.WritesPerWindow[writer][window] > 0,
                    $"writer {writer} completed no write in window {window}: {result.Describe()}");
            }
        }
    }

    private static CellValue[] Row(int id) =>
    [
        CellValue.From(id),
        CellValue.From($"name-{id}"),
        CellValue.From(id),
        CellValue.From(id * 1.5d),
    ];

    private static void ExecuteOk(DataVoContext context, string sql)
    {
        QueryResult result = context.Execute(sql).Last();
        Assert.False(result.IsError, string.Join(" | ", result.Messages));
    }

    private static void TryDelete(string directory)
    {
        try
        {
            Directory.Delete(directory, recursive: true);
        }
        catch (IOException)
        {
        }
    }
}
