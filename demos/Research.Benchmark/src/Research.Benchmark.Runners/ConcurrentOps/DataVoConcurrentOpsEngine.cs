using System.Diagnostics;
using DataVo.Core;
using DataVo.Core.CompiledQueries;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;
using Research.Benchmark.Abstractions;

namespace Research.Benchmark.Runners.ConcurrentOps;

public sealed class DataVoConcurrentOpsEngine : IConcurrentOpsEngine
{
    private static readonly ReactiveRowSchema Schema = new("Id", "Name", "Value", "Score");
    private static readonly DataVoCompiledQueryPlan LookupPlan = DataVoCompiledQueryPlan.SelectSingle(
        "Records", ["Id", "Name", "Value", "Score"], whereColumn: "Id", parameterName: "id");
    private static readonly DataVoCompiledQueryPlan UpdatePlan = DataVoCompiledQueryPlan.Update(
        "Records",
        new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["Value"] = "value",
            ["Score"] = "score"
        },
        whereColumn: "Id",
        whereParameterName: "id");
    private static readonly CompiledRowMapper<FlatRecord> RowMapper = static row => new FlatRecord(
        row.GetInt32(0),
        row.GetString(1) ?? string.Empty,
        row.GetInt32(2),
        row.GetDouble(3));

    private DataVoContext? _context;
    private DataVoPreparedSelectSingle<FlatRecord>? _lookup;
    private int _initialRecords;
    private long _nextInsertId;

    public string Name => "DataVo";

    public async Task InitializeAsync(ConcurrentOpsOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        _context?.Dispose();
        _context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        ExecuteOk("CREATE DATABASE ConcurrentOpsBenchmark");
        ExecuteOk("USE ConcurrentOpsBenchmark");
        ExecuteOk("CREATE TABLE Records (Id INT PRIMARY KEY, Name VARCHAR(40), Value INT, Score FLOAT)");

        var rows = new List<CellValue[]>(options.InitialRecords);
        for (int i = 1; i <= options.InitialRecords; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            rows.Add(
            [
                CellValue.From(i),
                CellValue.From($"name-{i}"),
                CellValue.From(i),
                CellValue.From(i * 1.5d)
            ]);
        }

        Ctx().InsertTypedBatch("Records", Schema, rows);
        _lookup = DataVoCompiledQuery.PrepareSelectSingleTyped(Ctx(), LookupPlan, RowMapper);
        _initialRecords = options.InitialRecords;
        _nextInsertId = options.InitialRecords;
        await Task.CompletedTask;
    }

    public async Task<ConcurrentOpsResult> RunAsync(ConcurrentOpsOptions options, CancellationToken cancellationToken = default)
    {
        EnsureInitialized();
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        linkedCts.CancelAfter(options.Duration);
        CancellationToken token = linkedCts.Token;

        var stopwatch = Stopwatch.StartNew();
        Task<ConcurrentWorkerResult>[] readTasks = Enumerable.Range(0, options.ReaderWorkers)
            .Select(workerId => Task.Run(() => RunReadWorker(workerId, token), CancellationToken.None))
            .ToArray();
        Task<ConcurrentWorkerResult>[] writeTasks = Enumerable.Range(0, options.WriterWorkers)
            .Select(workerId => Task.Run(() => RunWriteWorker(workerId, token), CancellationToken.None))
            .ToArray();

        await Task.WhenAll(readTasks.Concat(writeTasks));
        stopwatch.Stop();

        return ConcurrentOpsMetrics.Build(
            readTasks.Select(static task => task.Result).ToArray(),
            writeTasks.Select(static task => task.Result).ToArray(),
            stopwatch.Elapsed);
    }

    public ValueTask DisposeAsync()
    {
        _context?.Dispose();
        _context = null;
        _lookup = null;
        return ValueTask.CompletedTask;
    }

    private ConcurrentWorkerResult RunReadWorker(int workerId, CancellationToken cancellationToken)
    {
        var random = new Random(unchecked(Environment.TickCount * 31 + workerId));
        var latencies = new List<double>(16_384);
        long operations = 0;

        while (!cancellationToken.IsCancellationRequested)
        {
            int id = random.Next(1, _initialRecords + 1);
            long start = Stopwatch.GetTimestamp();
            FlatRecord? record = Lookup().Execute(id);
            latencies.Add(ConcurrentOpsMetrics.ElapsedMs(start));
            if (record is not null)
            {
                operations++;
            }
        }

        return new ConcurrentWorkerResult(operations, latencies);
    }

    private ConcurrentWorkerResult RunWriteWorker(int workerId, CancellationToken cancellationToken)
    {
        var random = new Random(unchecked(Environment.TickCount * 37 + workerId));
        var latencies = new List<double>(4096);
        long operations = 0;
        bool insertNext = workerId % 2 == 0;

        while (!cancellationToken.IsCancellationRequested)
        {
            long start = Stopwatch.GetTimestamp();
            if (insertNext)
            {
                int id = checked((int)Interlocked.Increment(ref _nextInsertId));
                Ctx().InsertTyped("Records", Schema,
                [
                    CellValue.From(id),
                    CellValue.From($"name-{id}"),
                    CellValue.From(id),
                    CellValue.From(id * 1.5d)
                ]);
            }
            else
            {
                int id = random.Next(1, _initialRecords + 1);
                int value = random.Next();
                DataVoCompiledQuery.Update(
                    Ctx(),
                    UpdatePlan,
                    [
                        new DataVoCompiledQueryParameter("id", id),
                        new DataVoCompiledQueryParameter("value", value),
                        new DataVoCompiledQueryParameter("score", value * 0.5d)
                    ]);
            }

            latencies.Add(ConcurrentOpsMetrics.ElapsedMs(start));
            operations++;
            insertNext = !insertNext;
        }

        return new ConcurrentWorkerResult(operations, latencies);
    }

    private void EnsureInitialized()
    {
        _ = Ctx();
        _ = Lookup();
    }

    private DataVoContext Ctx() =>
        _context ?? throw new InvalidOperationException("DataVo concurrent-ops engine has not been initialized.");

    private DataVoPreparedSelectSingle<FlatRecord> Lookup() =>
        _lookup ?? throw new InvalidOperationException("DataVo concurrent-ops lookup has not been prepared.");

    private void ExecuteOk(string sql)
    {
        QueryResult result = Ctx().Execute(sql).Last();
        if (result.IsError)
        {
            throw new InvalidOperationException($"{sql}{Environment.NewLine}{string.Join(" | ", result.Messages)}");
        }
    }
}
