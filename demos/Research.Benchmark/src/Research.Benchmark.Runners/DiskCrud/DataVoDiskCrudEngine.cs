using DataVo.Core;
using DataVo.Core.CompiledQueries;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;
using Research.Benchmark.Abstractions;

namespace Research.Benchmark.Runners.DiskCrud;

/// <summary>
/// DataVo disk-CRUD engine: a Disk-mode, WAL-enabled table that bulk-inserts via the typed insert fast lane
/// and applies point updates via a prepared compiled UPDATE plan (index-accelerated by the integer primary
/// key). Constructed in two durability modes:
/// <list type="bullet">
/// <item><b>Disk</b> — default semantics: appends are flushed to the OS page cache on close (process-crash durable).</item>
/// <item><b>Disk+fsync</b> — <see cref="DataVoConfig.SyncDiskWrites"/> forces an fsync per write (power-crash durable, like SQLite <c>synchronous=FULL</c>).</item>
/// </list>
/// Each autocommit UPDATE is out-of-place (tombstone old + append new), so the durable mode pays up to two
/// fsyncs per update — an honest reflection of the engine's current update strategy.
/// </summary>
public sealed class DataVoDiskCrudEngine : IDiskCrudEngine
{
    private static readonly ReactiveRowSchema Schema = new("Id", "Name", "Value", "Score");
    private static readonly DataVoCompiledQueryPlan UpdatePlan = DataVoCompiledQueryPlan.Update(
        "Records",
        new Dictionary<string, string> { ["Value"] = "value", ["Score"] = "score" },
        whereColumn: "Id",
        whereParameterName: "id");

    private readonly bool _durable;
    private readonly IoSchedulerMode _ioSchedulerMode;
    private readonly int? _walCheckpointIntervalMs;
    private readonly string _name;
    private string? _workingDirectory;
    private DataVoContext? _context;
    private List<CellValue[]>? _batchRows;

    public DataVoDiskCrudEngine(
        bool durable,
        IoSchedulerMode ioSchedulerMode = IoSchedulerMode.Off,
        int? walCheckpointIntervalMs = null)
    {
        _durable = durable;
        _ioSchedulerMode = ioSchedulerMode;
        _walCheckpointIntervalMs = walCheckpointIntervalMs;
        string poolingSuffix = ioSchedulerMode switch
        {
            IoSchedulerMode.PoolingOnly => "+pooled",
            IoSchedulerMode.GroupCommit => "+groupcommit",
            _ => string.Empty,
        };
        // Surface a non-default checkpoint cadence in the engine label so A/B runs are self-describing.
        string checkpointSuffix = ioSchedulerMode == IoSchedulerMode.GroupCommit && walCheckpointIntervalMs is int ms
            ? $"+ckpt{ms}ms"
            : string.Empty;
        _name = durable
            ? $"DataVo (Disk{poolingSuffix}{checkpointSuffix}+fsync)"
            : $"DataVo (Disk{poolingSuffix}{checkpointSuffix})";
    }

    public string Name => _name;

    public void Initialize(string workingDirectory)
    {
        _context?.Dispose();
        _workingDirectory = workingDirectory;
        Directory.CreateDirectory(workingDirectory);

        var config = new DataVoConfig
        {
            StorageMode = StorageMode.Disk,
            DiskStoragePath = workingDirectory,
            WalEnabled = true,
            WalFilePath = "datavo.wal",
            SyncDiskWrites = _durable,
            IoSchedulerMode = _ioSchedulerMode,
        };

        if (_walCheckpointIntervalMs is int intervalMs)
        {
            config.WalCheckpointIntervalMs = intervalMs;
        }

        _context = new DataVoContext(config);

        ExecuteOk("CREATE DATABASE DiskCrudBenchmark");
        ExecuteOk("USE DiskCrudBenchmark");
        ExecuteOk("CREATE TABLE Records (Id INT PRIMARY KEY, Name VARCHAR(40), Value INT, Score FLOAT)");
    }

    public void BeginInsertBatch()
    {
        _batchRows = new List<CellValue[]>(65_536);
    }

    public void CompleteInsertBatch()
    {
        if (_batchRows is { Count: > 0 } rows)
        {
            Ctx().InsertTypedBatch("Records", Schema, rows);
        }

        _batchRows = null;
    }

    public void Insert(FlatRecord record)
    {
        var cells = new CellValue[4]
        {
            CellValue.From(checked((int)record.Id)),
            CellValue.From(record.Name),
            CellValue.From(record.Value),
            CellValue.From(record.Score),
        };

        if (_batchRows is not null)
        {
            _batchRows.Add(cells);
            return;
        }

        Ctx().InsertTyped("Records", Schema, cells);
    }

    public void Update(long id, int newValue, double newScore)
    {
        int affected = DataVoCompiledQuery.Update(Ctx(), UpdatePlan,
        [
            new DataVoCompiledQueryParameter("value", newValue),
            new DataVoCompiledQueryParameter("score", newScore),
            new DataVoCompiledQueryParameter("id", checked((int)id)),
        ]);

        if (affected != 1)
        {
            throw new InvalidOperationException(
                $"DataVo disk-CRUD update for Id={id} affected {affected} rows (expected 1).");
        }
    }

    public void Dispose()
    {
        _context?.Dispose();
        _context = null;
        _batchRows = null;

        if (_workingDirectory is not null && Directory.Exists(_workingDirectory))
        {
            try { Directory.Delete(_workingDirectory, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    private DataVoContext Ctx() =>
        _context ?? throw new InvalidOperationException("DataVo disk-CRUD engine has not been initialized.");

    private void ExecuteOk(string sql)
    {
        QueryResult result = Ctx().Execute(sql).Last();
        if (result.IsError)
        {
            throw new InvalidOperationException($"{sql}{Environment.NewLine}{string.Join(" | ", result.Messages)}");
        }
    }
}
