using DataVo.Core;
using DataVo.Core.CompiledQueries;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;
using Research.Benchmark.Abstractions;
using System.Globalization;

namespace Research.Benchmark.Runners.DiskCrud;

/// <summary>
/// DataVo disk-CRUD engine: an Lsm table that bulk-inserts via the typed insert fast lane and applies point updates
/// through the fixed-width compiled UPDATE by primary key (batched in one write unit inside an update batch).
/// Constructed in two durability modes:
/// <list type="bullet">
/// <item><b>Production</b> — <see cref="DataVoConfig.LsmStrictFsync"/>: every commit-log append is fsync'd (power-crash durable).</item>
/// <item><b>Relaxed</b> — commit-log appends reach the OS page cache (process-crash durable).</item>
/// </list>
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
    private readonly bool _zeroAllocUpdate;
    private readonly string _name;
    private string? _workingDirectory;
    private DataVoContext? _context;
    private List<CellValue[]>? _batchRows;
    private bool _updateTransactionActive;
    private List<DataVoFixedWidthUpdateBatchEntry>? _pendingLsmUpdates;

    public DataVoDiskCrudEngine(bool durable, bool zeroAllocUpdate = true)
    {
        _durable = durable;
        _zeroAllocUpdate = zeroAllocUpdate;
        // The dictionary update path is the A/B baseline; flag it so the two runs are distinguishable.
        string updateSuffix = zeroAllocUpdate ? string.Empty : " +dictupd";
        _name = DataVoBenchmarkName.Format(durable ? $"DataVo (LSM Production{updateSuffix})" : $"DataVo (LSM Relaxed{updateSuffix})");
    }

    public string Name => _name;

    public void Initialize(string workingDirectory)
    {
        _context?.Dispose();
        _workingDirectory = workingDirectory;
        Directory.CreateDirectory(workingDirectory);

        var config = new DataVoConfig
        {
            StorageMode = StorageMode.Lsm,
            DataDirectory = workingDirectory,
            LsmStrictFsync = _durable,
            EnableZeroAllocCompiledUpdate = _zeroAllocUpdate,
        };

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

    public void BeginUpdateBatch()
    {
        if (_updateTransactionActive)
        {
            throw new InvalidOperationException("DataVo disk-CRUD update batch is already active.");
        }

        _pendingLsmUpdates = new List<DataVoFixedWidthUpdateBatchEntry>(65_536);

        _updateTransactionActive = true;
    }

    public void Update(long id, int newValue, double newScore)
    {
        if (_updateTransactionActive)
        {
            _pendingLsmUpdates!.Add(new DataVoFixedWidthUpdateBatchEntry(
                DataVoFixedWidthValue.From(checked((int)id)),
                DataVoFixedWidthValue.From(newValue),
                DataVoFixedWidthValue.From(newScore)));

            return;
        }

        Span<DataVoFixedWidthValue> assignments = stackalloc DataVoFixedWidthValue[2];
        assignments[0] = DataVoFixedWidthValue.From(newValue);
        assignments[1] = DataVoFixedWidthValue.From(newScore);
        int affected = DataVoCompiledQuery.UpdateFixedWidthByPrimaryKey(
            Ctx(),
            UpdatePlan,
            DataVoFixedWidthValue.From(checked((int)id)),
            assignments);

        if (affected != 1)
        {
            throw new InvalidOperationException(
                $"DataVo disk-CRUD update for Id={id} affected {affected} rows (expected 1).");
        }
    }

    public void CompleteUpdateBatch()
    {
        if (!_updateTransactionActive)
        {
            return;
        }

        if (_pendingLsmUpdates is { Count: > 0 } updates)
        {
            int affected = DataVoCompiledQuery.UpdateFixedWidthByPrimaryKeyBatch(Ctx(), UpdatePlan, updates);
            if (affected != updates.Count)
            {
                throw new InvalidOperationException(
                    $"DataVo disk-CRUD update batch affected {affected} rows (expected {updates.Count}).");
            }

        }

        _pendingLsmUpdates = null;
        _updateTransactionActive = false;
    }

    public void Dispose()
    {
        if (_updateTransactionActive && _context is not null)
        {
            _pendingLsmUpdates = null;
            _updateTransactionActive = false;
        }

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
