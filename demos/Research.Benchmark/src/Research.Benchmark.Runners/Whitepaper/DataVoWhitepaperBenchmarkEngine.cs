using DataVo.Core;
using DataVo.Core.CompiledQueries;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;
using Research.Benchmark.Abstractions;

namespace Research.Benchmark.Runners.Whitepaper;

public sealed class DataVoWhitepaperBenchmarkEngine : IWhitepaperBenchmarkEngine
{
    private const int BatchSize = 65_536;
    private static readonly ReactiveRowSchema Schema = new("Id", "Name", "Value", "Score");
    private static readonly DataVoCompiledQueryPlan LookupPlan = DataVoCompiledQueryPlan.SelectSingle(
        "Records", ["Id", "Name", "Value", "Score"], whereColumn: "Id", parameterName: "id");
    private static readonly DataVoCompiledQueryPlan UpdatePlan = DataVoCompiledQueryPlan.Update(
        "Records",
        new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["Value"] = "value",
            ["Score"] = "score",
        },
        whereColumn: "Id",
        whereParameterName: "id");
    private static readonly CompiledRowMapper<FlatRecord> RowMapper = static row => new FlatRecord(
        row.GetInt32(0),
        row.GetString(1) ?? string.Empty,
        row.GetInt32(2),
        row.GetDouble(3));

    private readonly bool _durable;
    private DataVoContext? _context;
    private DataVoPreparedSelectSingle<FlatRecord>? _lookup;
    private string? _workingDirectory;

    public DataVoWhitepaperBenchmarkEngine(bool durable)
    {
        _durable = durable;
    }

    public string Name => _durable ? "DataVo (LSM Production)" : "DataVo (LSM Relaxed)";

    public string WorkingDirectory => _workingDirectory
        ?? throw new InvalidOperationException("DataVo whitepaper engine has not been initialized.");

    public void Initialize(string workingDirectory, bool fresh)
    {
        CloseContext();
        _workingDirectory = workingDirectory;
        if (fresh && Directory.Exists(workingDirectory))
        {
            Directory.Delete(workingDirectory, recursive: true);
        }

        Directory.CreateDirectory(workingDirectory);
        _context = new DataVoContext(new DataVoConfig
        {
            StorageMode = StorageMode.Lsm,
            DiskStoragePath = workingDirectory,
            LsmStrictFsync = _durable,
            EnableZeroAllocCompiledUpdate = true,
        });

        ExecuteOk("CREATE DATABASE WhitepaperBenchmark");
        ExecuteOk("USE WhitepaperBenchmark");
        ExecuteOk("CREATE TABLE Records (Id INT PRIMARY KEY, Name VARCHAR(40), Value INT, Score FLOAT)");
        _lookup = DataVoCompiledQuery.PrepareSelectSingleTyped(Ctx(), LookupPlan, RowMapper);
    }

    public void Preload(int records)
    {
        var rows = new List<CellValue[]>(Math.Min(records, BatchSize));
        for (int i = 1; i <= records; i++)
        {
            rows.Add(
            [
                CellValue.From(i),
                CellValue.From($"name-{i}"),
                CellValue.From(i),
                CellValue.From(i * 1.5d),
            ]);

            if (rows.Count == BatchSize)
            {
                Ctx().InsertTypedBatch("Records", Schema, rows);
                rows.Clear();
            }
        }

        if (rows.Count > 0)
        {
            Ctx().InsertTypedBatch("Records", Schema, rows);
        }
    }

    public FlatRecord? Read(long id) => Lookup().Execute(checked((int)id));

    public void Update(long id, int newValue, double newScore)
    {
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
            throw new InvalidOperationException($"DataVo whitepaper update affected {affected} rows for Id={id}.");
        }
    }

    public void CloseForRecovery() => CloseContext();

    public void OpenExisting()
    {
        string directory = WorkingDirectory;
        Initialize(directory, fresh: false);
    }

    public void Dispose() => CloseContext();

    private DataVoContext Ctx() =>
        _context ?? throw new InvalidOperationException("DataVo whitepaper engine has not been initialized.");

    private DataVoPreparedSelectSingle<FlatRecord> Lookup() =>
        _lookup ?? throw new InvalidOperationException("DataVo whitepaper lookup has not been prepared.");

    private void ExecuteOk(string sql)
    {
        QueryResult result = Ctx().Execute(sql).Last();
        if (result.IsError)
        {
            throw new InvalidOperationException($"{sql}{Environment.NewLine}{string.Join(" | ", result.Messages)}");
        }
    }

    private void CloseContext()
    {
        _lookup = null;
        _context?.Dispose();
        _context = null;
    }
}
