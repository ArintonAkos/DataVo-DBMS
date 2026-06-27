using DataVo.Core;
using DataVo.Core.CompiledQueries;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;
using Research.Benchmark.Abstractions;

namespace Research.Benchmark.Runners.FlatCrud;

/// <summary>
/// DataVo flat-CRUD engine: typed in-memory table, typed insert fast lane, and a compiled, index-accelerated
/// point-lookup (the idiomatic prepared keyed read — built once, executed per id — analogous to LiteDB's
/// <c>FindById</c>, rather than re-parsing ad-hoc SQL per lookup).
/// </summary>
public sealed class DataVoFlatCrudEngine : IFlatCrudEngine
{
    private static readonly ReactiveRowSchema Schema = new("Id", "Name", "Value", "Score");
    private static readonly DataVoCompiledQueryPlan LookupPlan = DataVoCompiledQueryPlan.SelectSingle(
        "Records", ["Id", "Name", "Value", "Score"], whereColumn: "Id", parameterName: "id");
    private static readonly CompiledRowMapper<FlatRecord> RowMapper = MapRow;

    private readonly CellValue[] _cells = new CellValue[4];
    private DataVoContext? _context;
    private DataVoPreparedSelectSingle<FlatRecord>? _lookup;

    public string Name => "DataVo";

    public void Initialize()
    {
        _context?.Dispose();
        _context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        ExecuteOk("CREATE DATABASE FlatCrudBenchmark");
        ExecuteOk("USE FlatCrudBenchmark");
        ExecuteOk("CREATE TABLE Records (Id INT PRIMARY KEY, Name VARCHAR(40), Value INT, Score FLOAT)");
        _lookup = DataVoCompiledQuery.PrepareSelectSingleTyped(Ctx(), LookupPlan, RowMapper);
    }

    // DataVo's typed insert is in-memory with no per-write commit, so batching is a no-op.
    public void BeginBatch() { }

    public void CompleteBatch() { }

    public void Insert(FlatRecord record)
    {
        _cells[0] = CellValue.From(checked((int)record.Id));
        _cells[1] = CellValue.From(record.Name);
        _cells[2] = CellValue.From(record.Value);
        _cells[3] = CellValue.From(record.Score);
        Ctx().InsertTyped("Records", Schema, _cells);
    }

    public FlatRecord? GetById(long id)
    {
        return Lookup().Execute(checked((int)id));
    }

    public void Dispose()
    {
        _context?.Dispose();
        _context = null;
        _lookup = null;
    }

    private DataVoContext Ctx() =>
        _context ?? throw new InvalidOperationException("DataVo flat-CRUD engine has not been initialized.");

    private DataVoPreparedSelectSingle<FlatRecord> Lookup() =>
        _lookup ?? throw new InvalidOperationException("DataVo flat-CRUD lookup has not been prepared.");

    private static FlatRecord MapRow(CompiledRowReader row) => new(
        row.GetInt32("Id"),
        row.GetString("Name") ?? string.Empty,
        row.GetInt32("Value"),
        row.GetDouble("Score"));

    private void ExecuteOk(string sql)
    {
        QueryResult result = Ctx().Execute(sql).Last();
        if (result.IsError)
        {
            throw new InvalidOperationException($"{sql}{Environment.NewLine}{string.Join(" | ", result.Messages)}");
        }
    }
}
