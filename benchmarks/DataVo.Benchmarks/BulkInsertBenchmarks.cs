using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using DataVo.Core;
using DuckDB.NET.Data;
using Microsoft.Data.Sqlite;

namespace DataVo.Benchmarks;

/// <summary>
/// Loads an empty table with rows built during the measurement, the way each engine loads in bulk: DataVo's typed
/// batch insert, a prepared SQLite statement inside one transaction, and DuckDB's appender. Every iteration starts
/// from a new, empty database; opening and closing it is not timed.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(RunStrategy.Monitoring, warmupCount: 2, iterationCount: 10, invocationCount: 1)]
public class BulkInsertBenchmarks
{
    private ScratchDirectory _scratch = null!;
    private DataVoContext? _dataVo;
    private SqliteConnection? _sqlite;
    private DuckDBConnection? _duck;

    [Params(100_000)]
    public int Rows { get; set; }

    [ParamsAllValues]
    public Storage Storage { get; set; }

    [IterationSetup(Target = nameof(DataVo))]
    public void OpenDataVo()
    {
        _scratch = new ScratchDirectory();
        _dataVo = Engines.OpenDataVo(Storage, _scratch.Path);
    }

    [IterationSetup(Target = nameof(Sqlite))]
    public void OpenSqlite()
    {
        _scratch = new ScratchDirectory();
        _sqlite = Engines.OpenSqlite(Storage, _scratch.Path);
    }

    [IterationSetup(Target = nameof(DuckDb))]
    public void OpenDuckDb()
    {
        _scratch = new ScratchDirectory();
        _duck = Engines.OpenDuckDb(Storage, _scratch.Path);
    }

    [IterationSetup(Target = nameof(DuckDb1Thread))]
    public void OpenDuckDbSingleThreaded()
    {
        _scratch = new ScratchDirectory();
        _duck = Engines.OpenDuckDb(Storage, _scratch.Path, singleThreaded: true);
    }

    [IterationCleanup]
    public void Close()
    {
        _dataVo?.Dispose();
        _sqlite?.Dispose();
        _duck?.Dispose();
        _dataVo = null;
        _sqlite = null;
        _duck = null;
        _scratch.Dispose();
    }

    [Benchmark(Baseline = true)]
    public void DataVo() => Engines.Load(_dataVo!, 1, Rows);

    [Benchmark]
    public void Sqlite() => Engines.Load(_sqlite!, 1, Rows);

    [Benchmark]
    public void DuckDb() => Engines.Load(_duck!, 1, Rows);

    [Benchmark]
    public void DuckDb1Thread() => Engines.Load(_duck!, 1, Rows);
}
