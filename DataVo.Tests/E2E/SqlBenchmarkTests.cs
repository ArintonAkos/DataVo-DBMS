using System.Diagnostics;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Parser;
using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Config;
using Xunit.Abstractions;

namespace DataVo.Tests.E2E;

[Collection("SequentialStorageTests")]
public class SqlBenchmarkTests : IDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly string _testDb = "BenchmarkDb";
    private readonly string _testTable = "PerfTable";
    private readonly string _diskPath = "./test_datavo_benchmark";
    private readonly Guid _session = Guid.NewGuid();

    // Static lock to prevent parallel test collections from stepping on the global context
    private static readonly object GlobalEngineLock = new object();

    public SqlBenchmarkTests(ITestOutputHelper output)
    {
        _output = output;

        Monitor.Enter(GlobalEngineLock);

        try { Catalog.DropDatabase(_testDb); } catch { }
        try { if (Directory.Exists(_diskPath)) Directory.Delete(_diskPath, true); } catch { }
    }

    [Fact]
    public void E2E_Benchmark_InMemory_vs_Disk_FullParsing()
    {
        // 10K iterations for E2E SQL parsing (Lexer -> Parser -> Evaluator -> Storage)
        // is very dense overhead compared to raw driver inserts. 
        int iterations = 10_000;

        // --- 1. IN-MEMORY PIPELINE BENCHMARK ---
        var inMemoryConfig = new DataVoConfig { StorageMode = StorageMode.InMemory };
        StorageContext.Initialize(inMemoryConfig);

        Execute($"CREATE DATABASE {_testDb}");
        Execute($"USE {_testDb}");
        Execute($"CREATE TABLE {_testTable} (Id INT, Data1 VARCHAR, Data2 VARCHAR)");

        var stopwatch = Stopwatch.StartNew();

        for (int i = 0; i < iterations; i++)
        {
            Execute($"INSERT INTO {_testTable} (Id, Data1, Data2) VALUES ({i}, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ')");
        }

        stopwatch.Stop();
        var memoryInsertTime = stopwatch.ElapsedMilliseconds;

        stopwatch.Restart();
        var memoryResults = ExecuteAndReturn($"SELECT * FROM {_testTable}").Data;
        stopwatch.Stop();
        var memoryReadTime = stopwatch.ElapsedMilliseconds;


        // Cleanup manually before starting Disk test in the same context
        Catalog.DropDatabase(_testDb);


        // --- 2. DISK PIPELINE BENCHMARK ---
        var diskConfig = new DataVoConfig { StorageMode = StorageMode.Disk, DiskStoragePath = _diskPath };
        StorageContext.Initialize(diskConfig);

        Execute($"CREATE DATABASE {_testDb}");
        Execute($"USE {_testDb}");
        Execute($"CREATE TABLE {_testTable} (Id INT, Data1 VARCHAR, Data2 VARCHAR)");

        stopwatch.Restart();

        for (int i = 0; i < iterations; i++)
        {
            Execute($"INSERT INTO {_testTable} (Id, Data1, Data2) VALUES ({i}, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ')");
        }

        stopwatch.Stop();
        var diskInsertTime = stopwatch.ElapsedMilliseconds;

        stopwatch.Restart();
        var diskResults = ExecuteAndReturn($"SELECT * FROM {_testTable}").Data;
        stopwatch.Stop();
        var diskReadTime = stopwatch.ElapsedMilliseconds;

        // Print Out fully E2E Parsed results
        _output.WriteLine($"[Full E2E SQL Parse Benchmark - {iterations:N0} queries]");
        _output.WriteLine("---------------------------------------------");
        _output.WriteLine($"[InMemory] Lex->Parse->Evaluator Insert Time: {memoryInsertTime} ms | ~{iterations / Math.Max(1, (memoryInsertTime / 1000.0)):N0} ops/sec");
        _output.WriteLine($"[InMemory] Lex->Parse->Evaluator Read   Time: {memoryReadTime} ms | ~{iterations / Math.Max(1, (memoryReadTime / 1000.0)):N0} ops/sec");
        _output.WriteLine("---------------------------------------------");
        _output.WriteLine($"[Disk]     Lex->Parse->Evaluator Insert Time: {diskInsertTime} ms | ~{iterations / Math.Max(1, (diskInsertTime / 1000.0)):N0} ops/sec");
        _output.WriteLine($"[Disk]     Lex->Parse->Evaluator Read   Time: {diskReadTime} ms | ~{iterations / Math.Max(1, (diskReadTime / 1000.0)):N0} ops/sec");
        _output.WriteLine("---------------------------------------------");

        Assert.Equal(iterations, memoryResults?.Count ?? 0);
        Assert.Equal(iterations, diskResults?.Count ?? 0);
    }

    private void Execute(string sql)
    {
        var engine = new QueryEngine(sql, _session);
        var res = engine.Parse();
        if (res.Count != 0 && res.Last().IsError)
        {
            var errors = string.Join(", ", res.Last().Messages);
            throw new Exception(errors);
        }
    }

    private Core.Contracts.Results.QueryResult ExecuteAndReturn(string sql)
    {
        var engine = new QueryEngine(sql, _session);
        return engine.Parse().Last();
    }

    public void Dispose()
    {
        try { Catalog.DropDatabase(_testDb); } catch { }
        try { if (Directory.Exists(_diskPath)) Directory.Delete(_diskPath, true); } catch { }

        Monitor.Exit(GlobalEngineLock);
    }
}
