using System;
using System.Collections.Generic;
using Server.Server.BTree.BPlus; // For BPlus Disk Pager
using System.Diagnostics;
using BenchmarkDotNet.Attributes;

namespace Server.Benchmarks;

[MemoryDiagnoser]
public class SqlExecutionBenchmarker
{
    private BinaryBPlusTreeIndex _bplusIndex = null!;
    private readonly string _bplusIndexFile = "benchmark_evaluator_bplus_age.btree";
    private readonly Dictionary<string, Dictionary<string, dynamic>> _mockTableContent = new();
    private readonly int _targetAge = 40;

    [GlobalSetup]
    public void Setup()
    {
        if (File.Exists(_bplusIndexFile)) File.Delete(_bplusIndexFile);

        var tempIndex = new BinaryBPlusTreeIndex();
        tempIndex.Load(_bplusIndexFile);

        int numRows = 1_000_000;
        var random = new Random(42);

        for (int i = 0; i < numRows; i++)
        {
            string rowId = Guid.NewGuid().ToString();
            int age = (i % 100 == 0) ? _targetAge : random.Next(18, 80);

            tempIndex.Insert(age.ToString(), rowId);

            _mockTableContent[rowId] = new Dictionary<string, dynamic>
            {
                { "Id", rowId },
                { "Age", age },
                { "Name", $"User_{i}" }
            };
        }

        tempIndex.Save(_bplusIndexFile);
        tempIndex.Dispose();

        _bplusIndex = BinaryBPlusTreeIndex.LoadFile(_bplusIndexFile);
    }

    [Benchmark(Baseline = true)]
    public int FullTableScan_DictionaryMock()
    {
        int count = 0;
        foreach (var row in _mockTableContent.Values)
        {
            if ((int)row["Age"] == _targetAge)
            {
                count++;
            }
        }
        return count;
    }

    [Benchmark]
    public int BPlusTree_LazyDeserialization_MockEngine()
    {
        // 1. Index Manager fetches IDs from the B+Tree
        var resultIds = _bplusIndex.Search(_targetAge.ToString());
        
        // 2. Mock 'DbContext.SelectFromTable' which pulls from our in-memory dictionary
        int loadedRows = 0;
        foreach (string id in resultIds)
        {
            if (_mockTableContent.TryGetValue(id, out var row))
            {
                loadedRows++;
            }
        }

        return loadedRows;
    }
}
