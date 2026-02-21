using BenchmarkDotNet.Attributes;
using Server.Server.BTree.Core;
using System.Collections.Generic;

using System.IO;

namespace Server.Benchmarks;

[MemoryDiagnoser]
[RankColumn]
public class QueryBenchmarker
{
    [Params(100_000, 1_000_000)]
    public int RowCount { get; set; }

    // Dictionaries for standard matching
    private Dictionary<string, Dictionary<string, dynamic>> _tableContent = new();
    private IIndex _ageIndex = null!;
    private IIndex _binaryIndex = null!;
    private IIndex _bplusIndex = null!;
    
    // Arrays for columnar (SIMD) matching
    private int[] _ageColumnArray = null!;
    private string[] _rowIdArray = null!;

    private const int TargetAge = 40;

    [GlobalSetup]
    public void Setup()
    {
        var data = DataGenerator.GenerateData(RowCount);
        _tableContent = data.tableContent;
        _ageIndex = data.index;
        
        // Extract to array for C# hardware acceleration
        _ageColumnArray = new int[RowCount];
        _rowIdArray = new string[RowCount];
        
        int i = 0;
        
        string binaryIndexFile = "benchmark_binary_age.btree";
        if (File.Exists(binaryIndexFile)) File.Delete(binaryIndexFile);
        
        var binIndex = new global::Server.Server.BTree.Binary.BinaryBTreeIndex();
        binIndex.Load(binaryIndexFile);
        
        string bplusIndexFile = "benchmark_bplus_age.btree";
        if (File.Exists(bplusIndexFile)) File.Delete(bplusIndexFile);
        
        var bplusIndex = new global::Server.Server.BTree.BPlus.BinaryBPlusTreeIndex();
        bplusIndex.Load(bplusIndexFile);

        foreach (var kvp in _tableContent)
        {
            _rowIdArray[i] = kvp.Key;
            _ageColumnArray[i] = (int)kvp.Value["Age"];
            binIndex.Insert(_ageColumnArray[i].ToString(), kvp.Key);
            bplusIndex.Insert(_ageColumnArray[i].ToString(), kvp.Key);
            i++;
        }
        
        binIndex.Save(binaryIndexFile);
        bplusIndex.Save(bplusIndexFile);
        binIndex.Dispose();
        bplusIndex.Dispose();
        
        // Re-load representing a cold start but memory mapped
        _binaryIndex = global::Server.Server.BTree.Binary.BinaryBTreeIndex.LoadFile(binaryIndexFile);
        _bplusIndex = global::Server.Server.BTree.BPlus.BinaryBPlusTreeIndex.LoadFile(bplusIndexFile);
    }

    // [Benchmark(Baseline = true)]
    // public int FullTableScan_Dictionary()
    // {
    //     int matchCount = 0;
    //     foreach (var row in _tableContent.Values)
    //     {
    //         if (row["Age"] == TargetAge)
    //         {
    //             matchCount++;
    //         }
    //     }
    //     return matchCount;
    // }

    [Benchmark(Baseline = true)]
    public int FullTableScan_ArraySeq()
    {
        int matchCount = 0;
        for (int i = 0; i < _ageColumnArray.Length; i++)
        {
            if (_ageColumnArray[i] == TargetAge)
            {
                matchCount++;
            }
        }
        return matchCount;
    }

    [Benchmark]
    public int FullTableScan_ArraySIMD()
    {
        // This is where AVX2 vectorization shines (Server.Engines.SIMD)
        var matchedIndices = global::Server.Server.Engines.SIMD.SimdScanner.FilterNumericEquals(_ageColumnArray, TargetAge);
        return matchedIndices.Count;
    }

    [Benchmark]
    public int JsonBTreeIndexSeek()
    {
        var resultIds = _ageIndex.Search(TargetAge.ToString());
        return resultIds.Count;
    }

    [Benchmark]
    public int BinaryBTreeIndexSeek()
    {
        var resultIds = _binaryIndex.Search(TargetAge.ToString());
        return resultIds.Count;
    }

    [Benchmark]
    public int BinaryBPlusTreeIndexSeek()
    {
        var resultIds = _bplusIndex.Search(TargetAge.ToString());
        return resultIds.Count;
    }
}
