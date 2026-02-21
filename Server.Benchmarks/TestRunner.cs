using System;
using DataVo.Core.BTree.Binary;
using System.IO;

namespace Server.Benchmarks;

public class TestRunner
{
    public static void Run()
    {
        Console.WriteLine("Generating data...");
        var data = DataGenerator.GenerateData(1_000_000);
        
        string binaryIndexFile = "test_binary.btree";
        if (File.Exists(binaryIndexFile)) File.Delete(binaryIndexFile);
        
        Console.WriteLine("Creating Binary index...");
        var binIndex = new global::DataVo.Core.BTree.Binary.BinaryBTreeIndex();
        binIndex.Load(binaryIndexFile);
        
        string bplusIndexFile = "test_bplus.btree";
        if (File.Exists(bplusIndexFile)) File.Delete(bplusIndexFile);
        
        var bplusIndex = new global::DataVo.Core.BTree.BPlus.BinaryBPlusTreeIndex();
        bplusIndex = global::DataVo.Core.BTree.BPlus.BinaryBPlusTreeIndex.LoadFile(bplusIndexFile);

        int i = 0;
        foreach (var kvp in data.tableContent)
        {
            if (i % 100000 == 0) Console.WriteLine($"Inserting row {i}...");
            binIndex.Insert(kvp.Value["Age"].ToString(), kvp.Key);
            bplusIndex.Insert(kvp.Value["Age"].ToString(), kvp.Key);
            i++;
        }
        
        binIndex.Save(binaryIndexFile);
        bplusIndex.Save(bplusIndexFile);
        binIndex.Dispose();
        bplusIndex.Dispose();
        Console.WriteLine("Done saving.");
        
        var loadedBin = new global::DataVo.Core.BTree.Binary.BinaryBTreeIndex();
        loadedBin.Load(binaryIndexFile);
        
        var loadedBplus = global::DataVo.Core.BTree.BPlus.BinaryBPlusTreeIndex.LoadFile(bplusIndexFile);
        
        Console.WriteLine($"Found {loadedBin.Search("40").Count} matches for 40 in B-Tree");
        Console.WriteLine($"Found {loadedBplus.Search("40").Count} matches for 40 in B+Tree");
    }
}
