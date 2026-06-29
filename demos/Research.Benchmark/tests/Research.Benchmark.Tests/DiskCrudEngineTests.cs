using Research.Benchmark.Runners.DiskCrud;
using Research.Benchmark.Abstractions;

namespace Research.Benchmark.Tests;

public sealed class DiskCrudEngineTests
{
    [Fact]
    public void DataVoLsmVariant_HasDistinctBenchmarkName()
    {
        using var engine = new DataVoDiskCrudEngine(
            durable: false,
            storageMode: DataVoDiskCrudStorageMode.Lsm);

        Assert.Equal("DataVo (LSM experimental)", engine.Name);
    }

    [Fact]
    public void DataVoLsmVariant_RunsInsertAndUpdateLoop()
    {
        string root = Path.Combine(Path.GetTempPath(), $"datavo-lsm-disk-crud-test-{Guid.NewGuid():N}");
        using var engine = new DataVoDiskCrudEngine(
            durable: false,
            storageMode: DataVoDiskCrudStorageMode.Lsm);

        try
        {
            engine.Initialize(root);
            engine.BeginInsertBatch();
            engine.Insert(new FlatRecord(1, "one", 10, 1.5d));
            engine.CompleteInsertBatch();

            engine.Update(id: 1, newValue: 99, newScore: 9.5d);
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }

    [Fact]
    public void DataVoLsmVariant_UpdateAllocationsStayBelowOneHundredBytesPerOperation()
    {
        const int records = 1_024;
        string root = Path.Combine(Path.GetTempPath(), $"datavo-lsm-disk-crud-alloc-test-{Guid.NewGuid():N}");
        using var engine = new DataVoDiskCrudEngine(
            durable: false,
            storageMode: DataVoDiskCrudStorageMode.Lsm);

        try
        {
            engine.Initialize(root);
            engine.BeginInsertBatch();
            for (int i = 1; i <= records; i++)
            {
                engine.Insert(new FlatRecord(i, $"r{i}", i, i * 0.5d));
            }

            engine.CompleteInsertBatch();

            engine.Update(id: 1, newValue: 42, newScore: 21.0d);
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int i = 1; i <= records; i++)
            {
                engine.Update(id: i, newValue: records - i, newScore: i * 1.25d);
            }

            long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            double bytesPerOperation = (double)allocated / records;

            Assert.True(
                bytesPerOperation < 100d,
                $"Expected < 100 B/op, got {bytesPerOperation:N1} B/op ({allocated:N0} bytes total).");
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }
}
