using Research.Benchmark.Runners.DiskCrud;
using Research.Benchmark.Abstractions;

namespace Research.Benchmark.Tests;

public sealed class DiskCrudEngineTests
{
    [Fact]
    public void DataVoLsmVariants_HaveDistinctBenchmarkNames()
    {
        using var production = new DataVoDiskCrudEngine(
            durable: true,
            storageMode: DataVoDiskCrudStorageMode.Lsm);
        using var relaxed = new DataVoDiskCrudEngine(
            durable: false,
            storageMode: DataVoDiskCrudStorageMode.Lsm);

        Assert.Equal("DataVo (LSM Production)", production.Name);
        Assert.Equal("DataVo (LSM Relaxed)", relaxed.Name);
    }

    [Fact]
    public void DiskCrudWalAllMatrix_ContainsOnlyModernCompetitiveEngines()
    {
        using var engines = new CompositeDisposable(
            DiskCrudEngineMatrix.Create(engineFilter: "all", checkpointIntervalMs: null, zeroAllocUpdate: true));

        string[] names = engines.Items.Select(engine => engine.Name).ToArray();

        Assert.Equal(
        [
            "DataVo (LSM Production)",
            "DataVo (LSM Relaxed)",
            "SQLite (WAL,normal)",
            "SQLite (WAL,full)",
        ], names);
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
    public void DataVoLsmVariant_RunsExplicitUpdateBatch()
    {
        string root = Path.Combine(Path.GetTempPath(), $"datavo-lsm-disk-crud-update-batch-test-{Guid.NewGuid():N}");
        using var engine = new DataVoDiskCrudEngine(
            durable: false,
            storageMode: DataVoDiskCrudStorageMode.Lsm);

        try
        {
            engine.Initialize(root);
            engine.BeginInsertBatch();
            engine.Insert(new FlatRecord(1, "one", 10, 1.5d));
            engine.Insert(new FlatRecord(2, "two", 20, 2.5d));
            engine.CompleteInsertBatch();

            engine.BeginUpdateBatch();
            engine.Update(id: 1, newValue: 99, newScore: 9.5d);
            engine.Update(id: 2, newValue: 88, newScore: 8.5d);
            engine.CompleteUpdateBatch();
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

    [Fact]
    public void DataVoLsmVariant_BatchedUpdateAllocationsStayBelowNineHundredBytesPerOperation()
    {
        const int records = 4_096;
        string root = Path.Combine(Path.GetTempPath(), $"datavo-lsm-disk-crud-batch-alloc-test-{Guid.NewGuid():N}");
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

            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            long before = GC.GetAllocatedBytesForCurrentThread();
            engine.BeginUpdateBatch();
            for (int i = 1; i <= records; i++)
            {
                engine.Update(id: i, newValue: records - i, newScore: i * 1.25d);
            }

            engine.CompleteUpdateBatch();
            long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            double bytesPerOperation = (double)allocated / records;

            Assert.True(
                bytesPerOperation < 900d,
                $"Expected < 900 B/op, got {bytesPerOperation:N1} B/op ({allocated:N0} bytes total).");
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }

    private sealed class CompositeDisposable : IDisposable
    {
        public CompositeDisposable(IReadOnlyList<IDiskCrudEngine> items)
        {
            Items = items;
        }

        public IReadOnlyList<IDiskCrudEngine> Items { get; }

        public void Dispose()
        {
            foreach (IDiskCrudEngine item in Items)
            {
                item.Dispose();
            }
        }
    }
}
