using DataVo.Core.StorageEngine.Disk;
using DataVo.Core.StorageEngine.Config;
using System.Text;

namespace DataVo.Tests.StorageEngine;

public class DiskStorageEngineConcurrencyTests
{
    [Fact]
    public void PooledMode_InsertReadDeleteAndCompact_RoundTrips()
    {
        string root = Path.Combine(Path.GetTempPath(), $"datavo_storage_pooling_{Guid.NewGuid():N}");
        Directory.CreateDirectory(root);

        try
        {
            using var engine = new DiskStorageEngine(root, syncWrites: true, IoSchedulerMode.PoolingOnly);

            const string database = "PooledDb";
            const string table = "Users";
            byte[] alice = Encoding.UTF8.GetBytes("alice");
            byte[] bob = Encoding.UTF8.GetBytes("bob");

            long aliceId = engine.InsertRow(database, table, alice);
            long bobId = engine.InsertRow(database, table, bob);

            Assert.Equal(alice, engine.ReadRow(database, table, aliceId));
            Assert.Equal(bob, engine.ReadRow(database, table, bobId));

            engine.DeleteRow(database, table, aliceId);
            var remaining = engine.CompactTable(database, table);

            Assert.Single(remaining);
            Assert.Equal(bob, remaining[0].RawRow);
            Assert.Equal(bob, engine.ReadRow(database, table, remaining[0].NewRowId));
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
    public async Task ConcurrentInsertsAcrossEngineInstances_SamePath_DoNotFailAndPreserveRowCount()
    {
        string root = Path.Combine(Path.GetTempPath(), $"datavo_storage_concurrency_{Guid.NewGuid():N}");
        Directory.CreateDirectory(root);

        try
        {
            var engineA = new DiskStorageEngine(root);
            var engineB = new DiskStorageEngine(root);

            const string database = "ConcurrentDb";
            const string table = "Users";
            const int perEngineWrites = 100;

            Task[] writers =
            [
                Task.Run(() =>
                {
                    for (int i = 0; i < perEngineWrites; i++)
                    {
                        byte[] bytes = Encoding.UTF8.GetBytes($"A:{i}");
                        engineA.InsertRow(database, table, bytes);
                    }
                }),
                Task.Run(() =>
                {
                    for (int i = 0; i < perEngineWrites; i++)
                    {
                        byte[] bytes = Encoding.UTF8.GetBytes($"B:{i}");
                        engineB.InsertRow(database, table, bytes);
                    }
                })
            ];

            await Task.WhenAll(writers);

            int count = engineA.ReadAllRows(database, table).Count();
            Assert.Equal(perEngineWrites * 2, count);
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
