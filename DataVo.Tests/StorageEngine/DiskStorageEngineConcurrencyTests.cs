using DataVo.Core.StorageEngine.Disk;
using System.Text;

namespace DataVo.Tests.StorageEngine;

public class DiskStorageEngineConcurrencyTests
{
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
