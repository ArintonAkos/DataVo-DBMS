using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Lsm;

/// <summary>
/// Closing an LSM database flushes every table and index side by side; each must reach disk intact.
/// </summary>
public class LsmShutdownFlushTests : IDisposable
{
    private const int TableCount = 12;
    private const int RowsPerTable = 200;

    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"datavo_close_flush_{Guid.NewGuid():N}");

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }

    [Fact]
    public void Close_WithManyTablesAndIndexes_LeavesEveryRowAndIndexEntryReadable()
    {
        using (DataVoContext writer = Open())
        {
            writer.Execute("CREATE DATABASE Flush");
            writer.Execute("USE Flush");
            for (int table = 0; table < TableCount; table++)
            {
                writer.Execute($"CREATE TABLE T{table} (Id INT PRIMARY KEY, Grp INT)");
                writer.Execute($"CREATE INDEX ix_t{table}_grp ON T{table} (Grp)");
                for (int id = 1; id <= RowsPerTable; id++)
                {
                    Assert.False(writer.Execute($"INSERT INTO T{table} (Id, Grp) VALUES ({id}, {id % 4})")[^1].IsError);
                }
            }
        }

        using DataVoContext reader = Open();
        reader.Execute("USE Flush");
        for (int table = 0; table < TableCount; table++)
        {
            QueryResult all = reader.Execute($"SELECT Id FROM T{table}")[^1];
            Assert.Equal(RowsPerTable, all.Data.Count);

            QueryResult group = reader.Execute($"SELECT Id FROM T{table} WHERE Grp = 1")[^1];
            Assert.Equal(RowsPerTable / 4, group.Data.Count);
        }
    }

    private DataVoContext Open() => new(new DataVoConfig
    {
        StorageMode = StorageMode.Lsm,
        DiskStoragePath = _directory,
        LsmStrictFsync = false,
    });
}
