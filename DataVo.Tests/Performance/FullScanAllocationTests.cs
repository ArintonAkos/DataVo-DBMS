using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Performance;

/// <summary>
/// A typed full scan allocates nothing per row on the active engines, however the rows were written. The
/// InMemory engine used to copy every SQL-inserted (serialized) row into a list per scan (about 53 bytes a row),
/// and the LSM engine copied every unflushed version of a large memtable into a fresh buffer (about 106 bytes a
/// row); rows inserted as typed cells or flushed to SSTables did not pay either cost.
/// </summary>
public sealed class FullScanAllocationTests
{
    private const int Rows = 40_000;

    public static TheoryData<StorageMode> Modes => new() { StorageMode.InMemory, StorageMode.Lsm };

    [Theory]
    [MemberData(nameof(Modes))]
    public void ScansOfSqlInsertedRows_AllocateNothingPerRow(StorageMode mode)
    {
        using DataVoContext context = Open(mode);
        foreach (string sql in new[]
        {
            "SELECT COUNT(*) FROM T",
            "SELECT COUNT(*) FROM T WHERE Code = 1",
            "SELECT SUM(Amount) FROM T WHERE Code > 0",
            "SELECT Code, COUNT(*), SUM(Amount) FROM T GROUP BY Code",
            "SELECT Status, COUNT(*) FROM T GROUP BY Status",
        })
        {
            Run(context, sql); // warm up
            long before = GC.GetAllocatedBytesForCurrentThread();
            Run(context, sql);
            double perRow = (GC.GetAllocatedBytesForCurrentThread() - before) / (double)Rows;
            Assert.True(perRow < 4, $"{mode} {sql} allocated {perRow:F1} B per row");
        }
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void BlockedScans_ReturnEveryLiveRowOnce(StorageMode mode)
    {
        using DataVoContext context = Open(mode);
        Run(context, "DELETE FROM T WHERE Id >= 1000 AND Id < 1500");
        Run(context, "DELETE FROM T WHERE Id >= 30000 AND Id < 30010");
        Run(context, "UPDATE T SET Code = 9 WHERE Id >= 20000 AND Id < 20008");

        // The scan crosses several pages of unflushed LSM versions and in-memory read-lock blocks.
        QueryResult rows = Run(context, "SELECT Id, Code FROM T WHERE Amount >= 0");
        int[] ids = [.. rows.Data.Select(static row => (int)row["Id"]!)];
        Assert.Equal(Enumerable.Range(0, Rows).Where(static id => id is not (>= 1000 and < 1500) and not (>= 30000 and < 30010)), ids.Order());

        // LSM rows are keyed by the primary key; an in-memory UPDATE writes the new version at a new position.
        if (mode == StorageMode.Lsm)
        {
            Assert.Equal(ids.Order(), ids);
        }

        Assert.Equal(8, rows.Data.Count(static row => (int)row["Code"]! == 9));
        Assert.Equal(5, Run(context, "SELECT Id FROM T WHERE Amount >= 0 LIMIT 5").Data.Count);
    }

    private static DataVoContext Open(StorageMode mode)
    {
        var context = new DataVoContext(new DataVoConfig
        {
            StorageMode = mode,
            DataDirectory = mode == StorageMode.Lsm ? TestStorageDirectories.Create("full_scan_alloc") : null,
        });
        Run(context, "CREATE DATABASE S");
        Run(context, "USE S");
        Run(context, "CREATE TABLE T (Id INT PRIMARY KEY, Status VARCHAR(12), Code INT, Amount DOUBLE)");
        string[] statuses = ["PAID", "SHIPPED", "NEW", "CANCELLED"];
        for (int start = 0; start < Rows; start += 1_000)
        {
            Run(context, "INSERT INTO T (Id, Status, Code, Amount) VALUES " + string.Join(", ", Enumerable.Range(start, 1_000)
                .Select(i => $"({i}, '{statuses[i % 4]}', {i % 4}, {i % 1000}.5)")));
        }

        return context;
    }

    private static QueryResult Run(DataVoContext context, string sql)
    {
        QueryResult result = context.Execute(sql)[^1];
        Assert.False(result.IsError, sql + ": " + string.Join("; ", result.Messages));
        return result;
    }
}
