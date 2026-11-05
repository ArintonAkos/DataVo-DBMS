using System.Globalization;
using System.Text;
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Performance;

/// <summary>
/// A SQL point read must cost the same on a small and a large table. A SELECT used to read-lock
/// every row of the table and the planner loaded the whole table just to estimate its size, so a
/// point SELECT cost O(rows): 275 ms on 10,000 rows. Allocation is measured instead of time because
/// it is deterministic on a loaded CI machine.
/// </summary>
public sealed class SqlPointSelectScalingTests
{
    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Lsm)]
    public void PointSelect_ByPrimaryKey_DoesNotScaleWithTableSize(StorageMode mode)
    {
        long small = MeasurePointSelectAllocation(mode, rows: 500, "SELECT Name FROM T WHERE Id = {0}");
        long large = MeasurePointSelectAllocation(mode, rows: 20_000, "SELECT Name FROM T WHERE Id = {0}");

        // Loading 20,000 rows allocates several MB per query; a point read allocates a few KB.
        Assert.True(large < small * 2 + 4096,
            $"point SELECT allocated {large} B/query on 20,000 rows vs {small} B/query on 500 rows");
    }

    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Lsm)]
    public void PointSelect_BySecondaryIndex_DoesNotScaleWithTableSize(StorageMode mode)
    {
        long small = MeasurePointSelectAllocation(mode, rows: 500, "SELECT Id FROM T WHERE Code = {0}", codeIsUnique: true);
        long large = MeasurePointSelectAllocation(mode, rows: 20_000, "SELECT Id FROM T WHERE Code = {0}", codeIsUnique: true);

        Assert.True(large < small * 2 + 4096,
            $"indexed SELECT allocated {large} B/query on 20,000 rows vs {small} B/query on 500 rows");
    }

    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Lsm)]
    public void PointSelect_ReturnsTheRequestedRow(StorageMode mode)
    {
        string? directory = mode == StorageMode.Lsm ? TestStorageDirectories.Create("point_select") : null;
        try
        {
            using var context = CreateSeededContext(mode, directory, rows: 1_000, codeIsUnique: false);

            var result = context.Execute("SELECT Name FROM T WHERE Id = 777");
            Assert.False(result[0].IsError, string.Join(" | ", result[0].Messages));
            Assert.Single(result[0].Data);
            Assert.Equal("name-777", result[0].Data[0]["Name"]?.ToString());

            var byIndex = context.Execute("SELECT Id FROM T WHERE Code = 7");
            Assert.False(byIndex[0].IsError, string.Join(" | ", byIndex[0].Messages));
            Assert.Equal(10, byIndex[0].Data.Count); // Code = Id % 100, 1,000 rows
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    private static long MeasurePointSelectAllocation(StorageMode mode, int rows, string sqlFormat, bool codeIsUnique = false)
    {
        string? directory = mode == StorageMode.Lsm ? TestStorageDirectories.Create("point_select_scaling") : null;
        try
        {
            using var context = CreateSeededContext(mode, directory, rows, codeIsUnique);

            string[] queries = new string[200];
            for (int i = 0; i < queries.Length; i++)
            {
                int key = 1 + (i * 37 % rows);
                queries[i] = string.Format(CultureInfo.InvariantCulture, sqlFormat, key);
            }

            foreach (string sql in queries)
            {
                AssertSingleRow(context.Execute(sql));
            }

            long before = GC.GetAllocatedBytesForCurrentThread();
            foreach (string sql in queries)
            {
                context.Execute(sql);
            }

            return (GC.GetAllocatedBytesForCurrentThread() - before) / queries.Length;
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    private static DataVoContext CreateSeededContext(StorageMode mode, string? directory, int rows, bool codeIsUnique)
    {
        var context = new DataVoContext(new DataVoConfig { StorageMode = mode, DiskStoragePath = directory });
        context.Execute("CREATE DATABASE Scaling");
        context.Execute("USE Scaling");
        context.Execute("CREATE TABLE T (Id INT PRIMARY KEY, Name VARCHAR(32), Code INT)");
        context.Execute("CREATE INDEX ix_code ON T (Code)");

        var sql = new StringBuilder();
        for (int start = 1; start <= rows; start += 500)
        {
            sql.Clear().Append("INSERT INTO T (Id, Name, Code) VALUES ");
            for (int id = start; id < start + 500 && id <= rows; id++)
            {
                if (id > start)
                {
                    sql.Append(',');
                }

                int code = codeIsUnique ? id : id % 100;
                sql.Append(CultureInfo.InvariantCulture, $"({id}, 'name-{id}', {code})");
            }

            var result = context.Execute(sql.ToString());
            Assert.False(result[0].IsError, string.Join(" | ", result[0].Messages));
        }

        return context;
    }

    private static void AssertSingleRow(List<DataVo.Core.Contracts.Results.QueryResult> results)
    {
        Assert.False(results[0].IsError, string.Join(" | ", results[0].Messages));
        Assert.Single(results[0].Data);
    }

    private static void DeleteDirectory(string? directory)
    {
        if (directory is not null && Directory.Exists(directory))
        {
            try
            {
                Directory.Delete(directory, recursive: true);
            }
            catch (IOException)
            {
                // Best effort; the directory is unique to this test.
            }
        }
    }
}
