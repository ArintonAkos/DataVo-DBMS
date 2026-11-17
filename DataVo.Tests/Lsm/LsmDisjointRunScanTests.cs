using System.Globalization;
using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Lsm;

/// <summary>
/// A scan over runs whose key ranges do not overlap reads them one after another instead of merging them; runs that
/// overlap, or unflushed rows, take the merge. Each close writes the MemTable out as a run, so the table below gains
/// disjoint runs (appends past the end), then overlapping ones (updates, deletes and inserts inside earlier ranges),
/// and after every step a scan, a key-range scan and a LIMIT must match the rows a dictionary oracle holds.
/// </summary>
public sealed class LsmDisjointRunScanTests
{
    [Fact]
    public void ScansMatchTheOracle_AsRunsBecomeDisjointThenOverlapping()
    {
        string directory = TestStorageDirectories.Create("lsm_disjoint_runs");
        var oracle = new SortedDictionary<int, int>();
        DataVoContext context = Open(directory, create: true);
        try
        {
            for (int batch = 0; batch < 4; batch++)
            {
                int start = batch * 2_000 + 1;
                Write(context, oracle, Enumerable.Range(start, 2_000).Select(id => (id, id % 97)));
                context = Reopen(context, directory);
                AssertScans(context, oracle);
            }

            // Changes inside earlier ranges: the new runs overlap the old ones, and tombstones must hide rows.
            Run(context, "UPDATE T SET V = 1000 WHERE Id >= 1500 AND Id < 1600");
            for (int id = 1500; id < 1600; id++)
            {
                oracle[id] = 1000;
            }

            Run(context, "DELETE FROM T WHERE Id >= 3000 AND Id < 3300");
            for (int id = 3000; id < 3300; id++)
            {
                oracle.Remove(id);
            }

            AssertScans(context, oracle); // unflushed changes: the merge
            context = Reopen(context, directory);
            AssertScans(context, oracle); // overlapping runs: the merge

            Write(context, oracle, Enumerable.Range(20_001, 500).Select(id => (id, 5)));
            context = Reopen(context, directory);
            AssertScans(context, oracle);
        }
        finally
        {
            context.Dispose();
        }
    }

    private static void AssertScans(DataVoContext context, SortedDictionary<int, int> oracle)
    {
        Assert.Equal(oracle.Select(pair => $"{pair.Key}:{pair.Value}"), Rows(Run(context, "SELECT Id, V FROM T WHERE V >= 0")));
        Assert.Equal(oracle.Where(pair => pair.Key >= 1_900 && pair.Key < 6_100).Select(pair => $"{pair.Key}:{pair.Value}"), Rows(Run(context, "SELECT Id, V FROM T WHERE Id >= 1900 AND Id < 6100 AND V >= 0")));
        Assert.Equal(oracle.Count(pair => pair.Value == 5), Convert.ToInt64(Assert.Single(Run(context, "SELECT COUNT(*) AS N FROM T WHERE V = 5").Data)["N"], CultureInfo.InvariantCulture));
        Assert.Equal(7, Run(context, "SELECT Id FROM T WHERE V >= 0 LIMIT 7").Data.Count);
    }

    private static IEnumerable<string> Rows(QueryResult result) =>
        result.Data.Select(row => $"{row["Id"]}:{row["V"]}");

    private static void Write(DataVoContext context, SortedDictionary<int, int> oracle, IEnumerable<(int Id, int V)> rows)
    {
        foreach ((int Id, int V)[] chunk in rows.Chunk(500))
        {
            Run(context, "INSERT INTO T (Id, V) VALUES " + string.Join(", ", chunk.Select(row => $"({row.Id}, {row.V})")));
            foreach ((int id, int v) in chunk)
            {
                oracle[id] = v;
            }
        }
    }

    private static DataVoContext Reopen(DataVoContext context, string directory)
    {
        context.Dispose();
        return Open(directory, create: false);
    }

    private static DataVoContext Open(string directory, bool create)
    {
        var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.Lsm, DiskStoragePath = directory });
        if (create)
        {
            Run(context, "CREATE DATABASE D");
        }

        Run(context, "USE D");
        if (create)
        {
            Run(context, "CREATE TABLE T (Id INT PRIMARY KEY, V INT)");
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
