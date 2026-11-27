using System.Globalization;
using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Sql;

/// <summary>
/// A GROUP BY over one indexed integer column that only counts rows is answered from the index. A persisted (LSM)
/// index also counts the rows whose key is NULL; in memory, a nullable column's NULL group is counted from the rows'
/// null flags. Each query is
/// checked against the same query over <c>Tag</c>, an unindexed copy of the column that the scan groups by hash, so
/// the two plans must agree exactly: NULL keys, HAVING (rejected before a group is built, or tested by the general
/// evaluator), ORDER BY with LIMIT and OFFSET, after updates and deletes, and inside a transaction, where the scan
/// answers because the index holds committed rows only.
/// </summary>
public sealed class SqlIndexGroupByTests
{
    private const string Index = "ix_o_customer";

    public static TheoryData<StorageMode, bool> Cases => new()
    {
        { StorageMode.InMemory, false },
        { StorageMode.InMemory, true },
        { StorageMode.Lsm, false },
        { StorageMode.Lsm, true },
    };

    private static readonly string[] Shapes =
    [
        "SELECT {c}, COUNT(*) AS N FROM O GROUP BY {c} HAVING COUNT(*) >= 3 ORDER BY N DESC, {c} ASC LIMIT 10",
        "SELECT {c}, COUNT(*) AS N, COUNT({c}) AS K FROM O GROUP BY {c} ORDER BY {c}",
        "SELECT {c}, COUNT(*) AS N FROM O GROUP BY {c} HAVING COUNT(*) < 4 AND COUNT(*) > 1 ORDER BY {c} DESC LIMIT 5 OFFSET 2",
        "SELECT COUNT(*) AS N, {c} FROM O GROUP BY {c} HAVING 2 <= COUNT({c}) ORDER BY {c} LIMIT 7",
        "SELECT {c}, COUNT(*) AS N FROM O GROUP BY {c} HAVING COUNT(*) + 0 >= 2 ORDER BY N, {c} DESC LIMIT 6",
        "SELECT {c} AS Customer, COUNT(*) AS N FROM O GROUP BY {c} ORDER BY Customer DESC LIMIT 4",
    ];

    [Theory]
    [MemberData(nameof(Cases))]
    public void IndexPlan_MatchesTheHashPlan_ThroughUpdatesAndDeletes(StorageMode mode, bool nulls)
    {
        using DataVoContext context = Open(mode, rows: 600, nulls);
        AssertAllShapesMatch(context, nulls);

        Run(context, "UPDATE O SET CustomerId = 5, Tag = 5 WHERE Id >= 100 AND Id < 160");
        Run(context, "DELETE FROM O WHERE Id > 400 AND Id <= 470");
        if (nulls)
        {
            Run(context, "UPDATE O SET CustomerId = NULL, Tag = NULL WHERE Id >= 200 AND Id < 230");
        }

        Run(context, "INSERT INTO O (Id, CustomerId, Tag, Amount) VALUES (5001, 77, 77, 1.5), (5002, 77, 77, 2.5), (5003, 999, 999, 3.5)");
        AssertAllShapesMatch(context, nulls);

        DataVoPreparedStatement byIndex = context.Prepare(Shapes[0].Replace("{c}", "CustomerId").Replace(">= 3", ">= @k"));
        DataVoPreparedStatement byTag = context.Prepare(Shapes[0].Replace("{c}", "Tag").Replace(">= 3", ">= @k"));
        foreach (int k in new[] { 1, 2, 4, 50 })
        {
            Assert.Equal(Rows(byTag.Execute(DataVoQueryParameter.FromObject("k", k)), "Tag"), Rows(byIndex.Execute(DataVoQueryParameter.FromObject("k", k)), "CustomerId"));
        }
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public void InsideATransaction_TheScanCountsTheTransactionsOwnWrites(StorageMode mode, bool nulls)
    {
        using DataVoContext context = Open(mode, rows: 300, nulls);
        Run(context, "BEGIN");
        try
        {
            Run(context, "INSERT INTO O (Id, CustomerId, Tag, Amount) VALUES (9001, 3, 3, 1.0), (9002, 3, 3, 1.0), (9003, 3, 3, 1.0), (9004, 3, 3, 1.0)");
            Run(context, "DELETE FROM O WHERE CustomerId = 4");
            foreach (string shape in Shapes)
            {
                Assert.Equal(Rows(Run(context, shape.Replace("{c}", "Tag")), "Tag"), Rows(Run(context, shape.Replace("{c}", "CustomerId")), "CustomerId"));
            }

            QueryResult top = Run(context, "SELECT CustomerId, COUNT(CustomerId) AS N FROM O GROUP BY CustomerId ORDER BY N DESC, CustomerId LIMIT 1");
            Assert.Equal(3, Convert.ToInt32(Assert.Single(top.Data)["CustomerId"], CultureInfo.InvariantCulture));
        }
        finally
        {
            Run(context, "ROLLBACK");
        }
    }

    /// <summary>
    /// The index plan reads no table row and builds only the groups HAVING keeps: grouping 40,000 rows into about
    /// 38,000 keys, of which 20 hold 100 rows each, allocates a fraction of what scanning and hashing them does.
    /// </summary>
    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Lsm)]
    public void IndexPlan_AllocatesAFractionOfTheHashPlan(StorageMode mode)
    {
        using DataVoContext context = Open(mode, rows: 0, nulls: false);
        for (int start = 1; start <= 40_000; start += 1_000)
        {
            Run(context, "INSERT INTO O (Id, CustomerId, Tag, Amount) VALUES " + string.Join(", ", Enumerable.Range(start, 1_000).Select(id =>
            {
                int customer = id <= 2_000 ? 100_000 + id % 20 : id;
                return $"({id}, {customer}, {customer}, 1.0)";
            })));
        }

        string sql = "SELECT {c}, COUNT(*) AS N FROM O GROUP BY {c} HAVING COUNT(*) >= 50 ORDER BY N DESC, {c} ASC LIMIT 10";
        Assert.Equal(Rows(Run(context, sql.Replace("{c}", "Tag")), "Tag"), Rows(Run(context, sql.Replace("{c}", "CustomerId")), "CustomerId"));
        long hashed = Measure(context, sql.Replace("{c}", "Tag"));
        long indexed = Measure(context, sql.Replace("{c}", "CustomerId"));
        Assert.True(indexed * 4 < hashed, $"{mode}: index plan {indexed:N0} B, hash plan {hashed:N0} B");
    }

    /// <summary>
    /// After a reopen the index is read from its persisted runs; updates and deletes made afterwards sit in the
    /// unflushed overlay on top of them, and the merge must let the newest version of each entry decide.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void IndexPlan_MergesPersistedRunsWithNewerChanges_AfterReopen(bool nulls)
    {
        string directory = TestStorageDirectories.Create("sql_index_group_by_reopen");
        using (DataVoContext first = Open(StorageMode.Lsm, rows: 3_000, nulls, directory))
        {
            Run(first, "DELETE FROM O WHERE Id > 1000 AND Id <= 1300");
        }

        using DataVoContext context = Reopen(directory);
        AssertAllShapesMatch(context, nulls);

        Run(context, "UPDATE O SET CustomerId = 7, Tag = 7 WHERE Id >= 10 AND Id < 400");
        Run(context, "DELETE FROM O WHERE Id >= 2000 AND Id < 2100");
        Run(context, "INSERT INTO O (Id, CustomerId, Tag, Amount) VALUES (1001, 7, 7, 1.0), (1002, 8, 8, 1.0)");
        AssertAllShapesMatch(context, nulls);
    }

    private static void AssertAllShapesMatch(DataVoContext context, bool nulls)
    {
        foreach (string shape in Shapes)
        {
            List<string> expected = Rows(Run(context, shape.Replace("{c}", "Tag")), "Tag");
            context.Diagnostics.Enabled = true;
            QueryResult actual = Run(context, shape.Replace("{c}", "CustomerId"));
            var stats = context.Diagnostics.LastQuery!;
            context.Diagnostics.Enabled = false;

            Assert.Equal(expected, Rows(actual, "CustomerId"));
            Assert.NotEmpty(expected);

            // LSM indexes hold NULL keys; in memory a nullable column's NULL group is counted from the rows' null flags.
            Assert.Contains(Index, stats.IndexesUsed, StringComparer.OrdinalIgnoreCase);
            Assert.Equal(nulls && !context.Engine.IndexManager.IndexesNullKeys, stats.FullTableScan);
        }
    }

    /// <summary>The rows of a result as text, with the grouped column's name made the same for both plans.</summary>
    private static List<string> Rows(QueryResult result, string column)
    {
        Assert.False(result.IsError, string.Join("; ", result.Messages));
        return [.. result.Data.Select(row => string.Join("|", row.Select(pair =>
            $"{(pair.Key == column ? "C" : pair.Key)}={Convert.ToString(pair.Value, CultureInfo.InvariantCulture) ?? "NULL"}")))];
    }

    private static long Measure(DataVoContext context, string sql)
    {
        Run(context, sql); // warm up
        long before = GC.GetAllocatedBytesForCurrentThread();
        Assert.NotEmpty(Run(context, sql).Data);
        return GC.GetAllocatedBytesForCurrentThread() - before;
    }

    private static DataVoContext Reopen(string directory)
    {
        var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.Lsm, DataDirectory = directory });
        Run(context, "USE G");
        return context;
    }

    private static DataVoContext Open(StorageMode mode, int rows, bool nulls, string? directory = null)
    {
        var context = new DataVoContext(new DataVoConfig
        {
            StorageMode = mode,
            DataDirectory = mode == StorageMode.Lsm ? directory ?? TestStorageDirectories.Create("sql_index_group_by") : null,
        });
        Run(context, "CREATE DATABASE G");
        Run(context, "USE G");
        Run(context, $"CREATE TABLE O (Id INT PRIMARY KEY, CustomerId INT{(nulls ? "" : " NOT NULL")}, Tag INT, Amount DOUBLE)");
        Run(context, $"CREATE INDEX {Index} ON O (CustomerId)");
        for (int start = 1; start <= rows; start += 1_000)
        {
            Run(context, "INSERT INTO O (Id, CustomerId, Tag, Amount) VALUES " + string.Join(", ", Enumerable.Range(start, Math.Min(1_000, rows - start + 1)).Select(id =>
            {
                string customer = nulls && id % 9 == 0 ? "NULL" : (id * 7919 % (rows / 4 + 3)).ToString(CultureInfo.InvariantCulture);
                return $"({id}, {customer}, {customer}, {id % 100}.25)";
            })));
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
