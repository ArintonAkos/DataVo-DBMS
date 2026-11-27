using System.Globalization;
using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Sql;

/// <summary>
/// A range on a single integer primary key reads only the rows in it: on the LSM engine through its clustered key,
/// on the InMemory engine through its primary-key lane, which keeps a sorted copy of the keys for ranges. These
/// tests compare every bound shape with LINQ, through literal SQL and prepared statements (with diagnostics on,
/// the typed scan answers; with them off, the prepared statement's own scan plan does), after inserts, deletes,
/// updates and re-inserts. Rows come back in the order a full scan returns them.
/// </summary>
public sealed class SqlPrimaryKeyRangeTests
{
    public static TheoryData<StorageMode> Modes => new() { StorageMode.InMemory, StorageMode.Lsm };

    private sealed record Item(long Id, int V);

    [Theory]
    [MemberData(nameof(Modes))]
    public void EveryBoundShape_MatchesLinq_BeforeAndAfterWrites(StorageMode mode)
    {
        var random = new Random(77);
        // Keys inserted in no particular order, negative ones included; the scan order is the insertion order on
        // InMemory and the key order on LSM.
        List<Item> items = [.. Enumerable.Range(-300, 1_500).Select(i => new Item(i * 3L, i)).OrderBy(_ => random.Next())];
        WithTable(mode, items, context =>
        {
            AssertRanges(context, items, mode);

            Execute(context, "INSERT INTO T (Id, V) VALUES (1, 1), (-1, -1), (10000, 5), (4, 4)");
            items.AddRange([new Item(1, 1), new Item(-1, -1), new Item(10_000, 5), new Item(4, 4)]);
            Execute(context, "DELETE FROM T WHERE Id >= 300 AND Id < 600");
            items.RemoveAll(item => item.Id is >= 300 and < 600);
            Execute(context, "UPDATE T SET V = V + 1000 WHERE Id < 0");
            items = [.. items.Select(item => item.Id < 0 ? item with { V = item.V + 1000 } : item)];
            Execute(context, "INSERT INTO T (Id, V) VALUES (300, 7), (303, 8)");
            items.AddRange([new Item(300, 7), new Item(303, 8)]);
            AssertRanges(context, items, mode);
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void NarrowRange_ReadsOnlyItsRows(StorageMode mode)
    {
        List<Item> items = [.. Enumerable.Range(1, 20_000).Select(i => new Item(i, i % 7))];
        WithTable(mode, items, context =>
        {
            context.Diagnostics.Enabled = true;
            QueryResult result = Run(context, "SELECT Id, V FROM T WHERE Id BETWEEN 5000 AND 5099");
            Assert.Equal(100, result.Data.Count);
            var stats = context.Diagnostics.LastQuery!;
            Assert.False(stats.FullTableScan);
            if (mode == StorageMode.InMemory)
            {
                Assert.Contains("_PK_T", stats.IndexesUsed);
                Assert.Equal(100, stats.RowsRead);
            }
        });
    }

    private static void AssertRanges(DataVoContext context, List<Item> items, StorageMode mode)
    {
        // The reference order is the engine's own full scan (an UPDATE moves a row on InMemory); its rows must be
        // exactly the expected ones.
        List<Item> scanOrder = [.. Run(context, "SELECT Id, V FROM T").Data.Select(row => new Item(Convert.ToInt64(row["Id"], CultureInfo.InvariantCulture), Convert.ToInt32(row["V"], CultureInfo.InvariantCulture)))];
        Assert.Equal(items.OrderBy(item => item.Id), scanOrder.OrderBy(item => item.Id));
        if (mode == StorageMode.Lsm)
        {
            Assert.Equal(scanOrder.OrderBy(item => item.Id), scanOrder);
        }

        (string Where, Func<long, bool> Holds)[] ranges =
        [
            ("Id BETWEEN -120 AND 450", id => id is >= -120 and <= 450),
            ("Id >= 0 AND Id < 99", id => id is >= 0 and < 99),
            ("Id > -4 AND Id <= 5", id => id is > -4 and <= 5),
            ("Id > 4000", id => id > 4000),
            ("Id < -850", id => id < -850),
            ("Id >= 7 AND Id < 7", _ => false),
            ("Id BETWEEN 10 AND 20 AND V > 3", id => id is >= 10 and <= 20),
        ];

        foreach ((string where, Func<long, bool> holds) in ranges)
        {
            string[] expected = [.. scanOrder.Where(item => holds(item.Id) && (!where.Contains("V > 3", StringComparison.Ordinal) || item.V > 3)).Select(item => $"{item.Id}:{item.V}")];
            foreach (bool diagnostics in new[] { true, false })
            {
                context.Diagnostics.Enabled = diagnostics;
                Assert.Equal(expected, Rows(Run(context, $"SELECT Id, V FROM T WHERE {where}")));
            }
        }

        // Prepared, bound bounds: diagnostics on (typed scan) and off (the prepared scan plan).
        DataVoPreparedStatement statement = context.Prepare("SELECT Id, V FROM T WHERE Id >= @lo AND Id < @hi");
        foreach ((long lo, long hi) in new[] { (-90L, 210L), (0L, 1L), (500L, 400L), (9_999L, 10_001L) })
        {
            string[] expected = [.. scanOrder.Where(item => item.Id >= lo && item.Id < hi).Select(item => $"{item.Id}:{item.V}")];
            foreach (bool diagnostics in new[] { true, false })
            {
                context.Diagnostics.Enabled = diagnostics;
                QueryResult result = statement.Execute(DataVoQueryParameter.FromObject("lo", lo), DataVoQueryParameter.FromObject("hi", hi));
                Assert.False(result.IsError, string.Join("; ", result.Messages));
                Assert.Equal(expected, Rows(result));
            }
        }

        context.Diagnostics.Enabled = false;
    }

    private static string[] Rows(QueryResult result) =>
        [.. result.Data.Select(row => string.Join(":", row.Values.Select(value => Convert.ToString(value, CultureInfo.InvariantCulture))))];

    private static QueryResult Run(DataVoContext context, string sql)
    {
        QueryResult result = context.Execute(sql)[^1];
        Assert.False(result.IsError, sql + ": " + string.Join("; ", result.Messages));
        return result;
    }

    private static void WithTable(StorageMode mode, List<Item> items, Action<DataVoContext> test)
    {
        string? directory = mode == StorageMode.Lsm ? TestStorageDirectories.Create("sql_pk_range") : null;
        using var context = new DataVoContext(new DataVoConfig { StorageMode = mode, DataDirectory = directory });
        Execute(context, "CREATE DATABASE KeyRange");
        Execute(context, "USE KeyRange");
        Execute(context, "CREATE TABLE T (Id BIGINT PRIMARY KEY, V INT)");
        foreach (Item[] chunk in items.Chunk(1_000))
        {
            Execute(context, "INSERT INTO T (Id, V) VALUES " + string.Join(", ", chunk.Select(item => $"({item.Id}, {item.V})")));
        }

        test(context);
    }

    private static void Execute(DataVoContext context, string sql)
    {
        QueryResult result = context.Execute(sql)[^1];
        Assert.False(result.IsError, sql + ": " + string.Join("; ", result.Messages));
    }
}
