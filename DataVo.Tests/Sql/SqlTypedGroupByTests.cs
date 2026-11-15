using System.Globalization;
using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Sql;

/// <summary>
/// GROUP BY on the typed paths numbers groups by key without allocating per row and applies HAVING, ORDER BY and
/// LIMIT to the groups before building rows. These tests compare high-cardinality groupings with LINQ over the
/// seed data under the general path's rules: HAVING on aggregates and on group columns, ORDER BY aggregate aliases
/// and group columns with LIMIT and OFFSET, ties kept in the order groups first appear, a NULL key forming one
/// group, DISTINCT after a LIMIT, and keys of type INT, BIGINT, FLOAT, VARCHAR and of two columns.
/// </summary>
public sealed class SqlTypedGroupByTests
{
    public static TheoryData<StorageMode> Modes => new() { StorageMode.InMemory, StorageMode.Lsm };

    private sealed record Order(int Id, int? CustomerId, string Status, double Amount, long Big, double Score);

    private static readonly string[] Statuses = ["NEW", "PAID", "SHIPPED", "VOID"];

    // About 400 customers with uneven order counts; every 29th order has no customer.
    private static readonly Order[] Orders = [.. Enumerable.Range(1, 3_000).Select(i => new Order(
        i,
        i % 29 == 0 ? null : (int)((long)i * i % 397) + 1,
        Statuses[i % 4],
        i * 7 % 300 + 0.5,
        (i % 50) + 5_000_000_000L,
        i % 8 * 0.5))];

    private static IEnumerable<IGrouping<int?, Order>> ByCustomer => Orders.GroupBy(o => o.CustomerId);

    [Theory]
    [MemberData(nameof(Modes))]
    public void HavingOrderByLimit_OverManyGroups_MatchLinq(StorageMode mode)
    {
        WithSchema(mode, context =>
        {
            foreach (int k in new[] { 5, 8, 12 })
            {
                AssertRows(context, $"SELECT CustomerId, COUNT(*) AS N FROM Orders GROUP BY CustomerId HAVING COUNT(*) >= {k} ORDER BY N DESC, CustomerId ASC LIMIT 7",
                    [.. ByCustomer.Where(g => g.Count() >= k).OrderByDescending(g => g.Count()).ThenBy(g => g.Key).Take(7).Select(g => Row(g.Key, g.Count()))]);
            }

            // OFFSET, and a bound HAVING value.
            AssertRows(context, "SELECT CustomerId, COUNT(*) AS N FROM Orders GROUP BY CustomerId HAVING COUNT(*) >= 6 ORDER BY N DESC, CustomerId LIMIT 5 OFFSET 4",
                [.. ByCustomer.Where(g => g.Count() >= 6).OrderByDescending(g => g.Count()).ThenBy(g => g.Key).Skip(4).Take(5).Select(g => Row(g.Key, g.Count()))]);
            QueryResult prepared = context.Prepare("SELECT CustomerId, COUNT(*) AS N FROM Orders GROUP BY CustomerId HAVING COUNT(*) >= @k ORDER BY N DESC, CustomerId ASC LIMIT 3")
                .Execute(DataVoQueryParameter.FromObject("k", 9));
            Assert.False(prepared.IsError, string.Join("; ", prepared.Messages));
            Assert.Equal(
                ByCustomer.Where(g => g.Count() >= 9).OrderByDescending(g => g.Count()).ThenBy(g => g.Key).Take(3).Select(g => Key(Row(g.Key, g.Count()))),
                prepared.Data.Select(r => Key(r.Values)));

            // Ties without a second key keep the order the groups first appeared in (a stable sort).
            AssertRows(context, "SELECT CustomerId, COUNT(*) AS N FROM Orders GROUP BY CustomerId ORDER BY N DESC LIMIT 12",
                [.. ByCustomer.OrderByDescending(g => g.Count()).Take(12).Select(g => Row(g.Key, g.Count()))]);

            // HAVING on the group column and on a SUM; ORDER BY the group column, NULL group included.
            AssertRows(context, "SELECT CustomerId, SUM(Amount) AS total FROM Orders GROUP BY CustomerId HAVING CustomerId > 380 AND SUM(Amount) > 600 ORDER BY CustomerId DESC LIMIT 6",
                [.. ByCustomer.Where(g => g.Key > 380 && g.Sum(o => o.Amount) > 600).OrderByDescending(g => g.Key).Take(6).Select(g => Row(g.Key, g.Sum(o => o.Amount)))]);
            AssertRows(context, "SELECT CustomerId, COUNT(*) AS N FROM Orders GROUP BY CustomerId ORDER BY CustomerId LIMIT 3",
                [.. ByCustomer.OrderBy(g => g.Key).Take(3).Select(g => Row(g.Key, g.Count()))]);

            // HAVING without a LIMIT returns every passing group.
            AssertSameRows(context, "SELECT CustomerId, COUNT(*), MIN(Amount), MAX(Id) FROM Orders GROUP BY CustomerId HAVING COUNT(*) < 4",
                [.. ByCustomer.Where(g => g.Count() < 4).Select(g => Row(g.Key, g.Count(), g.Min(o => o.Amount), g.Max(o => o.Id)))]);
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void DistinctAfterGrouping_SeesEveryGroupBeforeTheLimit(StorageMode mode)
    {
        WithSchema(mode, context =>
        {
            // The top three distinct counts, not the distinct values among the top three groups.
            AssertRows(context, "SELECT DISTINCT COUNT(*) AS N FROM Orders GROUP BY CustomerId ORDER BY N DESC LIMIT 3",
                [.. ByCustomer.Select(g => g.Count()).Distinct().OrderByDescending(n => n).Take(3).Select(n => Row(n))]);
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void KeysOfEveryType_GroupAsLinqDoes(StorageMode mode)
    {
        WithSchema(mode, context =>
        {
            AssertSameRows(context, "SELECT Status, COUNT(*), SUM(Amount) FROM Orders GROUP BY Status",
                [.. Orders.GroupBy(o => o.Status).Select(g => Row(g.Key, g.Count(), g.Sum(o => o.Amount)))]);
            AssertRows(context, "SELECT Status, COUNT(*) AS N FROM Orders GROUP BY Status HAVING COUNT(*) > 700 ORDER BY Status DESC LIMIT 2",
                [.. Orders.GroupBy(o => o.Status).Where(g => g.Count() > 700).OrderByDescending(g => g.Key, StringComparer.Ordinal).Take(2).Select(g => Row(g.Key, g.Count()))]);
            AssertSameRows(context, "SELECT Big, COUNT(*) FROM Orders GROUP BY Big",
                [.. Orders.GroupBy(o => o.Big).Select(g => Row(g.Key, g.Count()))]);
            AssertSameRows(context, "SELECT Score, COUNT(*), AVG(Amount) FROM Orders GROUP BY Score",
                [.. Orders.GroupBy(o => o.Score).Select(g => Row(g.Key, g.Count(), g.Average(o => o.Amount)))]);
            AssertRows(context, "SELECT CustomerId, Status, COUNT(*) AS N FROM Orders GROUP BY CustomerId, Status HAVING COUNT(*) >= 3 ORDER BY N DESC, CustomerId, Status LIMIT 8",
                [.. Orders.GroupBy(o => (o.CustomerId, o.Status)).Where(g => g.Count() >= 3)
                    .OrderByDescending(g => g.Count()).ThenBy(g => g.Key.CustomerId).ThenBy(g => g.Key.Status, StringComparer.Ordinal).Take(8)
                    .Select(g => Row(g.Key.CustomerId, g.Key.Status, g.Count()))]);
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void AggregatesOverNoMatchingRow_AnswerAsSqlDefines(StorageMode mode)
    {
        WithSchema(mode, context =>
        {
            // One row: COUNT 0 (an int, as the general path returns it), the other aggregates NULL.
            QueryResult empty = Run(context, "SELECT COUNT(*), COUNT(CustomerId), SUM(Amount), AVG(Amount), MIN(Status), MAX(Id) FROM Orders WHERE Amount > 100000");
            Assert.Equal([0, 0, null, null, null, null], Assert.Single(empty.Data).Values.ToArray());
            Assert.IsType<int>(empty.Data[0].Values.First());

            // ORDER BY and LIMIT see that row; with GROUP BY there is no group and no row.
            Assert.Single(Run(context, "SELECT COUNT(*) AS n FROM Orders WHERE Amount > 100000 ORDER BY n LIMIT 5").Data);
            Assert.Empty(Run(context, "SELECT CustomerId, COUNT(*) FROM Orders WHERE Amount > 100000 GROUP BY CustomerId").Data);

            // The same through a columnar index, which answers it without reading rows.
            Execute(context, "CREATE INDEX cx ON Orders (Amount, Id) USING COLUMNAR");
            Assert.Equal([0, null], Assert.Single(Run(context, "SELECT COUNT(*), SUM(Id) FROM Orders WHERE Amount > 100000").Data).Values.ToArray());
        });
    }

    /// <summary>
    /// An aggregate whose WHERE matches no row stays on the typed scan: it used to re-run on the general path,
    /// building a dictionary for every row (about 900 bytes per row).
    /// </summary>
    [Theory]
    [MemberData(nameof(Modes))]
    public void AggregatesOverNoMatchingRow_DoNotRescanOnTheGeneralPath(StorageMode mode)
    {
        WithSchema(mode, context =>
        {
            (QueryResult result, long bytes) = Measure(context, "SELECT COUNT(*), SUM(Amount) FROM Orders WHERE Amount > 100000");
            Assert.Equal([0, null], Assert.Single(result.Data).Values.ToArray());
            Assert.True(bytes / Orders.Length < 64, $"{mode}: {bytes / Orders.Length} B per row");
        });
    }

    /// <summary>
    /// A typed GROUP BY over many groups allocates per group, not per row. Measured as what grouping adds to the
    /// same scan without it: about 260 bytes per row before (a boxed <c>object[]</c> key per row, an object per
    /// group, two dictionaries per group for HAVING and ORDER BY), a few dozen now with a group per 8 rows.
    /// </summary>
    [Theory]
    [MemberData(nameof(Modes))]
    public void ManyGroups_AllocatePerGroupNotPerRow(StorageMode mode)
    {
        const int rows = 60_000;
        string? directory = mode == StorageMode.Lsm ? TestStorageDirectories.Create("sql_typed_group_alloc") : null;
        using var context = new DataVoContext(new DataVoConfig { StorageMode = mode, DiskStoragePath = directory });
        Execute(context, "CREATE DATABASE GroupAlloc");
        Execute(context, "USE GroupAlloc");
        Execute(context, "CREATE TABLE Orders (Id INT PRIMARY KEY, CustomerId INT, Amount DOUBLE)");
        for (int start = 1; start <= rows; start += 2_000)
        {
            Execute(context, "INSERT INTO Orders (Id, CustomerId, Amount) VALUES " + string.Join(", ", Enumerable.Range(start, 2_000).Select(i => $"({i}, {(long)i * 7919 % 7_500}, {i % 97}.5)")));
        }

        const string sql = "SELECT CustomerId, COUNT(*) AS N FROM Orders GROUP BY CustomerId HAVING COUNT(*) >= 8 ORDER BY N DESC, CustomerId ASC LIMIT 100";
        (QueryResult result, long grouped) = Measure(context, sql);
        (_, long scan) = Measure(context, "SELECT COUNT(*) FROM Orders");
        long perRow = (grouped - scan) / rows;

        Assert.False(result.IsError, string.Join("; ", result.Messages));
        Assert.Equal(100, result.Data.Count);
        Assert.True(perRow < 64, $"{mode}: grouping added {perRow} B per row");
    }

    /// <summary>
    /// A typed join grouped by one column of a hashed table looks the group up by value: GROUP BY adds nothing per
    /// joined row to the same join without it (it added an <c>object[]</c> key each).
    /// </summary>
    [Theory]
    [MemberData(nameof(Modes))]
    public void JoinGroupedByOneColumn_AllocatesNothingPerJoinedRow(StorageMode mode)
    {
        const int orders = 40_000;
        string? directory = mode == StorageMode.Lsm ? TestStorageDirectories.Create("sql_join_group_alloc") : null;
        using var context = new DataVoContext(new DataVoConfig { StorageMode = mode, DiskStoragePath = directory });
        Execute(context, "CREATE DATABASE JoinGroupAlloc");
        Execute(context, "USE JoinGroupAlloc");
        Execute(context, "CREATE TABLE Customers (Id INT PRIMARY KEY, Region VARCHAR(4))");
        Execute(context, "CREATE TABLE Orders (Id INT PRIMARY KEY, CustomerId INT, Amount DOUBLE)");
        Execute(context, "INSERT INTO Customers (Id, Region) VALUES " + string.Join(", ", Enumerable.Range(1, 500).Select(i => $"({i}, 'R{i % 8}')")));
        for (int start = 1; start <= orders; start += 2_000)
        {
            Execute(context, "INSERT INTO Orders (Id, CustomerId, Amount) VALUES " + string.Join(", ", Enumerable.Range(start, 2_000).Select(i => $"({i}, {i % 500 + 1}, {i % 97}.5)")));
        }

        context.Diagnostics.Enabled = true;
        (QueryResult result, long grouped) = Measure(context, "SELECT c.Region, COUNT(*), SUM(o.Amount) FROM Orders o JOIN Customers c ON o.CustomerId = c.Id GROUP BY c.Region");
        Assert.Equal(0, context.Diagnostics.LastQuery!.JoinExecutions);
        (_, long ungrouped) = Measure(context, "SELECT COUNT(*), SUM(o.Amount) FROM Orders o JOIN Customers c ON o.CustomerId = c.Id");
        long perRow = (grouped - ungrouped) / orders;

        Assert.False(result.IsError, string.Join("; ", result.Messages));
        Assert.Equal(8, result.Data.Count);
        Assert.Equal(orders, result.Data.Sum(r => Convert.ToInt64(r.Values.ElementAt(1), CultureInfo.InvariantCulture)));
        Assert.True(perRow < 8, $"{mode}: GROUP BY added {perRow} B per joined row");
    }

    /// <summary>Runs <paramref name="sql"/> once to warm up, then again, returning that result and the bytes it allocated.</summary>
    private static (QueryResult Result, long Bytes) Measure(DataVoContext context, string sql)
    {
        Assert.False(context.Execute(sql)[^1].IsError);
        long before = GC.GetAllocatedBytesForCurrentThread();
        QueryResult result = context.Execute(sql)[^1];
        return (result, GC.GetAllocatedBytesForCurrentThread() - before);
    }

    private static object?[] Row(params object?[] values) => values;

    private static string Key(IEnumerable<object?> row) => string.Join("|", row.Select(value => value switch
    {
        null => "NULL",
        int or long => Convert.ToInt64(value, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture),
        float or double or decimal => Math.Round(Convert.ToDouble(value, CultureInfo.InvariantCulture), 6).ToString("R", CultureInfo.InvariantCulture),
        _ => value.ToString(),
    }));

    private static void AssertRows(DataVoContext context, string sql, object?[][] expected)
    {
        QueryResult result = Run(context, sql);
        Assert.Equal(expected.Select(Key), result.Data.Select(r => Key(r.Values)));
    }

    private static void AssertSameRows(DataVoContext context, string sql, object?[][] expected)
    {
        QueryResult result = Run(context, sql);
        Assert.Equal(expected.Select(Key).Order(StringComparer.Ordinal), result.Data.Select(r => Key(r.Values)).Order(StringComparer.Ordinal));
    }

    private static QueryResult Run(DataVoContext context, string sql)
    {
        QueryResult result = context.Execute(sql)[^1];
        Assert.False(result.IsError, sql + ": " + string.Join("; ", result.Messages));
        return result;
    }

    private static void WithSchema(StorageMode mode, Action<DataVoContext> test)
    {
        string? directory = mode == StorageMode.Lsm ? TestStorageDirectories.Create("sql_typed_group") : null;
        using var context = new DataVoContext(new DataVoConfig { StorageMode = mode, DiskStoragePath = directory });
        Execute(context, "CREATE DATABASE TypedGroup");
        Execute(context, "USE TypedGroup");
        Execute(context, "CREATE TABLE Orders (Id INT PRIMARY KEY, CustomerId INT, Status VARCHAR(8), Amount DOUBLE, Big BIGINT, Score FLOAT)");
        foreach (Order[] chunk in Orders.Chunk(1_000))
        {
            Execute(context, "INSERT INTO Orders (Id, CustomerId, Status, Amount, Big, Score) VALUES " + string.Join(", ", chunk.Select(o =>
                $"({o.Id}, {o.CustomerId?.ToString(CultureInfo.InvariantCulture) ?? "NULL"}, '{o.Status}', {o.Amount.ToString(CultureInfo.InvariantCulture)}, {o.Big}, {o.Score.ToString(CultureInfo.InvariantCulture)})")));
        }

        test(context);
    }

    private static void Execute(DataVoContext context, string sql)
    {
        QueryResult result = context.Execute(sql)[^1];
        Assert.False(result.IsError, sql + ": " + string.Join("; ", result.Messages));
    }
}
