using System.Globalization;
using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Sql;

/// <summary>
/// <c>ORDER BY ... LIMIT k</c> without an index keeps the best k rows while it scans, and MIN and MAX keep one
/// value. A scanned row is compared with what is kept as typed cells, so the rows that lose (nearly all)
/// allocate nothing; they used to box their sort keys first, 84 bytes a row. These tests check the order
/// against LINQ for every key type, direction, NULLs, ties and OFFSET, over a table and over a join, and bound
/// the allocation per scanned row.
/// </summary>
public sealed class SqlTypedTopNTests
{
    public static TheoryData<StorageMode> Modes => new() { StorageMode.InMemory, StorageMode.Lsm };

    private sealed record Order(int Id, int? Qty, long Big, double Amount, float Score, string? Tag, int CustomerId);

    // Many ties on every key (Qty has 7 values, Score 9, Tag 5) and NULLs in Qty and Tag.
    private static readonly Order[] Orders = [.. Enumerable.Range(1, 900).Select(i => new Order(
        i,
        i % 13 == 0 ? null : i % 7,
        5_000_000_000L - (long)i * i % 1_000,
        i * 37 % 250 + 0.5,
        i % 9 * 0.25f - 1f,
        i % 11 == 0 ? null : "t" + (char)('a' + i % 5),
        i % 40 + 1))];

    [Theory]
    [MemberData(nameof(Modes))]
    public void TopRowsOfAScan_MatchLinqForEveryKeyTypeAndDirection(StorageMode mode)
    {
        WithOrders(mode, context =>
        {
            // The benchmark's shape: a DOUBLE key descending, ties broken by the primary key.
            AssertIds(context, "SELECT Id, Amount FROM Orders ORDER BY Amount DESC, Id ASC LIMIT 10",
                Orders.OrderByDescending(o => o.Amount).ThenBy(o => o.Id).Take(10));
            AssertIds(context, "SELECT Id FROM Orders ORDER BY Amount ASC, Id DESC LIMIT 25",
                Orders.OrderBy(o => o.Amount).ThenByDescending(o => o.Id).Take(25));

            // INT with NULLs (NULL sorts lowest), BIGINT, FLOAT and text keys; ties keep scan order.
            AssertIds(context, "SELECT Id FROM Orders ORDER BY Qty ASC LIMIT 80", Orders.OrderBy(o => o.Qty).Take(80));
            AssertIds(context, "SELECT Id FROM Orders ORDER BY Qty DESC, Big ASC, Id LIMIT 60",
                Orders.OrderByDescending(o => o.Qty).ThenBy(o => o.Big).ThenBy(o => o.Id).Take(60));
            AssertIds(context, "SELECT Id FROM Orders ORDER BY Big DESC, Id DESC LIMIT 15",
                Orders.OrderByDescending(o => o.Big).ThenByDescending(o => o.Id).Take(15));
            AssertIds(context, "SELECT Id FROM Orders ORDER BY Score DESC LIMIT 120", Orders.OrderByDescending(o => o.Score).Take(120));
            AssertIds(context, "SELECT Id FROM Orders ORDER BY Score ASC, Amount DESC, Id LIMIT 40",
                Orders.OrderBy(o => o.Score).ThenByDescending(o => o.Amount).ThenBy(o => o.Id).Take(40));
            AssertIds(context, "SELECT Id FROM Orders ORDER BY Tag ASC, Id DESC LIMIT 100",
                Orders.OrderBy(o => o.Tag, StringComparer.CurrentCulture).ThenByDescending(o => o.Id).Take(100));
            AssertIds(context, "SELECT Id FROM Orders ORDER BY Tag DESC, Qty ASC, Id LIMIT 100",
                Orders.OrderByDescending(o => o.Tag, StringComparer.CurrentCulture).ThenBy(o => o.Qty).ThenBy(o => o.Id).Take(100));

            // A filter, an OFFSET, a LIMIT larger than the table, and LIMIT 1.
            AssertIds(context, "SELECT Id FROM Orders WHERE Qty >= 3 AND Amount < 200 ORDER BY Amount DESC, Id LIMIT 12 OFFSET 30",
                Orders.Where(o => o.Qty >= 3 && o.Amount < 200).OrderByDescending(o => o.Amount).ThenBy(o => o.Id).Skip(30).Take(12));
            AssertIds(context, "SELECT Id FROM Orders ORDER BY Amount DESC, Id LIMIT 5000", Orders.OrderByDescending(o => o.Amount).ThenBy(o => o.Id));
            AssertIds(context, "SELECT Id FROM Orders ORDER BY Score ASC, Big DESC, Id DESC LIMIT 1",
                Orders.OrderBy(o => o.Score).ThenByDescending(o => o.Big).ThenByDescending(o => o.Id).Take(1));
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void TopRowsOfAJoin_AndMinMax_MatchLinq(StorageMode mode)
    {
        WithOrders(mode, context =>
        {
            const string join = " FROM Orders o JOIN Customers c ON o.CustomerId = c.Id ";
            AssertIds(context, "SELECT o.Id, c.Name" + join + "ORDER BY o.Amount DESC, o.Id ASC LIMIT 20",
                Orders.OrderByDescending(o => o.Amount).ThenBy(o => o.Id).Take(20));
            AssertIds(context, "SELECT o.Id" + join + "WHERE c.Tier = 2 ORDER BY o.Score ASC, o.Qty DESC, o.Id LIMIT 30",
                Orders.Where(o => o.CustomerId % 4 == 2).OrderBy(o => o.Score).ThenByDescending(o => o.Qty).ThenBy(o => o.Id).Take(30));
            AssertIds(context, "SELECT o.Id" + join + "ORDER BY c.Tier DESC, o.Big ASC, o.Id DESC LIMIT 45",
                Orders.OrderByDescending(o => o.CustomerId % 4).ThenBy(o => o.Big).ThenByDescending(o => o.Id).Take(45));

            // MIN and MAX of every numeric type and of text, with NULLs skipped; then per group.
            QueryResult extremes = Run(context, "SELECT MIN(Qty), MAX(Qty), MIN(Big), MAX(Big), MIN(Amount), MAX(Amount), MIN(Score), MAX(Score), MIN(Tag), MAX(Tag) FROM Orders WHERE Id > 3");
            Order[] rows = [.. Orders.Where(o => o.Id > 3)];
            Assert.Equal(
                new object?[] { rows.Min(o => o.Qty), rows.Max(o => o.Qty), rows.Min(o => o.Big), rows.Max(o => o.Big), rows.Min(o => o.Amount), rows.Max(o => o.Amount), rows.Min(o => o.Score), rows.Max(o => o.Score), rows.Min(o => o.Tag), rows.Max(o => o.Tag) }.Select(Text),
                Assert.Single(extremes.Data).Values.Select(Text));

            QueryResult grouped = Run(context, "SELECT Qty, MIN(Amount), MAX(Score), MAX(Big) FROM Orders GROUP BY Qty");
            Assert.Equal(
                Orders.GroupBy(o => o.Qty).Select(g => string.Join("|", new object?[] { g.Key, g.Min(o => o.Amount), g.Max(o => o.Score), g.Max(o => o.Big) }.Select(Text))).Order(StringComparer.Ordinal),
                grouped.Data.Select(row => string.Join("|", row.Values.Select(Text))).Order(StringComparer.Ordinal));
        });
    }

    /// <summary>
    /// A top-10 over a scan, a top-10 over a join and a MIN/MAX allocate per kept row, not per scanned row:
    /// measured against a COUNT over the same rows, they add what the kept rows cost (under 16 bytes per scanned
    /// row here, where a thousand rows enter the list), not 84, 60 and 48 bytes for every row as before.
    /// </summary>
    [Theory]
    [MemberData(nameof(Modes))]
    public void TopNAndMinMax_AllocateNothingPerScannedRow(StorageMode mode)
    {
        const int rowCount = 40_000;
        string? directory = mode == StorageMode.Lsm ? TestStorageDirectories.Create("sql_typed_topn_alloc") : null;
        using var context = new DataVoContext(new DataVoConfig { StorageMode = mode, DataDirectory = directory });
        Execute(context, "CREATE DATABASE TopNAlloc");
        Execute(context, "USE TopNAlloc");
        Execute(context, "CREATE TABLE Customers (Id INT PRIMARY KEY, Tier INT)");
        Execute(context, "CREATE TABLE Orders (Id INT PRIMARY KEY, CustomerId INT, Amount DOUBLE, Qty INT)");
        Execute(context, "INSERT INTO Customers (Id, Tier) VALUES " + string.Join(", ", Enumerable.Range(1, 200).Select(i => $"({i}, {i % 4})")));
        for (int start = 1; start <= rowCount; start += 2_000)
        {
            // Amount rises over the first 997 rows, so a descending top-N replaces a kept row at each of them
            // (about 375 B for the row it builds); after that nearly every row loses.
            Execute(context, "INSERT INTO Orders (Id, CustomerId, Amount, Qty) VALUES " + string.Join(", ", Enumerable.Range(start, 2_000).Select(i => $"({i}, {i % 200 + 1}, {i % 997}.25, {i % 50})")));
        }

        long scan = Bytes("SELECT COUNT(*) FROM Orders WHERE Qty >= 0");
        long joinScan = Bytes("SELECT COUNT(*) FROM Orders o JOIN Customers c ON o.CustomerId = c.Id");
        foreach ((string sql, long baseline) in new[]
        {
            ("SELECT Id, Amount FROM Orders ORDER BY Amount DESC, Id ASC LIMIT 10", scan),
            ("SELECT Id FROM Orders WHERE Qty >= 0 ORDER BY Qty ASC, Amount DESC LIMIT 10", scan),
            ("SELECT MIN(Amount), MAX(Amount), MIN(Qty), MAX(Qty) FROM Orders WHERE Qty >= 0", scan),
            ("SELECT o.Id, c.Tier FROM Orders o JOIN Customers c ON o.CustomerId = c.Id ORDER BY o.Amount DESC, o.Id ASC LIMIT 10", joinScan),
        })
        {
            long added = Bytes(sql) - baseline;
            Assert.True(added < rowCount * 16, $"{mode}: {sql} added {added:N0} B over the scan ({added / (double)rowCount:F1} B per row)");
        }

        long Bytes(string sql)
        {
            Assert.False(context.Execute(sql)[^1].IsError, sql); // warm up
            long before = GC.GetAllocatedBytesForCurrentThread();
            QueryResult result = context.Execute(sql)[^1];
            long bytes = GC.GetAllocatedBytesForCurrentThread() - before;
            Assert.False(result.IsError, sql + ": " + string.Join("; ", result.Messages));
            return bytes;
        }
    }

    private static string Text(object? value) => value switch
    {
        null => "NULL",
        int or long => Convert.ToInt64(value, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture),
        float or double => Math.Round(Convert.ToDouble(value, CultureInfo.InvariantCulture), 4).ToString("R", CultureInfo.InvariantCulture),
        _ => value.ToString()!,
    };

    private static void AssertIds(DataVoContext context, string sql, IEnumerable<Order> expected)
    {
        QueryResult result = Run(context, sql);
        int[] want = [.. expected.Select(o => o.Id)];
        int[] got = [.. result.Data.Select(row => Convert.ToInt32(row.Values.First(), CultureInfo.InvariantCulture))];
        Assert.True(want.SequenceEqual(got), $"{sql}: expected [{string.Join(",", want.Take(15))}], got [{string.Join(",", got.Take(15))}]");
    }

    private static QueryResult Run(DataVoContext context, string sql)
    {
        QueryResult result = context.Execute(sql)[^1];
        Assert.False(result.IsError, sql + ": " + string.Join("; ", result.Messages));
        return result;
    }

    private static void WithOrders(StorageMode mode, Action<DataVoContext> test)
    {
        string? directory = mode == StorageMode.Lsm ? TestStorageDirectories.Create("sql_typed_topn") : null;
        using var context = new DataVoContext(new DataVoConfig { StorageMode = mode, DataDirectory = directory });
        Execute(context, "CREATE DATABASE TopN");
        Execute(context, "USE TopN");
        Execute(context, "CREATE TABLE Customers (Id INT PRIMARY KEY, Tier INT, Name VARCHAR(8))");
        Execute(context, "CREATE TABLE Orders (Id INT PRIMARY KEY, Qty INT, Big BIGINT, Amount DOUBLE, Score FLOAT, Tag VARCHAR(8), CustomerId INT)");
        Execute(context, "INSERT INTO Customers (Id, Tier, Name) VALUES " + string.Join(", ", Enumerable.Range(1, 40).Select(i => $"({i}, {i % 4}, 'c{i}')")));
        Execute(context, "INSERT INTO Orders (Id, Qty, Big, Amount, Score, Tag, CustomerId) VALUES " + string.Join(", ", Orders.Select(o =>
            $"({o.Id}, {o.Qty?.ToString(CultureInfo.InvariantCulture) ?? "NULL"}, {o.Big}, {o.Amount.ToString(CultureInfo.InvariantCulture)}, {o.Score.ToString(CultureInfo.InvariantCulture)}, {(o.Tag is null ? "NULL" : $"'{o.Tag}'")}, {o.CustomerId})")));
        test(context);
    }

    private static void Execute(DataVoContext context, string sql)
    {
        QueryResult result = context.Execute(sql)[^1];
        Assert.False(result.IsError, sql[..Math.Min(sql.Length, 140)] + ": " + string.Join("; ", result.Messages));
    }
}
