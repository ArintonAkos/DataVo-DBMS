using System.Globalization;
using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Sql;

/// <summary>
/// The hashed side of the typed join keeps only the columns a joined row reads, and only key counts when that is
/// nothing but the join key (anti-joins, <c>COUNT(c.Id)</c>, existence-only INNER joins). These tests compare
/// those shapes with LINQ over the seed data under SQL's rules: NULL and dangling foreign keys, duplicate build
/// keys, empty tables and empty results, LEFT JOIN conditions tested after the join, and joined rows returned
/// without a bounded top-N. Every query must stay on the typed path (no general join).
/// </summary>
[Collection(AllocationSensitiveCollection.Name)]
public sealed class SqlTypedJoinBuildSideTests
{
    public static TheoryData<StorageMode> Modes => new() { StorageMode.InMemory, StorageMode.Lsm };

    private sealed record Customer(long Id, string Region, int Tier, string Name);

    private sealed record Order(int Id, long? CustomerId, int Qty, double Amount);

    // Customers 13 and 14 do not exist; orders 1, 18, 35, ... have no customer at all.
    private static readonly Customer[] Customers = [.. Enumerable.Range(1, 12).Select(i => new Customer(i, "R" + i % 3, i % 3, "c" + i))];

    private static readonly Order[] Orders = [.. Enumerable.Range(1, 200).Select(i => new Order(
        i,
        i % 17 == 1 ? null : i % 14 + 1,
        i % 4 + 1,
        i * 37 % 500 + 0.25))];

    private static Customer? CustomerOf(Order order) => Customers.FirstOrDefault(c => c.Id == order.CustomerId);

    private static IEnumerable<(Order O, Customer? C)> LeftC => Orders.Select(o => (o, CustomerOf(o)));

    private const string FromLeftC = " FROM Orders o LEFT JOIN Customers c ON o.CustomerId = c.Id";

    [Theory]
    [MemberData(nameof(Modes))]
    public void AntiJoin_CountsOrdersWithoutACustomer(StorageMode mode)
    {
        WithSchema(mode, context =>
        {
            var orphans = LeftC.Where(j => j.C is null).ToArray();
            Assert.Contains(orphans, j => j.O.CustomerId is null);
            Assert.Contains(orphans, j => j.O.CustomerId is not null);

            AssertTotals(context, "SELECT COUNT(*)" + FromLeftC + " WHERE c.Id IS NULL", [orphans.Length]);
            AssertTotals(context, "SELECT COUNT(*), SUM(o.Amount), MAX(o.Qty)" + FromLeftC + " WHERE c.Id IS NULL AND o.Qty > 1",
                [orphans.Count(j => j.O.Qty > 1), orphans.Where(j => j.O.Qty > 1).Sum(j => j.O.Amount), orphans.Where(j => j.O.Qty > 1).Max(j => j.O.Qty)]);

            // The semi-join: orders that have a customer, and the key counted and summed on the padded side.
            AssertTotals(context, "SELECT COUNT(*)" + FromLeftC + " WHERE c.Id IS NOT NULL", [Orders.Length - orphans.Length]);
            AssertTotals(context, "SELECT COUNT(*), COUNT(c.Id), SUM(c.Id), MIN(c.Id)" + FromLeftC,
                [Orders.Length, Orders.Length - orphans.Length, LeftC.Sum(j => j.C?.Id ?? 0), LeftC.Where(j => j.C is not null).Min(j => j.C!.Id)]);

            // A condition on another column of the padded side needs that column, not just the key.
            AssertTotals(context, "SELECT COUNT(*)" + FromLeftC + " WHERE c.Id IS NULL OR c.Tier = 2", [LeftC.Count(j => j.C is null || j.C.Tier == 2)]);

            // Prepared, as the benchmark runs it.
            QueryResult prepared = context.Prepare("SELECT COUNT(*)" + FromLeftC + " WHERE c.Id IS NULL").Execute();
            Assert.False(prepared.IsError, string.Join("; ", prepared.Messages));
            Assert.Equal(orphans.Length, Convert.ToInt32(Assert.Single(prepared.Data).Values.Single(), CultureInfo.InvariantCulture));
            AssertTyped(context);
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void DuplicateBuildKeys_RepeatEveryMatch(StorageMode mode)
    {
        WithSchema(mode, context =>
        {
            // Tier is not unique: an order matches every customer of its tier (Qty 1-4 against tiers 0-2).
            int matches(Order o) => Customers.Count(c => c.Tier == o.Qty);
            AssertTotals(context, "SELECT COUNT(*), COUNT(c.Tier) FROM Orders o LEFT JOIN Customers c ON o.Qty = c.Tier",
                [Orders.Sum(o => Math.Max(1, matches(o))), Orders.Sum(matches)]);
            AssertTotals(context, "SELECT COUNT(*) FROM Orders o LEFT JOIN Customers c ON o.Qty = c.Tier WHERE c.Tier IS NULL", [Orders.Count(o => matches(o) == 0)]);
            AssertTotals(context, "SELECT COUNT(*), SUM(o.Amount) FROM Orders o JOIN Customers c ON o.Qty = c.Tier",
                [Orders.Sum(matches), Orders.Sum(o => matches(o) * o.Amount)]);
            AssertTyped(context);
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void KeyOnlyHashedTable_InReturnedRows_KeepsEachRowsKey(StorageMode mode)
    {
        WithSchema(mode, context =>
        {
            // c.Id is the only column read from Customers, so its rows are not kept; each returned row must still
            // carry its own key (or NULL), not the last one matched.
            AssertRows(context, "SELECT o.Id, c.Id" + FromLeftC + " ORDER BY o.Amount DESC, o.Id LIMIT 12",
                [.. LeftC.OrderByDescending(j => j.O.Amount).ThenBy(j => j.O.Id).Take(12).Select(j => new object?[] { j.O.Id, j.C?.Id })]);
            AssertRows(context, "SELECT o.Id, c.Id AS cid FROM Orders o JOIN Customers c ON o.CustomerId = c.Id ORDER BY cid DESC, o.Id LIMIT 9",
                [.. LeftC.Where(j => j.C is not null).OrderByDescending(j => j.C!.Id).ThenBy(j => j.O.Id).Take(9).Select(j => new object?[] { j.O.Id, j.C!.Id })]);

            // BIGINT keys come back as BIGINT.
            QueryResult result = Run(context, "SELECT o.Id, c.Id" + FromLeftC + " WHERE c.Id IS NOT NULL ORDER BY o.Id LIMIT 1");
            Assert.IsType<long>(Assert.Single(result.Data).Values.Last());
            AssertTyped(context);
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void ChainedJoin_HashedMiddleTableKeepsTheNextStepsKey(StorageMode mode)
    {
        WithSchema(mode, context =>
        {
            // Orders are streamed; Customers are hashed by Id and must keep Tier, which keys the join to Tiers.
            Execute(context, "CREATE TABLE Tiers (Id INT PRIMARY KEY, Label VARCHAR(8))");
            Execute(context, "INSERT INTO Tiers (Id, Label) VALUES (0, 'basic'), (1, 'gold')");
            bool hasTier(Customer? c) => c is { Tier: 0 or 1 };

            AssertTotals(context, "SELECT COUNT(*), COUNT(c.Id), COUNT(t.Id)" + FromLeftC + " LEFT JOIN Tiers t ON c.Tier = t.Id",
                [Orders.Length, LeftC.Count(j => j.C is not null), LeftC.Count(j => hasTier(j.C))]);
            AssertTotals(context, "SELECT COUNT(*) FROM Orders o JOIN Customers c ON o.CustomerId = c.Id JOIN Tiers t ON c.Tier = t.Id WHERE t.Label = 'gold'",
                [LeftC.Count(j => j.C?.Tier == 1)]);
            AssertSameRows(
                [.. LeftC.Where(j => hasTier(j.C)).Select(j => new object?[] { j.O.Id, j.C!.Tier == 1 ? "gold" : "basic" })],
                Run(context, "SELECT o.Id, t.Label FROM Orders o JOIN Customers c ON o.CustomerId = c.Id JOIN Tiers t ON c.Tier = t.Id"));
            AssertTyped(context);
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void ProjectedHashedTable_GroupsSortsAndReturnsTheColumnsItKept(StorageMode mode)
    {
        WithSchema(mode, context =>
        {
            var groups = LeftC.GroupBy(j => j.C?.Region)
                .Select(g => (g.Key, (long)g.Count(), Math.Round(g.Sum(j => j.O.Amount), 6)))
                .OrderBy(g => g.Key ?? "", StringComparer.Ordinal)
                .ToArray();
            QueryResult grouped = Run(context, "SELECT c.Region, COUNT(*) AS n, SUM(o.Amount) AS total" + FromLeftC + " GROUP BY c.Region");
            Assert.Equal(groups, grouped.Data
                .Select(r => r.Values.ToArray())
                .Select(v => ((string?)v[0], Convert.ToInt64(v[1], CultureInfo.InvariantCulture), Math.Round(Convert.ToDouble(v[2], CultureInfo.InvariantCulture), 6)))
                .OrderBy(g => g.Item1 ?? "", StringComparer.Ordinal)
                .ToArray());
            AssertTyped(context);

            // Grouped by one hashed column, sorted by the aggregate's alias and the group column.
            var tiers = LeftC.Where(j => j.C is not null).GroupBy(j => j.C!.Tier).Select(g => new object?[] { g.Key, g.Count() })
                .OrderByDescending(r => (int)r[1]!).ThenBy(r => (int)r[0]!).ToArray();
            AssertRows(context, "SELECT c.Tier, COUNT(*) AS n FROM Orders o JOIN Customers c ON o.CustomerId = c.Id GROUP BY c.Tier ORDER BY n DESC, c.Tier", tiers);

            // A hashed column only the ORDER BY reads.
            AssertRows(context, "SELECT o.Id FROM Orders o JOIN Customers c ON o.CustomerId = c.Id ORDER BY c.Name DESC, o.Id LIMIT 6",
                [.. LeftC.Where(j => j.C is not null).OrderByDescending(j => j.C!.Name, StringComparer.Ordinal).ThenBy(j => j.O.Id).Take(6).Select(j => new object?[] { j.O.Id })]);

            // SELECT * keeps every column of every table.
            QueryResult star = Run(context, "SELECT *" + FromLeftC + " ORDER BY o.Id LIMIT 3");
            Assert.Equal(3, star.Data.Count);
            Assert.Equal(4 + 4, star.Data[0].Count);
            AssertTyped(context);
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void AggregatesOverNoJoinedRows_AnswerOnTheTypedPath(StorageMode mode)
    {
        WithSchema(mode, context =>
        {
            // One row: COUNT 0 and NULL for the rest, as SQL (and the general path) define it.
            QueryResult empty = Run(context, "SELECT COUNT(*), COUNT(c.Id), SUM(o.Amount), AVG(o.Qty), MIN(c.Name), MAX(o.Id)" + FromLeftC + " WHERE c.Region = 'R9'");
            object?[] row = [.. Assert.Single(empty.Data).Values];
            Assert.Equal([0L, 0L, null, null, null, null], row);
            AssertTyped(context);

            // With GROUP BY there is no group, so no row.
            Assert.Empty(Run(context, "SELECT c.Region, COUNT(*)" + FromLeftC + " WHERE o.Qty > 99 GROUP BY c.Region").Data);
            AssertTyped(context);
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void EmptyTables_JoinAsSqlDefines(StorageMode mode)
    {
        WithSchema(mode, context =>
        {
            Execute(context, "CREATE TABLE NoCustomers (Id BIGINT PRIMARY KEY, Region VARCHAR(4))");
            Execute(context, "CREATE TABLE NoOrders (Id INT PRIMARY KEY, CustomerId BIGINT)");

            // Nothing to match: every order is padded, and an INNER join has no row.
            AssertTotals(context, "SELECT COUNT(*), COUNT(c.Id) FROM Orders o LEFT JOIN NoCustomers c ON o.CustomerId = c.Id", [Orders.Length, 0]);
            AssertTotals(context, "SELECT COUNT(*) FROM Orders o LEFT JOIN NoCustomers c ON o.CustomerId = c.Id WHERE c.Id IS NULL", [Orders.Length]);
            AssertTotals(context, "SELECT COUNT(*) FROM Orders o JOIN NoCustomers c ON o.CustomerId = c.Id", [0]);

            // Nothing to stream.
            AssertTotals(context, "SELECT COUNT(*), SUM(c.Tier) FROM NoOrders o LEFT JOIN Customers c ON o.CustomerId = c.Id", [0, double.NaN]);
            Assert.Empty(Run(context, "SELECT o.Id, c.Name FROM NoOrders o LEFT JOIN Customers c ON o.CustomerId = c.Id").Data);
            AssertTyped(context);
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void JoinedRowsWithoutATopN_ReturnEveryJoinedRow(StorageMode mode)
    {
        WithSchema(mode, context =>
        {
            // No ORDER BY, no LIMIT: every joined row, NULL-padded ones included (order unspecified).
            object?[][] left = [.. LeftC.Select(j => new object?[] { j.O.Id, j.C?.Region, j.C?.Id })];
            AssertSameRows(left, Run(context, "SELECT o.Id, c.Region, c.Id" + FromLeftC));
            AssertTyped(context);

            object?[][] inner = [.. LeftC.Where(j => j.C is not null && j.O.Qty == 2).Select(j => new object?[] { j.O.Id, j.C!.Name })];
            AssertSameRows(inner, Run(context, "SELECT o.Id, c.Name FROM Orders o JOIN Customers c ON o.CustomerId = c.Id WHERE o.Qty = 2"));
            AssertTyped(context);

            // ORDER BY without LIMIT: every row, sorted.
            AssertRows(context, "SELECT o.Id, c.Id" + FromLeftC + " WHERE c.Id IS NULL ORDER BY o.Amount, o.Id",
                [.. LeftC.Where(j => j.C is null).OrderBy(j => j.O.Amount).ThenBy(j => j.O.Id).Select(j => new object?[] { j.O.Id, null })]);

            // LIMIT without ORDER BY: that many rows, each a real joined row; with OFFSET the rows after it.
            QueryResult limited = Run(context, "SELECT o.Id, c.Region" + FromLeftC + " WHERE c.Tier = 1 LIMIT 5");
            Assert.Equal(5, limited.Data.Count);
            var valid = LeftC.Where(j => j.C?.Tier == 1).ToDictionary(j => j.O.Id, j => j.C!.Region);
            Assert.All(limited.Data, r => Assert.Equal(valid[Convert.ToInt32(r.Values.First(), CultureInfo.InvariantCulture)], r.Values.Last()));
            QueryResult offset = Run(context, "SELECT o.Id" + FromLeftC + " WHERE c.Tier = 1 LIMIT 5 OFFSET 3");
            Assert.Equal(5, offset.Data.Count);
            Assert.Equal(
                limited.Data.Skip(3).Select(r => r.Values.First()),
                offset.Data.Take(2).Select(r => r.Values.First()));
            AssertTyped(context);

            // SELECT * returns every column of both tables.
            QueryResult star = Run(context, "SELECT *" + FromLeftC + " WHERE o.Qty = 3");
            Assert.Equal(Orders.Count(o => o.Qty == 3), star.Data.Count);
            AssertTyped(context);
        });
    }

    /// <summary>
    /// The anti-join hashes only the customers' keys. With the query's fixed costs this stays well under 200
    /// bytes per customer; hashing whole rows (cells, two strings and a list per key) costs several hundred.
    /// </summary>
    [Theory]
    [MemberData(nameof(Modes))]
    public void AntiJoin_HashesOnlyTheKeys(StorageMode mode)
    {
        const int customers = 40_000;
        const int orders = 10_000;
        string? directory = mode == StorageMode.Lsm ? TestStorageDirectories.Create("sql_anti_join_alloc") : null;
        using var context = new DataVoContext(new DataVoConfig { StorageMode = mode, DataDirectory = directory });
        Execute(context, "CREATE DATABASE AntiJoinAlloc");
        Execute(context, "USE AntiJoinAlloc");
        Execute(context, "CREATE TABLE Customers (Id INT PRIMARY KEY, Region VARCHAR(8), Tier INT, Name VARCHAR(16))");
        Execute(context, "CREATE TABLE Orders (Id INT PRIMARY KEY, CustomerId INT, Amount DOUBLE)");
        for (int start = 1; start <= customers; start += 2_000)
        {
            Execute(context, "INSERT INTO Customers (Id, Region, Tier, Name) VALUES " + string.Join(", ", Enumerable.Range(start, 2_000).Select(i => $"({i}, 'R{i % 8}', {i % 3}, 'customer-{i}')")));
        }

        for (int start = 1; start <= orders; start += 2_000)
        {
            Execute(context, "INSERT INTO Orders (Id, CustomerId, Amount) VALUES " + string.Join(", ", Enumerable.Range(start, 2_000).Select(i => $"({i}, {i * 5}, {i % 97}.5)")));
        }

        context.Diagnostics.Enabled = true;
        const string sql = "SELECT COUNT(*) FROM Orders o LEFT JOIN Customers c ON o.CustomerId = c.Id WHERE c.Id IS NULL";
        Assert.False(context.Execute(sql)[^1].IsError); // warm up
        long before = GC.GetAllocatedBytesForCurrentThread();
        QueryResult result = context.Execute(sql)[^1];
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.False(result.IsError, string.Join("; ", result.Messages));
        Assert.Equal(orders - customers / 5, Convert.ToInt32(Assert.Single(result.Data).Values.Single(), CultureInfo.InvariantCulture));
        Assert.Equal(0, context.Diagnostics.LastQuery!.JoinExecutions);
        Assert.True(allocated / customers < 192, $"{mode}: the anti-join allocated {allocated / customers} B per customer");
    }

    private static void AssertTyped(DataVoContext context) =>
        Assert.Equal(0, context.Diagnostics.LastQuery!.JoinExecutions);

    private static void AssertTotals(DataVoContext context, string sql, object[] want)
    {
        QueryResult result = Run(context, sql);
        object?[] row = [.. Assert.Single(result.Data).Values];
        Assert.Equal(want.Length, row.Length);
        for (int i = 0; i < want.Length; i++)
        {
            Assert.Equal(Convert.ToDouble(want[i], CultureInfo.InvariantCulture), Convert.ToDouble(row[i] ?? double.NaN, CultureInfo.InvariantCulture), 6);
        }

        AssertTyped(context);
    }

    private static void AssertRows(DataVoContext context, string sql, object?[][] expected)
    {
        QueryResult result = Run(context, sql);
        Assert.Equal(expected.Select(r => r.Select(Normalize).ToArray()), result.Data.Select(r => r.Values.Select(Normalize).ToArray()));
        AssertTyped(context);
    }

    private static void AssertSameRows(object?[][] expected, QueryResult result)
    {
        static string Key(IEnumerable<object?> row) => string.Join("|", row.Select(value => Normalize(value)?.ToString() ?? "NULL"));
        Assert.Equal(expected.Select(Key).Order(StringComparer.Ordinal), result.Data.Select(r => Key(r.Values)).Order(StringComparer.Ordinal));
    }

    private static object? Normalize(object? value) => value switch
    {
        int or long => Convert.ToInt64(value, CultureInfo.InvariantCulture),
        float or double or decimal => Math.Round(Convert.ToDouble(value, CultureInfo.InvariantCulture), 6),
        _ => value,
    };

    private static QueryResult Run(DataVoContext context, string sql)
    {
        QueryResult result = context.Execute(sql)[^1];
        Assert.False(result.IsError, sql + ": " + string.Join("; ", result.Messages));
        return result;
    }

    private static void WithSchema(StorageMode mode, Action<DataVoContext> test)
    {
        string? directory = mode == StorageMode.Lsm ? TestStorageDirectories.Create("sql_typed_join_build") : null;
        using var context = new DataVoContext(new DataVoConfig { StorageMode = mode, DataDirectory = directory });
        Execute(context, "CREATE DATABASE TypedJoinBuild");
        Execute(context, "USE TypedJoinBuild");
        Execute(context, "CREATE TABLE Customers (Id BIGINT PRIMARY KEY, Region VARCHAR(4), Tier INT, Name VARCHAR(8))");
        Execute(context, "CREATE TABLE Orders (Id INT PRIMARY KEY, CustomerId BIGINT, Qty INT, Amount DOUBLE)");
        Execute(context, "INSERT INTO Customers (Id, Region, Tier, Name) VALUES " + string.Join(", ", Customers.Select(c => $"({c.Id}, '{c.Region}', {c.Tier}, '{c.Name}')")));
        Execute(context, "INSERT INTO Orders (Id, CustomerId, Qty, Amount) VALUES " + string.Join(", ", Orders.Select(o =>
            $"({o.Id}, {o.CustomerId?.ToString(CultureInfo.InvariantCulture) ?? "NULL"}, {o.Qty}, {o.Amount.ToString(CultureInfo.InvariantCulture)})")));
        context.Diagnostics.Enabled = true;
        test(context);
    }

    private static void Execute(DataVoContext context, string sql)
    {
        QueryResult result = context.Execute(sql)[^1];
        Assert.False(result.IsError, sql + ": " + string.Join("; ", result.Messages));
    }
}
