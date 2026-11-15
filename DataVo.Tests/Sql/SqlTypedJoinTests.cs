using System.Globalization;
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Sql;

/// <summary>
/// The typed join path: INNER and LEFT joins of several tables on integer keys, aggregated or returned as the top
/// rows of an <c>ORDER BY ... LIMIT</c>. Expected values come from LINQ over the seed data written with SQL's
/// rules: an inner join drops an order whose customer or product is missing or NULL, a LEFT JOIN pads it with
/// NULLs, a WHERE condition on the NULL-supplied side is tested after the join, and a comparison with NULL is
/// unknown. Routing checks show these queries no longer run the general join.
/// </summary>
public sealed class SqlTypedJoinTests
{
    public static TheoryData<StorageMode> Modes => new() { StorageMode.InMemory, StorageMode.Lsm };

    private sealed record Customer(int Id, string Region, int Tier, string Name);

    private sealed record Product(int Id, int CategoryId, string Name);

    private sealed record Order(int Id, int? CustomerId, int? ProductId, int Qty, double Amount);

    private static readonly Customer[] Customers = [.. Enumerable.Range(1, 9).Select(i => new Customer(i, "R" + i % 3, i % 2, "c" + i))];

    private static readonly Product[] Products = [.. Enumerable.Range(1, 6).Select(i => new Product(i, i % 3, "p" + i))];

    // Customers 10 and 11 and product 7 do not exist; some orders have no customer or no product at all.
    private static readonly Order[] Orders = [.. Enumerable.Range(1, 150).Select(i => new Order(
        i,
        i % 17 == 0 ? null : i % 11 + 1,
        i % 19 == 0 ? null : i / 3 % 7 + 1,
        i % 4 + 1,
        i * 37 % 500 + 0.5))];

    private sealed record Joined(Order O, Customer? C, Product? P);

    private static Customer? CustomerOf(Order o) => Customers.FirstOrDefault(c => c.Id == o.CustomerId);

    private static Product? ProductOf(Order o) => Products.FirstOrDefault(p => p.Id == o.ProductId);

    private static IEnumerable<Joined> Inner3 => Orders
        .Select(o => new Joined(o, CustomerOf(o), ProductOf(o)))
        .Where(j => j.C is not null && j.P is not null);

    private static IEnumerable<Joined> LeftC => Orders.Select(o => new Joined(o, CustomerOf(o), null));

    private static IEnumerable<Joined> LeftCLeftP => Orders.Select(o => new Joined(o, CustomerOf(o), ProductOf(o)));

    private const string FromInner3 = " FROM Orders o JOIN Customers c ON o.CustomerId = c.Id JOIN Products p ON o.ProductId = p.Id";
    private const string FromLeftC = " FROM Orders o LEFT JOIN Customers c ON o.CustomerId = c.Id";

    [Theory]
    [MemberData(nameof(Modes))]
    public void ThreeTableInnerJoin_Aggregates(StorageMode mode)
    {
        WithSchema(mode, context =>
        {
            AssertTotals(context, "SELECT COUNT(*), SUM(o.Amount), MIN(c.Tier), MAX(p.CategoryId)" + FromInner3, [Inner3.Count(), Inner3.Sum(x => x.O.Amount), Inner3.Min(x => x.C!.Tier), Inner3.Max(x => x.P!.CategoryId)]);

            Joined[] filtered = [.. Inner3.Where(j => j.C!.Region == "R1" && j.P!.CategoryId == 2 && j.O.Qty > 1)];
            AssertTotals(context, "SELECT COUNT(*), SUM(o.Amount), AVG(o.Qty)" + FromInner3 + " WHERE c.Region = 'R1' AND p.CategoryId = 2 AND o.Qty > 1", [filtered.Length, filtered.Sum(x => x.O.Amount), filtered.Average(x => x.O.Qty)]);

            // An OR and a LIKE within one table.
            Joined[] mixed = [.. Inner3.Where(j => (j.C!.Region == "R0" || j.C.Tier == 1) && j.P!.Name.StartsWith("p1", StringComparison.Ordinal))];
            AssertTotals(context, "SELECT COUNT(*), SUM(o.Qty)" + FromInner3 + " WHERE (c.Region = 'R0' OR c.Tier = 1) AND p.Name LIKE 'p1%'", [mixed.Length, mixed.Sum(x => x.O.Qty)]);

            // The join conditions written the other way round and in the other order.
            AssertTotals(context, "SELECT COUNT(*), SUM(o.Amount) FROM Orders o JOIN Products p ON p.Id = o.ProductId JOIN Customers c ON c.Id = o.CustomerId", [Inner3.Count(), Inner3.Sum(x => x.O.Amount)]);

            // A small table first in FROM: the largest is still streamed.
            Joined[] small = [.. Inner3.Where(j => j.P!.CategoryId == 1)];
            AssertTotals(context, "SELECT COUNT(*), SUM(o.Amount) FROM Products p JOIN Orders o ON o.ProductId = p.Id JOIN Customers c ON o.CustomerId = c.Id WHERE p.CategoryId = 1", [small.Length, small.Sum(x => x.O.Amount)]);
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void ThreeTableInnerJoin_GroupBy(StorageMode mode)
    {
        WithSchema(mode, context =>
        {
            var expected = Inner3
                .GroupBy(j => (j.C!.Region, j.P!.CategoryId))
                .Select(g => (g.Key.Region, g.Key.CategoryId, (long)g.Count(), Math.Round(g.Sum(j => j.O.Amount), 6)))
                .Order()
                .ToArray();
            var result = Run(context, "SELECT c.Region, p.CategoryId, COUNT(*) AS n, SUM(o.Amount) AS total" + FromInner3 + " GROUP BY c.Region, p.CategoryId");
            Assert.Equal(expected, result.Data
                .Select(r => r.Values.ToArray())
                .Select(v => ((string)v[0]!, Convert.ToInt32(v[1]), Convert.ToInt64(v[2]), Math.Round(Convert.ToDouble(v[3]), 6)))
                .Order()
                .ToArray());
            AssertTyped(context);
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void ManyToManyKeys_MultiplyRows(StorageMode mode)
    {
        WithSchema(mode, context =>
        {
            // Qty and Tier are not keys: each order matches every customer of its tier.
            int expected = Orders.Sum(o => Customers.Count(c => c.Tier == o.Qty));
            AssertTotals(context, "SELECT COUNT(*) FROM Orders o JOIN Customers c ON o.Qty = c.Tier", [expected]);
            int expectedLeft = Orders.Sum(o => Math.Max(1, Customers.Count(c => c.Tier == o.Qty)));
            AssertTotals(context, "SELECT COUNT(*), COUNT(c.Id) FROM Orders o LEFT JOIN Customers c ON o.Qty = c.Tier", [expectedLeft, Orders.Sum(o => Customers.Count(c => c.Tier == o.Qty))]);
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void LeftJoin_CountsPadsAndFilters(StorageMode mode)
    {
        WithSchema(mode, context =>
        {
            Joined[] all = [.. LeftC];
            AssertTotals(context, "SELECT COUNT(*), COUNT(c.Id), SUM(o.Amount), MAX(c.Tier)" + FromLeftC, [all.Length, all.Count(j => j.C is not null), all.Sum(j => j.O.Amount), all.Where(j => j.C is not null).Max(j => j.C!.Tier)]);

            // Anti-join: orders whose customer is missing or NULL.
            Joined[] orphans = [.. LeftC.Where(j => j.C is null)];
            Assert.NotEmpty(orphans);
            AssertTotals(context, "SELECT COUNT(*), SUM(o.Amount)" + FromLeftC + " WHERE c.Id IS NULL", [orphans.Length, orphans.Sum(j => j.O.Amount)]);

            // A condition on the NULL-supplied side drops the padded rows; one on the FROM table keeps them.
            Joined[] r1 = [.. LeftC.Where(j => j.C?.Region == "R1")];
            AssertTotals(context, "SELECT COUNT(*)" + FromLeftC + " WHERE c.Region = 'R1'", [r1.Length]);
            Joined[] notR1 = [.. LeftC.Where(j => j.C is { Region: not "R1" })];
            AssertTotals(context, "SELECT COUNT(*)" + FromLeftC + " WHERE c.Region <> 'R1'", [notR1.Length]);
            Joined[] qty = [.. LeftC.Where(j => j.O.Qty == 2)];
            AssertTotals(context, "SELECT COUNT(*), COUNT(c.Id)" + FromLeftC + " WHERE o.Qty = 2", [qty.Length, qty.Count(j => j.C is not null)]);
            Joined[] padOrTier = [.. LeftC.Where(j => j.C is null || j.C.Tier == 0)];
            AssertTotals(context, "SELECT COUNT(*)" + FromLeftC + " WHERE c.Id IS NULL OR c.Tier = 0", [padOrTier.Length]);

            // GROUP BY a column of the padded side: the NULLs form one group.
            var groups = LeftC.GroupBy(j => j.C?.Region).Select(g => (g.Key, (long)g.Count())).OrderBy(g => g.Key ?? "").ToArray();
            var result = Run(context, "SELECT c.Region, COUNT(*) AS n" + FromLeftC + " GROUP BY c.Region");
            Assert.Equal(groups, result.Data.Select(r => r.Values.ToArray()).Select(v => ((string?)v[0], Convert.ToInt64(v[1]))).OrderBy(g => g.Item1 ?? "").ToArray());
            AssertTyped(context);
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void LeftJoinChains_AndInnerAfterLeft(StorageMode mode)
    {
        WithSchema(mode, context =>
        {
            Joined[] both = [.. LeftCLeftP];
            AssertTotals(context, "SELECT COUNT(*), COUNT(c.Id), COUNT(p.Id), SUM(p.CategoryId) FROM Orders o LEFT JOIN Customers c ON o.CustomerId = c.Id LEFT JOIN Products p ON o.ProductId = p.Id", [both.Length, both.Count(j => j.C is not null), both.Count(j => j.P is not null), both.Where(j => j.P is not null).Sum(j => j.P!.CategoryId)]);

            Joined[] neither = [.. LeftCLeftP.Where(j => j.C is null && j.P is null)];
            AssertTotals(context, "SELECT COUNT(*) FROM Orders o LEFT JOIN Customers c ON o.CustomerId = c.Id LEFT JOIN Products p ON o.ProductId = p.Id WHERE c.Id IS NULL AND p.Id IS NULL", [neither.Length]);

            // LEFT then INNER: the inner join drops the orders the LEFT JOIN padded only if they have no product.
            Joined[] leftInner = [.. LeftCLeftP.Where(j => j.P is not null)];
            AssertTotals(context, "SELECT COUNT(*), COUNT(c.Id) FROM Orders o LEFT JOIN Customers c ON o.CustomerId = c.Id JOIN Products p ON o.ProductId = p.Id", [leftInner.Length, leftInner.Count(j => j.C is not null)]);
            Joined[] leftInnerFiltered = [.. leftInner.Where(j => j.P!.CategoryId == 0 && j.C is null)];
            AssertTotals(context, "SELECT COUNT(*) FROM Orders o LEFT JOIN Customers c ON o.CustomerId = c.Id JOIN Products p ON o.ProductId = p.Id WHERE p.CategoryId = 0 AND c.Id IS NULL", [leftInnerFiltered.Length]);
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void OrderByLimit_ReturnsTheTopJoinedRows(StorageMode mode)
    {
        WithSchema(mode, context =>
        {
            Joined[] matches = [.. Inner3.Where(j => j.C!.Region != "R2" && j.P!.CategoryId != 0)];
            AssertRows(context,
                "SELECT o.Id, o.Amount, c.Name AS CustomerName, p.Name AS ProductName" + FromInner3 + " WHERE c.Region <> 'R2' AND p.CategoryId <> 0 ORDER BY o.Amount DESC, o.Id ASC LIMIT 7",
                [.. matches.OrderByDescending(j => j.O.Amount).ThenBy(j => j.O.Id).Take(7)
                    .Select(j => new object?[] { j.O.Id, j.O.Amount, j.C!.Name, j.P!.Name })]);

            // Sorted by columns of the hashed tables, with OFFSET.
            AssertRows(context,
                "SELECT o.Id, c.Region" + FromInner3 + " ORDER BY c.Region DESC, p.Id, o.Id LIMIT 5 OFFSET 3",
                [.. Inner3.OrderByDescending(j => j.C!.Region, StringComparer.Ordinal).ThenBy(j => j.P!.Id).ThenBy(j => j.O.Id).Skip(3).Take(5)
                    .Select(j => new object?[] { j.O.Id, j.C!.Region })]);

            // Sorted by a SELECT alias, and a LIMIT larger than the result.
            AssertRows(context,
                "SELECT o.Id, o.Qty AS q" + FromInner3 + " WHERE c.Tier = 1 ORDER BY q, o.Id LIMIT 1000",
                [.. Inner3.Where(j => j.C!.Tier == 1).OrderBy(j => j.O.Qty).ThenBy(j => j.O.Id).Select(j => new object?[] { j.O.Id, j.O.Qty })]);

            // LEFT JOIN rows, NULLs included, sorted by the probe table.
            AssertRows(context,
                "SELECT o.Id, c.Region" + FromLeftC + " WHERE o.Qty <> 3 ORDER BY o.Amount, o.Id LIMIT 9",
                [.. LeftC.Where(j => j.O.Qty != 3).OrderBy(j => j.O.Amount).ThenBy(j => j.O.Id).Take(9).Select(j => new object?[] { j.O.Id, j.C?.Region })]);

            // Prepared, with bound values.
            var statement = context.Prepare("SELECT o.Id" + FromInner3 + " WHERE c.Region = @r AND p.CategoryId = @cat ORDER BY o.Amount DESC, o.Id LIMIT 4");
            var prepared = statement.Execute([DataVoQueryParameter.FromObject("r", "R0"), DataVoQueryParameter.FromObject("cat", 1)]);
            Assert.False(prepared.IsError, string.Join("; ", prepared.Messages));
            Assert.Equal(
                Inner3.Where(j => j.C!.Region == "R0" && j.P!.CategoryId == 1).OrderByDescending(j => j.O.Amount).ThenBy(j => j.O.Id).Take(4).Select(j => j.O.Id).ToArray(),
                prepared.Data.Select(r => Convert.ToInt32(r.Values.First())).ToArray());
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void ShapesTheTypedJoinDoesNotTake_StayCorrect(StorageMode mode)
    {
        WithSchema(mode, context =>
        {
            // An OR across two tables, a RIGHT JOIN and an empty aggregate take the general path.
            Joined[] either = [.. Inner3.Where(j => j.C!.Region == "R1" || j.P!.CategoryId == 2)];
            AssertTotals(context, "SELECT COUNT(*)" + FromInner3 + " WHERE c.Region = 'R1' OR p.CategoryId = 2", [either.Length]);

            int right = Customers.Sum(c => Math.Max(1, Orders.Count(o => o.CustomerId == c.Id)));
            AssertTotals(context, "SELECT COUNT(*) FROM Orders o RIGHT JOIN Customers c ON o.CustomerId = c.Id", [right]);

            AssertTotals(context, "SELECT COUNT(*)" + FromInner3 + " WHERE c.Region = 'R9'", [0]);
        });
    }

    /// <summary>
    /// Typed joins read the streamed table without building a dictionary per row: a few dozen bytes per order,
    /// where the general join path allocated well over a kilobyte.
    /// </summary>
    [Theory]
    [MemberData(nameof(Modes))]
    public void TypedJoins_DoNotAllocatePerStreamedRow(StorageMode mode)
    {
        const int orderCount = 30_000;
        string? directory = mode == StorageMode.Lsm ? TestStorageDirectories.Create("sql_typed_join_alloc") : null;
        using var context = new DataVoContext(new DataVoConfig { StorageMode = mode, DiskStoragePath = directory });
        Execute(context, "CREATE DATABASE TypedJoinAlloc");
        Execute(context, "USE TypedJoinAlloc");
        Execute(context, "CREATE TABLE Customers (Id INT PRIMARY KEY, Region VARCHAR(4), Tier INT, Name VARCHAR(8))");
        Execute(context, "CREATE TABLE Products (Id INT PRIMARY KEY, CategoryId INT, Name VARCHAR(8))");
        Execute(context, "CREATE TABLE Orders (Id INT PRIMARY KEY, CustomerId INT, ProductId INT, Qty INT, Amount DOUBLE)");
        Execute(context, "INSERT INTO Customers (Id, Region, Tier, Name) VALUES " + string.Join(", ", Enumerable.Range(1, 500).Select(i => $"({i}, 'R{i % 8}', {i % 2}, 'c{i}')")));
        Execute(context, "INSERT INTO Products (Id, CategoryId, Name) VALUES " + string.Join(", ", Enumerable.Range(1, 50).Select(i => $"({i}, {i % 10}, 'p{i}')")));
        for (int start = 1; start <= orderCount; start += 1_000)
        {
            Execute(context, "INSERT INTO Orders (Id, CustomerId, ProductId, Qty, Amount) VALUES " + string.Join(", ", Enumerable.Range(start, 1_000)
                .Select(i => $"({i}, {i % 625 + 1}, {i % 50 + 1}, {i % 4 + 1}, {i % 997}.5)")));
        }

        context.Diagnostics.Enabled = true;
        string[] queries =
        [
            "SELECT COUNT(*), SUM(o.Amount) FROM Orders o JOIN Customers c ON o.CustomerId = c.Id JOIN Products p ON o.ProductId = p.Id",
            "SELECT COUNT(*), SUM(o.Amount) FROM Orders o JOIN Customers c ON o.CustomerId = c.Id JOIN Products p ON o.ProductId = p.Id WHERE c.Region = 'R3' AND p.CategoryId = 7",
            "SELECT o.Id, o.Amount, c.Name AS CustomerName, p.Name AS ProductName FROM Orders o JOIN Customers c ON o.CustomerId = c.Id JOIN Products p ON o.ProductId = p.Id WHERE c.Region = 'R3' AND p.CategoryId = 7 ORDER BY o.Amount DESC, o.Id ASC LIMIT 50",
            "SELECT COUNT(*), COUNT(c.Id), SUM(o.Amount) FROM Orders o LEFT JOIN Customers c ON o.CustomerId = c.Id",
            "SELECT COUNT(*) FROM Orders o LEFT JOIN Customers c ON o.CustomerId = c.Id WHERE c.Id IS NULL",
        ];

        foreach (string query in queries)
        {
            Assert.False(context.Execute(query)[^1].IsError); // warm up
            long before = GC.GetAllocatedBytesForCurrentThread();
            var result = context.Execute(query)[^1];
            long perRow = (GC.GetAllocatedBytesForCurrentThread() - before) / orderCount;
            Assert.False(result.IsError, string.Join("; ", result.Messages));
            Assert.Equal(0, context.Diagnostics.LastQuery!.JoinExecutions);
            Assert.True(perRow < 256, $"{mode}: {query} allocated {perRow} B per order");
        }
    }

    private static void AssertTyped(DataVoContext context) =>
        Assert.Equal(0, context.Diagnostics.LastQuery!.JoinExecutions);

    private static void AssertTotals(DataVoContext context, string sql, object[] want)
    {
        var result = Run(context, sql);
        object?[] row = [.. Assert.Single(result.Data).Values];
        Assert.Equal(want.Length, row.Length);
        for (int i = 0; i < want.Length; i++)
        {
            Assert.Equal(Convert.ToDouble(want[i], CultureInfo.InvariantCulture), Convert.ToDouble(row[i] ?? double.NaN, CultureInfo.InvariantCulture), 6);
        }
    }

    private static void AssertRows(DataVoContext context, string sql, object?[][] expected)
    {
        var result = Run(context, sql);
        object?[][] actual = [.. result.Data.Select(r => r.Values.Select(Normalize).ToArray())];
        Assert.Equal(expected.Select(r => r.Select(Normalize).ToArray()), actual);
        AssertTyped(context);

        static object? Normalize(object? value) => value switch
        {
            int or long => Convert.ToInt64(value, CultureInfo.InvariantCulture),
            float or double or decimal => Math.Round(Convert.ToDouble(value, CultureInfo.InvariantCulture), 6),
            _ => value,
        };
    }

    private static Core.Contracts.Results.QueryResult Run(DataVoContext context, string sql)
    {
        var result = context.Execute(sql)[^1];
        Assert.False(result.IsError, sql + ": " + string.Join("; ", result.Messages));
        return result;
    }

    private static void WithSchema(StorageMode mode, Action<DataVoContext> test)
    {
        string? directory = mode == StorageMode.Lsm ? TestStorageDirectories.Create("sql_typed_join") : null;
        using var context = new DataVoContext(new DataVoConfig { StorageMode = mode, DiskStoragePath = directory });
        Execute(context, "CREATE DATABASE TypedJoin");
        Execute(context, "USE TypedJoin");
        Execute(context, "CREATE TABLE Customers (Id INT PRIMARY KEY, Region VARCHAR(4), Tier INT, Name VARCHAR(8))");
        Execute(context, "CREATE TABLE Products (Id INT PRIMARY KEY, CategoryId INT, Name VARCHAR(8))");
        Execute(context, "CREATE TABLE Orders (Id INT PRIMARY KEY, CustomerId INT, ProductId INT, Qty INT, Amount DOUBLE)");
        Execute(context, "INSERT INTO Customers (Id, Region, Tier, Name) VALUES " + string.Join(", ", Customers.Select(c => $"({c.Id}, '{c.Region}', {c.Tier}, '{c.Name}')")));
        Execute(context, "INSERT INTO Products (Id, CategoryId, Name) VALUES " + string.Join(", ", Products.Select(p => $"({p.Id}, {p.CategoryId}, '{p.Name}')")));
        Execute(context, "INSERT INTO Orders (Id, CustomerId, ProductId, Qty, Amount) VALUES " + string.Join(", ", Orders.Select(o =>
            $"({o.Id}, {o.CustomerId?.ToString(CultureInfo.InvariantCulture) ?? "NULL"}, {o.ProductId?.ToString(CultureInfo.InvariantCulture) ?? "NULL"}, {o.Qty}, {o.Amount.ToString(CultureInfo.InvariantCulture)})")));
        context.Diagnostics.Enabled = true;
        test(context);
    }

    private static void Execute(DataVoContext context, string sql)
    {
        var result = context.Execute(sql)[^1];
        Assert.False(result.IsError, sql + ": " + string.Join("; ", result.Messages));
    }
}
