using System.Globalization;
using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Sql;

/// <summary>
/// An INNER typed join whose hashed table has few keys reads the streamed table through a secondary index on its
/// join column instead of scanning it. The answers must equal LINQ over the seed data: NULL and dangling foreign
/// keys, duplicate keys on the hashed side, rows updated and deleted after the index was built, groups and top
/// rows in the scan's order. A LEFT JOIN, or a hashed table that names too many rows, keeps the scan.
/// </summary>
public sealed class SqlTypedJoinIndexDrivenTests
{
    public static TheoryData<StorageMode> Modes => new() { StorageMode.InMemory, StorageMode.Lsm };

    private const int OrderCount = 4_000;

    private sealed record Product(int Id, int CategoryId, string Name);

    private sealed record Order(int Id, int? ProductId, int Qty, double Amount);

    // Product 41 does not exist; every 23rd order has no product.
    private static readonly Product[] Products = [.. Enumerable.Range(1, 40).Select(i => new Product(i, i % 20, "p" + i))];

    private static List<Order> SeedOrders() => [.. Enumerable.Range(1, OrderCount).Select(i => new Order(
        i,
        i % 23 == 0 ? null : i % 41 + 1,
        i % 5 + 1,
        i * 13 % 700 + 0.5))];

    [Theory]
    [MemberData(nameof(Modes))]
    public void SelectiveDimension_ReadsTheFactTableThroughItsIndex(StorageMode mode)
    {
        WithSchema(mode, (context, orders) =>
        {
            Product[] category = [.. Products.Where(p => p.CategoryId == 3)];
            Order[] matching = [.. orders.Where(o => category.Any(p => p.Id == o.ProductId))];

            AssertTotals(context, "SELECT COUNT(*), SUM(o.Amount) FROM Products p JOIN Orders o ON o.ProductId = p.Id WHERE p.CategoryId = 3",
                [matching.Length, matching.Sum(o => o.Amount)]);
            AssertIndexDriven(context, matching.Length);

            // A filter on the streamed table too, a group per product, and the top rows.
            var groups = matching.Where(o => o.Qty > 2).GroupBy(o => o.ProductId).Select(g => (Convert.ToInt64(g.Key), (long)g.Count())).Order().ToArray();
            QueryResult grouped = Run(context, "SELECT p.Id, COUNT(*) AS n FROM Orders o JOIN Products p ON o.ProductId = p.Id WHERE p.CategoryId = 3 AND o.Qty > 2 GROUP BY p.Id");
            Assert.Equal(groups, grouped.Data.Select(r => r.Values.ToArray()).Select(v => (Convert.ToInt64(v[0], CultureInfo.InvariantCulture), Convert.ToInt64(v[1], CultureInfo.InvariantCulture))).Order().ToArray());
            AssertIndexDriven(context, matching.Length);

            QueryResult top = Run(context, "SELECT o.Id, p.Name FROM Orders o JOIN Products p ON o.ProductId = p.Id WHERE p.CategoryId = 3 ORDER BY o.Amount DESC, o.Id LIMIT 7");
            Assert.Equal(
                matching.OrderByDescending(o => o.Amount).ThenBy(o => o.Id).Take(7).Select(o => $"{o.Id}:p{o.ProductId}"),
                top.Data.Select(r => $"{r.Values.First()}:{r.Values.Last()}"));
            AssertIndexDriven(context, matching.Length);

            // Rows without ORDER BY come in the order the scan returns them: by order id.
            QueryResult rows = Run(context, "SELECT o.Id FROM Orders o JOIN Products p ON o.ProductId = p.Id WHERE p.CategoryId = 3");
            Assert.Equal(matching.Select(o => (long)o.Id), rows.Data.Select(r => Convert.ToInt64(r.Values.Single(), CultureInfo.InvariantCulture)));
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void UpdatesAndDeletes_AfterTheIndexWasBuilt_AreSeen(StorageMode mode)
    {
        WithSchema(mode, (context, orders) =>
        {
            // Move some orders into category 3's products, out of them, to no product, and delete others.
            Execute(context, "UPDATE Orders SET ProductId = 3 WHERE Id <= 40");
            Execute(context, "UPDATE Orders SET ProductId = 41 WHERE Id > 40 AND Id <= 80");
            Execute(context, "UPDATE Orders SET ProductId = NULL WHERE Id > 80 AND Id <= 120");
            Execute(context, "DELETE FROM Orders WHERE Id > 3000");
            List<Order> changed = [.. orders
                .Where(o => o.Id <= 3000)
                .Select(o => o.Id switch
                {
                    <= 40 => o with { ProductId = 3 },
                    <= 80 => o with { ProductId = 41 },
                    <= 120 => o with { ProductId = null },
                    _ => o,
                })];

            Order[] matching = [.. changed.Where(o => Products.Any(p => p.Id == o.ProductId && p.CategoryId == 3))];
            AssertTotals(context, "SELECT COUNT(*), SUM(o.Amount) FROM Products p JOIN Orders o ON o.ProductId = p.Id WHERE p.CategoryId = 3",
                [matching.Length, matching.Sum(o => o.Amount)]);
            AssertIndexDriven(context, matching.Length);
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void LeftJoinsAndBroadDimensions_KeepTheScan(StorageMode mode)
    {
        WithSchema(mode, (context, orders) =>
        {
            // A LEFT JOIN keeps every order, so the index cannot replace the scan.
            Order[] qty1 = [.. orders.Where(o => o.Qty == 1)];
            AssertTotals(context, "SELECT COUNT(*), COUNT(p.Id) FROM Orders o LEFT JOIN Products p ON o.ProductId = p.Id WHERE o.Qty = 1",
                [qty1.Length, qty1.Count(o => Products.Any(p => p.Id == o.ProductId))]);
            Assert.DoesNotContain("ix_orders_product", context.Diagnostics.LastQuery!.IndexesUsed, StringComparer.OrdinalIgnoreCase);

            // Half of the products name half of the orders: scanning is cheaper.
            Order[] broad = [.. orders.Where(o => Products.Any(p => p.Id == o.ProductId && p.CategoryId < 10))];
            AssertTotals(context, "SELECT COUNT(*) FROM Products p JOIN Orders o ON o.ProductId = p.Id WHERE p.CategoryId < 10", [broad.Length]);
            Assert.DoesNotContain("ix_orders_product", context.Diagnostics.LastQuery!.IndexesUsed, StringComparer.OrdinalIgnoreCase);
        });
    }

    private static void AssertIndexDriven(DataVoContext context, int rowsRead)
    {
        var stats = context.Diagnostics.LastQuery!;
        Assert.Equal(0, stats.JoinExecutions);
        Assert.Contains("ix_orders_product", stats.IndexesUsed, StringComparer.OrdinalIgnoreCase);
        Assert.Equal(rowsRead, stats.RowsRead);
    }

    private static void AssertTotals(DataVoContext context, string sql, object[] want)
    {
        QueryResult result = Run(context, sql);
        object?[] row = [.. Assert.Single(result.Data).Values];
        Assert.Equal(want.Length, row.Length);
        for (int i = 0; i < want.Length; i++)
        {
            Assert.Equal(Convert.ToDouble(want[i], CultureInfo.InvariantCulture), Convert.ToDouble(row[i] ?? double.NaN, CultureInfo.InvariantCulture), 6);
        }

        Assert.Equal(0, context.Diagnostics.LastQuery!.JoinExecutions);
    }

    private static QueryResult Run(DataVoContext context, string sql)
    {
        QueryResult result = context.Execute(sql)[^1];
        Assert.False(result.IsError, sql + ": " + string.Join("; ", result.Messages));
        return result;
    }

    private static void WithSchema(StorageMode mode, Action<DataVoContext, List<Order>> test)
    {
        string? directory = mode == StorageMode.Lsm ? TestStorageDirectories.Create("sql_typed_join_index") : null;
        using var context = new DataVoContext(new DataVoConfig { StorageMode = mode, DiskStoragePath = directory });
        Execute(context, "CREATE DATABASE TypedJoinIndex");
        Execute(context, "USE TypedJoinIndex");
        Execute(context, "CREATE TABLE Products (Id INT PRIMARY KEY, CategoryId INT, Name VARCHAR(8))");
        Execute(context, "CREATE TABLE Orders (Id INT PRIMARY KEY, ProductId INT, Qty INT, Amount DOUBLE)");
        Execute(context, "CREATE INDEX ix_orders_product ON Orders (ProductId)");
        Execute(context, "INSERT INTO Products (Id, CategoryId, Name) VALUES " + string.Join(", ", Products.Select(p => $"({p.Id}, {p.CategoryId}, '{p.Name}')")));
        List<Order> orders = SeedOrders();
        foreach (Order[] chunk in orders.Chunk(1_000))
        {
            Execute(context, "INSERT INTO Orders (Id, ProductId, Qty, Amount) VALUES " + string.Join(", ", chunk.Select(o =>
                $"({o.Id}, {o.ProductId?.ToString(CultureInfo.InvariantCulture) ?? "NULL"}, {o.Qty}, {o.Amount.ToString(CultureInfo.InvariantCulture)})")));
        }

        context.Diagnostics.Enabled = true;
        test(context, orders);
    }

    private static void Execute(DataVoContext context, string sql)
    {
        QueryResult result = context.Execute(sql)[^1];
        Assert.False(result.IsError, sql + ": " + string.Join("; ", result.Messages));
    }
}
