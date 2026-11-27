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
[Collection(AllocationSensitiveCollection.Name)]
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

    /// <summary>
    /// After a reopen the LSM engine reads the indexed rows from its files. Rows changed since (updated, deleted,
    /// inserted) come from the in-memory overlay instead; both kinds are visited in key order, and a row read from
    /// a file is not copied: the join allocates far less than a row per row read (each was a new array, and each
    /// index lookup a new list).
    /// </summary>
    [Fact]
    public void Lsm_AfterReopen_ReadsIndexedRowsFromFilesAndOverlay()
    {
        string directory = TestStorageDirectories.Create("sql_typed_join_index_reopen");
        List<Order> orders = SeedOrders();
        using (var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.Lsm, DataDirectory = directory }))
        {
            CreateSchema(context, orders);
        }

        using var reopened = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.Lsm, DataDirectory = directory });
        Execute(reopened, "USE TypedJoinIndex");
        Execute(reopened, "UPDATE Orders SET Amount = 999.5 WHERE Id <= 400");
        Execute(reopened, "DELETE FROM Orders WHERE Id > 400 AND Id <= 800");
        Execute(reopened, "INSERT INTO Orders (Id, ProductId, Qty, Amount) VALUES (5001, 4, 1, 1.5), (5002, 24, 2, 2.5)");
        List<Order> current = [.. orders
            .Where(o => o.Id is <= 400 or > 800)
            .Select(o => o.Id <= 400 ? o with { Amount = 999.5 } : o)
            .Append(new Order(5001, 4, 1, 1.5))
            .Append(new Order(5002, 24, 2, 2.5))];
        reopened.Diagnostics.Enabled = true;

        Order[] matching = [.. current.Where(o => Products.Any(p => p.Id == o.ProductId && p.CategoryId == 3))];
        const string sql = "SELECT COUNT(*), SUM(o.Amount), MAX(o.Id) FROM Products p JOIN Orders o ON o.ProductId = p.Id WHERE p.CategoryId = 3";
        AssertTotals(reopened, sql, [matching.Length, matching.Sum(o => o.Amount), matching.Max(o => o.Id)]);
        AssertIndexDriven(reopened, matching.Length);

        QueryResult rows = Run(reopened, "SELECT o.Id, o.Amount FROM Orders o JOIN Products p ON o.ProductId = p.Id WHERE p.CategoryId = 3");
        Assert.Equal(
            matching.Select(o => $"{o.Id}:{o.Amount.ToString(CultureInfo.InvariantCulture)}"),
            rows.Data.Select(r => $"{r.Values.First()}:{Convert.ToDouble(r.Values.Last(), CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture)}"));

        // What reading the rows adds over the same query for a category with no product (same plan, no row read).
        long perRow = (Allocated(reopened, sql) - Allocated(reopened, sql.Replace("= 3", "= 99", StringComparison.Ordinal))) / matching.Length;
        Assert.True(perRow < 64, $"{perRow} B per indexed row read");
    }

    private static long Allocated(DataVoContext context, string sql)
    {
        Assert.False(context.Execute(sql)[^1].IsError); // warm up
        long before = GC.GetAllocatedBytesForCurrentThread();
        Assert.False(context.Execute(sql)[^1].IsError);
        return GC.GetAllocatedBytesForCurrentThread() - before;
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
        using var context = new DataVoContext(new DataVoConfig { StorageMode = mode, DataDirectory = directory });
        List<Order> orders = SeedOrders();
        CreateSchema(context, orders);
        context.Diagnostics.Enabled = true;
        test(context, orders);
    }

    private static void CreateSchema(DataVoContext context, List<Order> orders)
    {
        Execute(context, "CREATE DATABASE TypedJoinIndex");
        Execute(context, "USE TypedJoinIndex");
        Execute(context, "CREATE TABLE Products (Id INT PRIMARY KEY, CategoryId INT, Name VARCHAR(8))");
        Execute(context, "CREATE TABLE Orders (Id INT PRIMARY KEY, ProductId INT, Qty INT, Amount DOUBLE)");
        Execute(context, "CREATE INDEX ix_orders_product ON Orders (ProductId)");
        Execute(context, "INSERT INTO Products (Id, CategoryId, Name) VALUES " + string.Join(", ", Products.Select(p => $"({p.Id}, {p.CategoryId}, '{p.Name}')")));
        foreach (Order[] chunk in orders.Chunk(1_000))
        {
            Execute(context, "INSERT INTO Orders (Id, ProductId, Qty, Amount) VALUES " + string.Join(", ", chunk.Select(o =>
                $"({o.Id}, {o.ProductId?.ToString(CultureInfo.InvariantCulture) ?? "NULL"}, {o.Qty}, {o.Amount.ToString(CultureInfo.InvariantCulture)})")));
        }
    }

    private static void Execute(DataVoContext context, string sql)
    {
        QueryResult result = context.Execute(sql)[^1];
        Assert.False(result.IsError, sql + ": " + string.Join("; ", result.Messages));
    }
}
