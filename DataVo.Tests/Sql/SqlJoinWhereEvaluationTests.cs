using DataVo.Core;
using DataVo.Core.Runtime.Diagnostics;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Sql;

/// <summary>
/// The general join path evaluates a WHERE over joined tables by joining them once: conjuncts on one table
/// that may filter it before the join do so, the rest are tested on the joined rows. Expected rows come from
/// LINQ over the seed data, written with SQL's rules: inner joins drop orders whose customer is missing or
/// NULL, outer joins pad them with NULLs, and a comparison with NULL is unknown. Every query must also have
/// joined at most once (it used to join once per comparison).
/// </summary>
public sealed class SqlJoinWhereEvaluationTests
{
    public static TheoryData<StorageMode> Modes => new() { StorageMode.InMemory, StorageMode.Lsm };

    private sealed record Customer(int Id, string Region, int Tier);

    private sealed record Product(int Id, int CategoryId);

    private sealed record Order(int Id, int? CustomerId, int ProductId, int Qty, double Amount, string? Note);

    private static readonly Customer[] Customers = [.. Enumerable.Range(1, 8).Select(i => new Customer(i, "R" + i % 3, i % 2))];

    private static readonly Product[] Products = [.. Enumerable.Range(1, 5).Select(i => new Product(i, i % 3))];

    // Customer 9 and 10 do not exist (dangling keys); every 13th order has no customer at all.
    private static readonly Order[] Orders = [.. Enumerable.Range(1, 120).Select(i => new Order(
        i,
        i % 13 == 0 ? null : i % 10 + 1,
        i / 2 % 5 + 1,
        i % 4 + 1,
        i * 17 % 300 + 0.25,
        i % 7 == 0 ? null : "n" + i % 5))];

    private sealed record Joined(Order O, Customer? C, Product? P);

    private static IEnumerable<Joined> Inner3 =>
        from o in Orders
        join c in Customers on o.CustomerId equals c.Id
        join p in Products on o.ProductId equals p.Id
        select new Joined(o, c, p);

    private static IEnumerable<Joined> LeftCustomers =>
        Orders.Select(o => new Joined(o, Customers.FirstOrDefault(c => c.Id == o.CustomerId), null));

    private const string Inner3Sql = "SELECT o.Id FROM Orders o JOIN Customers c ON o.CustomerId = c.Id JOIN Products p ON o.ProductId = p.Id";
    private const string LeftSql = "SELECT o.Id FROM Orders o LEFT JOIN Customers c ON o.CustomerId = c.Id";

    [Theory]
    [MemberData(nameof(Modes))]
    public void InnerJoin_ConjunctsOnSeveralTables_JoinOnce(StorageMode mode)
    {
        WithSchema(mode, context =>
        {
            AssertJoined(context, Inner3Sql + " WHERE c.Region = 'R1' AND p.CategoryId = 2 ORDER BY o.Id",
                Inner3.Where(j => j.C!.Region == "R1" && j.P!.CategoryId == 2));
            AssertJoined(context, Inner3Sql + " WHERE p.CategoryId = 1 AND o.Qty >= 2 AND c.Tier = 1 AND o.Amount < 200 ORDER BY o.Id",
                Inner3.Where(j => j.P!.CategoryId == 1 && j.O.Qty >= 2 && j.C!.Tier == 1 && j.O.Amount < 200));

            // Several conjuncts on one table narrow it in turn; a conjunct may be an OR within that table.
            AssertJoined(context, Inner3Sql + " WHERE o.Qty > 1 AND o.Qty < 4 AND (o.Amount < 50 OR o.Amount > 250) AND c.Region <> 'R0' ORDER BY o.Id",
                Inner3.Where(j => j.O.Qty is > 1 and < 4 && (j.O.Amount < 50 || j.O.Amount > 250) && j.C!.Region != "R0"));

            // The joins listed the other way round.
            AssertJoined(context,
                "SELECT o.Id FROM Orders o JOIN Products p ON o.ProductId = p.Id JOIN Customers c ON o.CustomerId = c.Id WHERE c.Region = 'R2' AND p.CategoryId = 0 ORDER BY o.Id",
                Inner3.Where(j => j.C!.Region == "R2" && j.P!.CategoryId == 0));
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void InnerJoin_OrAcrossTables_IsTestedOnTheJoinedRows(StorageMode mode)
    {
        WithSchema(mode, context =>
        {
            AssertJoined(context, Inner3Sql + " WHERE c.Region = 'R1' OR p.CategoryId = 2 ORDER BY o.Id",
                Inner3.Where(j => j.C!.Region == "R1" || j.P!.CategoryId == 2));
            AssertJoined(context, Inner3Sql + " WHERE (c.Region = 'R0' OR p.CategoryId = 1) AND o.Qty = 3 AND (o.Note = 'n1' OR c.Tier = 0) ORDER BY o.Id",
                Inner3.Where(j => (j.C!.Region == "R0" || j.P!.CategoryId == 1) && j.O.Qty == 3 && (j.O.Note == "n1" || j.C.Tier == 0)));
            AssertJoined(context, Inner3Sql + " WHERE o.Note IS NULL OR c.Tier = 1 ORDER BY o.Id",
                Inner3.Where(j => j.O.Note is null || j.C!.Tier == 1));
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void InnerJoin_IndexedEqualities_ReadTheIndexAndStayExact(StorageMode mode)
    {
        WithSchema(mode, context =>
        {
            Execute(context, "CREATE INDEX ix_orders_qty ON Orders (Qty)");
            Execute(context, "CREATE INDEX ix_customers_region ON Customers (Region)");
            AssertJoined(context, Inner3Sql + " WHERE o.Qty = 2 AND c.Region = 'R1' ORDER BY o.Id",
                Inner3.Where(j => j.O.Qty == 2 && j.C!.Region == "R1"));
            AssertJoined(context, Inner3Sql + " WHERE o.Id = 17 AND p.CategoryId = " + Products[17 / 2 % 5].CategoryId + " ORDER BY o.Id",
                Inner3.Where(j => j.O.Id == 17));
            AssertJoined(context, Inner3Sql + " WHERE o.Qty = 2 AND o.Qty = 3 ORDER BY o.Id", []);
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void LeftJoin_FiltersOnBothSides_KeepOuterJoinSemantics(StorageMode mode)
    {
        WithSchema(mode, context =>
        {
            // The FROM table may be filtered first; the NULL-supplied side only after the join.
            AssertJoined(context, LeftSql + " WHERE o.Qty = 2 ORDER BY o.Id", LeftCustomers.Where(j => j.O.Qty == 2));
            AssertJoined(context, LeftSql + " WHERE c.Region = 'R1' ORDER BY o.Id", LeftCustomers.Where(j => j.C?.Region == "R1"));
            AssertJoined(context, LeftSql + " WHERE c.Id IS NULL ORDER BY o.Id", LeftCustomers.Where(j => j.C is null));
            AssertJoined(context, LeftSql + " WHERE c.Id IS NULL AND o.Qty > 2 ORDER BY o.Id", LeftCustomers.Where(j => j.C is null && j.O.Qty > 2));
            AssertJoined(context, LeftSql + " WHERE o.Qty = 1 OR c.Id IS NULL ORDER BY o.Id", LeftCustomers.Where(j => j.O.Qty == 1 || j.C is null));
            AssertJoined(context, LeftSql + " WHERE c.Tier <> 1 AND o.Amount >= 100 ORDER BY o.Id", LeftCustomers.Where(j => j.C is { Tier: not 1 } && j.O.Amount >= 100));

            var counts = context.Execute("SELECT COUNT(*), COUNT(c.Id) FROM Orders o LEFT JOIN Customers c ON o.CustomerId = c.Id WHERE o.Qty <> 4")[^1];
            Assert.False(counts.IsError, string.Join("; ", counts.Messages));
            object?[] row = [.. Assert.Single(counts.Data).Values];
            Assert.Equal((long)LeftCustomers.Count(j => j.O.Qty != 4), Convert.ToInt64(row[0]));
            Assert.Equal((long)LeftCustomers.Count(j => j.O.Qty != 4 && j.C is not null), Convert.ToInt64(row[1]));
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void RightJoin_FilterOnTheNullSuppliedFromTable_IsTestedAfterTheJoin(StorageMode mode)
    {
        WithSchema(mode, context =>
        {
            // Customers without orders keep a row with NULL order columns; o.Qty = 2 drops them.
            var result = context.Execute(
                "SELECT c.Id AS cid, o.Id AS oid FROM Orders o RIGHT JOIN Customers c ON o.CustomerId = c.Id WHERE o.Qty = 2 OR c.Id = 8")[^1];
            Assert.False(result.IsError, string.Join("; ", result.Messages));
            var expected = Customers
                .SelectMany(c => Orders.Where(o => o.CustomerId == c.Id).Select(o => (c.Id, (int?)o.Id)).DefaultIfEmpty((c.Id, null)))
                .Where(pair => (pair.Item2 is { } id && Orders[id - 1].Qty == 2) || pair.Item1 == 8)
                .Select(pair => (pair.Item1, pair.Item2))
                .Order()
                .ToArray();
            var actual = result.Data
                .Select(r => (Convert.ToInt32(r["cid"]), r["oid"] is null ? (int?)null : Convert.ToInt32(r["oid"])))
                .Order()
                .ToArray();
            Assert.Equal(expected, actual);
            Assert.True(context.Diagnostics.LastQuery!.JoinExecutions <= 1);
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void ConstantAndEmptyConjuncts(StorageMode mode)
    {
        WithSchema(mode, context =>
        {
            AssertJoined(context, Inner3Sql + " WHERE 1 = 1 AND c.Region = 'R1' ORDER BY o.Id", Inner3.Where(j => j.C!.Region == "R1"));
            AssertJoined(context, Inner3Sql + " WHERE 1 = 0 AND c.Region = 'R1' ORDER BY o.Id", []);
            AssertJoined(context, Inner3Sql + " WHERE c.Region = 'R9' AND p.CategoryId = 1 ORDER BY o.Id", []);
            AssertJoined(context, LeftSql + " WHERE o.Qty = 9 AND c.Id IS NULL ORDER BY o.Id", []);
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void PreparedJoin_WithBoundValues(StorageMode mode)
    {
        WithSchema(mode, context =>
        {
            var statement = context.Prepare("SELECT COUNT(*), SUM(o.Amount) FROM Orders o JOIN Customers c ON o.CustomerId = c.Id JOIN Products p ON o.ProductId = p.Id WHERE c.Region = @r AND p.CategoryId = @cat");
            foreach ((string region, int category) in new[] { ("R0", 1), ("R1", 2), ("R2", 0) })
            {
                var result = statement.Execute([DataVoQueryParameter.FromObject("r", region), DataVoQueryParameter.FromObject("cat", category)]);
                Assert.False(result.IsError, string.Join("; ", result.Messages));
                Joined[] expected = [.. Inner3.Where(j => j.C!.Region == region && j.P!.CategoryId == category)];
                object?[] row = [.. Assert.Single(result.Data).Values];
                Assert.Equal((long)expected.Length, Convert.ToInt64(row[0]));
                if (expected.Length > 0)
                {
                    Assert.Equal(expected.Sum(j => j.O.Amount), Convert.ToDouble(row[1]), 6);
                }
            }
        });
    }

    private static void AssertJoined(DataVoContext context, string sql, IEnumerable<Joined> expected)
    {
        var result = context.Execute(sql)[^1];
        Assert.False(result.IsError, string.Join("; ", result.Messages));
        Assert.Equal(expected.Select(j => j.O.Id).Order().ToArray(), result.Data.Select(r => Convert.ToInt32(r.Values.First())).ToArray());

        RuntimeQueryStats stats = context.Diagnostics.LastQuery!;
        Assert.True(stats.JoinExecutions <= 1, $"{sql}: joined {stats.JoinExecutions} times");
    }

    private static void WithSchema(StorageMode mode, Action<DataVoContext> test)
    {
        string? directory = mode == StorageMode.Lsm ? TestStorageDirectories.Create("sql_join_where") : null;
        using var context = new DataVoContext(new DataVoConfig { StorageMode = mode, DataDirectory = directory });
        Execute(context, "CREATE DATABASE JoinWhere");
        Execute(context, "USE JoinWhere");
        Execute(context, "CREATE TABLE Customers (Id INT PRIMARY KEY, Region VARCHAR(4), Tier INT)");
        Execute(context, "CREATE TABLE Products (Id INT PRIMARY KEY, CategoryId INT)");
        Execute(context, "CREATE TABLE Orders (Id INT PRIMARY KEY, CustomerId INT, ProductId INT, Qty INT, Amount DOUBLE, Note VARCHAR(8))");
        Execute(context, "INSERT INTO Customers (Id, Region, Tier) VALUES " + string.Join(", ", Customers.Select(c => $"({c.Id}, '{c.Region}', {c.Tier})")));
        Execute(context, "INSERT INTO Products (Id, CategoryId) VALUES " + string.Join(", ", Products.Select(p => $"({p.Id}, {p.CategoryId})")));
        Execute(context, "INSERT INTO Orders (Id, CustomerId, ProductId, Qty, Amount, Note) VALUES " + string.Join(", ", Orders.Select(o =>
            $"({o.Id}, {o.CustomerId?.ToString() ?? "NULL"}, {o.ProductId}, {o.Qty}, {o.Amount.ToString(System.Globalization.CultureInfo.InvariantCulture)}, {(o.Note is null ? "NULL" : $"'{o.Note}'")})")));
        context.Diagnostics.Enabled = true;
        test(context);
    }

    private static void Execute(DataVoContext context, string sql)
    {
        var result = context.Execute(sql)[^1];
        Assert.False(result.IsError, string.Join("; ", result.Messages));
    }
}
