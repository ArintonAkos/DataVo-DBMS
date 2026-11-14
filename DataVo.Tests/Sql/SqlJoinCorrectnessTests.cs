using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Sql;

/// <summary>
/// Multi-table joins whose WHERE clause filters more than one table must return exactly what SQL defines.
/// Expected values are computed with LINQ over the same seed rows. The fact table has foreign keys that
/// point at no customer, so inner joins drop rows and outer joins pad them with NULLs.
/// </summary>
public sealed class SqlJoinCorrectnessTests
{
    public static TheoryData<StorageMode> Modes => new() { StorageMode.InMemory, StorageMode.Lsm };

    private const int CustomerCount = 6;
    private const int ProductCount = 4;
    private const int OrderCount = 60;

    private static string Region(int customerId) => "R" + (customerId % 3);
    private static int Category(int productId) => productId % 2;
    private static int CustomerOf(int orderId) => (orderId % 8) + 1;          // 7 and 8 do not exist
    private static int ProductOf(int orderId) => (orderId / 3 % ProductCount) + 1;  // independent of CustomerOf
    private static bool HasCustomer(int orderId) => CustomerOf(orderId) <= CustomerCount;

    private static IEnumerable<int> Orders(Func<int, bool> predicate) => Enumerable.Range(1, OrderCount).Where(predicate);

    [Theory]
    [MemberData(nameof(Modes))]
    public void ThreeTableJoin_AndAcrossBothDimensions_ReturnsMatchingRows(StorageMode mode)
    {
        WithSchema(mode, context =>
        {
            int[] expected = [.. Orders(i => HasCustomer(i) && Region(CustomerOf(i)) == "R1" && Category(ProductOf(i)) == 1)];
            Assert.NotEmpty(expected);

            Assert.Equal(expected, Ints(context,
                "SELECT o.Id FROM Orders o JOIN Customers c ON o.CustomerId = c.Id JOIN Products p ON o.ProductId = p.Id " +
                "WHERE c.Region = 'R1' AND p.CategoryId = 1 ORDER BY o.Id"));

            Assert.Equal(expected.Length, Scalar(context,
                "SELECT COUNT(*) FROM Orders o JOIN Customers c ON o.CustomerId = c.Id JOIN Products p ON o.ProductId = p.Id " +
                "WHERE c.Region = 'R1' AND p.CategoryId = 1"));

            // Same query with the joins listed the other way round.
            Assert.Equal(expected.Length, Scalar(context,
                "SELECT COUNT(*) FROM Orders o JOIN Products p ON o.ProductId = p.Id JOIN Customers c ON o.CustomerId = c.Id " +
                "WHERE p.CategoryId = 1 AND c.Region = 'R1'"));
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void ThreeTableJoin_NonEqualityAndOrAcrossDimensions_ReturnsMatchingRows(StorageMode mode)
    {
        WithSchema(mode, context =>
        {
            Assert.Equal(
                Orders(i => HasCustomer(i) && Region(CustomerOf(i)) != "R1" && ProductOf(i) > 2).Count(),
                Scalar(context,
                    "SELECT COUNT(*) FROM Orders o JOIN Customers c ON o.CustomerId = c.Id JOIN Products p ON o.ProductId = p.Id " +
                    "WHERE c.Region <> 'R1' AND p.Id > 2"));

            Assert.Equal(
                [.. Orders(i => HasCustomer(i) && (Region(CustomerOf(i)) == "R2" || Category(ProductOf(i)) == 0))],
                Ints(context,
                    "SELECT o.Id FROM Orders o JOIN Customers c ON o.CustomerId = c.Id JOIN Products p ON o.ProductId = p.Id " +
                    "WHERE c.Region = 'R2' OR p.CategoryId = 0 ORDER BY o.Id"));

            // Fact-table and dimension filters combined.
            Assert.Equal(
                [.. Orders(i => HasCustomer(i) && i > 20 && Region(CustomerOf(i)) == "R0" && Category(ProductOf(i)) == 0)],
                Ints(context,
                    "SELECT o.Id FROM Orders o JOIN Customers c ON o.CustomerId = c.Id JOIN Products p ON o.ProductId = p.Id " +
                    "WHERE o.Id > 20 AND c.Region = 'R0' AND p.CategoryId = 0 ORDER BY o.Id"));
        });
    }

    internal static void WithSchema(StorageMode mode, Action<DataVoContext> test)
    {
        string? directory = mode == StorageMode.Lsm ? TestStorageDirectories.Create("sql_join_correctness") : null;
        using var context = new DataVoContext(new DataVoConfig { StorageMode = mode, DiskStoragePath = directory });
        Execute(context, "CREATE DATABASE JoinCorrectness");
        Execute(context, "USE JoinCorrectness");
        Execute(context, "CREATE TABLE Customers (Id INT PRIMARY KEY, Region VARCHAR(4))");
        Execute(context, "CREATE TABLE Products (Id INT PRIMARY KEY, CategoryId INT)");
        Execute(context, "CREATE TABLE Orders (Id INT PRIMARY KEY, CustomerId INT, ProductId INT, Amount INT)");
        Execute(context, "CREATE INDEX ix_orders_customer ON Orders (CustomerId)");
        Execute(context, "INSERT INTO Customers (Id, Region) VALUES " +
            string.Join(", ", Enumerable.Range(1, CustomerCount).Select(i => $"({i}, '{Region(i)}')")));
        Execute(context, "INSERT INTO Products (Id, CategoryId) VALUES " +
            string.Join(", ", Enumerable.Range(1, ProductCount).Select(i => $"({i}, {Category(i)})")));
        Execute(context, "INSERT INTO Orders (Id, CustomerId, ProductId, Amount) VALUES " +
            string.Join(", ", Enumerable.Range(1, OrderCount).Select(i => $"({i}, {CustomerOf(i)}, {ProductOf(i)}, {i * 10})")));
        test(context);
    }

    private static void Execute(DataVoContext context, string sql)
    {
        var result = context.Execute(sql)[^1];
        Assert.False(result.IsError, string.Join("; ", result.Messages));
    }

    private static int[] Ints(DataVoContext context, string sql)
    {
        var result = context.Execute(sql)[^1];
        Assert.False(result.IsError, string.Join("; ", result.Messages));
        return [.. result.Data.Select(row => Convert.ToInt32(row.Values.First()))];
    }

    private static long Scalar(DataVoContext context, string sql)
    {
        var result = context.Execute(sql)[^1];
        Assert.False(result.IsError, string.Join("; ", result.Messages));
        return Convert.ToInt64(Assert.Single(result.Data).Values.First());
    }
}
