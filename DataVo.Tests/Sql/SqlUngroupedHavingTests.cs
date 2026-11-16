using System.Globalization;
using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Sql;

/// <summary>
/// <c>HAVING</c> without <c>GROUP BY</c> filters the single row an ungrouped aggregate query returns, also when
/// the query has a <c>WHERE</c>. The expected answer comes from LINQ over the seed data under SQL's rules: the
/// aggregates are computed over the rows WHERE keeps (COUNT 0 and NULL for the others when there is none), the
/// row is returned only when HAVING is true, and a comparison with NULL is not true.
/// </summary>
public sealed class SqlUngroupedHavingTests
{
    public static TheoryData<StorageMode> Modes => new() { StorageMode.InMemory, StorageMode.Lsm };

    private sealed record Order(int Id, int CustomerId, string Status, double Amount, int? Qty);

    private static readonly string[] Statuses = ["NEW", "PAID", "SHIPPED"];

    private static readonly Order[] Orders = [.. Enumerable.Range(1, 600).Select(i => new Order(
        i,
        i % 40 + 1,
        Statuses[i % 3],
        i * 13 % 500 + 0.25,
        i % 7 == 0 ? null : i % 5 + 1))];

    [Theory]
    [MemberData(nameof(Modes))]
    public void WhereWithHaving_ReturnsTheRowOnlyWhenHavingHolds(StorageMode mode)
    {
        WithOrders(mode, Orders, context =>
        {
            Order[] paid = [.. Orders.Where(o => o.Status == "PAID")];

            // Passing and failing HAVING over the same matching rows.
            AssertAggregateRow(context, "SELECT COUNT(*) FROM Orders WHERE Status = 'PAID' HAVING COUNT(*) > 0",
                paid.Length > 0, paid.Length);
            AssertAggregateRow(context, $"SELECT COUNT(*) FROM Orders WHERE Status = 'PAID' HAVING COUNT(*) > {paid.Length}",
                false, paid.Length);
            AssertAggregateRow(context, $"SELECT COUNT(*), SUM(Amount) FROM Orders WHERE Status = 'PAID' HAVING COUNT(*) = {paid.Length}",
                true, paid.Length, paid.Sum(o => o.Amount));

            // A range on the primary key, which the scan narrows to a key range.
            Order[] firstHundred = [.. Orders.Where(o => o.Id <= 100)];
            AssertAggregateRow(context, "SELECT COUNT(*), MAX(Amount) FROM Orders WHERE Id <= 100 HAVING MAX(Amount) >= 400",
                firstHundred.Max(o => o.Amount) >= 400, firstHundred.Length, firstHundred.Max(o => o.Amount));
            AssertAggregateRow(context, "SELECT COUNT(*), MAX(Amount) FROM Orders WHERE Id <= 100 HAVING MAX(Amount) > 100000",
                false, firstHundred.Length, firstHundred.Max(o => o.Amount));

            // HAVING without WHERE keeps working.
            AssertAggregateRow(context, "SELECT COUNT(*) FROM Orders HAVING COUNT(*) > 0", true, Orders.Length);
            AssertAggregateRow(context, $"SELECT COUNT(*) FROM Orders HAVING COUNT(*) > {Orders.Length}", false, Orders.Length);
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void WhereMatchingNoRow_AggregatesOverNothing(StorageMode mode)
    {
        WithOrders(mode, Orders, context =>
        {
            // COUNT over no row is 0, so "> 0" drops the row and "= 0" keeps it.
            AssertAggregateRow(context, "SELECT COUNT(*) FROM Orders WHERE Amount > 100000 HAVING COUNT(*) > 0", false, 0);
            AssertAggregateRow(context, "SELECT COUNT(*) FROM Orders WHERE Amount > 100000 HAVING COUNT(*) = 0", true, 0);
            AssertAggregateRow(context, "SELECT COUNT(*), SUM(Amount) FROM Orders WHERE Amount > 100000 HAVING COUNT(*) = 0", true, 0, null);

            // SUM, MIN and MAX over no row are NULL: no comparison with them is true.
            AssertAggregateRow(context, "SELECT COUNT(*), SUM(Amount) FROM Orders WHERE Amount > 100000 HAVING SUM(Amount) > 0", false, 0, null);
            AssertAggregateRow(context, "SELECT COUNT(*), SUM(Amount) FROM Orders WHERE Amount > 100000 HAVING SUM(Amount) <= 0", false, 0, null);
            AssertAggregateRow(context, "SELECT COUNT(*), MIN(Id) FROM Orders WHERE Status = 'NONE' HAVING MIN(Id) >= 0", false, 0, null);
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void EmptyTable_AggregatesOverNothing(StorageMode mode)
    {
        WithOrders(mode, [], context =>
        {
            AssertAggregateRow(context, "SELECT COUNT(*) FROM Orders WHERE Status = 'PAID' HAVING COUNT(*) > 0", false, 0);
            AssertAggregateRow(context, "SELECT COUNT(*) FROM Orders WHERE Status = 'PAID' HAVING COUNT(*) = 0", true, 0);
            AssertAggregateRow(context, "SELECT COUNT(*), SUM(Amount) FROM Orders WHERE Id > 0 HAVING SUM(Amount) > 0", false, 0, null);
            AssertAggregateRow(context, "SELECT COUNT(*) FROM Orders HAVING COUNT(*) = 0", true, 0);
            AssertAggregateRow(context, "SELECT COUNT(*) FROM Orders HAVING COUNT(*) > 0", false, 0);
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void HavingOverSeveralAggregates_CombinesThemWithAndOr(StorageMode mode)
    {
        WithOrders(mode, Orders, context =>
        {
            Order[] rows = [.. Orders.Where(o => o.Amount >= 100 && o.CustomerId <= 20)];
            int count = rows.Length;
            double sum = rows.Sum(o => o.Amount);
            double min = rows.Min(o => o.Amount);
            int maxId = rows.Max(o => o.Id);
            const string query = "SELECT COUNT(*), SUM(Amount), MIN(Amount), MAX(Id) FROM Orders WHERE Amount >= 100 AND CustomerId <= 20 HAVING ";

            AssertAggregateRow(context, query + $"COUNT(*) >= {count} AND SUM(Amount) > 1000 AND MIN(Amount) >= 100",
                sum > 1000 && min >= 100, count, sum, min, maxId);
            AssertAggregateRow(context, query + $"COUNT(*) > {count} AND SUM(Amount) > 1000",
                false, count, sum, min, maxId);
            AssertAggregateRow(context, query + $"COUNT(*) > {count} OR MAX(Id) = {maxId}",
                true, count, sum, min, maxId);
            AssertAggregateRow(context, query + $"(COUNT(*) < 3 OR SUM(Amount) < 0) AND MAX(Id) = {maxId}",
                count < 3, count, sum, min, maxId);

            // COUNT of a nullable column counts its non-NULL values; AVG is the mean of the kept rows.
            int quantities = rows.Count(o => o.Qty is not null);
            double average = rows.Average(o => o.Amount);
            AssertAggregateRow(context,
                $"SELECT COUNT(Qty), AVG(Amount) FROM Orders WHERE Amount >= 100 AND CustomerId <= 20 HAVING COUNT(Qty) = {quantities} AND AVG(Amount) > 50",
                average > 50, quantities, average);
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void WhereWithHaving_OnTheGeneralAndColumnarPaths_MatchesTheOracle(StorageMode mode)
    {
        WithOrders(mode, Orders, context =>
        {
            // A DISTINCT aggregate is answered by the general path.
            Order[] shipped = [.. Orders.Where(o => o.Status == "SHIPPED")];
            int customers = shipped.Select(o => o.CustomerId).Distinct().Count();
            AssertAggregateRow(context, $"SELECT COUNT(DISTINCT CustomerId) FROM Orders WHERE Status = 'SHIPPED' HAVING COUNT(DISTINCT CustomerId) = {customers}",
                true, customers);
            AssertAggregateRow(context, $"SELECT COUNT(DISTINCT CustomerId) FROM Orders WHERE Status = 'SHIPPED' HAVING COUNT(DISTINCT CustomerId) > {customers}",
                false, customers);
            AssertAggregateRow(context, "SELECT COUNT(DISTINCT CustomerId) FROM Orders WHERE Status = 'NONE' HAVING COUNT(DISTINCT CustomerId) > 0",
                false, 0);

            // A self-contained subquery in WHERE, then ORDER BY and LIMIT after HAVING.
            AssertAggregateRow(context, "SELECT COUNT(*) AS n FROM Orders WHERE Id IN (SELECT Id FROM Orders WHERE Id <= 50) HAVING COUNT(*) = 50 ORDER BY n LIMIT 5",
                true, 50);
            AssertAggregateRow(context, "SELECT COUNT(*) AS n FROM Orders WHERE Id IN (SELECT Id FROM Orders WHERE Id <= 50) HAVING COUNT(*) > 50 ORDER BY n LIMIT 5",
                false, 50);

            // A columnar index answers the aggregate without reading rows.
            Execute(context, "CREATE INDEX cx ON Orders (Amount, Id) USING COLUMNAR");
            Order[] large = [.. Orders.Where(o => o.Amount >= 250)];
            AssertAggregateRow(context, "SELECT COUNT(*), SUM(Id) FROM Orders WHERE Amount >= 250 HAVING COUNT(*) > 0",
                true, large.Length, large.Sum(o => o.Id));
            AssertAggregateRow(context, $"SELECT COUNT(*), SUM(Id) FROM Orders WHERE Amount >= 250 HAVING SUM(Id) > {large.Sum(o => o.Id)}",
                false, large.Length, large.Sum(o => o.Id));
            AssertAggregateRow(context, "SELECT COUNT(*), SUM(Id) FROM Orders WHERE Amount > 100000 HAVING COUNT(*) > 0", false, 0, null);
            AssertAggregateRow(context, "SELECT COUNT(*), SUM(Id) FROM Orders WHERE Amount > 100000 HAVING COUNT(*) = 0", true, 0, null);
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void WhereWithHaving_OverAJoinAndWithBoundValues_MatchesTheOracle(StorageMode mode)
    {
        WithOrders(mode, Orders, context =>
        {
            Execute(context, "CREATE TABLE Customers (Id INT PRIMARY KEY, Region VARCHAR(4))");
            Execute(context, "INSERT INTO Customers (Id, Region) VALUES " + string.Join(", ", Enumerable.Range(1, 30).Select(i => $"({i}, 'R{i % 3}')")));

            Order[] joined = [.. Orders.Where(o => o.CustomerId <= 30 && o.CustomerId % 3 == 1 && o.Amount > 50)];
            const string join = "SELECT COUNT(*), SUM(o.Amount) FROM Orders o JOIN Customers c ON o.CustomerId = c.Id WHERE c.Region = 'R1' AND o.Amount > 50 HAVING ";
            AssertAggregateRow(context, join + "COUNT(*) > 0", joined.Length > 0, joined.Length, joined.Sum(o => o.Amount));
            AssertAggregateRow(context, join + $"COUNT(*) > {joined.Length}", false, joined.Length, joined.Sum(o => o.Amount));
            AssertAggregateRow(context, "SELECT COUNT(*) FROM Orders o JOIN Customers c ON o.CustomerId = c.Id WHERE c.Region = 'NONE' HAVING COUNT(*) > 0", false, 0);

            // Bound values on both sides of HAVING.
            Order[] cheap = [.. Orders.Where(o => o.Amount < 120)];
            var prepared = context.Prepare("SELECT COUNT(*) FROM Orders WHERE Amount < @limit HAVING COUNT(*) >= @floor");
            QueryResult passing = prepared.Execute(DataVoQueryParameter.FromObject("limit", 120), DataVoQueryParameter.FromObject("floor", cheap.Length));
            Assert.False(passing.IsError, string.Join("; ", passing.Messages));
            Assert.Equal(cheap.Length, Convert.ToInt32(Assert.Single(passing.Data).Values.Single(), CultureInfo.InvariantCulture));

            QueryResult failing = prepared.Execute(DataVoQueryParameter.FromObject("limit", 120), DataVoQueryParameter.FromObject("floor", cheap.Length + 1));
            Assert.False(failing.IsError, string.Join("; ", failing.Messages));
            Assert.Empty(failing.Data);
        });
    }

    /// <summary>
    /// Asserts that <paramref name="sql"/> returns the one aggregate row <paramref name="expected"/> when
    /// <paramref name="havingHolds"/>, and no row otherwise.
    /// </summary>
    private static void AssertAggregateRow(DataVoContext context, string sql, bool havingHolds, params object?[] expected)
    {
        QueryResult result = context.Execute(sql)[^1];
        Assert.False(result.IsError, sql + ": " + string.Join("; ", result.Messages));

        if (!havingHolds)
        {
            Assert.True(result.Data.Count == 0, sql + ": expected no row, got " + result.Data.Count);
            return;
        }

        Assert.True(result.Data.Count == 1, sql + ": expected one row, got " + result.Data.Count);
        Assert.Equal(expected.Select(Text), result.Data[0].Values.Select(Text));
    }

    private static string Text(object? value) => value switch
    {
        null => "NULL",
        int or long => Convert.ToInt64(value, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture),
        float or double or decimal => Math.Round(Convert.ToDouble(value, CultureInfo.InvariantCulture), 6).ToString("R", CultureInfo.InvariantCulture),
        _ => value.ToString()!,
    };

    private static void WithOrders(StorageMode mode, Order[] orders, Action<DataVoContext> test)
    {
        string? directory = mode == StorageMode.Lsm ? TestStorageDirectories.Create("sql_ungrouped_having") : null;
        using var context = new DataVoContext(new DataVoConfig { StorageMode = mode, DiskStoragePath = directory });
        Execute(context, "CREATE DATABASE UngroupedHaving");
        Execute(context, "USE UngroupedHaving");
        Execute(context, "CREATE TABLE Orders (Id INT PRIMARY KEY, CustomerId INT, Status VARCHAR(8), Amount DOUBLE, Qty INT)");
        foreach (Order[] chunk in orders.Chunk(300))
        {
            Execute(context, "INSERT INTO Orders (Id, CustomerId, Status, Amount, Qty) VALUES " + string.Join(", ", chunk.Select(o =>
                $"({o.Id}, {o.CustomerId}, '{o.Status}', {o.Amount.ToString(CultureInfo.InvariantCulture)}, {o.Qty?.ToString(CultureInfo.InvariantCulture) ?? "NULL"})")));
        }

        test(context);
    }

    private static void Execute(DataVoContext context, string sql)
    {
        QueryResult result = context.Execute(sql)[^1];
        Assert.False(result.IsError, sql + ": " + string.Join("; ", result.Messages));
    }
}
