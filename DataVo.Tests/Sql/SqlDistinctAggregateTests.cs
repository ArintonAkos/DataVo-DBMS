using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Sql;

/// <summary>
/// <c>COUNT(DISTINCT x)</c> and the other DISTINCT aggregates read only distinct non-NULL values. They used to
/// be parsed as their plain form, so <c>COUNT(DISTINCT x)</c> silently returned <c>COUNT(x)</c>. Expected
/// values come from LINQ over the seed rows; the queries hit the typed scan, Volcano, typed join and general
/// aggregate paths, which must all agree.
/// </summary>
public sealed class SqlDistinctAggregateTests
{
    public static TheoryData<StorageMode> Modes => new() { StorageMode.InMemory, StorageMode.Lsm };

    private const int OrderCount = 40;

    private static int? CustomerOf(int id) => id % 9 == 0 ? null : (id % 5) + 1;    // 1..5, some NULL
    private static int Qty(int id) => id % 4 + 1;
    private static double Price(int id) => (id % 3) * 0.5 + 1;                        // 1.0, 1.5, 2.0
    private static string Region(int customerId) => "R" + (customerId % 2);

    private static IEnumerable<int> Orders => Enumerable.Range(1, OrderCount);

    [Theory]
    [MemberData(nameof(Modes))]
    public void CountDistinct_CountsDistinctNonNullValues(StorageMode mode)
    {
        WithSchema(mode, context =>
        {
            Assert.Equal(new object?[] { 5L, (long)Orders.Count(i => CustomerOf(i) != null), 40L },
                Row(context, "SELECT COUNT(DISTINCT CustomerId), COUNT(CustomerId), COUNT(*) FROM Orders"));

            Assert.Equal(4L, Row(context, "SELECT COUNT(DISTINCT Qty) FROM Orders WHERE Id > 3")[0]);
            Assert.Equal(3L, Row(context, "SELECT COUNT(DISTINCT Price) FROM Orders")[0]);
            Assert.Equal(0L, Row(context, "SELECT COUNT(DISTINCT CustomerId) FROM Orders WHERE Id > 1000")[0]);
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void SumAndAvgDistinct_UseEachValueOnce(StorageMode mode)
    {
        WithSchema(mode, context =>
        {
            object?[] row = Row(context, "SELECT SUM(DISTINCT Qty), AVG(DISTINCT Qty), SUM(Qty), MIN(DISTINCT Qty), MAX(DISTINCT Qty) FROM Orders");
            Assert.Equal(10d, Convert.ToDouble(row[0]));
            Assert.Equal(2.5d, Convert.ToDouble(row[1]));
            Assert.Equal((double)Orders.Sum(Qty), Convert.ToDouble(row[2]));
            Assert.Equal(1d, Convert.ToDouble(row[3]));
            Assert.Equal(4d, Convert.ToDouble(row[4]));

            Assert.Equal(4.5d, Convert.ToDouble(Row(context, "SELECT SUM(DISTINCT Price) FROM Orders")[0]));
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void CountDistinct_PerGroup(StorageMode mode)
    {
        WithSchema(mode, context =>
        {
            var expected = Orders
                .GroupBy(Qty)
                .OrderBy(g => g.Key)
                .Select(g => (g.Key, (long)g.Select(CustomerOf).Where(c => c != null).Distinct().Count()))
                .ToArray();

            var result = context.Execute("SELECT Qty, COUNT(DISTINCT CustomerId) AS customers FROM Orders GROUP BY Qty ORDER BY Qty")[^1];
            Assert.False(result.IsError, string.Join("; ", result.Messages));
            Assert.Equal(expected, result.Data.Select(r => (Convert.ToInt32(r["Qty"]), Convert.ToInt64(r["customers"]))).ToArray());
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void CountDistinct_OverJoin(StorageMode mode)
    {
        WithSchema(mode, context =>
        {
            int[] joined = [.. Orders.Where(i => CustomerOf(i) != null)];
            Assert.Equal(
                new object?[] { (long)joined.Select(i => Region(CustomerOf(i)!.Value)).Distinct().Count(), (long)joined.Length },
                Row(context, "SELECT COUNT(DISTINCT c.Region), COUNT(c.Region) FROM Orders o JOIN Customers c ON o.CustomerId = c.Id"));

            Assert.Equal(
                (long)joined.Where(i => Region(CustomerOf(i)!.Value) == "R1").Select(Qty).Distinct().Count(),
                Row(context, "SELECT COUNT(DISTINCT o.Qty) FROM Orders o JOIN Customers c ON o.CustomerId = c.Id WHERE c.Region = 'R1'")[0]);
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void CountDistinct_PreparedWithParameters(StorageMode mode)
    {
        WithSchema(mode, context =>
        {
            var statement = context.Prepare("SELECT COUNT(DISTINCT CustomerId) FROM Orders WHERE Qty = @q");
            var result = statement.Execute([DataVoQueryParameter.FromObject("q", 2)]);
            Assert.False(result.IsError, string.Join("; ", result.Messages));
            Assert.Equal(
                (long)Orders.Where(i => Qty(i) == 2).Select(CustomerOf).Where(c => c != null).Distinct().Count(),
                Convert.ToInt64(Assert.Single(result.Data).Values.First()));
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void CountDistinctStar_IsRejected(StorageMode mode)
    {
        WithSchema(mode, context =>
        {
            Assert.True(context.Execute("SELECT COUNT(DISTINCT *) FROM Orders")[^1].IsError);
        });
    }

    [Fact]
    public void ReactiveSubscription_RejectsDistinctAggregate()
    {
        WithSchema(StorageMode.InMemory, context =>
        {
            var error = Assert.Throws<NotSupportedException>(() =>
                context.Subscribe("SELECT COUNT(DISTINCT CustomerId) FROM Orders", _ => { }));
            Assert.Contains("DISTINCT", error.Message);
        });
    }

    private static void WithSchema(StorageMode mode, Action<DataVoContext> test)
    {
        string? directory = mode == StorageMode.Lsm ? TestStorageDirectories.Create("sql_distinct_aggregate") : null;
        using var context = new DataVoContext(new DataVoConfig { StorageMode = mode, DataDirectory = directory });
        Execute(context, "CREATE DATABASE DistinctAgg");
        Execute(context, "USE DistinctAgg");
        Execute(context, "CREATE TABLE Customers (Id INT PRIMARY KEY, Region VARCHAR(4))");
        Execute(context, "CREATE TABLE Orders (Id INT PRIMARY KEY, CustomerId INT, Qty INT, Price DOUBLE)");
        Execute(context, "INSERT INTO Customers (Id, Region) VALUES " +
            string.Join(", ", Enumerable.Range(1, 5).Select(i => $"({i}, '{Region(i)}')")));
        Execute(context, "INSERT INTO Orders (Id, CustomerId, Qty, Price) VALUES " +
            string.Join(", ", Orders.Select(i =>
                $"({i}, {(CustomerOf(i)?.ToString() ?? "NULL")}, {Qty(i)}, {Price(i).ToString(System.Globalization.CultureInfo.InvariantCulture)})")));
        test(context);
    }

    private static void Execute(DataVoContext context, string sql)
    {
        var result = context.Execute(sql)[^1];
        Assert.False(result.IsError, string.Join("; ", result.Messages));
    }

    private static object?[] Row(DataVoContext context, string sql)
    {
        var result = context.Execute(sql)[^1];
        Assert.False(result.IsError, string.Join("; ", result.Messages));
        return [.. Assert.Single(result.Data).Values.Select(v => v is int n ? (long)n : v)];
    }
}
