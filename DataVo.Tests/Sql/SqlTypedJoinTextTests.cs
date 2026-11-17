using System.Globalization;
using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Sql;

/// <summary>
/// The typed join reads a hashed table's serialized rows in two steps: its text conditions are compared on the stored
/// UTF-8 bytes, and the columns the join keeps are decoded only for rows that pass, with kept text shared per
/// distinct value. These tests check the answers against LINQ over the seed data (accented and CJK regions, NULLs,
/// LIKE, a LEFT JOIN whose condition runs after the join, GROUP BY on a hashed text column) and that a text condition
/// on the hashed table costs no more than an integer one.
/// </summary>
public sealed class SqlTypedJoinTextTests
{
    public static TheoryData<StorageMode> Modes => new() { StorageMode.InMemory, StorageMode.Lsm };

    private sealed record Customer(int Id, string Name, string? Region, int Segment);

    private sealed record Order(int Id, int CustomerId, double Amount);

    private static readonly string?[] Regions = ["R1", "R3", "é", "日本", "R1 ", null, "r1", "Zoë"];

    private static readonly Customer[] Customers = [.. Enumerable.Range(1, 400).Select(i => new Customer(i, $"N{i % 37}", Regions[i % Regions.Length], i % 5))];

    private static readonly Order[] Orders = [.. Enumerable.Range(1, 1_500).Select(i => new Order(i, i * 7 % 450 + 1, i % 90 + 0.5))];

    [Theory]
    [MemberData(nameof(Modes))]
    public void HashedTextConditionsAndKeptText_MatchLinq(StorageMode mode)
    {
        using DataVoContext context = Open(mode, Customers, Orders);
        var joined = (from o in Orders join c in Customers on o.CustomerId equals c.Id select (o, c)).ToArray();

        foreach (string? region in Regions.Where(r => r is not null).Append("missing"))
        {
            QueryResult totals = Run(context, $"SELECT COUNT(*) AS N, SUM(o.Amount) AS S FROM Orders o JOIN Customers c ON o.CustomerId = c.Id WHERE c.Region = '{region}' AND c.Segment < 4");
            var match = joined.Where(j => j.c.Region == region && j.c.Segment < 4).ToArray();
            Assert.Equal(match.Length, Convert.ToInt32(Assert.Single(totals.Data)["N"], CultureInfo.InvariantCulture));

            QueryResult rows = Run(context, $"SELECT o.Id, c.Name, c.Region FROM Orders o JOIN Customers c ON o.CustomerId = c.Id WHERE c.Region = '{region}' ORDER BY o.Id LIMIT 25");
            Assert.Equal(
                joined.Where(j => j.c.Region == region).OrderBy(j => j.o.Id).Take(25).Select(j => $"{j.o.Id}|{j.c.Name}|{j.c.Region}"),
                rows.Data.Select(row => $"{row.Values.ElementAt(0)}|{row.Values.ElementAt(1)}|{row.Values.ElementAt(2)}"));
        }

        QueryResult like = Run(context, "SELECT o.Id FROM Orders o JOIN Customers c ON o.CustomerId = c.Id WHERE c.Name LIKE 'N1%' AND c.Region IS NOT NULL ORDER BY o.Id");
        Assert.Equal(joined.Where(j => j.c.Name.StartsWith("N1", StringComparison.Ordinal) && j.c.Region is not null).Select(j => j.o.Id).OrderBy(id => id),
            like.Data.Select(row => Convert.ToInt32(row.Values.Single(), CultureInfo.InvariantCulture)));

        QueryResult groups = Run(context, "SELECT c.Region, COUNT(*) AS N FROM Orders o JOIN Customers c ON o.CustomerId = c.Id GROUP BY c.Region");
        Assert.Equal(
            joined.GroupBy(j => j.c.Region).Select(g => $"{g.Key ?? "<null>"}|{g.Count()}").Order(StringComparer.Ordinal),
            groups.Data.Select(row => $"{(string?)row.Values.First() ?? "<null>"}|{Convert.ToInt32(row.Values.Last(), CultureInfo.InvariantCulture)}").Order(StringComparer.Ordinal));

        // A LEFT JOIN keeps every order; its condition on the joined table is tested after the join.
        QueryResult left = Run(context, "SELECT COUNT(*) AS N FROM Orders o LEFT JOIN Customers c ON o.CustomerId = c.Id WHERE c.Region = 'é'");
        Assert.Equal(joined.Count(j => j.c.Region == "é"), Convert.ToInt32(Assert.Single(left.Data)["N"], CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// Hashing a table filtered on a text column decodes no text for rows that fail the condition: it costs about what
    /// the same join filtered on an integer column costs (before, every customer's region and name were decoded).
    /// </summary>
    [Theory]
    [MemberData(nameof(Modes))]
    public void TextConditionOnTheHashedTable_DecodesNoTextForRejectedRows(StorageMode mode)
    {
        Customer[] customers = [.. Enumerable.Range(1, 20_000).Select(i => new Customer(i, $"Customer-{i:D6}", $"Region-{i % 8}", i % 8))];
        Order[] orders = [.. Enumerable.Range(1, 2_000).Select(i => new Order(i, i % 20_000 + 1, 1.5))];
        using DataVoContext context = Open(mode, customers, orders);

        const string join = "SELECT o.Id, c.Name FROM Orders o JOIN Customers c ON o.CustomerId = c.Id WHERE ";
        long byInteger = Measure(context, join + "c.Segment = 3");
        long byText = Measure(context, join + "c.Region = 'Region-3'");
        Assert.Equal(Run(context, join + "c.Segment = 3").Data.Count, Run(context, join + "c.Region = 'Region-3'").Data.Count);
        Assert.True(byText < byInteger + 64 * 1024, $"{mode}: text condition {byText:N0} B, integer condition {byInteger:N0} B");
    }

    private static long Measure(DataVoContext context, string sql)
    {
        Run(context, sql); // warm up
        long before = GC.GetAllocatedBytesForCurrentThread();
        Assert.NotEmpty(Run(context, sql).Data);
        return GC.GetAllocatedBytesForCurrentThread() - before;
    }

    private static DataVoContext Open(StorageMode mode, Customer[] customers, Order[] orders)
    {
        var context = new DataVoContext(new DataVoConfig
        {
            StorageMode = mode,
            DiskStoragePath = mode == StorageMode.Lsm ? TestStorageDirectories.Create("sql_typed_join_text") : null,
        });
        Run(context, "CREATE DATABASE J");
        Run(context, "USE J");
        Run(context, "CREATE TABLE Customers (Id INT PRIMARY KEY, Name VARCHAR(24), Region VARCHAR(12), Segment INT)");
        Run(context, "CREATE TABLE Orders (Id INT PRIMARY KEY, CustomerId INT, Amount DOUBLE)");
        foreach (Customer[] chunk in customers.Chunk(1_000))
        {
            Run(context, "INSERT INTO Customers (Id, Name, Region, Segment) VALUES " + string.Join(", ", chunk.Select(c =>
                $"({c.Id}, '{c.Name}', {(c.Region is null ? "NULL" : $"'{c.Region}'")}, {c.Segment})")));
        }

        foreach (Order[] chunk in orders.Chunk(1_000))
        {
            Run(context, "INSERT INTO Orders (Id, CustomerId, Amount) VALUES " + string.Join(", ", chunk.Select(o =>
                $"({o.Id}, {o.CustomerId}, {o.Amount.ToString(CultureInfo.InvariantCulture)})")));
        }

        return context;
    }

    private static QueryResult Run(DataVoContext context, string sql)
    {
        QueryResult result = context.Execute(sql)[^1];
        Assert.False(result.IsError, sql + ": " + string.Join("; ", result.Messages));
        return result;
    }
}
