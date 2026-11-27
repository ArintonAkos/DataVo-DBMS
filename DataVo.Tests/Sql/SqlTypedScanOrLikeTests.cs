using System.Globalization;
using System.Text.RegularExpressions;
using DataVo.Core;
using DataVo.Core.Indexing.Columnar;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Sql;

/// <summary>
/// <c>OR</c> and <c>LIKE</c> in a single-table WHERE are answered by the typed scan (and an OR of numeric
/// comparisons by the columnar index) instead of the dictionary-per-row general path. Expected rows come from
/// LINQ over the seed data with SQL's rules written out: a comparison with NULL is unknown, a WHERE keeps
/// only true rows, and LIKE is a case-sensitive whole-string match where <c>%</c> is any run and <c>_</c> one
/// character.
/// </summary>
public sealed class SqlTypedScanOrLikeTests
{
    public static TheoryData<StorageMode> Modes => new() { StorageMode.InMemory, StorageMode.Lsm };

    private sealed record Order(int Id, string? Status, int Qty, double Amount, double? Discount, string? Sku, int CustomerId);

    private static readonly string[] Statuses = ["PAID", "SHIPPED", "NEW", "paid"];

    private static readonly Order[] Orders = [.. Enumerable.Range(1, 240).Select(i => new Order(
        i,
        i % 11 == 0 ? null : Statuses[i % Statuses.Length],
        i % 4 + 1,
        i * 37 % 1000 + 0.5,
        i % 5 == 0 ? null : i % 7 * 1.25,
        SkuOf(i),
        i % 6 + 1))];

    private static string? SkuOf(int i) => (i % 13) switch
    {
        0 => null,
        1 => "S_42-A",          // a literal underscore: '_' still matches it as any character
        2 => "S%42-B",          // a literal percent sign
        3 => "s0042-a",         // lower case: LIKE is case-sensitive
        4 => "S0042-A\nX",      // a newline inside the value
        5 => "S0042-A\n",       // a trailing newline: 'S0042-A' must not match it
        _ => $"S{i % 97:D4}-{(char)('A' + i % 3)}",
    };

    [Theory]
    [MemberData(nameof(Modes))]
    public void Or_ReturnsTheRowsOfEitherBranch(StorageMode mode)
    {
        WithOrders(mode, context =>
        {
            AssertIds(context, "SELECT Id FROM Orders WHERE Status = 'PAID' OR Status = 'SHIPPED' ORDER BY Id",
                o => o.Status is "PAID" or "SHIPPED");

            // A NULL on one side of the OR is unknown, not false for the whole row.
            AssertIds(context, "SELECT Id FROM Orders WHERE Discount < 2 OR Status = 'PAID' ORDER BY Id",
                o => o.Discount < 2 || o.Status == "PAID");

            // Nested AND inside OR, and an OR inside an AND.
            AssertIds(context,
                "SELECT Id FROM Orders WHERE (Qty = 1 AND Amount > 500) OR (Qty = 3 AND Status <> 'PAID') OR Sku LIKE 'S00%' ORDER BY Id",
                o => (o.Qty == 1 && o.Amount > 500) || (o.Qty == 3 && o.Status is not null && o.Status != "PAID") || Like(o.Sku, "S00%"));
            AssertIds(context,
                "SELECT Id FROM Orders WHERE Amount >= 100 AND (Discount IS NULL OR Discount >= 5) AND (Qty = 2 OR Qty = 4) ORDER BY Id",
                o => o.Amount >= 100 && (o.Discount is null || o.Discount >= 5) && o.Qty is 2 or 4);

            // An OR on the clustered key must not narrow the key range the scan reads.
            AssertIds(context, "SELECT Id FROM Orders WHERE Id < 10 OR Id > 230 ORDER BY Id", o => o.Id is < 10 or > 230);
            AssertIds(context, "SELECT Id FROM Orders WHERE Id > 100 AND (Id < 103 OR Id = 200) ORDER BY Id", o => o.Id is > 100 and (< 103 or 200));
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void Or_OnAnIndexedColumn_DoesNotUseTheIndexForOneBranch(StorageMode mode)
    {
        WithOrders(mode, context =>
        {
            Execute(context, "CREATE INDEX ix_orders_qty ON Orders (Qty)");
            AssertIds(context, "SELECT Id FROM Orders WHERE Qty = 1 OR Qty = 2 ORDER BY Id", o => o.Qty is 1 or 2);
            AssertIds(context, "SELECT Id FROM Orders WHERE Id = 5 OR Qty = 3 ORDER BY Id", o => o.Id == 5 || o.Qty == 3);
            AssertIds(context, "SELECT Id FROM Orders WHERE Qty = 2 AND (Status = 'NEW' OR Status = 'paid') ORDER BY Id",
                o => o.Qty == 2 && o.Status is "NEW" or "paid");
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void Or_WithAggregates_MatchesTheOracle(StorageMode mode)
    {
        WithOrders(mode, context =>
        {
            Order[] matched = [.. Orders.Where(o => o.Status is "PAID" or "SHIPPED" && o.Amount >= 200 && o.Amount < 500)];
            object?[] row = Row(context, "SELECT COUNT(*), SUM(Amount) FROM Orders WHERE (Status = 'PAID' OR Status = 'SHIPPED') AND Amount >= 200 AND Amount < 500");
            Assert.Equal((long)matched.Length, Convert.ToInt64(row[0], CultureInfo.InvariantCulture));
            Assert.Equal(matched.Sum(o => o.Amount), Convert.ToDouble(row[1], CultureInfo.InvariantCulture), 6);

            var groups = Orders.Where(o => o.Qty == 1 || o.Discount > 6)
                .GroupBy(o => o.Qty)
                .OrderBy(g => g.Key)
                .Select(g => (g.Key, (long)g.Count()))
                .ToArray();
            var result = context.Execute("SELECT Qty, COUNT(*) AS n FROM Orders WHERE Qty = 1 OR Discount > 6 GROUP BY Qty ORDER BY Qty")[^1];
            Assert.False(result.IsError, string.Join("; ", result.Messages));
            Assert.Equal(groups, result.Data.Select(r => (Convert.ToInt32(r["Qty"]), Convert.ToInt64(r["n"]))).ToArray());
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void Like_FollowsSqlWildcardsCaseAndNulls(StorageMode mode)
    {
        WithOrders(mode, context =>
        {
            string[] patterns =
            [
                "S004%",        // prefix
                "%-A",          // suffix
                "%42-%",        // substring
                "S0042-A",      // no wildcard
                "S_42-A",       // one character
                "S00_2-_",
                "%4_-%A",       // mixed
                "%",            // every non-NULL value
                "s004%",        // case-sensitive
                "S%42-B",       // a percent sign in the data matches the wildcard
                "S0042-A%",     // matches across the newline
                "%X",
                "S0042-A",      // the value with a newline after it must not match
            ];

            foreach (string pattern in patterns)
            {
                AssertIds(context, $"SELECT Id FROM Orders WHERE Sku LIKE '{pattern}' ORDER BY Id", o => Like(o.Sku, pattern));
                AssertIds(context, $"SELECT Id FROM Orders WHERE Sku NOT LIKE '{pattern}' ORDER BY Id", o => o.Sku is not null && !Like(o.Sku, pattern));
            }

            AssertIds(context, "SELECT Id FROM Orders WHERE Sku LIKE 'S00%' AND Qty = 2 ORDER BY Id", o => Like(o.Sku, "S00%") && o.Qty == 2);
            AssertIds(context, "SELECT Id FROM Orders WHERE Sku LIKE '%-B' OR Status LIKE 'SHIP%' ORDER BY Id",
                o => Like(o.Sku, "%-B") || Like(o.Status, "SHIP%"));

            // LIKE on a number compares its text, as the general comparer does.
            AssertIds(context, "SELECT Id FROM Orders WHERE Qty LIKE '1%' ORDER BY Id", o => o.Qty.ToString(CultureInfo.InvariantCulture).StartsWith('1'));

            Assert.Equal((long)Orders.Count(o => Like(o.Sku, "%42-%")), Convert.ToInt64(Row(context, "SELECT COUNT(*) FROM Orders WHERE Sku LIKE '%42-%'")[0]));
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void PreparedOrAndLike_BindParameters(StorageMode mode)
    {
        WithOrders(mode, context =>
        {
            var like = context.Prepare("SELECT COUNT(*) FROM Orders WHERE Sku LIKE @pattern");
            foreach (string pattern in new[] { "S004%", "%42-%", "S_42-A", "%" })
            {
                var result = like.Execute([DataVoQueryParameter.FromObject("pattern", pattern)]);
                Assert.False(result.IsError, string.Join("; ", result.Messages));
                Assert.Equal((long)Orders.Count(o => Like(o.Sku, pattern)), Convert.ToInt64(Assert.Single(result.Data).Values.First()));
            }

            var or = context.Prepare("SELECT Id FROM Orders WHERE (Status = @a OR Status = @b) AND Amount >= @lo ORDER BY Id");
            var rows = or.Execute([
                DataVoQueryParameter.FromObject("a", "NEW"),
                DataVoQueryParameter.FromObject("b", "paid"),
                DataVoQueryParameter.FromObject("lo", 300)]);
            Assert.False(rows.IsError, string.Join("; ", rows.Messages));
            Assert.Equal(
                Orders.Where(o => o.Status is "NEW" or "paid" && o.Amount >= 300).Select(o => o.Id).ToArray(),
                rows.Data.Select(r => Convert.ToInt32(r["Id"])).ToArray());
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void StreamQuery_FiltersWithOrAndLike(StorageMode mode)
    {
        WithOrders(mode, context =>
        {
            int[] streamed = [.. context.StreamQuery("SELECT Id FROM Orders WHERE Sku LIKE '%-C' OR Qty = 4").Select(r => Convert.ToInt32(r["Id"]))];
            Assert.Equal(
                Orders.Where(o => Like(o.Sku, "%-C") || o.Qty == 4).Select(o => o.Id).Order().ToArray(),
                streamed.Order().ToArray());
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void TypedJoin_TakesAnOrOnOneSide_AndAnOrAcrossSidesStaysCorrect(StorageMode mode)
    {
        WithOrders(mode, context =>
        {
            Execute(context, "CREATE TABLE Customers (Id INT PRIMARY KEY, Region VARCHAR(8))");
            Execute(context, "INSERT INTO Customers (Id, Region) VALUES " + string.Join(", ", Enumerable.Range(1, 6).Select(i => $"({i}, '{RegionOf(i)}')")));

            Assert.Equal(
                (long)Orders.Count(o => RegionOf(o.CustomerId) is "R1" or "R2" && o.Qty > 1),
                Convert.ToInt64(Row(context, "SELECT COUNT(*) FROM Orders o JOIN Customers c ON o.CustomerId = c.Id WHERE (c.Region = 'R1' OR c.Region = 'R2') AND o.Qty > 1")[0]));
            Assert.Equal(
                (long)Orders.Count(o => o.Qty == 1 || RegionOf(o.CustomerId) == "R0"),
                Convert.ToInt64(Row(context, "SELECT COUNT(*) FROM Orders o JOIN Customers c ON o.CustomerId = c.Id WHERE o.Qty = 1 OR c.Region = 'R0'")[0]));
            Assert.Equal(
                (long)Orders.Count(o => Like(o.Sku, "S00%") && RegionOf(o.CustomerId) == "R2"),
                Convert.ToInt64(Row(context, "SELECT COUNT(*) FROM Orders o JOIN Customers c ON o.CustomerId = c.Id WHERE o.Sku LIKE 'S00%' AND c.Region = 'R2'")[0]));
        });

        static string RegionOf(int customerId) => "R" + customerId % 3;
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void ColumnarIndex_AnswersAnOrOfNumericComparisons(StorageMode mode)
    {
        WithOrders(mode, context =>
        {
            Execute(context, "CREATE INDEX cx_orders ON Orders (Qty, Amount) USING COLUMNAR");
            ColumnarIndex? index = context.Engine.Columnar.Find("OrLike", "Orders");
            long scansBefore = index?.Scans ?? 0;

            // An integer sum: a floating-point one is answered from rows once the index is out of key order.
            Order[] matched = [.. Orders.Where(o => o.Qty is 1 or 2 && o.Amount >= 200)];
            object?[] row = Row(context, "SELECT COUNT(*), SUM(Qty) FROM Orders WHERE (Qty = 1 OR Qty = 2) AND Amount >= 200");
            Assert.Equal((long)matched.Length, Convert.ToInt64(row[0], CultureInfo.InvariantCulture));
            Assert.Equal((long)matched.Sum(o => o.Qty), Convert.ToInt64(row[1], CultureInfo.InvariantCulture));

            Assert.Equal(
                (long)Orders.Count(o => o.Qty == 3 || (o.Qty == 4 && o.Amount < 100) || o.Amount > 990),
                Convert.ToInt64(Row(context, "SELECT COUNT(*) FROM Orders WHERE Qty = 3 OR (Qty = 4 AND Amount < 100) OR Amount > 990")[0]));

            if (index is not null)
            {
                Assert.Equal(2, index.Scans - scansBefore);
            }

            // A branch on a column outside the index: answered from rows, and still right.
            Assert.Equal(
                (long)Orders.Count(o => o.Qty == 1 || o.Discount > 7),
                Convert.ToInt64(Row(context, "SELECT COUNT(*) FROM Orders WHERE Qty = 1 OR Discount > 7")[0]));
            if (index is not null)
            {
                Assert.Equal(2, index.Scans - scansBefore);
            }
        });
    }

    /// <summary>
    /// The OR and LIKE filters stay on the typed scan: they cost no more per scanned row than a typed text
    /// equality over the same rows. The general path builds a dictionary for every row it reads (1.2 to 2.9 KB
    /// a row here).
    /// </summary>
    [Theory]
    [MemberData(nameof(Modes))]
    public void OrAndLikeFilters_DoNotAllocatePerScannedRow(StorageMode mode)
    {
        const int rowCount = 20_000;
        string? directory = mode == StorageMode.Lsm ? TestStorageDirectories.Create("sql_typed_or_like_alloc") : null;
        using var context = new DataVoContext(new DataVoConfig { StorageMode = mode, DataDirectory = directory });
        Execute(context, "CREATE DATABASE OrLikeAlloc");
        Execute(context, "USE OrLikeAlloc");
        Execute(context, "CREATE TABLE T (Id INT PRIMARY KEY, Status VARCHAR(8), Amount DOUBLE, Sku VARCHAR(16))");
        for (int start = 0; start < rowCount; start += 1_000)
        {
            Execute(context, "INSERT INTO T (Id, Status, Amount, Sku) VALUES " + string.Join(", ", Enumerable.Range(start, 1_000)
                .Select(i => $"({i}, '{Statuses[i % 3]}', {i % 1000}.5, 'S{i:D5}-{i % 7}')")));
        }

        string[] queries =
        [
            "SELECT COUNT(*), SUM(Amount) FROM T WHERE (Status = 'PAID' OR Status = 'SHIPPED') AND Amount >= 200 AND Amount < 500",
            "SELECT COUNT(*) FROM T WHERE Sku LIKE 'S0042%'",
            "SELECT COUNT(*) FROM T WHERE Sku LIKE '%042-%'",
            "SELECT Id FROM T WHERE Sku LIKE '%77-3' OR Amount = 1.5",
        ];

        long baseline = BytesPerRow("SELECT COUNT(*), SUM(Amount) FROM T WHERE Status = 'PAID' AND Sku = 'S00042-0'");
        foreach (string query in queries)
        {
            long perRow = BytesPerRow(query);
            Assert.True(perRow <= baseline + 32, $"{mode}: {query} allocated {perRow} B per scanned row; a typed equality {baseline} B");
        }

        long BytesPerRow(string query)
        {
            Assert.False(context.Execute(query)[^1].IsError); // warm up
            long before = GC.GetAllocatedBytesForCurrentThread();
            var result = context.Execute(query)[^1];
            long perRow = (GC.GetAllocatedBytesForCurrentThread() - before) / rowCount;
            Assert.False(result.IsError, string.Join("; ", result.Messages));
            return perRow;
        }
    }

    [Fact]
    public void LikePattern_MatchesTheRegexDefinition_OnRandomInputs()
    {
        var random = new Random(20261115);
        const string alphabet = "ab%_\n";
        for (int i = 0; i < 20_000; i++)
        {
            string pattern = RandomText(random, alphabet, 6);
            string input = RandomText(random, alphabet.Replace("%", "x"), 8);
            bool expected = Like(input, pattern);
            Assert.True(expected == new DataVo.Core.Parser.Utils.SqlLikePattern(pattern).IsMatch(input), $"'{pattern}' on '{input}'");
            Assert.True(expected == DataVo.Core.Parser.Utils.SqlLikePattern.IsMatch(input, pattern), $"'{pattern}' on '{input}' (uncompiled)");
        }

        static string RandomText(Random random, string alphabet, int maxLength) =>
            new([.. Enumerable.Range(0, random.Next(maxLength + 1)).Select(_ => alphabet[random.Next(alphabet.Length)])]);
    }

    private static bool Like(string? value, string pattern) =>
        value is not null
        && Regex.IsMatch(value, "^" + Regex.Escape(pattern).Replace("%", ".*").Replace("_", ".") + @"\z", RegexOptions.Singleline);

    private static void AssertIds(DataVoContext context, string sql, Func<Order, bool> expected)
    {
        var result = context.Execute(sql)[^1];
        Assert.False(result.IsError, string.Join("; ", result.Messages));
        Assert.Equal(
            Orders.Where(expected).Select(o => o.Id).ToArray(),
            result.Data.Select(r => Convert.ToInt32(r["Id"])).ToArray());
    }

    private static void WithOrders(StorageMode mode, Action<DataVoContext> test)
    {
        string? directory = mode == StorageMode.Lsm ? TestStorageDirectories.Create("sql_typed_or_like") : null;
        using var context = new DataVoContext(new DataVoConfig { StorageMode = mode, DataDirectory = directory });
        Execute(context, "CREATE DATABASE OrLike");
        Execute(context, "USE OrLike");
        Execute(context, "CREATE TABLE Orders (Id INT PRIMARY KEY, Status VARCHAR(16), Qty INT, Amount DOUBLE, Discount DOUBLE, Sku VARCHAR(32), CustomerId INT)");
        Execute(context, "INSERT INTO Orders (Id, Status, Qty, Amount, Discount, Sku, CustomerId) VALUES " + string.Join(", ", Orders.Select(o =>
            $"({o.Id}, {Text(o.Status)}, {o.Qty}, {Number(o.Amount)}, {(o.Discount is { } d ? Number(d) : "NULL")}, {Text(o.Sku)}, {o.CustomerId})")));
        test(context);

        static string Text(string? value) => value is null ? "NULL" : $"'{value}'";
        static string Number(double value) => value.ToString(CultureInfo.InvariantCulture);
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
        return [.. Assert.Single(result.Data).Values];
    }
}
