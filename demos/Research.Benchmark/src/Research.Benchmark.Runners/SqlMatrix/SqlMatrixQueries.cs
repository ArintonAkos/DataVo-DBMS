namespace Research.Benchmark.Runners.SqlMatrix;

/// <summary>One bound parameter: the name without its prefix, and a CLR value (int, double or string).</summary>
public sealed record SqlMatrixParameter(string Name, object Value);

/// <summary>Whether a query is a full scan (few timed calls) or touches few rows (many timed calls).</summary>
public enum SqlMatrixCost
{
    /// <summary>Reads the whole table or a large part of it.</summary>
    Scan,

    /// <summary>Reads thousands of rows.</summary>
    Medium,

    /// <summary>Reads at most about a thousand rows.</summary>
    Selective,
}

/// <summary>
/// One query of the matrix: its SQL text (parameters written as <c>@name</c>), the parameter sets it is run
/// with, and an oracle that computes its exact answer from <see cref="SqlMatrixData"/>.
/// </summary>
/// <param name="Ordered">The SQL fixes the row order (ORDER BY with a full tie-break); otherwise rows are compared as a set.</param>
/// <param name="Columnar">The query is part of the columnar comparison and is re-run after DataVo creates a columnar index.</param>
public sealed record SqlMatrixQuery(
    string Id,
    string Section,
    string Title,
    string Sql,
    bool Ordered,
    SqlMatrixCost Cost,
    IReadOnlyList<SqlMatrixParameter[]> ParameterSets,
    Func<SqlMatrixData, SqlMatrixParameter[], List<object?[]>> Oracle,
    bool Columnar = false);

/// <summary>The query catalog of the SQL capability matrix.</summary>
public static class SqlMatrixQueries
{
    public const string Joins = "Joins";
    public const string Filters = "Filters";
    public const string Ranges = "Primary-key ranges";
    public const string GroupBy = "GROUP BY cardinality";
    public const string ColumnarSection = "Columnar index";

    public static IReadOnlyList<SqlMatrixQuery> Create(SqlMatrixData data)
    {
        var random = new Random(20261114);
        int RangeStart(int width) => random.Next(1, data.Orders - width);
        SqlMatrixParameter[] Range(int width, bool inclusive)
        {
            int lo = RangeStart(width);
            return [P("lo", lo), P("hi", inclusive ? lo + width - 1 : lo + width)];
        }

        SqlMatrixParameter[][] narrowBetween = [.. Enumerable.Range(0, 8).Select(_ => Range(100, inclusive: true))];
        SqlMatrixParameter[][] narrowHalfOpen = [.. Enumerable.Range(0, 8).Select(_ => Range(100, inclusive: false))];
        SqlMatrixParameter[][] wideBetween = [.. Enumerable.Range(0, 8).Select(_ => Range(10_000, inclusive: true))];
        SqlMatrixParameter[][] wideHalfOpen = [.. Enumerable.Range(0, 8).Select(_ => Range(10_000, inclusive: false))];
        int[] productIds = [7, data.Products / 3, data.Products / 2, data.Products - 3];

        return
        [
            // ---- joins -------------------------------------------------------------------------------------
            new("J1", Joins, "3-table inner join, no filter",
                "SELECT COUNT(*), SUM(o.Amount) FROM Orders o JOIN Customers c ON o.CustomerId = c.Id JOIN Products p ON o.ProductId = p.Id",
                Ordered: true, SqlMatrixCost.Scan, [[]],
                static (d, _) => CountSum(d, i => d.CustomerExists(d.CustomerId[i]))),

            new("J2", Joins, "3-table inner join, filter on both dimensions (~0.2%)",
                "SELECT COUNT(*), SUM(o.Amount) FROM Orders o JOIN Customers c ON o.CustomerId = c.Id JOIN Products p ON o.ProductId = p.Id WHERE c.Region = @region AND p.CategoryId = @cat",
                Ordered: true, SqlMatrixCost.Scan,
                [[P("region", "R3"), P("cat", 7)], [P("region", "R5"), P("cat", 12)], [P("region", "R0"), P("cat", 33)], [P("region", "R6"), P("cat", 48)]],
                static (d, p) => CountSum(d, i => JoinFilter(d, i, p))),

            new("J3", Joins, "3-table join, selective filter, ORDER BY ... LIMIT 50",
                "SELECT o.Id, o.Amount, c.Name AS CustomerName, p.Name AS ProductName FROM Orders o JOIN Customers c ON o.CustomerId = c.Id JOIN Products p ON o.ProductId = p.Id WHERE c.Region = @region AND p.CategoryId = @cat ORDER BY o.Amount DESC, o.Id ASC LIMIT 50",
                Ordered: true, SqlMatrixCost.Scan,
                [[P("region", "R3"), P("cat", 7)], [P("region", "R5"), P("cat", 12)], [P("region", "R0"), P("cat", 33)], [P("region", "R6"), P("cat", 48)]],
                static (d, p) => [.. Ids(d, i => JoinFilter(d, i, p))
                    .OrderByDescending(i => d.Amount[i]).ThenBy(i => i).Take(50)
                    .Select(i => new object?[] { i, d.Amount[i], SqlMatrixData.CustomerName(d.CustomerId[i]), SqlMatrixData.ProductName(d.ProductId[i]) })]),

            new("J4", Joins, "LEFT JOIN with ~25% unmatched keys: COUNT(*), COUNT(c.Id), SUM",
                "SELECT COUNT(*), COUNT(c.Id), SUM(o.Amount) FROM Orders o LEFT JOIN Customers c ON o.CustomerId = c.Id",
                Ordered: true, SqlMatrixCost.Scan, [[]],
                static (d, _) =>
                {
                    long matched = 0;
                    double sum = 0;
                    for (int i = 1; i <= d.Orders; i++)
                    {
                        sum += d.Amount[i];
                        if (d.CustomerExists(d.CustomerId[i])) matched++;
                    }

                    return [[(long)d.Orders, matched, sum]];
                }),

            new("J5", Joins, "LEFT JOIN anti-join: orders without a customer (WHERE c.Id IS NULL)",
                "SELECT COUNT(*) FROM Orders o LEFT JOIN Customers c ON o.CustomerId = c.Id WHERE c.Id IS NULL",
                Ordered: true, SqlMatrixCost.Scan, [[]],
                static (d, _) => [[Ids(d, i => !d.CustomerExists(d.CustomerId[i])).LongCount()]]),

            new("J6", Joins, "Large x medium: Orders x Customers, GROUP BY Region",
                "SELECT c.Region, COUNT(*), SUM(o.Amount) FROM Orders o JOIN Customers c ON o.CustomerId = c.Id GROUP BY c.Region",
                Ordered: false, SqlMatrixCost.Scan, [[]],
                static (d, _) => GroupCountSum(d, i => d.CustomerExists(d.CustomerId[i]), i => SqlMatrixData.CustomerRegion(d.CustomerId[i]))),

            new("J7", Joins, "Small x large: Products (1 category, 2%) x Orders",
                "SELECT COUNT(*), SUM(o.Amount) FROM Products p JOIN Orders o ON o.ProductId = p.Id WHERE p.CategoryId = @cat",
                Ordered: true, SqlMatrixCost.Scan,
                [[P("cat", 3)], [P("cat", 17)], [P("cat", 29)], [P("cat", 41)]],
                static (d, p) => CountSum(d, i => SqlMatrixData.ProductCategory(d.ProductId[i]) == Int(p, "cat"))),

            // ---- filters -----------------------------------------------------------------------------------
            new("W1", Filters, "Equality, 0.1% of rows, secondary index (ProductId)",
                "SELECT Id, Amount FROM Orders WHERE ProductId = @p",
                Ordered: false, SqlMatrixCost.Selective, [.. productIds.Select(id => new[] { P("p", id) })],
                static (d, p) => Rows(d, i => d.ProductId[i] == Int(p, "p"))),

            new("W2", Filters, "Equality, 0.1% of rows, no index (ProductTag)",
                "SELECT Id, Amount FROM Orders WHERE ProductTag = @p",
                Ordered: false, SqlMatrixCost.Scan, [.. productIds.Select(id => new[] { P("p", id) })],
                static (d, p) => Rows(d, i => d.ProductId[i] == Int(p, "p"))),

            new("W3", Filters, "(Status = A OR Status = B) AND Amount range",
                "SELECT COUNT(*), SUM(Amount) FROM Orders WHERE (Status = 'PAID' OR Status = 'SHIPPED') AND Amount >= @lo AND Amount < @hi",
                Ordered: true, SqlMatrixCost.Scan,
                [[P("lo", 200d), P("hi", 500d)], [P("lo", 0d), P("hi", 250d)], [P("lo", 600d), P("hi", 900d)]],
                static (d, p) => CountSum(d, i => StatusPaidOrShipped(d, i) && InRange(d.Amount[i], p))),

            new("W4", Filters, "W3 + NULL-aware: Discount IS NOT NULL AND Discount < x",
                "SELECT COUNT(*), SUM(Amount) FROM Orders WHERE (Status = 'PAID' OR Status = 'SHIPPED') AND Amount >= @lo AND Amount < @hi AND Discount IS NOT NULL AND Discount < @disc",
                Ordered: true, SqlMatrixCost.Scan,
                [[P("lo", 200d), P("hi", 500d), P("disc", 5d)], [P("lo", 0d), P("hi", 250d), P("disc", 2.5d)]],
                static (d, p) => CountSum(d, i => StatusPaidOrShipped(d, i) && InRange(d.Amount[i], p)
                    && d.Discount[i] is double disc && disc < Dbl(p, "disc"))),

            new("W5", Filters, "Prefix LIKE 'S0042%' (0.01%, no index)",
                "SELECT COUNT(*) FROM Orders WHERE Sku LIKE @pattern",
                Ordered: true, SqlMatrixCost.Scan,
                [[P("pattern", "S0042%")], [P("pattern", "S1234%")], [P("pattern", "S9001%")]],
                static (d, p) => [[Ids(d, i => d.Sku[i].StartsWith(Str(p, "pattern").TrimEnd('%'), StringComparison.Ordinal)).LongCount()]]),

            new("W6", Filters, "Substring LIKE '%042-%' (0.1%, no index)",
                "SELECT COUNT(*) FROM Orders WHERE Sku LIKE @pattern",
                Ordered: true, SqlMatrixCost.Scan,
                [[P("pattern", "%042-%")], [P("pattern", "%777-%")], [P("pattern", "%123-%")]],
                static (d, p) => [[Ids(d, i => d.Sku[i].Contains(Str(p, "pattern").Trim('%'), StringComparison.Ordinal)).LongCount()]]),

            // ---- primary-key ranges ------------------------------------------------------------------------
            new("R1", Ranges, "PK range, 100 rows, BETWEEN",
                "SELECT Id, Amount FROM Orders WHERE Id BETWEEN @lo AND @hi",
                Ordered: false, SqlMatrixCost.Selective, narrowBetween,
                static (d, p) => Rows(d, i => i >= Int(p, "lo") && i <= Int(p, "hi"))),

            new("R2", Ranges, "PK range, 100 rows, >= AND <",
                "SELECT Id, Amount FROM Orders WHERE Id >= @lo AND Id < @hi",
                Ordered: false, SqlMatrixCost.Selective, narrowHalfOpen,
                static (d, p) => Rows(d, i => i >= Int(p, "lo") && i < Int(p, "hi"))),

            new("R3", Ranges, "PK range, 10,000 rows, BETWEEN",
                "SELECT Id, Amount FROM Orders WHERE Id BETWEEN @lo AND @hi",
                Ordered: false, SqlMatrixCost.Medium, wideBetween,
                static (d, p) => Rows(d, i => i >= Int(p, "lo") && i <= Int(p, "hi"))),

            new("R4", Ranges, "PK range, 10,000 rows, >= AND <",
                "SELECT Id, Amount FROM Orders WHERE Id >= @lo AND Id < @hi",
                Ordered: false, SqlMatrixCost.Medium, wideHalfOpen,
                static (d, p) => Rows(d, i => i >= Int(p, "lo") && i < Int(p, "hi"))),

            // ---- GROUP BY cardinality ----------------------------------------------------------------------
            new("G1", GroupBy, "GROUP BY Status (4 groups)",
                "SELECT Status, COUNT(*), SUM(Amount) FROM Orders GROUP BY Status",
                Ordered: false, SqlMatrixCost.Scan, [[]],
                static (d, _) => GroupCountSum(d, _ => true, i => d.Status(i))),

            new("G2", GroupBy, "GROUP BY Bucket (1,000 groups)",
                "SELECT Bucket, COUNT(*), SUM(Amount) FROM Orders GROUP BY Bucket",
                Ordered: false, SqlMatrixCost.Scan, [[]],
                static (d, _) => GroupCountSum(d, _ => true, i => (object)d.Bucket[i])),

            new("G3", GroupBy, "GROUP BY CustomerId (~125,000 groups) HAVING COUNT(*) >= k ORDER BY count LIMIT 100",
                "SELECT CustomerId, COUNT(*) AS N FROM Orders GROUP BY CustomerId HAVING COUNT(*) >= @k ORDER BY N DESC, CustomerId ASC LIMIT 100",
                Ordered: true, SqlMatrixCost.Scan, [[P("k", 12)], [P("k", 16)]],
                static (d, p) =>
                {
                    var counts = new Dictionary<int, long>();
                    for (int i = 1; i <= d.Orders; i++)
                    {
                        counts[d.CustomerId[i]] = counts.GetValueOrDefault(d.CustomerId[i]) + 1;
                    }

                    int k = Int(p, "k");
                    return [.. counts.Where(kv => kv.Value >= k).OrderByDescending(kv => kv.Value).ThenBy(kv => kv.Key).Take(100)
                        .Select(kv => new object?[] { kv.Key, kv.Value })];
                }),

            // ---- columnar comparison -----------------------------------------------------------------------
            new("C1", ColumnarSection, "COUNT(*) WHERE Amount range",
                "SELECT COUNT(*) FROM Orders WHERE Amount >= @lo AND Amount < @hi",
                Ordered: true, SqlMatrixCost.Scan, [[P("lo", 200d), P("hi", 500d)], [P("lo", 0d), P("hi", 100d)]],
                static (d, p) => [[Ids(d, i => InRange(d.Amount[i], p)).LongCount()]], Columnar: true),

            new("C2", ColumnarSection, "SUM(Qty) WHERE Bucket < b (INT sum)",
                "SELECT SUM(Qty) FROM Orders WHERE Bucket < @b",
                Ordered: true, SqlMatrixCost.Scan, [[P("b", 100)], [P("b", 500)]],
                static (d, p) => [[Ids(d, i => d.Bucket[i] < Int(p, "b")).Sum(i => (long)d.Qty[i])]], Columnar: true),

            new("C3", ColumnarSection, "SUM(Amount) WHERE Qty >= q (DOUBLE sum)",
                "SELECT SUM(Amount) FROM Orders WHERE Qty >= @q",
                Ordered: true, SqlMatrixCost.Scan, [[P("q", 5)], [P("q", 9)]],
                static (d, p) => [[Ids(d, i => d.Qty[i] >= Int(p, "q")).Sum(i => d.Amount[i])]], Columnar: true),

            new("C4", ColumnarSection, "COUNT, SUM(Qty), MIN/MAX(Amount) WHERE Bucket < b AND Discount IS NOT NULL",
                "SELECT COUNT(*), SUM(Qty), MIN(Amount), MAX(Amount) FROM Orders WHERE Bucket < @b AND Discount IS NOT NULL",
                Ordered: true, SqlMatrixCost.Scan, [[P("b", 100)], [P("b", 700)]],
                static (d, p) =>
                {
                    int[] ids = [.. Ids(d, i => d.Bucket[i] < Int(p, "b") && d.Discount[i] is not null)];
                    return [[(long)ids.Length, ids.Sum(i => (long)d.Qty[i]), ids.Min(i => d.Amount[i]), ids.Max(i => d.Amount[i])]];
                }, Columnar: true),

            new("C5", ColumnarSection, "COUNT(*) WHERE Status = s AND Amount >= x (string column)",
                "SELECT COUNT(*) FROM Orders WHERE Status = @status AND Amount >= @lo",
                Ordered: true, SqlMatrixCost.Scan, [[P("status", "PAID"), P("lo", 200d)], [P("status", "NEW"), P("lo", 900d)]],
                static (d, p) => [[Ids(d, i => d.Status(i) == Str(p, "status") && d.Amount[i] >= Dbl(p, "lo")).LongCount()]], Columnar: true),

            new("C6", ColumnarSection, "COUNT(*) WHERE (Qty = 1 OR Qty = 2) AND Amount >= x",
                "SELECT COUNT(*) FROM Orders WHERE (Qty = 1 OR Qty = 2) AND Amount >= @lo",
                Ordered: true, SqlMatrixCost.Scan, [[P("lo", 200d)], [P("lo", 900d)]],
                static (d, p) => [[Ids(d, i => (d.Qty[i] == 1 || d.Qty[i] == 2) && d.Amount[i] >= Dbl(p, "lo")).LongCount()]], Columnar: true),

            new("C7", ColumnarSection, "GROUP BY Bucket: COUNT, SUM(Qty)",
                "SELECT Bucket, COUNT(*), SUM(Qty) FROM Orders GROUP BY Bucket",
                Ordered: false, SqlMatrixCost.Scan, [[]],
                static (d, _) => [.. Enumerable.Range(1, d.Orders).GroupBy(i => d.Bucket[i])
                    .Select(g => new object?[] { g.Key, g.LongCount(), g.Sum(i => (long)d.Qty[i]) })], Columnar: true),

            new("C8", ColumnarSection, "ORDER BY Amount DESC LIMIT 10",
                "SELECT Id, Amount FROM Orders ORDER BY Amount DESC, Id ASC LIMIT 10",
                Ordered: true, SqlMatrixCost.Scan, [[]],
                static (d, _) => [.. Enumerable.Range(1, d.Orders).OrderByDescending(i => d.Amount[i]).ThenBy(i => i).Take(10)
                    .Select(i => new object?[] { i, d.Amount[i] })], Columnar: true),
        ];
    }

    private static SqlMatrixParameter P(string name, object value) => new(name, value);

    private static object Value(SqlMatrixParameter[] p, string name) =>
        Array.Find(p, x => x.Name == name)?.Value ?? throw new KeyNotFoundException(name);

    private static int Int(SqlMatrixParameter[] p, string name) => Convert.ToInt32(Value(p, name));

    private static double Dbl(SqlMatrixParameter[] p, string name) => Convert.ToDouble(Value(p, name));

    private static string Str(SqlMatrixParameter[] p, string name) => (string)Value(p, name);

    private static bool InRange(double value, SqlMatrixParameter[] p) => value >= Dbl(p, "lo") && value < Dbl(p, "hi");

    private static bool StatusPaidOrShipped(SqlMatrixData d, int i) => d.StatusIndex[i] is 1 or 2;

    private static bool JoinFilter(SqlMatrixData d, int i, SqlMatrixParameter[] p) =>
        d.CustomerExists(d.CustomerId[i])
        && SqlMatrixData.CustomerRegion(d.CustomerId[i]) == Str(p, "region")
        && SqlMatrixData.ProductCategory(d.ProductId[i]) == Int(p, "cat");

    private static IEnumerable<int> Ids(SqlMatrixData d, Func<int, bool> filter)
    {
        for (int i = 1; i <= d.Orders; i++)
        {
            if (filter(i))
            {
                yield return i;
            }
        }
    }

    private static List<object?[]> CountSum(SqlMatrixData d, Func<int, bool> filter)
    {
        long count = 0;
        double sum = 0;
        foreach (int i in Ids(d, filter))
        {
            count++;
            sum += d.Amount[i];
        }

        return [[count, sum]];
    }

    private static List<object?[]> Rows(SqlMatrixData d, Func<int, bool> filter) =>
        [.. Ids(d, filter).Select(i => new object?[] { i, d.Amount[i] })];

    private static List<object?[]> GroupCountSum(SqlMatrixData d, Func<int, bool> filter, Func<int, object> key) =>
        [.. Ids(d, filter).GroupBy(key).Select(g => new object?[] { g.Key, g.LongCount(), g.Sum(i => d.Amount[i]) })];
}
