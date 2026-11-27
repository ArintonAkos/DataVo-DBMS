using System.Globalization;
using System.Text;
using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Runtime.Diagnostics;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Sql;

/// <summary>
/// <c>COUNT</c>, <c>SUM</c>, <c>MIN</c>, <c>MAX</c> and <c>AVG</c> over a range of a single integer primary key
/// (<c>WHERE Id &gt;= @lo AND Id &lt; @hi</c> and the other bound shapes) are compared with LINQ over the same rows:
/// through the prepared statement's fused range aggregate (diagnostics off), the general path (diagnostics on) and
/// literal SQL, which must agree in fields, values and value types. The ranges include empty, inverted, negative and
/// out-of-data ones, and are read again after writes, inside a transaction with its own uncommitted inserts,
/// updates and deletes, and from a snapshot while another session commits. A range reads only its rows.
/// </summary>
public sealed class SqlPrimaryKeyRangeAggregateTests : IDisposable
{
    private const string Aggregates =
        "COUNT(*), COUNT(S), SUM(S), MIN(S), MAX(S), AVG(S), SUM(B), MIN(B), MAX(B), "
        + "SUM(F), MIN(F), MAX(F), AVG(F), SUM(D), AVG(D), MAX(D) AS top_d";

    private static readonly string[] Shapes =
    [
        "Id >= @lo AND Id < @hi",
        "Id > @lo AND Id <= @hi",
        "Id BETWEEN @lo AND @hi",
        "@lo <= Id AND @hi > Id",
        "Id >= @lo AND Id < @hi AND S > 3",
    ];

    private static readonly (long Lo, long Hi)[] Ranges =
    [
        (-50, 50), (100, 100), (200, 150), (-300, -250), (-1000, -900), (5000, 6000), (0, 1), (-3, 4), (700, 742),
    ];

    private readonly List<string> _directories = [];

    public static TheoryData<StorageMode> Modes => new() { StorageMode.InMemory, StorageMode.Lsm };

    private sealed record Item(long Id, int? S, long? B, float? F, double? D);

    /// <inheritdoc />
    public void Dispose()
    {
        foreach (string directory in _directories)
        {
            try
            {
                Directory.Delete(directory, recursive: true);
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
        }
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void RangeAggregates_MatchLinq_OnEveryPathAndBoundShape_BeforeAndAfterWrites(StorageMode mode)
    {
        using DataVoContext context = Open(mode);
        List<Item> items = SeedItems();
        Insert(context, items);
        AssertAllRanges(context, items);

        Run(context, "INSERT INTO T (Id, S, B, F, D) VALUES (7, 70, 700, 3.5, 1.75), (-7, NULL, -700, NULL, -1.75), (735, 1, 2, 0.5, 0.25)");
        items.AddRange([new Item(7, 70, 700, 3.5f, 1.75), new Item(-7, null, -700, null, -1.75), new Item(735, 1, 2, 0.5f, 0.25)]);
        Run(context, "DELETE FROM T WHERE Id >= 10 AND Id < 30");
        items.RemoveAll(item => item.Id is >= 10 and < 30);
        Run(context, "UPDATE T SET S = 1000, D = 9.5 WHERE Id >= -20 AND Id < 0");
        items = [.. items.Select(item => item.Id is >= -20 and < 0 ? item with { S = 1000, D = 9.5 } : item)];
        AssertAllRanges(context, items);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void RangeAggregate_OverNoRows_ReturnsZeroCountsAndNulls(StorageMode mode)
    {
        using DataVoContext context = Open(mode);
        Insert(context, SeedItems());
        DataVoPreparedStatement statement = context.Prepare($"SELECT {Aggregates} FROM T WHERE Id >= @lo AND Id < @hi");

        foreach ((long lo, long hi) in new[] { (100L, 100L), (5L, -5L), (10_000L, 20_000L) })
        {
            QueryResult result = statement.Execute(("lo", lo), ("hi", hi));
            Assert.False(result.IsError, string.Join(" | ", result.Messages));
            Dictionary<string, object?> row = Assert.Single(result.Data);
            Assert.Equal(0L, row["COUNT(*)"]);
            Assert.Equal(0L, row["COUNT(S)"]);
            Assert.All(row.Where(cell => !cell.Key.StartsWith("COUNT", StringComparison.Ordinal)), cell => Assert.Null(cell.Value));
        }
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void RangeAggregates_InsideTransaction_SeeOwnWrites_WhileOtherSessionsSeeCommittedRows(StorageMode mode)
    {
        using DataVoContext context = Open(mode);
        List<Item> committed = SeedItems();
        Insert(context, committed);
        Guid other = Guid.NewGuid();
        Run(context, "USE RangeAggDb", other);

        Run(context, "BEGIN TRANSACTION");
        List<Item> own = [.. committed];
        Run(context, "INSERT INTO T (Id, S, B, F, D) VALUES (0, 5, 50, 1.5, 0.5), (14, NULL, 140, 2.5, NULL)");
        own.AddRange([new Item(0, 5, 50, 1.5f, 0.5), new Item(14, null, 140, 2.5f, null)]);
        Run(context, "UPDATE T SET S = 999, F = 7.5 WHERE Id >= -10 AND Id < 10");
        own = [.. own.Select(item => item.Id is >= -10 and < 10 ? item with { S = 999, F = 7.5f } : item)];
        Run(context, "DELETE FROM T WHERE Id >= 20 AND Id < 26");
        own.RemoveAll(item => item.Id is >= 20 and < 26);
        Run(context, "UPDATE T SET B = NULL WHERE Id = 40");
        own = [.. own.Select(item => item.Id == 40 ? item with { B = null } : item)];

        // Re-key one row into the range from outside it, and one out of it.
        Run(context, "UPDATE T SET Id = 42 WHERE Id = 600");
        own = [.. own.Select(item => item.Id == 600 ? item with { Id = 42 } : item)];
        Run(context, "UPDATE T SET Id = 2000 WHERE Id = 31");
        own = [.. own.Select(item => item.Id == 31 ? item with { Id = 2000 } : item)];

        foreach ((long lo, long hi) in new[] { (-50L, 50L), (-10L, 10L), (20L, 26L), (14L, 15L), (100L, 100L), (590L, 610L), (1990L, 2010L) })
        {
            AssertRange(context, own, "Id >= @lo AND Id < @hi", lo, hi, sessionId: null);
            AssertRange(context, committed, "Id >= @lo AND Id < @hi", lo, hi, sessionId: other);
        }

        Run(context, "COMMIT");
        foreach ((long lo, long hi) in new[] { (-50L, 50L), (20L, 26L) })
        {
            AssertRange(context, own, "Id >= @lo AND Id < @hi", lo, hi, sessionId: null);
            AssertRange(context, own, "Id >= @lo AND Id < @hi", lo, hi, sessionId: other);
        }
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void RangeAggregates_InSnapshot_IgnoreWritesCommittedAfterItBegan(StorageMode mode)
    {
        using DataVoContext context = Open(mode);
        List<Item> before = SeedItems();
        Insert(context, before);
        Guid reader = Guid.NewGuid();
        Run(context, "USE RangeAggDb", reader);

        Run(context, "BEGIN TRANSACTION", reader);
        AssertRange(context, before, "Id >= @lo AND Id < @hi", -50, 50, reader);

        // Another session commits an insert, an update, a delete and a re-keying across the range's edges.
        List<Item> after = [.. before];
        Run(context, "INSERT INTO T (Id, S, B, F, D) VALUES (35, 1, 1, 1.5, 1.5)");
        after.Add(new Item(35, 1, 1, 1.5f, 1.5));
        Run(context, "UPDATE T SET S = -5 WHERE Id >= 0 AND Id < 20");
        after = [.. after.Select(item => item.Id is >= 0 and < 20 ? item with { S = -5 } : item)];
        Run(context, "DELETE FROM T WHERE Id >= -40 AND Id < -30");
        after.RemoveAll(item => item.Id is >= -40 and < -30);

        AssertRange(context, before, "Id >= @lo AND Id < @hi", -50, 50, reader);
        AssertRange(context, after, "Id >= @lo AND Id < @hi", -50, 50, sessionId: null);

        Run(context, "COMMIT", reader);
        AssertRange(context, after, "Id >= @lo AND Id < @hi", -50, 50, reader);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void NarrowRangeAggregate_ReadsOnlyItsRows_InAndOutsideTransactions(StorageMode mode)
    {
        using DataVoContext context = Open(mode);
        List<Item> items = [.. Enumerable.Range(1, 20_000).Select(i => new Item(i, i % 100, i, null, null))];
        Insert(context, items);
        context.Diagnostics.Enabled = true;

        AssertBoundedRead(context, mode, "SELECT COUNT(*), SUM(S) FROM T WHERE Id >= 5000 AND Id < 5100", expectedSum: items.Where(i => i.Id is >= 5000 and < 5100).Sum(i => (long)i.S!));

        Run(context, "BEGIN TRANSACTION");
        Run(context, "INSERT INTO T (Id, S, B) VALUES (-1, 1, 1)");
        AssertBoundedRead(context, mode, "SELECT COUNT(*), SUM(S) FROM T WHERE Id >= 5000 AND Id < 5100", expectedSum: items.Where(i => i.Id is >= 5000 and < 5100).Sum(i => (long)i.S!));
        Run(context, "ROLLBACK");
    }

    private static void AssertBoundedRead(DataVoContext context, StorageMode mode, string sql, long expectedSum)
    {
        QueryResult result = context.Execute(sql)[^1];
        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Dictionary<string, object?> row = Assert.Single(result.Data);
        Assert.Equal(100L, row["COUNT(*)"]);
        Assert.Equal(expectedSum, row["SUM(S)"]);

        RuntimeQueryStats stats = context.Diagnostics.LastQuery!;
        Assert.False(stats.FullTableScan, $"'{sql}' scanned the table ({stats.RowsScanned} rows)");
        Assert.Equal(0, stats.RowsScanned);
        if (mode == StorageMode.InMemory)
        {
            Assert.Contains("_PK_T", stats.IndexesUsed);
            Assert.Equal(100, stats.RowsRead);
        }
    }

    private static void AssertAllRanges(DataVoContext context, List<Item> items)
    {
        foreach (string shape in Shapes)
        {
            foreach ((long lo, long hi) in Ranges)
            {
                AssertRange(context, items, shape, lo, hi, sessionId: null);
            }
        }
    }

    /// <summary>
    /// Runs the range aggregate prepared (fast path when the session has no transaction), on the general path and
    /// as literal SQL, and checks each against LINQ.
    /// </summary>
    private static void AssertRange(DataVoContext context, List<Item> items, string shape, long lo, long hi, Guid? sessionId)
    {
        string sql = $"SELECT {Aggregates} FROM T WHERE {shape}";
        Func<Item, bool> holds = Predicate(shape, lo, hi);
        List<Item> matched = [.. items.Where(holds).OrderBy(item => item.Id)];
        string label = $"{shape} [{lo}, {hi}]";

        DataVoPreparedStatement statement = sessionId is { } session ? context.Prepare(sql, session) : context.Prepare(sql);
        bool diagnostics = context.Diagnostics.Enabled;
        try
        {
            context.Diagnostics.Enabled = false;
            AssertAggregateRow(Expected(matched), statement.Execute([("lo", lo), ("hi", hi)], sessionId ?? context.SessionId), label + " prepared");
            AssertAggregateRow(Expected(matched), statement.ExecuteBorrowed([("lo", (int)lo), ("hi", (int)hi)], sessionId ?? context.SessionId), label + " borrowed int");

            context.Diagnostics.Enabled = true;
            AssertAggregateRow(Expected(matched), statement.Execute([("lo", lo), ("hi", hi)], sessionId ?? context.SessionId), label + " general");
        }
        finally
        {
            context.Diagnostics.Enabled = diagnostics;
        }

        string literal = sql.Replace("@lo", lo.ToString(CultureInfo.InvariantCulture)).Replace("@hi", hi.ToString(CultureInfo.InvariantCulture));
        List<QueryResult> results = sessionId is { } id ? context.Execute(literal, id) : context.Execute(literal);
        AssertAggregateRow(Expected(matched), results[^1], label + " literal");
    }

    private static Func<Item, bool> Predicate(string shape, long lo, long hi) => shape switch
    {
        "Id >= @lo AND Id < @hi" or "@lo <= Id AND @hi > Id" => item => item.Id >= lo && item.Id < hi,
        "Id > @lo AND Id <= @hi" => item => item.Id > lo && item.Id <= hi,
        "Id BETWEEN @lo AND @hi" => item => item.Id >= lo && item.Id <= hi,
        "Id >= @lo AND Id < @hi AND S > 3" => item => item.Id >= lo && item.Id < hi && item.S > 3,
        _ => throw new ArgumentOutOfRangeException(nameof(shape)),
    };

    /// <summary>The expected row, typed as the engine types aggregates (see <c>Select.TypedScanGroups</c>).</summary>
    private static List<(string Field, object? Value)> Expected(List<Item> rows)
    {
        List<int> s = [.. rows.Where(r => r.S.HasValue).Select(r => r.S!.Value)];
        List<long> b = [.. rows.Where(r => r.B.HasValue).Select(r => r.B!.Value)];
        List<float> f = [.. rows.Where(r => r.F.HasValue).Select(r => r.F!.Value)];
        List<double> d = [.. rows.Where(r => r.D.HasValue).Select(r => r.D!.Value)];
        return
        [
            ("COUNT(*)", (long)rows.Count),
            ("COUNT(S)", (long)s.Count),
            ("SUM(S)", s.Count == 0 ? null : s.Sum(v => (long)v)),
            ("MIN(S)", s.Count == 0 ? null : s.Min()),
            ("MAX(S)", s.Count == 0 ? null : s.Max()),
            ("AVG(S)", s.Count == 0 ? null : s.Sum(v => (double)v) / s.Count),
            ("SUM(B)", b.Count == 0 ? null : b.Sum()),
            ("MIN(B)", b.Count == 0 ? null : b.Min()),
            ("MAX(B)", b.Count == 0 ? null : b.Max()),
            ("SUM(F)", f.Count == 0 ? null : f.Sum(v => (double)v)),
            ("MIN(F)", f.Count == 0 ? null : f.Min()),
            ("MAX(F)", f.Count == 0 ? null : f.Max()),
            ("AVG(F)", f.Count == 0 ? null : f.Sum(v => (double)v) / f.Count),
            ("SUM(D)", d.Count == 0 ? null : d.Sum()),
            ("AVG(D)", d.Count == 0 ? null : d.Sum() / d.Count),
            ("top_d", d.Count == 0 ? null : d.Max()),
        ];
    }

    private static void AssertAggregateRow(List<(string Field, object? Value)> expected, QueryResult result, string label)
    {
        Assert.False(result.IsError, $"{label}: {string.Join(" | ", result.Messages)}");
        Assert.Equal(expected.Select(e => e.Field), result.Fields);
        Dictionary<string, object?> row = Assert.Single(result.Data);
        foreach ((string field, object? value) in expected)
        {
            object? actual = row[field];
            Assert.True(Equals(value, actual) && value?.GetType() == actual?.GetType(),
                $"{label}: {field} expected {Describe(value)} but was {Describe(actual)}");
        }
    }

    private static string Describe(object? value) =>
        value is null ? "NULL" : $"{Convert.ToString(value, CultureInfo.InvariantCulture)} ({value.GetType().Name})";

    /// <summary>Keys from -300 to 1,199 with every seventh missing, NULLs in every value column, inserted out of order.</summary>
    private static List<Item> SeedItems()
    {
        var random = new Random(4242);
        return
        [
            .. Enumerable.Range(-300, 1_500)
                .Where(id => id % 7 != 0)
                .Select(id => new Item(
                    id,
                    id % 5 == 0 ? null : id % 97,
                    id % 7 == 1 ? null : (long)id * 1_000_003,
                    id % 11 == 0 ? null : (id % 40) * 0.5f,
                    id % 13 == 0 ? null : id * 0.25))
                .OrderBy(_ => random.Next()),
        ];
    }

    private static void Insert(DataVoContext context, List<Item> items)
    {
        foreach (Item[] chunk in items.Chunk(200))
        {
            var sql = new StringBuilder("INSERT INTO T (Id, S, B, F, D) VALUES ");
            sql.AppendJoin(", ", chunk.Select(item => string.Create(
                CultureInfo.InvariantCulture,
                $"({item.Id}, {Literal(item.S)}, {Literal(item.B)}, {Literal(item.F)}, {Literal(item.D)})")));
            Run(context, sql.ToString());
        }
    }

    private static string Literal(object? value) =>
        value is null ? "NULL" : Convert.ToString(value, CultureInfo.InvariantCulture)!;

    private DataVoContext Open(StorageMode mode)
    {
        string directory = Path.Combine(Path.GetTempPath(), "datavo-range-agg-" + Guid.NewGuid().ToString("N"));
        _directories.Add(directory);
        var context = new DataVoContext(new DataVoConfig { StorageMode = mode, DataDirectory = directory, LsmStrictFsync = false });
        Run(context, "CREATE DATABASE RangeAggDb");
        Run(context, "USE RangeAggDb");
        Run(context, "CREATE TABLE T (Id INT PRIMARY KEY, S INT, B BIGINT, F FLOAT, D DOUBLE, N VARCHAR(16))");
        return context;
    }

    private static void Run(DataVoContext context, string sql, Guid? sessionId = null)
    {
        QueryResult result = (sessionId is { } id ? context.Execute(sql, id) : context.Execute(sql))[^1];
        Assert.False(result.IsError, $"{sql}: {string.Join(" | ", result.Messages)}");
    }
}
