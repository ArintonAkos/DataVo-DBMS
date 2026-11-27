using System.Globalization;
using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Sql;

/// <summary>
/// A typed <c>GROUP BY</c> on one text column finds each serialized row's group by the stored UTF-8 bytes instead
/// of decoding a string per row. These tests check that the groups are the ordinal string groups (accented, CJK,
/// emoji and case-different keys, NULL, quotes), including when decoded rows and serialized rows meet in one scan
/// (an open transaction's own inserts on the InMemory engine), and that a scan builds no string per row.
/// </summary>
public sealed class SqlUtf8GroupKeyTests
{
    public static TheoryData<StorageMode> Modes => new() { StorageMode.InMemory, StorageMode.Lsm };

    private sealed record Item(int Id, string? Name, int Qty);

    private static readonly string?[] Names =
    [
        "café", "cafe", "CAFÉ", "Zoë", "Zoe", "日本語", "日本", "emoji😀", "emoji", "", " ", "a b", "ß", "ss", null, "it's", "long-" + new string('x', 40),
    ];

    private static readonly Item[] Items = [.. Enumerable.Range(1, 340).Select(i => new Item(i, Names[i % Names.Length], i % 6))];

    [Theory]
    [MemberData(nameof(Modes))]
    public void GroupsByText_MatchOrdinalStringGroups(StorageMode mode)
    {
        using DataVoContext context = Open(mode, Items);
        AssertGroups(context, "SELECT Name, COUNT(*) AS N, SUM(Qty) AS S FROM Items GROUP BY Name", Items);
        AssertGroups(context, "SELECT Name, COUNT(*) AS N, SUM(Qty) AS S FROM Items WHERE Qty > 2 GROUP BY Name", [.. Items.Where(i => i.Qty > 2)]);
        AssertGroups(context, "SELECT Name, COUNT(*) AS N, SUM(Qty) AS S FROM Items WHERE Name LIKE '%a%' GROUP BY Name", [.. Items.Where(i => i.Name?.Contains('a') == true)]);

        QueryResult top = Run(context, "SELECT Name, COUNT(*) AS N FROM Items GROUP BY Name ORDER BY N DESC, Name LIMIT 3");
        Assert.Equal(
            Items.GroupBy(i => i.Name).Select(g => (g.Key, g.Count())).OrderByDescending(g => g.Item2).ThenBy(g => g.Key, StringComparer.Ordinal).Take(3),
            top.Data.Select(row => ((string?)row["Name"], Convert.ToInt32(row["N"], CultureInfo.InvariantCulture))));

        // An aggregate that reads the key column decodes it, as before.
        QueryResult extremes = Run(context, "SELECT Name, MIN(Name) AS M FROM Items GROUP BY Name");
        Assert.All(extremes.Data, row => Assert.Equal(row["Name"], row["M"]));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void GroupsByText_InsideATransaction_MergeDecodedAndStoredKeys(StorageMode mode)
    {
        using DataVoContext context = Open(mode, Items);
        Run(context, "BEGIN");
        try
        {
            Run(context, "INSERT INTO Items (Id, Name, Qty) VALUES (1001, 'café', 1), (1002, '日本語', 2), (1003, NULL, 3), (1004, 'new', 4)");
            Run(context, "UPDATE Items SET Name = 'Zoë' WHERE Id = 3");
            Run(context, "DELETE FROM Items WHERE Id = 4");
            Item[] seen =
            [
                .. Items.Where(i => i.Id != 4).Select(i => i.Id == 3 ? i with { Name = "Zoë" } : i),
                new(1001, "café", 1), new(1002, "日本語", 2), new(1003, null, 3), new(1004, "new", 4),
            ];

            AssertGroups(context, "SELECT Name, COUNT(*) AS N, SUM(Qty) AS S FROM Items GROUP BY Name", seen);
        }
        finally
        {
            Run(context, "ROLLBACK");
        }

        AssertGroups(context, "SELECT Name, COUNT(*) AS N, SUM(Qty) AS S FROM Items GROUP BY Name", Items);
    }

    /// <summary>
    /// Grouping serialized rows by a text column builds a string per group, not per row: it costs no more per
    /// row than grouping by an integer column of the same rows.
    /// </summary>
    [Fact]
    public void GroupByTextOverSerializedRows_AllocatesNothingPerRow()
    {
        const int rowCount = 20_000;
        using var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.Lsm, DataDirectory = TestStorageDirectories.Create("sql_utf8_group_alloc") });
        Run(context, "CREATE DATABASE Utf8Group");
        Run(context, "USE Utf8Group");
        Run(context, "CREATE TABLE T (Id INT PRIMARY KEY, Status VARCHAR(12), Code INT, Amount DOUBLE)");
        string[] statuses = ["PAID", "SHIPPED", "NEW", "CANCELLED"];
        for (int start = 0; start < rowCount; start += 1_000)
        {
            Run(context, "INSERT INTO T (Id, Status, Code, Amount) VALUES " + string.Join(", ", Enumerable.Range(start, 1_000)
                .Select(i => $"({i}, '{statuses[i % 4]}', {i % 4}, {i % 1000}.5)")));
        }

        long numeric = BytesPerRow(context, "SELECT Code, COUNT(*), SUM(Amount) FROM T GROUP BY Code", rowCount);
        long text = BytesPerRow(context, "SELECT Status, COUNT(*), SUM(Amount) FROM T GROUP BY Status", rowCount);
        Assert.True(text <= numeric + 2, $"GROUP BY Status allocated {text} B per row; GROUP BY Code {numeric} B");
    }

    private static long BytesPerRow(DataVoContext context, string sql, int rowCount)
    {
        Run(context, sql); // warm up
        long before = GC.GetAllocatedBytesForCurrentThread();
        Run(context, sql);
        return (GC.GetAllocatedBytesForCurrentThread() - before) / rowCount;
    }

    private static void AssertGroups(DataVoContext context, string sql, Item[] rows)
    {
        QueryResult result = Run(context, sql);
        string[] want = [.. rows.GroupBy(i => i.Name).Select(g => $"{g.Key ?? "<null>"}|{g.Count()}|{g.Sum(i => i.Qty)}").Order(StringComparer.Ordinal)];
        string[] got = [.. result.Data.Select(row => $"{(string?)row["Name"] ?? "<null>"}|{Convert.ToInt32(row["N"], CultureInfo.InvariantCulture)}|{Convert.ToInt64(row["S"], CultureInfo.InvariantCulture)}").Order(StringComparer.Ordinal)];
        Assert.Equal(want, got);
    }

    private static DataVoContext Open(StorageMode mode, Item[] items)
    {
        string? directory = mode == StorageMode.Lsm ? TestStorageDirectories.Create("sql_utf8_group") : null;
        var context = new DataVoContext(new DataVoConfig { StorageMode = mode, DataDirectory = directory });
        Run(context, "CREATE DATABASE Utf8Group");
        Run(context, "USE Utf8Group");
        Run(context, "CREATE TABLE Items (Id INT PRIMARY KEY, Name VARCHAR(60), Qty INT)");
        Run(context, "INSERT INTO Items (Id, Name, Qty) VALUES " + string.Join(", ", items.Select(i =>
            $"({i.Id}, {(i.Name is null ? "NULL" : $"'{i.Name.Replace("'", "''")}'")}, {i.Qty})")));
        return context;
    }

    private static QueryResult Run(DataVoContext context, string sql)
    {
        QueryResult result = context.Execute(sql)[^1];
        Assert.False(result.IsError, sql + ": " + string.Join("; ", result.Messages));
        return result;
    }
}
