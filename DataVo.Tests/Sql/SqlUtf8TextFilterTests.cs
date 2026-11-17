using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Parser.Utils;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Sql;

/// <summary>
/// A typed scan over serialized rows (the LSM engine) tests text equality, <c>LIKE</c> and NULL conditions on
/// the stored UTF-8 bytes instead of decoding a string per row. These tests check that the bytes give the same
/// answer as the strings: for accented, CJK and emoji text, for <c>_</c> (one UTF-16 character, so the bytes
/// decide only for ASCII text), for a column that is filtered and returned, and for a column another
/// condition still needs as a string. Expected rows come from LINQ over the seed data; the InMemory engine,
/// whose rows are not serialized, runs the same queries.
/// </summary>
public sealed class SqlUtf8TextFilterTests
{
    public static TheoryData<StorageMode> Modes => new() { StorageMode.InMemory, StorageMode.Lsm };

    private sealed record Item(int Id, string? Name, string? Code, int Qty);

    private static readonly string?[] Names =
    [
        "café", "cafe", "Zoë", "Zoe", "日本語テキスト", "日本", "naïve-42", "naive-42", "emoji😀x", "emoji_x",
        "plain", "PLAIN", "a_b", "aXb", "50%", "50 percent", "tail\n", "tail", "é", "ß-straße", null, "caf", "cafés",
    ];

    private static readonly Item[] Items = [.. Enumerable.Range(1, 460).Select(i => new Item(
        i,
        Names[i % Names.Length],
        i % 9 == 0 ? null : (i % 50).ToString(CultureInfo.InvariantCulture),
        i % 5))];

    [Theory]
    [MemberData(nameof(Modes))]
    public void Equality_ComparesTextOrdinally(StorageMode mode)
    {
        WithItems(mode, context =>
        {
            foreach (string name in new[] { "café", "cafe", "日本語テキスト", "emoji😀x", "é", "plain", "PLAIN", "tail\n", "missing" })
            {
                AssertIds(context, $"SELECT Id FROM Items WHERE Name = '{name}' ORDER BY Id", i => i.Name == name);
                AssertIds(context, $"SELECT Id FROM Items WHERE Name <> '{name}' ORDER BY Id", i => i.Name is not null && i.Name != name);
            }

            AssertIds(context, "SELECT Id FROM Items WHERE Name IS NULL ORDER BY Id", i => i.Name is null);
            AssertIds(context, "SELECT Id FROM Items WHERE Name IS NOT NULL AND Name <> 'Zoë' AND Qty = 2 ORDER BY Id", i => i.Name is not null && i.Name != "Zoë" && i.Qty == 2);
            AssertIds(context, "SELECT Id FROM Items WHERE Name = 'café' OR Name = '日本' OR Name IS NULL ORDER BY Id", i => i.Name is null or "café" or "日本");
            AssertIds(context, "SELECT Id FROM Items WHERE (Name = 'Zoë' AND Qty > 1) OR (Name = 'é' AND Code = '7') ORDER BY Id",
                i => (i.Name == "Zoë" && i.Qty > 1) || (i.Name == "é" && i.Code == "7"));
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void Like_MatchesWholeCharactersOfAnyScript(StorageMode mode)
    {
        WithItems(mode, context =>
        {
            foreach (string pattern in new[]
            {
                // % only: prefix, suffix, substring, several runs, non-ASCII literals.
                "caf%", "%é", "%é%", "%本語%", "日本%", "%😀%", "emoji%x", "%-42", "na%ve-%", "%a%e%", "%", "%%", "",
                // _ is one UTF-16 character: one byte in ASCII text, more in other text; an emoji takes two.
                "caf_", "Zo_", "_", "__", "_____", "a_b", "emoji_x", "emoji__x", "emoji___x", "na_ve-42", "_afé", "日_", "__%", "%_é",
                "tail_", "50_", "5__",
            })
            {
                AssertIds(context, $"SELECT Id FROM Items WHERE Name LIKE '{pattern}' ORDER BY Id", i => Like(i.Name, pattern));
                AssertIds(context, $"SELECT Id FROM Items WHERE Name NOT LIKE '{pattern}' ORDER BY Id", i => i.Name is not null && !Like(i.Name, pattern));
            }

            AssertIds(context, "SELECT Id FROM Items WHERE Name LIKE 'caf%' AND Code LIKE '1_' ORDER BY Id", i => Like(i.Name, "caf%") && Like(i.Code, "1_"));
            AssertIds(context, "SELECT Id FROM Items WHERE Name LIKE '%é' OR Qty = 3 ORDER BY Id", i => Like(i.Name, "%é") || i.Qty == 3);
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void AFilteredTextColumn_IsStillReturnedGroupedAndAggregated(StorageMode mode)
    {
        WithItems(mode, context =>
        {
            // The column is compared as bytes and then decoded for the rows that match.
            QueryResult rows = Run(context, "SELECT Id, Name FROM Items WHERE Name LIKE 'caf%' ORDER BY Id");
            Assert.Equal(
                Items.Where(i => Like(i.Name, "caf%")).Select(i => (i.Id, i.Name)),
                rows.Data.Select(row => (Convert.ToInt32(row["Id"], CultureInfo.InvariantCulture), (string?)row["Name"])));

            QueryResult top = Run(context, "SELECT Id, Name FROM Items WHERE Name <> 'plain' ORDER BY Id DESC LIMIT 7");
            Assert.Equal(
                Items.Where(i => i.Name is not null && i.Name != "plain").OrderByDescending(i => i.Id).Take(7).Select(i => (i.Id, i.Name)),
                top.Data.Select(row => (Convert.ToInt32(row["Id"], CultureInfo.InvariantCulture), (string?)row["Name"])));

            QueryResult groups = Run(context, "SELECT Name, COUNT(*) AS n FROM Items WHERE Name LIKE '%a%' GROUP BY Name");
            Assert.Equal(
                Items.Where(i => Like(i.Name, "%a%")).GroupBy(i => i.Name).Select(g => $"{g.Key}|{g.Count()}").Order(StringComparer.Ordinal),
                groups.Data.Select(row => $"{row.Values.First()}|{Convert.ToInt32(row.Values.Last(), CultureInfo.InvariantCulture)}").Order(StringComparer.Ordinal));

            QueryResult counted = Run(context, "SELECT COUNT(*), COUNT(Name), COUNT(Code) FROM Items WHERE Name LIKE '%é%' OR Name IS NULL");
            Item[] kept = [.. Items.Where(i => i.Name is null || Like(i.Name, "%é%"))];
            Assert.Equal([kept.Length, kept.Count(i => i.Name is not null), kept.Count(i => i.Code is not null)],
                Assert.Single(counted.Data).Values.Select(value => Convert.ToInt32(value, CultureInfo.InvariantCulture)));
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void AColumnAnotherConditionReadsAsAString_AndBoundValues_MatchTheOracle(StorageMode mode)
    {
        WithItems(mode, context =>
        {
            // A number against a text column compares numerically, which needs the decoded value.
            AssertIds(context, "SELECT Id FROM Items WHERE Code = 42 AND Code LIKE '4%' ORDER BY Id", i => i.Code == "42");
            AssertIds(context, "SELECT Id FROM Items WHERE Code = 7 AND Name LIKE '%é%' ORDER BY Id", i => i.Code == "7" && Like(i.Name, "%é%"));
            AssertIds(context, "SELECT Id FROM Items WHERE Code = '7' AND Name NOT LIKE 'caf_' ORDER BY Id", i => i.Code == "7" && i.Name is not null && !Like(i.Name, "caf_"));

            DataVoPreparedStatement equal = context.Prepare("SELECT Id FROM Items WHERE Name = @n ORDER BY Id");
            DataVoPreparedStatement like = context.Prepare("SELECT Id FROM Items WHERE Name LIKE @p AND Qty >= @q ORDER BY Id");
            foreach (string name in new[] { "café", "日本語テキスト", "emoji😀x", "50%", "nothing" })
            {
                QueryResult result = equal.Execute(DataVoQueryParameter.FromObject("n", name));
                Assert.False(result.IsError, string.Join("; ", result.Messages));
                Assert.Equal(Items.Where(i => i.Name == name).Select(i => i.Id), result.Data.Select(row => Convert.ToInt32(row["Id"], CultureInfo.InvariantCulture)));
            }

            foreach ((string pattern, int qty) in new[] { ("%語%", 0), ("caf_", 2), ("emoji__x", 0), ("%ß%", 1) })
            {
                QueryResult result = like.Execute(DataVoQueryParameter.FromObject("p", pattern), DataVoQueryParameter.FromObject("q", qty));
                Assert.False(result.IsError, string.Join("; ", result.Messages));
                Assert.Equal(Items.Where(i => Like(i.Name, pattern) && i.Qty >= qty).Select(i => i.Id), result.Data.Select(row => Convert.ToInt32(row["Id"], CultureInfo.InvariantCulture)));
            }
        });
    }

    /// <summary>
    /// A text filter over serialized rows builds no string per row: an OR of equalities, a prefix and a substring
    /// LIKE cost no more than a numeric filter over the same rows (they cost 36 to 46 bytes a row before).
    /// </summary>
    [Fact]
    public void TextFiltersOverSerializedRows_AllocateNothingPerRow()
    {
        const int rowCount = 20_000;
        using var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.Lsm, DiskStoragePath = TestStorageDirectories.Create("sql_utf8_filter_alloc") });
        Execute(context, "CREATE DATABASE Utf8Alloc");
        Execute(context, "USE Utf8Alloc");
        Execute(context, "CREATE TABLE T (Id INT PRIMARY KEY, Status VARCHAR(8), Amount DOUBLE, Sku VARCHAR(16))");
        string[] statuses = ["PAID", "SHIPPED", "NEW"];
        for (int start = 0; start < rowCount; start += 1_000)
        {
            Execute(context, "INSERT INTO T (Id, Status, Amount, Sku) VALUES " + string.Join(", ", Enumerable.Range(start, 1_000)
                .Select(i => $"({i}, '{statuses[i % 3]}', {i % 1000}.5, 'S{i:D5}-{i % 7}')")));
        }

        long numeric = BytesPerRow("SELECT COUNT(*) FROM T WHERE Amount >= 200 AND Amount < 500");
        foreach (string query in new[]
        {
            "SELECT COUNT(*) FROM T WHERE (Status = 'PAID' OR Status = 'SHIPPED') AND Amount >= 200 AND Amount < 500",
            "SELECT COUNT(*) FROM T WHERE Status <> 'NEW' AND Sku IS NOT NULL",
            "SELECT COUNT(*) FROM T WHERE Sku LIKE 'S0042%'",
            "SELECT COUNT(*) FROM T WHERE Sku LIKE '%042-%'",
            "SELECT COUNT(*) FROM T WHERE Sku LIKE 'S0_042-_'",
        })
        {
            long perRow = BytesPerRow(query);
            Assert.True(perRow <= numeric + 4, $"{query} allocated {perRow} B per scanned row; a numeric filter {numeric} B");
        }

        long BytesPerRow(string query)
        {
            Assert.False(context.Execute(query)[^1].IsError); // warm up
            long before = GC.GetAllocatedBytesForCurrentThread();
            QueryResult result = context.Execute(query)[^1];
            long perRow = (GC.GetAllocatedBytesForCurrentThread() - before) / rowCount;
            Assert.False(result.IsError, string.Join("; ", result.Messages));
            return perRow;
        }
    }

    /// <summary>
    /// Whenever the byte matcher gives an answer, it is the answer of the character matcher; it declines only
    /// a pattern with <c>_</c> against text that is not ASCII, or a pattern that is not valid Unicode.
    /// </summary>
    [Fact]
    public void LikeOnUtf8Bytes_AgreesWithLikeOnCharacters_OnRandomInputs()
    {
        var random = new Random(20261116);
        string[] alphabet = ["a", "b", "%", "_", "\n", "é", "日", "😀", "ß"];
        int decided = 0;
        for (int i = 0; i < 40_000; i++)
        {
            string patternText = RandomText(random, alphabet, 6);
            string input = RandomText(random, [.. alphabet.Where(part => part != "%")], 8);
            var pattern = new SqlLikePattern(patternText);
            bool expected = pattern.IsMatch(input);
            bool answered = pattern.TryIsMatchUtf8(Encoding.UTF8.GetBytes(input), out bool isMatch);

            bool mustAnswer = !patternText.Contains('_') || input.All(char.IsAscii);
            Assert.True(answered || !mustAnswer, $"'{patternText}' on '{input}' was not answered from the bytes");
            if (answered)
            {
                decided++;
                Assert.True(expected == isMatch, $"'{patternText}' on '{input}': characters {expected}, bytes {isMatch}");
            }
        }

        Assert.True(decided > 20_000, $"only {decided} cases were decided on bytes");

        // A lone surrogate has no UTF-8 form: the bytes never decide.
        Assert.False(new SqlLikePattern("a\uD83D%").TryIsMatchUtf8("a"u8, out _));
        Assert.Null(Utf8Text.TryEncode("x\uDE00"));
        Assert.Equal("é😀"u8.ToArray(), Utf8Text.TryEncode("é😀"));

        static string RandomText(Random random, string[] alphabet, int maxLength) =>
            string.Concat(Enumerable.Range(0, random.Next(maxLength + 1)).Select(_ => alphabet[random.Next(alphabet.Length)]));
    }

    private static bool Like(string? value, string pattern) =>
        value is not null
        && Regex.IsMatch(value, "^" + Regex.Escape(pattern).Replace("%", ".*").Replace("_", ".") + @"\z", RegexOptions.Singleline);

    private static void AssertIds(DataVoContext context, string sql, Func<Item, bool> expected)
    {
        QueryResult result = Run(context, sql);
        int[] want = [.. Items.Where(expected).Select(i => i.Id)];
        int[] got = [.. result.Data.Select(row => Convert.ToInt32(row["Id"], CultureInfo.InvariantCulture))];
        Assert.True(want.SequenceEqual(got), $"{sql.Replace("\n", "\\n")}: expected {want.Length} rows [{string.Join(",", want.Take(12))}], got {got.Length} [{string.Join(",", got.Take(12))}]");
    }

    private static QueryResult Run(DataVoContext context, string sql)
    {
        QueryResult result = context.Execute(sql)[^1];
        Assert.False(result.IsError, sql + ": " + string.Join("; ", result.Messages));
        return result;
    }

    private static void WithItems(StorageMode mode, Action<DataVoContext> test)
    {
        string? directory = mode == StorageMode.Lsm ? TestStorageDirectories.Create("sql_utf8_filter") : null;
        using var context = new DataVoContext(new DataVoConfig { StorageMode = mode, DiskStoragePath = directory });
        Execute(context, "CREATE DATABASE Utf8Filter");
        Execute(context, "USE Utf8Filter");
        Execute(context, "CREATE TABLE Items (Id INT PRIMARY KEY, Name VARCHAR(40), Code VARCHAR(8), Qty INT)");
        Execute(context, "INSERT INTO Items (Id, Name, Code, Qty) VALUES " + string.Join(", ", Items.Select(i =>
            $"({i.Id}, {(i.Name is null ? "NULL" : $"'{i.Name}'")}, {(i.Code is null ? "NULL" : $"'{i.Code}'")}, {i.Qty})")));
        test(context);
    }

    private static void Execute(DataVoContext context, string sql)
    {
        QueryResult result = context.Execute(sql)[^1];
        Assert.False(result.IsError, sql[..Math.Min(sql.Length, 120)] + ": " + string.Join("; ", result.Messages));
    }
}
