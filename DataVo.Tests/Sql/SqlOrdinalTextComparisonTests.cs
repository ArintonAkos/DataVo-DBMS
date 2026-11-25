using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Sql;

/// <summary>
/// Text comparisons (<c>&lt;</c>, <c>&lt;=</c>, <c>&gt;</c>, <c>&gt;=</c>) are ordinal on every path, as documented:
/// <c>'/'</c> (0x2F) sorts before <c>'_'</c> (0x5F), which a culture-aware comparison reverses.
/// </summary>
public sealed class SqlOrdinalTextComparisonTests : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"datavo_ordinal_{Guid.NewGuid():N}");

    public static TheoryData<StorageMode> Modes => new() { StorageMode.InMemory, StorageMode.Lsm };

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void RangeComparisons_OnText_AreOrdinal(StorageMode mode)
    {
        using var db = new DataVoContext(new DataVoConfig { StorageMode = mode, DiskStoragePath = mode == StorageMode.Lsm ? _directory : null, LsmStrictFsync = false });
        Ok(db, "CREATE DATABASE O; USE O; CREATE TABLE T (Id INT PRIMARY KEY, P VARCHAR)");
        string[] paths = ["docs/guide/2.md", "docs/guide_x/7.md", "docs/reference/1.md", "Docs/upper.md", "docs-dash.md"];
        for (int i = 0; i < paths.Length; i++)
        {
            Ok(db, $"INSERT INTO T (Id, P) VALUES ({i}, '{paths[i]}')");
        }

        foreach ((string op, Func<int, bool> keep) in new (string, Func<int, bool>)[] { (">=", c => c >= 0), (">", c => c > 0), ("<", c => c < 0), ("<=", c => c <= 0) })
        {
            List<int> expected = [.. Enumerable.Range(0, paths.Length).Where(i => keep(string.CompareOrdinal(paths[i], "docs/guide_")))];
            Assert.Equal(expected, Ids(db.Execute($"SELECT Id FROM T WHERE P {op} 'docs/guide_' ORDER BY Id")[^1]));
            Assert.Equal(expected, Ids(db.Prepare($"SELECT Id FROM T WHERE P {op} @p ORDER BY Id").Execute(DataVoQueryParameter.FromObject("p", "docs/guide_"))));
            Ok(db, "BEGIN");
            Assert.Equal(expected, Ids(db.Execute($"SELECT Id FROM T WHERE P {op} 'docs/guide_' ORDER BY Id")[^1]));
            Ok(db, "ROLLBACK");
        }

        List<string> ordered = [.. db.Execute("SELECT P FROM T ORDER BY P")[^1].Data.Select(row => (string)row["P"]!)];
        Assert.Equal([.. paths.Order(StringComparer.Ordinal)], ordered);
    }

    private static List<int> Ids(QueryResult result)
    {
        Assert.False(result.IsError, string.Join("; ", result.Messages));
        return [.. result.Data.Select(row => Convert.ToInt32(row["Id"]))];
    }

    private static void Ok(DataVoContext db, string sql)
    {
        QueryResult result = db.Execute(sql)[^1];
        Assert.False(result.IsError, sql + ": " + string.Join("; ", result.Messages));
    }
}
