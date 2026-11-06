using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Sql;

/// <summary>
/// A GUID column compared with a quoted literal compares as GUIDs. Found by the SQLite differential test:
/// the literal kept its quotes, so <c>Id &lt;&gt; '...'</c> was true for the row it names and a DELETE
/// with it removed that row too.
/// </summary>
public sealed class SqlGuidComparisonTests : IDisposable
{
    private const string First = "018f2c1e-0001-7000-8000-000000000000";
    private const string Second = "0a0b0c0d-1234-4abc-9def-000000000007";

    private readonly DataVoContext _db = new(new DataVoConfig { StorageMode = StorageMode.InMemory });

    public SqlGuidComparisonTests()
    {
        Run("CREATE DATABASE D");
        Run("USE D");
        Run("CREATE TABLE G (Id GUID PRIMARY KEY, A INT)");
        Run($"INSERT INTO G (Id, A) VALUES ('{First}', 1), ('{Second}', 2)");
    }

    public void Dispose() => _db.Dispose();

    [Fact]
    public void NotEquals_ExcludesTheNamedRow()
    {
        Assert.Equal([2], As("SELECT A FROM G WHERE Id <> '" + First + "'"));
        Assert.Equal([1], As("SELECT A FROM G WHERE Id <> '" + Second.ToUpperInvariant() + "'"));
    }

    [Fact]
    public void DeleteWithNotEquals_KeepsTheNamedRow()
    {
        Run($"DELETE FROM G WHERE Id <> '{First}'");
        Assert.Equal([1], As("SELECT A FROM G"));
    }

    private List<int> As(string sql)
    {
        var result = _db.Execute(sql)[0];
        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        return [.. result.Data.Select(row => Convert.ToInt32(row["A"])).Order()];
    }

    private void Run(string sql)
    {
        var result = _db.Execute(sql)[0];
        Assert.False(result.IsError, $"{sql}: {string.Join(" | ", result.Messages)}");
    }
}
