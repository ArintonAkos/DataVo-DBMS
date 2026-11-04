using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Indexing;

/// <summary>
/// Rows reach a secondary index through both the typed path (which maintains key fast lanes) and the SQL
/// path; an equality lookup must return every matching row no matter which path wrote it.
/// </summary>
public sealed class MixedPathSecondaryIndexTests
{
    private static readonly ReactiveRowSchema Schema = new("Id", "Code");

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void EqualityLookup_ReturnsRowsFromTypedAndSqlInserts(bool indexBeforeRows)
    {
        using var db = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        AssertOk(db.Execute("CREATE DATABASE Mixed"));
        AssertOk(db.Execute("USE Mixed"));
        AssertOk(db.Execute("CREATE TABLE T (Id INT PRIMARY KEY, Code INT)"));

        if (indexBeforeRows)
        {
            AssertOk(db.Execute("CREATE INDEX ix_code ON T (Code)"));
        }

        db.InsertTyped("T", Schema, [CellValue.From(1), CellValue.From(5)]);
        AssertOk(db.Execute("INSERT INTO T (Id, Code) VALUES (2, 5)"));

        if (!indexBeforeRows)
        {
            AssertOk(db.Execute("CREATE INDEX ix_code ON T (Code)"));
        }

        db.InsertTyped("T", Schema, [CellValue.From(3), CellValue.From(5)]);
        AssertOk(db.Execute("INSERT INTO T (Id, Code) VALUES (4, 5)"));

        List<Dictionary<string, object?>> rows = db.Execute("SELECT * FROM T WHERE Code = 5")[0].Data;
        Assert.Equal([1, 2, 3, 4], rows.Select(r => Convert.ToInt32(r["Id"])).Order());
    }

    private static void AssertOk(List<QueryResult> results)
    {
        QueryResult? error = results.FirstOrDefault(r => r.IsError);
        Assert.True(error is null, error is null ? null : string.Join("; ", error.Messages));
    }
}
