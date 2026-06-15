using DataVo.Core;
using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Storage;

/// <summary>
/// GC Reduction Slice 4, P2.1: typed storage reads must return the same logical values as the legacy
/// dictionary read path while avoiding dictionary materialization for typed consumers.
/// </summary>
public class TypedReadParityTests
{
    [Fact]
    public void GetTypedTableContents_MatchesLegacyGetTableContents_ForAllCatalogTypes()
    {
        using DataVoContext ctx = NewContext();
        ctx.BulkInsert("T",
        [
            new Dictionary<string, object?>
            {
                ["I"] = 42,
                ["F"] = 3.5d,
                ["B"] = true,
                ["D"] = new DateOnly(2026, 6, 22),
                ["S"] = "hello",
                ["V"] = new float[] { 1f, 2f, 3f },
            },
        ]);

        Dictionary<long, Dictionary<string, object?>> legacy = ctx.Engine.StorageContext.GetTableContents("T", "D");
        Dictionary<long, StoredRow> typed = ctx.Engine.StorageContext.GetTypedTableContents("T", "D");

        Assert.Equal(legacy.Keys, typed.Keys);
        StoredRow row = typed.Values.Single();
        Assert.Equal(42, row["I"].AsInt32());
        Assert.Equal(3.5d, row["F"].AsDouble());
        Assert.True(row["B"].AsBoolean());
        Assert.Equal(new DateOnly(2026, 6, 22), row["D"].AsDate());
        Assert.Equal("hello", row["S"].AsString());
        Assert.Equal(new float[] { 1f, 2f, 3f }, row["V"].AsVector());
        AssertDictionaryEquivalent(legacy.Values.Single(), row.AsDictionary());
    }

    [Fact]
    public void GetTypedTableContents_ById_FiltersRowsAndMatchesLegacyValues()
    {
        using DataVoContext ctx = NewContext();
        IReadOnlyList<long> rowIds = ctx.BulkInsert("T",
        [
            Row(1, "one"),
            Row(2, "two"),
        ]);

        Dictionary<long, Dictionary<string, object?>> legacy =
            ctx.Engine.StorageContext.GetTableContents([rowIds[1]], "T", "D");
        Dictionary<long, StoredRow> typed =
            ctx.Engine.StorageContext.GetTypedTableContents([rowIds[1]], "T", "D");

        KeyValuePair<long, StoredRow> row = Assert.Single(typed);
        Assert.Equal(rowIds[1], row.Key);
        AssertDictionaryEquivalent(legacy[rowIds[1]], row.Value.AsDictionary());
    }

    private static DataVoContext NewContext()
    {
        var ctx = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        ctx.Execute("CREATE DATABASE D");
        ctx.Execute("USE D");
        ctx.Execute("CREATE TABLE T (I INT, F FLOAT, B BIT, D DATE, S VARCHAR(20), V VECTOR(3))");
        return ctx;
    }

    private static Dictionary<string, object?> Row(int id, string text) => new(StringComparer.OrdinalIgnoreCase)
    {
        ["I"] = id,
        ["F"] = id + 0.5d,
        ["B"] = id % 2 == 0,
        ["D"] = new DateOnly(2026, 6, 20 + id),
        ["S"] = text,
        ["V"] = new float[] { id, id + 1, id + 2 },
    };

    private static void AssertDictionaryEquivalent(
        IReadOnlyDictionary<string, object?> expected,
        IReadOnlyDictionary<string, object?> actual)
    {
        Assert.Equal(expected.Keys, actual.Keys);
        foreach (string key in expected.Keys)
        {
            if (key.Equals("F", StringComparison.OrdinalIgnoreCase))
            {
                Assert.Equal(Convert.ToDouble(expected[key]), Convert.ToDouble(actual[key]));
                continue;
            }

            if (expected[key] is float[] expectedVector)
            {
                Assert.Equal(expectedVector, Assert.IsType<float[]>(actual[key]));
                continue;
            }

            Assert.Equal(expected[key], actual[key]);
        }
    }
}
