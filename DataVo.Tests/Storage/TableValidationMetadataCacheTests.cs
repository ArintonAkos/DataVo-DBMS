using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Storage;

/// <summary>
/// GC Reduction Slice 4, Step 1: the per-table validation metadata (primary/unique/foreign keys,
/// indexes, columns) is computed once and cached by schema version, so the hot insert path stops
/// re-walking the XML catalog + rebuilding LINQ structures on every row. The schema-version key makes
/// invalidation automatic — any DDL that bumps the version yields fresh metadata.
/// </summary>
public class TableValidationMetadataCacheTests
{
    private static DataVoContext NewContext()
    {
        var ctx = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        ctx.Execute("CREATE DATABASE D");
        ctx.Execute("USE D");
        ctx.Execute("CREATE TABLE T (Id INT PRIMARY KEY, Name VARCHAR(20))");
        return ctx;
    }

    [Fact]
    public void ReturnsCorrectMetadata()
    {
        using DataVoContext ctx = NewContext();
        var m = ctx.Engine.Catalog.GetTableValidationMetadata("T", "D");

        Assert.Equal(new[] { "Id" }, m.PrimaryKeys);
        Assert.Contains(m.Columns, c => c.Name.Equals("Name", StringComparison.OrdinalIgnoreCase));
        Assert.True(m.ColumnNames.Contains("Id"));
        Assert.True(m.ColumnNames.Contains("Name"));
    }

    [Fact]
    public void CachesBySchemaVersion_ReturnsSameInstance()
    {
        using DataVoContext ctx = NewContext();
        var cat = ctx.Engine.Catalog;

        var m1 = cat.GetTableValidationMetadata("T", "D");
        var m2 = cat.GetTableValidationMetadata("T", "D");

        Assert.Same(m1, m2); // same schema version -> cached instance, no rebuild
    }

    [Fact]
    public void InvalidatesWhenSchemaVersionChanges()
    {
        using DataVoContext ctx = NewContext();
        var cat = ctx.Engine.Catalog;

        var m1 = cat.GetTableValidationMetadata("T", "D");
        ctx.Execute("ALTER TABLE T ADD COLUMN Age INT"); // bumps the table's schema version
        var m2 = cat.GetTableValidationMetadata("T", "D");

        Assert.NotSame(m1, m2);
        Assert.Contains(m2.Columns, c => c.Name.Equals("Age", StringComparison.OrdinalIgnoreCase));
    }
}
