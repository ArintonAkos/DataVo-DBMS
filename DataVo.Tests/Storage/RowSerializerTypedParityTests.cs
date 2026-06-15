using DataVo.Core;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.StorageEngine.Serialization;

namespace DataVo.Tests.Storage;

/// <summary>
/// GC Reduction Slice 4, P1.1: typed <c>RowSerializer.SerializeCells</c>/<c>DeserializeCells</c> must
/// produce/consume the exact same binary wire format as the dictionary path, across every catalog type
/// (INT/FLOAT/BIT/DATE/VARCHAR/VECTOR), so existing on-disk data stays readable.
/// </summary>
public class RowSerializerTypedParityTests
{
    private static DataVoContext NewContext()
    {
        var ctx = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        ctx.Execute("CREATE DATABASE D");
        ctx.Execute("USE D");
        ctx.Execute("CREATE TABLE T (I INT, F FLOAT, B BIT, D DATE, S VARCHAR(20), V VECTOR(3))");
        return ctx;
    }

    private static Dictionary<string, object?> SampleDict() => new(StringComparer.OrdinalIgnoreCase)
    {
        ["I"] = 42,
        ["F"] = 3.5,
        ["B"] = true,
        ["D"] = new DateOnly(2026, 6, 22),
        ["S"] = "hello",
        ["V"] = new float[] { 1f, 2f, 3f },
    };

    [Fact]
    public void TypedSerialize_IsByteIdenticalToDictionarySerialize()
    {
        using DataVoContext ctx = NewContext();
        List<Column> columns = ctx.Engine.Catalog.GetTableColumns("T", "D");
        Dictionary<string, object?> dict = SampleDict();
        CellValue[] cells = columns.Select(c => CellValue.From(dict[c.Name])).ToArray();

        byte[] dictBytes = RowSerializer.Serialize("D", "T", dict, ctx.Engine.Catalog, "test");
        byte[] cellBytes = RowSerializer.SerializeCells(columns, cells);

        Assert.Equal(dictBytes, cellBytes);
    }

    [Fact]
    public void TypedRoundTrip_PreservesAllCatalogTypes()
    {
        using DataVoContext ctx = NewContext();
        List<Column> columns = ctx.Engine.Catalog.GetTableColumns("T", "D");
        Dictionary<string, object?> dict = SampleDict();
        CellValue[] cells = columns.Select(c => CellValue.From(dict[c.Name])).ToArray();

        byte[] bytes = RowSerializer.SerializeCells(columns, cells);
        CellValue[] round = RowSerializer.DeserializeCells(bytes, columns);

        Assert.Equal(42, round[0].AsInt32());
        Assert.Equal(3.5, round[1].AsDouble());
        Assert.True(round[2].AsBoolean());
        Assert.Equal(new DateOnly(2026, 6, 22), round[3].AsDate());
        Assert.Equal("hello", round[4].AsString());
        Assert.Equal(new float[] { 1f, 2f, 3f }, round[5].AsVector());
    }

    [Fact]
    public void DeserializeCells_ReadsLegacyDictionaryBytes()
    {
        using DataVoContext ctx = NewContext();
        List<Column> columns = ctx.Engine.Catalog.GetTableColumns("T", "D");
        byte[] legacyBytes = RowSerializer.Serialize("D", "T", SampleDict(), ctx.Engine.Catalog, "test");

        CellValue[] round = RowSerializer.DeserializeCells(legacyBytes, columns);

        Assert.Equal(42, round[0].AsInt32());
        Assert.Equal("hello", round[4].AsString());
        Assert.Equal(new float[] { 1f, 2f, 3f }, round[5].AsVector());
    }
}
