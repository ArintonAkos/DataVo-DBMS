using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine;

namespace DataVo.Tests.Storage;

/// <summary>
/// GC Reduction Slice 4, P0.4: the read-only dictionary adapter over a typed row lets legacy consumers
/// see an <see cref="IReadOnlyDictionary{TKey,TValue}"/> during the migration. It boxes on demand and
/// returns VECTOR clones, so stored row state can never be mutated through the view.
/// </summary>
public class StoredRowDictionaryViewTests
{
    private static readonly ReactiveRowSchema Schema = new("Id", "Name", "Vec");

    private static StoredRowDictionaryView NewView() =>
        new StoredRow(Schema, [CellValue.From(1), CellValue.Null, CellValue.From(new float[] { 1f, 2f })])
            .AsDictionary();

    [Fact]
    public void CaseInsensitive_ContainsKey_TryGetValue_Indexer()
    {
        var d = NewView();

        Assert.True(d.ContainsKey("id"));
        Assert.True(d.TryGetValue("Id", out object? id));
        Assert.Equal(1, id);
        Assert.Equal(1, d["ID"]);
    }

    [Fact]
    public void Count_And_EnumerationOrder_MatchSchema()
    {
        var d = NewView();

        Assert.Equal(3, d.Count);
        Assert.Equal(new[] { "Id", "Name", "Vec" }, d.Keys.ToArray());
        Assert.Equal(new[] { "Id", "Name", "Vec" }, d.Select(kv => kv.Key).ToArray());
    }

    [Fact]
    public void MissingKey_ThrowsOnIndexer_FalseElsewhere()
    {
        var d = NewView();

        Assert.False(d.ContainsKey("Nope"));
        Assert.False(d.TryGetValue("Nope", out _));
        Assert.Throws<KeyNotFoundException>(() => d["Nope"]);
    }

    [Fact]
    public void NullCell_ReturnsNull()
    {
        var d = NewView();

        Assert.True(d.TryGetValue("Name", out object? name));
        Assert.Null(name);
    }

    [Fact]
    public void VectorValue_IsClone_MutationDoesNotAffectRow()
    {
        var d = NewView();

        var vec = Assert.IsType<float[]>(d["Vec"]);
        vec[0] = 99f; // mutate the value returned through the adapter

        var again = Assert.IsType<float[]>(d["Vec"]);
        Assert.Equal(new float[] { 1f, 2f }, again);
    }
}
