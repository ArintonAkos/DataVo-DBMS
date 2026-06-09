using DataVo.Core.Runtime.Reactive;

namespace DataVo.Tests.Reactive;

public class QueryChangeRefTests
{
    private static readonly ReactiveRowSchema Schema = new("Category", "TotalExposure");

    [Fact]
    public void RowSet_SlicesFlatCellsIntoFixedWidthRows()
    {
        // two rows, width 2
        CellValue[] added =
        [
            CellValue.From("sports"), CellValue.From(10m),
            CellValue.From("casino"), CellValue.From(20m),
        ];
        var change = new QueryChangeRef(Schema, added, [], [], []);

        Assert.Equal(2, change.Added.Count);
        Assert.Equal("sports", change.Added[0]["Category"].AsString());
        Assert.Equal(20m, change.Added[1]["TotalExposure"].AsDecimal());
    }

    [Fact]
    public void IsEmpty_TrueWhenNoAddedRemovedUpdated()
    {
        var empty = new QueryChangeRef(Schema, [], [], [], []);
        Assert.True(empty.IsEmpty);

        CellValue[] one = [CellValue.From("sports"), CellValue.From(1m)];
        var nonEmpty = new QueryChangeRef(Schema, one, [], [], []);
        Assert.False(nonEmpty.IsEmpty);
    }

    [Fact]
    public void Materialize_ProducesOwnedQueryChange_WithMatchingShape()
    {
        CellValue[] added = [CellValue.From("sports"), CellValue.From(10m)];
        CellValue[] removed = [CellValue.From("casino"), CellValue.From(20m)];
        CellValue[] updated = [CellValue.From("poker"), CellValue.From(30m)];
        CellValue[] updatedBefore = [CellValue.From("poker"), CellValue.From(25m)];

        var change = new QueryChangeRef(Schema, added, removed, updated, updatedBefore);
        QueryChange owned = change.Materialize();

        Assert.Single(owned.Added);
        Assert.Single(owned.Removed);
        Assert.Single(owned.Updated);
        Assert.Single(owned.UpdatedBefore);
        Assert.Equal("sports", owned.Added[0]["Category"]);
        Assert.Equal(30m, owned.Updated[0]["TotalExposure"]);
        Assert.Equal(25m, owned.UpdatedBefore[0]["TotalExposure"]);
    }

    [Fact]
    public void RowSet_Count_IsZero_ForZeroColumnSchema()
    {
        var emptySchema = new ReactiveRowSchema();
        var change = new QueryChangeRef(emptySchema, [], [], [], []);
        Assert.Equal(0, change.Added.Count);
    }

    [Fact]
    public void IsEmpty_IgnoresUpdatedBefore()
    {
        CellValue[] before = [CellValue.From("poker"), CellValue.From(25m)];
        var change = new QueryChangeRef(Schema, [], [], [], before);
        Assert.True(change.IsEmpty);
    }
}
