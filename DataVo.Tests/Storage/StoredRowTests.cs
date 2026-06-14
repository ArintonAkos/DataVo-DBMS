using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine;

namespace DataVo.Tests.Storage;

/// <summary>
/// GC Reduction Slice 4, P0.3: <see cref="StoredRow"/> is the owned typed storage row (clones its cells,
/// never exposes a mutable array); <see cref="StoredRowView"/> is a borrowed, no-copy view over the cells.
/// </summary>
public class StoredRowTests
{
    private static readonly ReactiveRowSchema Schema = new("Id", "Name", "Score");

    private static CellValue[] SampleCells() =>
        [CellValue.From(1), CellValue.From("Ada"), CellValue.From(7)];

    [Fact]
    public void OrdinalAndByName_CaseInsensitiveAccess()
    {
        var row = new StoredRow(Schema, SampleCells());

        Assert.Equal(3, row.Count);
        Assert.Equal(1, row[0].AsInt32());
        Assert.Equal("Ada", row["Name"].AsString());
        Assert.Equal("Ada", row["name"].AsString()); // case-insensitive
        Assert.Equal(7, row["Score"].AsInt32());
    }

    [Fact]
    public void MissingColumn_ThrowsOnIndexer_FalseOnTryGet()
    {
        var row = new StoredRow(Schema, SampleCells());

        Assert.Throws<KeyNotFoundException>(() => row["Nope"]);
        Assert.False(row.TryGet("Nope", out _));
        Assert.True(row.TryGet("Id", out CellValue id));
        Assert.Equal(1, id.AsInt32());
    }

    [Fact]
    public void Constructor_ClonesCells_MutatingSourceDoesNotAffectRow()
    {
        CellValue[] cells = SampleCells();
        var row = new StoredRow(Schema, cells);

        cells[0] = CellValue.From(999); // mutate the source array after construction

        Assert.Equal(1, row[0].AsInt32());
    }

    [Fact]
    public void Constructor_WrongCellCount_Throws()
    {
        Assert.Throws<ArgumentException>(() => new StoredRow(Schema, [CellValue.From(1)]));
    }

    [Fact]
    public void AsView_BorrowedView_ReadsSameCells()
    {
        var row = new StoredRow(Schema, SampleCells());
        StoredRowView view = row.AsView();

        Assert.Equal(3, view.Count);
        Assert.Equal(1, view[0].AsInt32());
        Assert.Equal("Ada", view["Name"].AsString());
        Assert.True(view.TryGet("Score", out CellValue s));
        Assert.Equal(7, s.AsInt32());
    }
}
