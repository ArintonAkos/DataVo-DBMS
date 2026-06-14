using DataVo.Core.Runtime.Reactive;

namespace DataVo.Tests.Reactive;

/// <summary>
/// GC Reduction Slice 4, P0.1: <see cref="CellValue"/> gains <see cref="DateOnly"/> support (the catalog
/// DATE type) so storage rows can hold dates without boxing into a dictionary.
/// </summary>
public class CellValueDateTests
{
    [Fact]
    public void From_DateOnly_RoundTripsViaAsDate()
    {
        var d = new DateOnly(2026, 6, 22);
        CellValue cell = CellValue.From(d);

        Assert.Equal(CellType.Date, cell.Type);
        Assert.False(cell.IsNull);
        Assert.Equal(d, cell.AsDate());
    }

    [Fact]
    public void FromObject_BoxedDateOnly_ProducesDateCell()
    {
        object boxed = new DateOnly(2026, 1, 5);
        CellValue cell = CellValue.From(boxed);

        Assert.Equal(CellType.Date, cell.Type);
        Assert.Equal(new DateOnly(2026, 1, 5), cell.AsDate());
    }

    [Fact]
    public void ToObject_DateCell_ReturnsDateOnly()
    {
        var d = new DateOnly(2025, 12, 31);
        object? obj = CellValue.From(d).ToObject();

        Assert.IsType<DateOnly>(obj);
        Assert.Equal(d, (DateOnly)obj!);
    }

    [Fact]
    public void Null_RemainsNull_AndIsNull()
    {
        Assert.True(CellValue.Null.IsNull);
        Assert.Null(CellValue.Null.ToObject());
    }

    [Fact]
    public void TypeMismatch_Throws_BothDirections()
    {
        CellValue date = CellValue.From(new DateOnly(2026, 6, 22));
        Assert.Throws<InvalidOperationException>(() => date.AsInt32()); // reading a Date cell as another type

        CellValue notDate = CellValue.From(42);
        Assert.Throws<InvalidOperationException>(() => notDate.AsDate()); // reading another cell as Date
    }
}
