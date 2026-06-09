using DataVo.Core.Runtime.Reactive;

namespace DataVo.Tests.Reactive;

public class RowRefTests
{
    private static readonly ReactiveRowSchema Schema = new("Category", "TotalExposure");

    [Fact]
    public void OrdinalAndNameLookup_ReturnCorrectCells()
    {
        CellValue[] cells = [CellValue.From("sports"), CellValue.From(99.5m)];
        var row = new RowRef(Schema, cells);

        Assert.Equal(2, row.Count);
        Assert.Equal("sports", row[0].AsString());
        Assert.Equal(99.5m, row[1].AsDecimal());

        Assert.Equal("sports", row["Category"].AsString());
        Assert.Equal(99.5m, row["totalexposure"].AsDecimal());

        Assert.True(row.TryGet("Category", out CellValue value));
        Assert.Equal("sports", value.AsString());
        Assert.False(row.TryGet("Nope", out _));
    }

    [Fact]
    public void Ctor_RejectsWrongWidth()
    {
        CellValue[] cells = [CellValue.From("only-one")];
        try
        {
            _ = new RowRef(Schema, cells);
            Assert.Fail("Expected ArgumentException");
        }
        catch (ArgumentException)
        {
            // Expected
        }
    }

    [Fact]
    public void MissingColumnIndexer_Throws()
    {
        CellValue[] cells = [CellValue.From("sports"), CellValue.From(1m)];

        // Test the exception directly without lambda capture
        var row = new RowRef(Schema, cells);
        try
        {
            _ = row["Missing"];
            Assert.Fail("Expected KeyNotFoundException");
        }
        catch (KeyNotFoundException)
        {
            // Expected
        }
    }

    [Fact]
    public void ToOwnedDictionary_ProducesBoxedCaseInsensitiveRow()
    {
        CellValue[] cells = [CellValue.From("sports"), CellValue.From(99.5m)];
        var row = new RowRef(Schema, cells);

        IReadOnlyDictionary<string, object?> dict = row.ToOwnedDictionary();
        Assert.Equal("sports", dict["category"]);
        Assert.Equal(99.5m, dict["TotalExposure"]);
    }
}
