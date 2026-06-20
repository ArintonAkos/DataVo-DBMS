using DataVo.Core.Runtime.Changes;
using DataVo.Core.Runtime.Reactive;

namespace DataVo.Tests.Reactive;

public class TypedRowFromOwnedCellsTests
{
    [Fact]
    public void FromOwnedCells_WrapsWithoutCopying_AndReadsCells()
    {
        var schema = new ReactiveRowSchema("Id", "Name");
        CellValue[] cells = [CellValue.From(7), CellValue.From("ada")];

        TypedRow row = TypedRow.FromOwnedCells(schema, cells);

        Assert.Equal(2, row.Cells.Length);
        Assert.Equal(7, row.AsRowRef()[0].AsInt32());
        Assert.Equal("ada", row.AsRowRef()[1].AsString());
    }
}
