using DataVo.Core.Runtime.Reactive;

namespace DataVo.Tests.Reactive;

public sealed class CellValueGuidTests
{
    [Fact]
    public void FromGuid_RoundTripsWithoutBoxingOnRead()
    {
        Guid id = Guid.Parse("2f8f0a22-1f70-4f4b-89c6-2ca6f0843c78");

        CellValue cell = CellValue.From(id);

        Assert.Equal(CellType.Guid, cell.Type);
        Assert.False(cell.IsNull);
        Assert.Equal(id, cell.AsGuid());
        Assert.Equal(id, cell.ToObject());
    }

    [Fact]
    public void FromObject_AcceptsGuid()
    {
        Guid id = Guid.Parse("63be54f1-79bd-4877-9f42-f019f0e4ff89");

        CellValue cell = CellValue.From((object)id);

        Assert.Equal(CellType.Guid, cell.Type);
        Assert.Equal(id, cell.AsGuid());
    }
}
