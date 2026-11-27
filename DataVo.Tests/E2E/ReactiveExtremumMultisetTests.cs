using DataVo.Core.Runtime.Reactive;

namespace DataVo.Tests.E2E;

public class ReactiveExtremumMultisetTests
{
    [Fact]
    public void MinMax_UpdateWithoutScanningSortedDictionaryValues()
    {
        var values = new ReactiveExtremumMultiset();

        values.Add(CellValue.From(10));
        values.Add(CellValue.From(30));
        values.Add(CellValue.From(20));
        values.Add(CellValue.From(30));

        Assert.Equal(10, values.Min.AsInt32());
        Assert.Equal(30, values.Max.AsInt32());

        values.Remove(CellValue.From(30));
        Assert.Equal(30, values.Max.AsInt32());

        values.Remove(CellValue.From(30));
        Assert.Equal(20, values.Max.AsInt32());

        values.Remove(CellValue.From(10));
        Assert.Equal(20, values.Min.AsInt32());

        values.Remove(CellValue.From(20));
        Assert.True(values.Min.IsNull);
        Assert.True(values.Max.IsNull);
    }
}
