using DataVo.Core.Runtime.Reactive;

namespace DataVo.Tests.E2E;

public class ReactiveExtremumMultisetTests
{
    [Fact]
    public void MinMax_UpdateWithoutScanningSortedDictionaryValues()
    {
        var values = new ReactiveExtremumMultiset();

        values.Add(10);
        values.Add(30);
        values.Add(20);
        values.Add(30);

        Assert.Equal(10, values.Min);
        Assert.Equal(30, values.Max);

        values.Remove(30);
        Assert.Equal(30, values.Max);

        values.Remove(30);
        Assert.Equal(20, values.Max);

        values.Remove(10);
        Assert.Equal(20, values.Min);

        values.Remove(20);
        Assert.Null(values.Min);
        Assert.Null(values.Max);
    }
}
